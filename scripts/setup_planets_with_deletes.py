"""Create `opteryx.test.planets_with_deletes` in the live catalog.

A copy of `opteryx.test.planets` with a merge-on-read delete sidecar: the data
files are appended verbatim, then Pluto's row is deleted via
`SimpleDataset.delete_rows()` — no data-file rewrite, the deletion lives in
`metadata/deletes-<snapshot_id>.parquet` and the manifest's
delete_file_path/deleted_record_count columns (see MOR_DELETES_DESIGN.md).

The dataset is a live fixture for the engine's MOR read path: a scan must
serve 8 planets, not 9, and `SELECT COUNT(*)` from the manifest must say 8.
Until the deployed services carry the MOR-aware engine, queries THROUGH THEM
will still show 9 rows (their readers ignore the delete columns — the
designed compatibility behaviour); a locally-built opteryx-core shows 8.

Usage:
    python scripts/setup_planets_with_deletes.py [--recreate]

Credentials: uses GOOGLE_APPLICATION_CREDENTIALS if already set, else the
mabeldev service-account key path used by the sibling repos.
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

DEFAULT_CREDENTIALS = "/Users/justin/mabel/mabeldev-b37f651c2916.json"

WORKSPACE = "opteryx"
SOURCE = "test.planets"
TARGET = "test.planets_with_deletes"
DELETE_NAMES = {"Pluto"}  # the rows to delete — one can always re-litigate Pluto
AUTHOR = "justin.joyce@joocer.com"


def _read_file_morsel(io, file_path: str):
    """Decode one parquet data file into a single Morsel."""
    from rugo.parquet import read_parquet

    inp = io.new_input(file_path)
    with inp.open() as f:
        data = f.read()
    morsels = []
    with read_parquet(bytes(data)) as reader:
        for morsel in reader:
            morsels.append(morsel)
    if not morsels:
        raise ValueError(f"{file_path} decoded to no row groups")
    return morsels[0] if len(morsels) == 1 else morsels[0].combine(morsels)


def _column_as_strings(morsel, name: str) -> list:
    for col_name in morsel.column_names:
        text = col_name.decode() if isinstance(col_name, (bytes, bytearray)) else col_name
        if text == name:
            return [
                v.decode() if isinstance(v, (bytes, bytearray)) else v
                for v in morsel.column(col_name).to_pylist()
            ]
    raise KeyError(f"column {name!r} not in {morsel.column_names}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="drop the target dataset first if it already exists",
    )
    args = parser.parse_args()

    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", DEFAULT_CREDENTIALS)

    from opteryx_catalog import OpteryxCatalog
    from opteryx_catalog.exceptions import DatasetAlreadyExists

    catalog = OpteryxCatalog(
        workspace=WORKSPACE,
        firestore_project="mabeldev",
        firestore_database="catalogs",
        gcs_bucket="opteryx_data",
    )

    # ── Source: read every data file of the current planets snapshot ────────
    source = catalog.load_dataset(SOURCE)
    source_files = [df.file_path for df in source.scan()]
    if not source_files:
        raise SystemExit(f"{WORKSPACE}.{SOURCE} has no data files to copy")
    print(f"source {WORKSPACE}.{SOURCE}: {len(source_files)} data file(s)")

    # ── Target dataset ───────────────────────────────────────────────────────
    if args.recreate:
        try:
            catalog.drop_dataset(TARGET, author=AUTHOR)
            print(f"dropped existing {WORKSPACE}.{TARGET}")
        except Exception:
            pass

    first_morsel = _read_file_morsel(source.io, source_files[0])
    try:
        target = catalog.create_dataset(TARGET, first_morsel, author=AUTHOR)
    except DatasetAlreadyExists:
        raise SystemExit(
            f"{WORKSPACE}.{TARGET} already exists — rerun with --recreate to rebuild it"
        )
    print(f"created {WORKSPACE}.{TARGET}")

    for file_path in source_files:
        morsel = _read_file_morsel(source.io, file_path)
        target.append(
            morsel,
            author=AUTHOR,
            commit_message=f"copy of {SOURCE} data file {file_path.rsplit('/', 1)[-1]}",
        )
        print(f"appended {morsel.num_rows} rows from {file_path.rsplit('/', 1)[-1]}")

    # ── Delete the target rows by (file, ordinal) ────────────────────────────
    positions: dict[str, list[int]] = {}
    for df in target.scan():
        names = _column_as_strings(_read_file_morsel(target.io, df.file_path), "name")
        ordinals = [i for i, n in enumerate(names) if n in DELETE_NAMES]
        if ordinals:
            positions[df.file_path] = ordinals
    if not positions:
        raise SystemExit(f"none of {sorted(DELETE_NAMES)} found in the copied data")

    snap = target.delete_rows(
        positions,
        author=AUTHOR,
        commit_message=f"MOR delete of {', '.join(sorted(DELETE_NAMES))}",
    )
    print(
        f"delete snapshot {snap.snapshot_id}: deleted-records="
        f"{snap.summary['deleted-records']}, "
        f"total-records={snap.summary['total-records']} (physical), "
        f"total-deleted-records={snap.summary['total-deleted-records']}"
    )

    # ── Verify through the read-side resolver ────────────────────────────────
    reloaded = catalog.load_dataset(TARGET)
    vectors = reloaded.delete_vectors()
    print(f"delete_vectors(): { {p.rsplit('/', 1)[-1]: v for p, v in vectors.items()} }")

    survivors = []
    for df in reloaded.scan():
        names = _column_as_strings(_read_file_morsel(reloaded.io, df.file_path), "name")
        deleted = set(vectors.get(df.file_path, ()))
        survivors.extend(n for i, n in enumerate(names) if i not in deleted)
    print(f"live rows after MOR subtraction ({len(survivors)}): {survivors}")
    assert not (set(survivors) & DELETE_NAMES), "deleted names still visible!"
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
