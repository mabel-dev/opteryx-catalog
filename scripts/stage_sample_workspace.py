#!/usr/bin/env python3
"""Register the staged sample bundles as datasets in the `samples` workspace.

FORKS_DESIGN.md S9. The bundles are already in storage - `generate_tpch_samples.py`
put them there - and this is the step that makes them CATALOG DATASETS, which is
what `CREATE TABLE ... CLONE` needs: a fork borrows its upstream's manifest, so
the upstream has to have one.

REGISTERED IN PLACE, NOT COPIED. Each dataset's manifest names the staged files
where they already are, under the sample root, which is outside the dataset's
own `location`. That is safe here for exactly the reason it would not be safe
for a user's dataset: nothing may delete a path outside its own location
(`catalog/ownership.py`), so the staged originals cannot be reclaimed by the
datasets that reference them, by their forks, or by any sweep. They are
operator-managed bytes with an operator-managed lifetime.

STATISTICS ARE READ ONCE, HERE. `add_files(files=...)` downloads each file and
decodes it to build the manifest entry. That is the cost the whole fork design
exists to stop USERS paying: pay it once, in this job, and every fork of every
sample afterwards is a manifest write. It is also why this script goes smallest
first and is resumable - a scale factor that is already registered is skipped,
so an interrupted run is re-run rather than unpicked.

Usage:
    python scripts/stage_sample_workspace.py --dry-run
    python scripts/stage_sample_workspace.py --scale 001 01
    python scripts/stage_sample_workspace.py                 # every staged scale
"""

from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../opteryx-core"))

WORKSPACE = "samples"

# Where generate_tpch_samples.py put the bundles. A bucket of its own, separate
# from the workspace data bucket, which is what keeps "operator-managed
# original" and "somebody's dataset" in different places on disk as well as in
# the design.
SAMPLE_ROOT = "gs://opteryx/tpch"

TABLES = (
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
)

# Staged scale-factor labels, smallest first. The label is what appears in the
# path and in the collection name; `sf001` is scale factor 0.01.
SCALES = ("001", "01", "1", "5", "10")

AUTHOR = "xb500"  # a platform identity - the only principal that may write here


def collection_for(label: str) -> str:
    return f"tpch_sf{label}"


def _catalog():
    from opteryx_catalog import OpteryxCatalog

    return OpteryxCatalog(
        workspace=WORKSPACE,
        firestore_project=os.environ.get("GCP_PROJECT_ID"),
        firestore_database=os.environ.get("FIRESTORE_DATABASE"),
        gcs_bucket=os.environ.get("GCS_BUCKET"),
    )


def _staged_files(catalog, label: str, table: str) -> list[str]:
    """The parquet files staged for one table at one scale factor."""
    prefix = f"{SAMPLE_ROOT}/sf{label}/{table}"
    return sorted(p for p in catalog.io.list_files(prefix) if p.endswith(".parquet"))


def _schema_from(catalog, path: str, table: str):
    """The relation schema of a staged file, in the platform's type vocabulary.

    Read through rugo rather than pyarrow for the reason the engine's own
    reader did: `create_dataset` wants a RelationSchema in the platform's
    types, and rugo is what produces one. Only the footer is needed, but
    `FileIO.new_input` has no ranged read, so this pulls the whole file - which
    is free here, because `add_files` is about to read it again anyway.
    """
    from rugo.parquet import read_metadata_from_memoryview

    from opteryx.connectors._rugo_schema import rugo_to_relation_schema

    with catalog.io.new_input(path).open() as handle:
        data = handle.read()
    return rugo_to_relation_schema(
        read_metadata_from_memoryview(memoryview(data)), schema_name=table
    )


def ensure_workspace(catalog, dry_run: bool) -> None:
    """Turn the reserved name into a real workspace, with the right posture.

    `egress_protection` OFF, because a fork out of `samples` into somebody's
    own workspace is the entire point and the guard would refuse every one of
    them. `listed` OFF, because five scale factors is forty datasets that would
    otherwise sit in the catalog tree of every account on the platform forever
    to be forked once - see LISTED_PROPERTY. Neither changes who may READ it:
    that is opteryx-access's implicit `reader` on `samples.*`, and nothing
    here can grant or revoke it.
    """
    properties = {
        "egress_protection": False,
        "listed": False,
        "owner": None,
        "billing-account-id": WORKSPACE,
        # `timestamp-ms` is NOT set here: it is a reserved lifecycle field that
        # `set_workspace_properties` refuses, and rightly - it is written by the
        # methods that own a workspace's lifecycle, not by a settings write.
        #
        # The reservation marker goes: the name is no longer merely held.
        "reserved": None,
        "status": None,
        "reserved_at": None,
    }
    print(f"workspace {WORKSPACE}: egress_protection=OFF listed=OFF")
    if dry_run:
        return
    catalog.set_workspace_properties(properties, author=AUTHOR)


def stage_table(catalog, label: str, table: str, dry_run: bool) -> str:
    """Register one table at one scale factor. Returns a one-word outcome."""
    collection = collection_for(label)
    identifier = f"{collection}.{table}"

    # `load_dataset` RAISES for an unknown name rather than returning None, so
    # "already registered?" is the absence of that exception. This is what makes
    # the script resumable: an interrupted run is re-run, not unpicked.
    from opteryx_catalog.exceptions import DatasetNotFound

    try:
        catalog.load_dataset(identifier)
        return "exists"
    except DatasetNotFound:
        pass

    files = _staged_files(catalog, label, table)
    if not files:
        return "missing"

    if dry_run:
        return f"would-register({len(files)})"

    schema = _schema_from(catalog, files[0], table)
    dataset = catalog.create_dataset(identifier, schema, author=AUTHOR)
    dataset.add_files(
        files=files,
        author=AUTHOR,
        commit_message=f"stage TPC-H sf{label} {table}",
        # Read no catalog relation: these bytes came from a staging prefix, not
        # from a dataset anyone can name.
        read_sources=[],
    )
    return f"registered({len(files)})"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", nargs="*", default=list(SCALES), choices=list(SCALES))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    catalog = _catalog()
    ensure_workspace(catalog, args.dry_run)

    failures = 0
    for label in sorted(args.scale, key=lambda x: SCALES.index(x)):
        collection = collection_for(label)
        print(f"\n{WORKSPACE}.{collection}")
        for table in TABLES:
            started = time.monotonic()
            try:
                outcome = stage_table(catalog, label, table, args.dry_run)
            except Exception as exc:  # noqa: BLE001 - one table must not stop the run
                failures += 1
                print(f"  {table:<10} FAILED  {type(exc).__name__}: {exc}")
                continue
            print(f"  {table:<10} {outcome:<18} {time.monotonic() - started:6.1f}s")

    print("\nfailures:", failures)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
