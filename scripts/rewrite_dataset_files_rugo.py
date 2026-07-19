"""Rewrite every data file of a dataset in place using rugo's writer, and
optionally refresh the manifest statistics afterwards.

"In place" means: same file_path in the manifest, same GCS object key. Each
file is read, re-encoded through rugo (its own compression/row-group layout),
sanity-checked (row count must match before/after), and then the SAME object
is overwritten with a single upload (atomic from GCS's point of view — a
reader never sees a partially-written object).

This does NOT merge or split files (unlike opteryx_catalog.catalog.compaction).
It is a 1:1 re-encode of every file currently listed in the manifest.

Usage (run with the venv created for this repo, which has google-cloud-*
and rugo installed):

    .venv/bin/python3 scripts/rewrite_dataset_files_rugo.py \\
        --dataset github.events --collection public --dry-run --limit 2

    .venv/bin/python3 scripts/rewrite_dataset_files_rugo.py \\
        --dataset github.events --collection public --execute --limit 2

    .venv/bin/python3 scripts/rewrite_dataset_files_rugo.py \\
        --dataset github.events --collection public --execute

    .venv/bin/python3 scripts/rewrite_dataset_files_rugo.py \\
        --dataset github.events --collection public --refresh-manifest
"""

from __future__ import annotations

import argparse
import os
import sys
import time


def _load_env(env_path: str) -> None:
    if not os.path.isfile(env_path):
        raise FileNotFoundError(f"expected .env at {env_path}")
    with open(env_path) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k, v.strip('"'))


def _load_catalog(collection: str):
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    return OpteryxCatalog(
        collection,
        firestore_project=os.environ["GCP_PROJECT_ID"],
        firestore_database=os.environ["FIRESTORE_DATABASE"],
        gcs_bucket=os.environ["GCS_BUCKET"],
    )


def _sketch_missing(entry: dict) -> bool:
    hashes = entry.get("min_k_hashes") or []
    return all(len(col) == 0 for col in hashes)


def rewrite_one_file(io, file_path: str, dry_run: bool) -> dict:
    from rugo.parquet import read_parquet
    from rugo.parquet import write_parquet
    from draken.morsels.morsel import Morsel

    from opteryx_catalog.iops.fileio import WRITE_PARQUET_OPTIONS

    inp = io.new_input(file_path)
    with inp.open() as f:
        orig_bytes = bytes(f.read())
    orig_size = len(orig_bytes)

    with read_parquet(orig_bytes) as reader:
        row_group_morsels = list(reader)
    if not row_group_morsels:
        raise RuntimeError(f"{file_path}: rugo read back zero row groups")
    morsel = Morsel.combine(row_group_morsels) if len(row_group_morsels) > 1 else row_group_morsels[0]
    orig_rows = morsel.num_rows

    new_bytes = write_parquet(morsel, **WRITE_PARQUET_OPTIONS)
    new_size = len(new_bytes)

    # Safety gate: re-decode what we are about to upload and confirm the row
    # count is unchanged before touching the live object. Never overwrite on
    # a mismatch.
    with read_parquet(new_bytes) as reader2:
        check_rows = sum(m.num_rows for m in reader2)
    if check_rows != orig_rows:
        raise RuntimeError(
            f"{file_path}: row count mismatch after rugo rewrite "
            f"({orig_rows} -> {check_rows}); NOT overwritten"
        )

    if not dry_run:
        out = io.new_output(file_path).create()
        out.write(new_bytes)
        out.close()

    return {
        "file_path": file_path,
        "rows": orig_rows,
        "orig_size": orig_size,
        "new_size": new_size,
    }


def cmd_rewrite(args) -> None:
    from opteryx_catalog.catalog.manifest_arrow import get_arrow_manifest

    catalog = _load_catalog(args.collection)
    identifier = f"{args.collection}.{args.dataset}" if "." not in args.dataset else args.dataset
    ds = catalog.load_dataset(identifier)
    snap = ds.metadata.current_snapshot()
    if snap is None or not snap.manifest_list:
        raise RuntimeError(f"{identifier}: no current snapshot/manifest")

    tbl = get_arrow_manifest(ds.io, snap.manifest_list)
    entries = tbl.to_pylist()
    print(f"{identifier}: {len(entries)} files in current manifest ({snap.manifest_list})")

    if args.only_missing_stats:
        entries = [e for e in entries if _sketch_missing(e)]
        print(f"  filtered to {len(entries)} files with missing NDV/histogram sketches")

    entries = sorted(entries, key=lambda e: e["file_path"])
    if args.offset:
        entries = entries[args.offset :]
    if args.limit is not None:
        entries = entries[: args.limit]

    mode = "DRY RUN (no writes)" if args.dry_run else "EXECUTE (overwriting in place)"
    print(f"  mode: {mode}, files targeted: {len(entries)}")

    total_orig = 0
    total_new = 0
    total_rows = 0
    batch_orig = 0
    batch_new = 0
    t0 = time.time()
    for i, entry in enumerate(entries, 1):
        fp = entry["file_path"]
        result = rewrite_one_file(ds.io, fp, dry_run=args.dry_run)
        total_orig += result["orig_size"]
        total_new += result["new_size"]
        total_rows += result["rows"]
        batch_orig += result["orig_size"]
        batch_new += result["new_size"]
        delta = result["new_size"] - result["orig_size"]
        print(
            f"  [{i}/{len(entries)}] {fp} rows={result['rows']} "
            f"size {result['orig_size']} -> {result['new_size']} ({delta:+d})"
        )
        if args.report_every and i % args.report_every == 0:
            print(
                f"  -- batch of {args.report_every} done: "
                f"{batch_orig} -> {batch_new} bytes ({batch_new - batch_orig:+d}); "
                f"cumulative so far: {total_orig} -> {total_new} bytes ({total_new - total_orig:+d}) --"
            )
            batch_orig = 0
            batch_new = 0

    elapsed = time.time() - t0
    print(
        f"done: {len(entries)} files, {total_rows} rows, "
        f"{total_orig} -> {total_new} bytes ({total_new - total_orig:+d}), "
        f"{elapsed:.1f}s"
    )
    if args.dry_run:
        print("dry run only — no objects were overwritten, no snapshot committed")


def cmd_refresh_manifest(args) -> None:
    catalog = _load_catalog(args.collection)
    identifier = f"{args.collection}.{args.dataset}" if "." not in args.dataset else args.dataset
    ds = catalog.load_dataset(identifier)

    new_snapshot_id = ds.refresh_manifest(agent="rewrite_dataset_files_rugo.py")
    if new_snapshot_id is None:
        raise RuntimeError(f"{identifier}: refresh_manifest returned None (no manifest to refresh?)")
    print(f"{identifier}: committed statistics-refresh snapshot {new_snapshot_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--collection", required=True, help="e.g. public")
    parser.add_argument("--dataset", required=True, help="e.g. github.events (or 'events' with --collection public)")
    parser.add_argument("--env-file", default="/Users/justin/Nextcloud/opteryx-core/.env")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="rewrite in memory + verify, do not upload")
    mode.add_argument("--execute", action="store_true", help="rewrite and overwrite the live objects in place")
    mode.add_argument("--refresh-manifest", action="store_true", help="rebuild manifest stats for every file, commit new snapshot")
    parser.add_argument("--limit", type=int, default=None, help="only process the first N files (sorted by file_path)")
    parser.add_argument("--offset", type=int, default=0, help="skip the first N files (sorted by file_path)")
    parser.add_argument("--report-every", type=int, default=None, help="print a cumulative storage-savings summary every N files")
    parser.add_argument("--only-missing-stats", action="store_true", help="restrict to files whose NDV/histogram sketches are empty")
    args = parser.parse_args()

    _load_env(args.env_file)

    if args.refresh_manifest:
        cmd_refresh_manifest(args)
    else:
        cmd_rewrite(args)


if __name__ == "__main__":
    main()
