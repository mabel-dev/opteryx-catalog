"""Validate Parquet manifests and referenced data files across a catalog namespace.

Scans tables under a namespace and reports missing data files referenced in Parquet
manifests or snapshot entries. Useful for bulk validation and triage.

Usage:
    python scripts/validate_manifests.py [namespace]

Env:
    OPTERYX_WORKSPACE, GCP_PROJECT_ID, FIRESTORE_DATABASE, GCS_BUCKET
"""

import os
import sys

import pyarrow.parquet as pq

from pyiceberg_firestore_gcs import FirestoreCatalog

sys.path.insert(0, os.path.join(sys.path[0], ".."))


def validate_namespace(namespace: str = "tests_temp"):
    workspace = os.environ.get("OPTERYX_WORKSPACE", "opteryx")
    project = os.environ.get("GCP_PROJECT_ID")
    database = os.environ.get("FIRESTORE_DATABASE")
    bucket = os.environ.get("GCS_BUCKET")

    catalog = FirestoreCatalog(
        catalog_name=workspace,
        firestore_project=project,
        firestore_database=database,
        gcs_bucket=bucket,
    )

    tables = catalog.list_tables(namespace)
    if not tables:
        print(f"No tables found in namespace {namespace}")
        return 0

    total_missing = 0
    for _, table_name in tables:
        table_id = f"{namespace}.{table_name}"
        try:
            table = catalog.load_table((namespace, table_name), include_full_history=True)
        except Exception as e:
            print(f"[ERROR] failed to load {table_id}: {e}")
            continue

        print(f"Checking {table_id}")
        io_impl = catalog._load_file_io(table.metadata.properties, table.metadata.location)

        # Check snapshots for parquet manifests
        for snap in table.metadata.snapshots:
            snap_id = snap.snapshot_id
            manifest_path = getattr(snap, "manifest_list", None) or snap.to_dict().get(
                "parquet-manifest"
            )
            if not manifest_path:
                print(f"  Snapshot {snap_id}: no parquet manifest recorded")
                continue

            # Try to read parquet manifest
            try:
                input_file = io_impl.new_input(manifest_path)
                if not input_file.exists():
                    print(f"  Snapshot {snap_id}: manifest missing -> {manifest_path}")
                    total_missing += 1
                    continue
                with input_file.open() as f:
                    table_p = pq.read_table(f)
                    recs = table_p.to_pylist()
            except Exception as e:
                print(f"  Snapshot {snap_id}: failed to read manifest {manifest_path}: {e}")
                total_missing += 1
                continue

            # Check each referenced data file exists
            for r in recs:
                # Handle nested shapes
                data = r.get("data_file") if isinstance(r, dict) and "data_file" in r else r
                file_path = None
                if isinstance(data, dict):
                    file_path = data.get("file_path") or data.get("file_path")
                if not file_path:
                    print(f"    Entry with no file_path in manifest {manifest_path}")
                    total_missing += 1
                    continue
                try:
                    data_input = io_impl.new_input(file_path)
                    if not data_input.exists():
                        print(f"    Missing data file: {file_path}")
                        total_missing += 1
                except Exception as e:
                    print(f"    Error checking {file_path}: {e}")
                    total_missing += 1

    print(f"Validation complete. Missing/errored items: {total_missing}")
    return total_missing


if __name__ == "__main__":
    ns = sys.argv[1] if len(sys.argv) > 1 else "tests_temp"
    sys.exit(0 if validate_namespace(ns) == 0 else 2)
