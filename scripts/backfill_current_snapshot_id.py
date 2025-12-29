"""Backfill current_snapshot_id for tables missing it.

This walks all namespaces and tables in the Firestore catalog and
sets metadata.current_snapshot_id to the latest snapshot when it is
missing. This is safe to run multiple times.

Env vars:
- OPTERYX_WORKSPACE (catalog name, default "opteryx")
- GCP_PROJECT_ID
- FIRESTORE_DATABASE
- GCS_BUCKET
"""

import os
import sys
from typing import Optional

from pyiceberg_firestore_gcs import FirestoreCatalog

sys.path.insert(0, os.path.join(sys.path[0], ".."))


def _snapshot_key(snapshot) -> tuple:
    """Order snapshots by sequence_number, then timestamp_ms, then snapshot_id."""
    return (
        getattr(snapshot, "sequence_number", 0) or 0,
        getattr(snapshot, "timestamp_ms", 0) or 0,
        getattr(snapshot, "snapshot_id", 0) or 0,
    )


def _latest_snapshot_id(snapshots) -> Optional[int]:
    if not snapshots:
        return None
    return max(snapshots, key=_snapshot_key).snapshot_id


def main():
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

    namespaces = catalog.list_namespaces()
    for ns_tuple in namespaces:
        namespace = ".".join(ns_tuple)
        tables = catalog.list_tables(namespace)
        for _, table_name in tables:
            table_id = f"{namespace}.{table_name}"
            try:
                table = catalog.load_table((namespace, table_name), include_full_history=True)
            except Exception as exc:  # pragma: no cover - defensive
                print(f"[WARN] failed to load {table_id}: {exc}")
                continue

            metadata = table.metadata
            if metadata.current_snapshot_id is not None:
                continue

            latest_id = _latest_snapshot_id(metadata.snapshots)
            if latest_id is None:
                print(f"[INFO] no snapshots for {table_id}; skipping")
                continue

            updated = metadata.model_copy(update={"current_snapshot_id": latest_id})
            catalog._save_metadata_to_firestore(namespace, table_name, updated)
            print(f"[FIXED] set current_snapshot_id={latest_id} for {table_id}")


if __name__ == "__main__":
    main()
