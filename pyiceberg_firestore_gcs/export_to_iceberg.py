"""Export utilities to generate Iceberg-compatible Avro artifacts from
parquet-first storage.

This module is intentionally separate and may import Avro/pyiceberg-avro
dependencies lazily so the runtime hot path remains Avro-free.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def export_table_to_iceberg(catalog, namespace: str, table: str, *, dry_run: bool = True) -> None:
    """Export a table's current snapshot to Iceberg-compatible Avro artifacts.

    This function is intentionally explicit: it will perform Avro reads/writes
    and should be run out-of-band (not in the hot path).
    """
    # Lazy import of Avro/pyiceberg Avro helpers
    try:
        pass  # type: ignore
    except Exception:
        logger.warning("pyiceberg Avro helpers not available; install extras 'export' to enable")
        raise

    logger.info(f"Exporting {catalog}.{namespace}.{table} to Iceberg artifacts (dry_run={dry_run})")
    # TODO: implement steps:
    # 1. Read parquet-manifest (fast) to obtain data file entries
    # 2. Generate Avro manifest(s) and manifest-list
    # 3. Optionally write to GCS and update Firestore snapshot docs
    raise NotImplementedError("export_to_iceberg.export_table_to_iceberg not implemented")


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Export table metadata to Iceberg Avro artifacts")
    p.add_argument("catalog")
    p.add_argument("namespace")
    p.add_argument("table")
    p.add_argument("--apply", action="store_true", help="Write artifacts and update Firestore")
    args = p.parse_args()

    # Very small convenience: open catalog and run
    from pyiceberg_firestore_gcs import FirestoreCatalog

    cat = FirestoreCatalog(args.catalog, gcs_bucket=None)
    export_table_to_iceberg(cat, args.namespace, args.table, dry_run=not args.apply)
