"""Rewrite Parquet manifests for tables in a FirestoreCatalog.

This script reads the table metadata and pyiceberg manifests (may use Avro),
converts ManifestEntry objects to the optimized Parquet manifest format and
writes Parquet manifests via `write_parquet_manifest(entries=...)`.

Usage:
    export GCP_PROJECT_ID=... GCS_BUCKET=... FIRESTORE_DATABASE=...
    python scripts/rewrite_manifests.py --namespace ops --workspace opteryx

Be cautious: this will read manifests (Avro) and write Parquet manifests to GCS.
"""

from __future__ import annotations

import argparse
import logging
from typing import List

from pyiceberg_firestore_gcs.firestore_catalog import FirestoreCatalog
from pyiceberg_firestore_gcs.parquet_manifest import ManifestOptimizationConfig
from pyiceberg_firestore_gcs.parquet_manifest import entry_to_dict
from pyiceberg_firestore_gcs.parquet_manifest import write_parquet_manifest

logger = logging.getLogger("rewrite_manifests")
logging.basicConfig(level=logging.INFO)


def rewrite_namespace(
    workspace: str,
    namespace: str,
    gcs_bucket: str,
    firestore_project: str | None = None,
    firestore_database: str | None = None,
):
    catalog = FirestoreCatalog(
        catalog_name=workspace,
        firestore_project=firestore_project,
        firestore_database=firestore_database,
        gcs_bucket=gcs_bucket,
    )

    tables = catalog.list_tables(namespace)
    logger.info(f"Found {len(tables)} tables in namespace {namespace}")

    for ns, table_name in tables:
        logger.info(f"Processing table {ns}.{table_name}")
        try:
            table = catalog.load_table((ns, table_name), include_full_history=False)
        except Exception as e:
            logger.warning(f"Failed to load table {ns}.{table_name}: {e}")
            continue

        metadata = table.metadata
        io = catalog._load_file_io(metadata.properties, metadata.location)

        snapshot = metadata.current_snapshot()
        if not snapshot:
            logger.info(f"No snapshot for {ns}.{table_name}, skipping")
            continue

        all_entries: List[dict] = []
        try:
            # Read manifest lists and fetch entries (this may use Avro via pyiceberg internals)
            for manifest in snapshot.manifests(table.io):
                for entry in manifest.fetch_manifest_entry(io=table.io, discard_deleted=False):
                    try:
                        d = entry_to_dict(entry, metadata.schema(), ManifestOptimizationConfig())
                        all_entries.append(d)
                    except Exception as ee:
                        logger.warning(f"Failed to convert entry for {ns}.{table_name}: {ee}")

            if not all_entries:
                logger.info(f"No manifest entries found for {ns}.{table_name}, skipping")
                continue

            parquet_path = write_parquet_manifest(
                metadata, io, metadata.location, entries=all_entries
            )
            if parquet_path:
                logger.info(f"Wrote Parquet manifest for {ns}.{table_name} -> {parquet_path}")
            else:
                logger.warning(f"Failed to write Parquet manifest for {ns}.{table_name}")

        except Exception as e:
            logger.exception(f"Error rewriting manifests for {ns}.{table_name}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rewrite Parquet manifests for a namespace")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--gcs-bucket", required=True)
    parser.add_argument("--firestore-project", required=False)
    parser.add_argument("--firestore-database", required=False)

    args = parser.parse_args()
    rewrite_namespace(
        args.workspace,
        args.namespace,
        args.gcs_bucket,
        args.firestore_project,
        args.firestore_database,
    )
