"""Import utilities to ingest Iceberg Avro artifacts into parquet-first storage.

This module reads Iceberg Avro manifest-list / manifests and writes
parquet-manifests and Firestore snapshot docs so the main catalog code can
operate without Avro in the hot path.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def import_table_from_iceberg(catalog, manifest_list_path: str, *, dry_run: bool = True) -> None:
    """Import an Iceberg table (manifest-list) into this catalog's storage model.

    Steps (high-level):
      - Read manifest-list (Avro) and manifests (Avro)
      - Convert to Parquet manifest and write to GCS
      - Create Firestore snapshot documents with necessary summary info
    """
    # Lazy import to keep Avro optional
    try:
        pass  # type: ignore
    except Exception:
        logger.warning("pyiceberg Avro helpers not available; install extras to enable import")
        raise

    logger.info(f"Importing from {manifest_list_path} into catalog (dry_run={dry_run})")
    raise NotImplementedError("import_from_iceberg.import_table_from_iceberg not implemented")
