from typing import Optional

from .firestore_catalog import FirestoreCatalog
from .view import View
from .view import ViewAlreadyExistsError
from .view import ViewMetadata


def create_catalog(
    catalog_name: str,
    firestore_project: Optional[str] = None,
    firestore_database: Optional[str] = None,
    gcs_bucket: Optional[str] = None,
    **properties: str,
) -> FirestoreCatalog:
    """Factory helper for the Firestore+GCS catalog.

    This catalog implementation does not write Iceberg Avro/manifest-list
    artifacts in the hot path. Use `export_to_iceberg` / `import_from_iceberg`
    for interoperability when needed.

    Args:
        catalog_name: Name of the catalog
        firestore_project: GCP project for Firestore
        firestore_database: Firestore database name
        gcs_bucket: GCS bucket for table data and metadata
        **properties: Additional catalog properties

    Returns:
        FirestoreCatalog instance
    """
    return FirestoreCatalog(
        catalog_name,
        firestore_project=firestore_project,
        firestore_database=firestore_database,
        gcs_bucket=gcs_bucket,
        **properties,
    )


__all__ = ["create_catalog", "FirestoreCatalog", "View", "ViewMetadata", "ViewAlreadyExistsError"]
