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
    iceberg_compatible: bool = True,
    **properties: str,
) -> FirestoreCatalog:
    """Factory helper for the Firestore+GCS catalog.

    Args:
        catalog_name: Name of the catalog
        firestore_project: GCP project for Firestore
        firestore_database: Firestore database name
        gcs_bucket: GCS bucket for table data and metadata
        iceberg_compatible: If True (default), write standard Iceberg metadata JSON and Avro
            manifests to GCS. If False, only write to Firestore and Parquet manifests.
            When True at catalog level, all tables are forced to be compatible.
        **properties: Additional catalog properties

    Returns:
        FirestoreCatalog instance
    """
    return FirestoreCatalog(
        catalog_name,
        firestore_project=firestore_project,
        firestore_database=firestore_database,
        gcs_bucket=gcs_bucket,
        iceberg_compatible=iceberg_compatible,
        **properties,
    )


__all__ = ["create_catalog", "FirestoreCatalog", "View", "ViewMetadata", "ViewAlreadyExistsError"]
