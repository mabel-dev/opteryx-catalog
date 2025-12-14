from typing import Optional

from .firestore_catalog import FirestoreCatalog


def create_catalog(
    catalog_name: str,
    firestore_project: Optional[str] = None,
    firestore_database: Optional[str] = None,
    gcs_bucket: Optional[str] = None,
    **properties: str,
) -> FirestoreCatalog:
    """Factory helper for the Firestore+GCS catalog."""
    return FirestoreCatalog(
        catalog_name,
        firestore_project=firestore_project,
        firestore_database=firestore_database,
        gcs_bucket=gcs_bucket,
        **properties,
    )


__all__ = ["create_catalog", "FirestoreCatalog"]
