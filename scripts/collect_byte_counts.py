import os
import sys

from google.cloud import firestore

from opteryx_catalog import OpteryxCatalog

sys.path.insert(0, os.path.join(sys.path[0], ".."))  # Add parent dir for pyiceberg_firestore_gcs
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))


FIRESTORE_DATABASE = os.environ.get("FIRESTORE_DATABASE")
BUCKET_NAME = os.environ.get("GCS_BUCKET")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")


def get_workspaces():
    firestore_client = firestore.Client(project=GCP_PROJECT_ID, database=FIRESTORE_DATABASE)
    workspaces = firestore_client.collections()
    yield from [w.id for w in workspaces]


for workspace in get_workspaces():
    catalog = OpteryxCatalog(
        workspace,
        firestore_project=GCP_PROJECT_ID,
        firestore_database=FIRESTORE_DATABASE,
        gcs_bucket=BUCKET_NAME,
    )

    print(f"\nWorkspace '{workspace}':")
    collections = catalog.list_collections()
    for collection_name in collections:
        print(f" Collection: {collection_name}")
        collection = catalog.list_datasets(collection_name)
        for dataset_name in collection:
            print(f"  Dataset: {collection_name}.{dataset_name}")
            dataset = catalog.load_dataset(f"{collection_name}.{dataset_name}")
            print(f"   - {dataset_name} ({dataset.snapshot().summary.get('total-data-size')})")

if __name__ == "__main__":
    pass
