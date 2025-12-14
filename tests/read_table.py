import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../pyiceberg_firestore_gcs"))
sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)
os.environ["GCP_PROJECT_ID"] = "mabeldev"
os.environ["FIRESTORE_DATABASE"] = "catalogs"
os.environ["GCS_BUCKET"] = "opteryx_data"

import opteryx
from firestore_catalog import FirestoreCatalog
from opteryx.connectors.iceberg_connector import IcebergConnector

workspace = "test_workspace"
schema_name = "testdata"
table = "twinkle_star_data"


# Step 1: Create a local Iceberg catalog
catalog = FirestoreCatalog(
    workspace,
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
)

opteryx.register_store(
    prefix="_default",
    connector=IcebergConnector,
    remove_prefix=True,
    catalog=FirestoreCatalog,
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
)

# catalog.create_namespace(workspace)

df = opteryx.query_to_arrow("SELECT * FROM $planets")

# s = catalog.create_table(f"{schema_name}.{table}", df.schema)

# s = catalog.load_table(f"{schema_name}.{table}")
# s.append(df)

# print(s.scan().to_arrow())
