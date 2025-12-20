import os
import sys

# Add local paths to sys.path to use local code instead of installed packages
sys.path.insert(0, os.path.join(sys.path[0], ".."))  # Add parent dir for pyiceberg_firestore_gcs
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))

import opteryx

print("Opteryx version:", opteryx.__version__)

import opteryx
from pyiceberg_firestore_gcs import FirestoreCatalog

from opteryx.connectors.iceberg_connector import IcebergConnector

workspace = "public"
schema_name = "examples"
table = "planets"

# Step 1: Create a local Iceberg catalog
catalog = FirestoreCatalog(
    workspace,
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
    iceberg_compatible=False,  # Allow table-level control
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

catalog.create_namespace_if_not_exists(schema_name, properties={"iceberg_compatible": "false"})

df = opteryx.query_to_arrow("SELECT * FROM $planets")

# Drop table if it exists
try:
    catalog.drop_table(f"{schema_name}.{table}")
except Exception:
    pass

s = catalog.create_table(
    f"{schema_name}.{table}", df.schema, properties={"iceberg_compatible": "false"}
)

# s = catalog.load_table(f"{schema_name}.{table}")
s.append(df)
