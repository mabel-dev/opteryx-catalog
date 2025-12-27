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

workspace = "opteryx"
schema_name = "ops"
table = "audit_log"


opteryx.set_default_connector(
    IcebergConnector,
    catalog=FirestoreCatalog,
    firestore_project=os.environ["GCP_PROJECT_ID"],
    firestore_database=os.environ["FIRESTORE_DATABASE"],
    gcs_bucket=os.environ["GCS_BUCKET"],
)

df = opteryx.query(
    f"SELECT * FROM {workspace}.{schema_name}.{table} WHERE receive_timestamp > '2025-12-25'::TIMESTAMP"
)
print(df)
