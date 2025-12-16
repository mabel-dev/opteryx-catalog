import os
import sys
import time

# Add local paths to sys.path to use local code instead of installed packages
sys.path.insert(0, os.path.join(sys.path[0], ".."))  # Add parent dir for pyiceberg_firestore_gcs
sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)
os.environ["GCP_PROJECT_ID"] = "mabeldev"
os.environ["FIRESTORE_DATABASE"] = "catalogs"
os.environ["GCS_BUCKET"] = "opteryx_data"

import opteryx
from pyiceberg_firestore_gcs import FirestoreCatalog
from opteryx.connectors.iceberg_connector import IcebergConnector

workspace = "public"
schema_name = "space"
table = "planets"

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

catalog.create_namespace_if_not_exists(workspace)

df = opteryx.query_to_arrow("SELECT * FROM $planets")

s = catalog.create_table(f"{schema_name}.{table}", df.schema)

# s = catalog.load_table(f"{schema_name}.{table}")
s.append(df)

quit()

print(f"Table format version: {s.metadata.format_version}")
print(f"Table location: {s.metadata.location}")

print("Starting to scan table...")
t = time.monotonic_ns()
scan = s.scan()
files = list(scan.plan_files())
print(f"Planned {len(files)} files, took {(time.monotonic_ns() - t) / 1e6:.2f} ms")


print("\nTesting to_arrow_batch_reader()...")
reader = scan.to_arrow_batch_reader()
print(f"Reader created: {type(reader)}")

batch_count = 0
try:
    for b in reader:
        batch_count += 1
        print(f"Got batch {batch_count}: {b}")
        quit()
except Exception as e:
    print(f"ERROR iterating batches: {e}")
    import traceback

    traceback.print_exc()

print(f"Table {schema_name}.{table} wasn't scanned successfully. Got {batch_count} batches.")
