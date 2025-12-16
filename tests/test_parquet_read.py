"""Test Parquet manifest reading."""

import os
import sys

sys.path.insert(0, "pyiceberg_firestore_gcs")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)

# Enable logging
import logging

logging.basicConfig(level=logging.DEBUG)

from firestore_catalog import FirestoreCatalog

catalog = FirestoreCatalog(
    "test",
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
)

table = catalog.load_table("ops.varlog")
print(f"\nTable: {table.name()}")
print(f"Table type: {type(table).__name__}")

scan = table.scan()
print(f"Scan type: {type(scan).__name__}")

print("\n=== Planning files ===")
files = list(scan.plan_files())
print(f"\nFound {len(files)} data files")
