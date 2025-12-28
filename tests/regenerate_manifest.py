#!/usr/bin/env python3
"""
Regenerate Parquet manifest after fixing date decoding bug.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)

from pyiceberg_firestore_gcs import FirestoreCatalog
from pyiceberg_firestore_gcs.parquet_manifest import write_parquet_manifest
from pyiceberg.io.pyarrow import PyArrowFileIO

catalog = FirestoreCatalog(
    "prune_testing",
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
)

table = catalog.load_table("default.events")
print(f"Current snapshot: {table.current_snapshot().snapshot_id}")

# Regenerate the Parquet manifest with fixed date decoding
io = PyArrowFileIO()
manifest_path = write_parquet_manifest(table.metadata, io, table.location())

print(f"✓ Regenerated Parquet manifest: {manifest_path}")
