#!/usr/bin/env python3
"""
Debug date statistics in Parquet manifest.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)

from pyiceberg_firestore_gcs import FirestoreCatalog
import pyarrow.parquet as pq
from pyiceberg.io.pyarrow import PyArrowFileIO

# Load catalog and table
catalog = FirestoreCatalog(
    "prune_testing",
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
    iceberg_compatible=False,
)

table = catalog.load_table("default.events")
snapshot = table.current_snapshot()

print(f"Current snapshot: {snapshot.snapshot_id}")
print(
    f"Parquet manifest path: gs://opteryx_data/prune_testing/default/events/metadata/manifest-{snapshot.snapshot_id}.parquet"
)

# Read the Parquet manifest directly
io = PyArrowFileIO()
manifest_path = f"gs://opteryx_data/prune_testing/default/events/metadata/manifest-{snapshot.snapshot_id}.parquet"
manifest_file = io.new_input(manifest_path)

with manifest_file.open() as f:
    parquet_table = pq.read_table(f)

print(f"\n{'=' * 70}")
print("PARQUET MANIFEST SCHEMA")
print(f"{'=' * 70}")
print(parquet_table.schema)

print(f"\n{'=' * 70}")
print("SAMPLE RECORDS (first 3)")
print(f"{'=' * 70}")

for i in range(min(3, len(parquet_table))):
    record = parquet_table.slice(i, 1).to_pydict()
    print(f"\nRecord {i}:")
    print(f"  file_path: {record['file_path'][0]}")
    print(f"  record_count: {record['record_count'][0]}")

    lower = record["lower_bounds"][0]
    upper = record["upper_bounds"][0]

    print(f"  lower_bounds (length={len(lower) if lower else 0}): {lower}")
    print(f"  upper_bounds (length={len(upper) if upper else 0}): {upper}")

    # Check field 4 (event_date)
    if lower and len(lower) > 4:
        print(f"    Field 4 (event_date) lower: {lower[4]}")
        print(f"    Field 4 (event_date) upper: {upper[4]}")

        # Check if it's INT64_MIN
        INT64_MIN = -9223372036854775808
        if lower[4] == INT64_MIN:
            print("    ⚠️  Field 4 has INT64_MIN - likely NULL/missing!")

print(f"\n{'=' * 70}")
print("CHECK AVRO MANIFEST FOR COMPARISON")
print(f"{'=' * 70}")

# Read one of the Avro manifests to see what statistics look like there

manifest_list_path = snapshot.manifest_list
print(f"Manifest list: {manifest_list_path}")

# Get first manifest
manifests = list(snapshot.manifests(io))
if manifests:
    first_manifest = manifests[0]
    print(f"\nFirst manifest: {first_manifest.manifest_path}")

    entries = first_manifest.fetch_manifest_entry(io, discard_deleted=False)
    for i, entry in enumerate(entries):
        if i >= 1:  # Just check first entry
            break

        df = entry.data_file
        print(f"\nData file: {df.file_path}")
        print(f"  lower_bounds keys: {list(df.lower_bounds.keys()) if df.lower_bounds else None}")
        print(f"  upper_bounds keys: {list(df.upper_bounds.keys()) if df.upper_bounds else None}")

        if df.lower_bounds and 4 in df.lower_bounds:
            print("  Field 4 (event_date) in Avro:")
            print(f"    lower (raw bytes): {df.lower_bounds[4].hex()}")
            print(f"    upper (raw bytes): {df.upper_bounds[4].hex()}")

            # Decode using PyIceberg
            from pyiceberg.types import DateType
            from pyiceberg_firestore_gcs.parquet_manifest import decode_iceberg_value

            lower_val = decode_iceberg_value(df.lower_bounds[4], DateType())
            upper_val = decode_iceberg_value(df.upper_bounds[4], DateType())

            print(f"    lower (decoded): {lower_val}")
            print(f"    upper (decoded): {upper_val}")

            # Convert to int
            from pyiceberg_firestore_gcs.to_int import to_int

            lower_int = to_int(lower_val)
            upper_int = to_int(upper_val)

            print(f"    lower (to_int): {lower_int}")
            print(f"    upper (to_int): {upper_int}")
