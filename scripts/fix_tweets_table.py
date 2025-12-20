"""
Fix the tweets table by unifying schemas across files.
"""

import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

from pyiceberg_firestore_gcs import FirestoreCatalog

sys.path.insert(0, os.path.join(sys.path[0], ".."))

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)
os.environ["GCP_PROJECT_ID"] = "mabeldev"
os.environ["FIRESTORE_DATABASE"] = "catalogs"
os.environ["GCS_BUCKET"] = "opteryx_data"


workspace = "opteryx"

catalog = FirestoreCatalog(
    workspace,
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
    iceberg_compatible=False,
)

gcs_client = storage.Client()
bucket = gcs_client.bucket("opteryx_data")

TARGET_FILE_SIZE = 128 * 1024 * 1024

print("=" * 80)
print("Processing tweets table with schema unification")
print("=" * 80)

namespace = "testdata"
table_name = "tweets"
source_prefix = "opteryx/testdata/tweets/data/"

# List all Parquet files
blobs = list(bucket.list_blobs(prefix=source_prefix))
parquet_files = [
    f"gs://opteryx_data/{blob.name}" for blob in blobs if blob.name.endswith(".parquet")
]

print(f"\nFound {len(parquet_files)} source files")

# Drop existing table if it exists
identifier = (namespace, table_name)
if catalog.table_exists(identifier):
    print("Dropping existing table...")
    catalog.drop_table(identifier)

# Read files and unify schema
print("Reading and unifying schemas...")
tables = []
schemas = []

for i, pf in enumerate(parquet_files):
    table = pq.read_table(pf)
    schemas.append(table.schema)
    if i == 0 or i % 10 == 0:
        print(f"  Read {i + 1}/{len(parquet_files)} files...")

# Find the union of all fields
all_fields = {}
for schema in schemas:
    for field in schema:
        if field.name not in all_fields:
            all_fields[field.name] = field

# Create unified schema
unified_schema = pa.schema(list(all_fields.values()))
print(f"\nUnified schema: {unified_schema.names}")

# Re-read and cast all tables to unified schema
print("\nRe-reading with unified schema...")
tables = []
total_rows = 0

for i, pf in enumerate(parquet_files):
    table = pq.read_table(pf)

    # Add missing columns as null
    for field in unified_schema:
        if field.name not in table.schema.names:
            null_array = pa.nulls(len(table), type=field.type)
            table = table.append_column(field, null_array)

    # Reorder columns to match unified schema
    table = table.select(unified_schema.names)
    tables.append(table)
    total_rows += len(table)

    if i == 0 or (i + 1) % 10 == 0:
        print(f"  Processed {i + 1}/{len(parquet_files)} files...")

# Concatenate
print("\nConcatenating tables...")
combined_table = pa.concat_tables(tables)
print(f"Total: {total_rows:,} rows, {combined_table.nbytes / (1024 * 1024):.1f}MB uncompressed")

# Create table
print("\nCreating Iceberg table...")
iceberg_table = catalog.create_table(
    identifier=identifier,
    schema=unified_schema,
    properties={
        "write.parquet.compression-codec": "snappy",
        "write.target-file-size-bytes": str(TARGET_FILE_SIZE),
    },
)
print("✓ Table created")

# Write data
print("\nWriting data...")
avg_row_size = combined_table.nbytes / len(combined_table)
rows_per_file = int(TARGET_FILE_SIZE / avg_row_size)

rows_written = 0
for i in range(0, len(combined_table), rows_per_file):
    batch = combined_table.slice(i, min(rows_per_file, len(combined_table) - i))
    iceberg_table.append(batch)
    rows_written += len(batch)

    if (i // rows_per_file) % 5 == 0:
        print(
            f"  Progress: {rows_written:,}/{total_rows:,} rows ({100 * rows_written / total_rows:.1f}%)"
        )

print(f"\n✅ opteryx.testdata.tweets complete - wrote {rows_written:,} rows")
print("=" * 80)
