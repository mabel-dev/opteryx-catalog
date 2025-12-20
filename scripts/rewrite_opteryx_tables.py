"""
Script to rewrite opteryx datasets as Iceberg tables with optimal file sizes.
This script creates Iceberg tables for:
- opteryx.testdata.tweets
- opteryx.ops.request_log
- opteryx.ops.stderr_log
- opteryx.ops.stdout_log
- opteryx.ops.varlog

The data is read from existing Parquet files and rewritten with ~128MB uncompressed file sizes.
"""

import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

from pyiceberg_firestore_gcs import FirestoreCatalog

# Add local paths to sys.path to use local code instead of installed packages
sys.path.insert(0, os.path.join(sys.path[0], ".."))

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)
os.environ["GCP_PROJECT_ID"] = "mabeldev"
os.environ["FIRESTORE_DATABASE"] = "catalogs"
os.environ["GCS_BUCKET"] = "opteryx_data"


workspace = "opteryx"

# Step 1: Create a local Iceberg catalog
catalog = FirestoreCatalog(
    workspace,
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
    iceberg_compatible=False,
)

# Initialize GCS client for listing files
gcs_client = storage.Client()
bucket = gcs_client.bucket("opteryx_data")

# Define the datasets to rewrite
datasets = [
    {"namespace": "testdata", "table": "tweets", "source_prefix": "opteryx/testdata/tweets/data/"},
    {"namespace": "ops", "table": "request_log", "source_prefix": "opteryx/ops/request_log/data/"},
    {"namespace": "ops", "table": "stderr_log", "source_prefix": "opteryx/ops/stderr_log/data/"},
    {"namespace": "ops", "table": "stdout_log", "source_prefix": "opteryx/ops/stdout_log/data/"},
    {"namespace": "ops", "table": "varlog", "source_prefix": "opteryx/ops/varlog/data/"},
]

# Target size per file (128MB uncompressed)
TARGET_FILE_SIZE = 128 * 1024 * 1024  # 128MB

print("=" * 80)
print(f"Rewriting Iceberg tables in workspace: {workspace}")
print(f"Target file size: {TARGET_FILE_SIZE / (1024 * 1024):.0f}MB uncompressed")
print("=" * 80)

for dataset in datasets:
    namespace = dataset["namespace"]
    table_name = dataset["table"]
    source_prefix = dataset["source_prefix"]

    full_table_name = f"{workspace}.{namespace}.{table_name}"

    print(f"\n📋 Processing: {full_table_name}")
    print(f"   Source: gs://opteryx_data/{source_prefix}")

    # List all Parquet files in the source location
    blobs = list(bucket.list_blobs(prefix=source_prefix))
    parquet_files = [
        f"gs://opteryx_data/{blob.name}" for blob in blobs if blob.name.endswith(".parquet")
    ]

    if not parquet_files:
        print(f"   ⚠️  No parquet files found in {source_prefix}")
        continue

    print(f"   Found {len(parquet_files)} source Parquet files")

    # Create namespace if it doesn't exist
    try:
        catalog.create_namespace_if_not_exists(namespace)
    except Exception as e:
        print(f"   ⚠️  Namespace warning: {e}")

    # Drop existing table if it exists
    identifier = (namespace, table_name)
    if catalog.table_exists(identifier):
        print("   Dropping existing table...")
        try:
            catalog.drop_table(identifier)
        except Exception as e:
            print(f"   ✗ Failed to drop existing table: {e}")
            continue

    # Read all data from source files
    print("   Reading source data...")
    try:
        # Read all parquet files into a single table
        tables = []
        total_rows = 0
        for pf in parquet_files:
            table = pq.read_table(pf)
            tables.append(table)
            total_rows += len(table)

        # Concatenate all tables
        combined_table = pa.concat_tables(tables)
        print(
            f"   Read {total_rows:,} rows, {combined_table.nbytes / (1024 * 1024):.1f}MB uncompressed"
        )

        schema = combined_table.schema

    except Exception as e:
        print(f"   ✗ Failed to read source data: {e}")
        continue

    # Create the Iceberg table
    try:
        iceberg_table = catalog.create_table(
            identifier=identifier,
            schema=schema,
            properties={
                "write.parquet.compression-codec": "snappy",
                "write.target-file-size-bytes": str(TARGET_FILE_SIZE),
            },
        )
        print("   ✓ Table created")
    except Exception as e:
        print(f"   ✗ Failed to create table: {e}")
        continue

    # Write data to Iceberg table
    try:
        print("   Writing data to Iceberg table...")

        # Calculate optimal batch size to get ~128MB files
        avg_row_size = combined_table.nbytes / len(combined_table)
        rows_per_file = int(TARGET_FILE_SIZE / avg_row_size)

        # Write in batches
        rows_written = 0
        for i in range(0, len(combined_table), rows_per_file):
            batch = combined_table.slice(i, min(rows_per_file, len(combined_table) - i))
            iceberg_table.append(batch)
            rows_written += len(batch)

            # Progress indicator
            if (i // rows_per_file) % 10 == 0:
                print(
                    f"   Progress: {rows_written:,}/{total_rows:,} rows ({100 * rows_written / total_rows:.1f}%)"
                )

        print(f"   ✓ Wrote {rows_written:,} rows")
        print(f"   ✅ {full_table_name} complete")

    except Exception as e:
        print(f"   ✗ Failed to write data: {e}")
        import traceback

        traceback.print_exc()
        continue

print("\n" + "=" * 80)
print("Dataset rewrite complete!")
print("=" * 80)
