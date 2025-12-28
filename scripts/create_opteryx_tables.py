"""
Script to create Iceberg tables for opteryx datasets.
This script creates Iceberg tables for:
- opteryx.testdata.tweets
- opteryx.ops.request_log
- opteryx.ops.stderr_log
- opteryx.ops.stdout_log
- opteryx.ops.varlog

The data files already exist in GCS. This script registers them with the Iceberg catalog.
"""

import os
import sys

import pyarrow.parquet as pq
from google.cloud import storage
from pyiceberg.io.pyarrow import PyArrowFileIO

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
)

# Initialize GCS client for listing files
gcs_client = storage.Client()
bucket = gcs_client.bucket("opteryx_data")

# Initialize PyArrow FileIO for reading Parquet metadata
io = PyArrowFileIO()

# Define the datasets to create
datasets = [
    {"namespace": "testdata", "table": "tweets", "source_prefix": "opteryx/testdata/tweets/data/"},
    {"namespace": "ops", "table": "request_log", "source_prefix": "opteryx/ops/request_log/data/"},
    {"namespace": "ops", "table": "stderr_log", "source_prefix": "opteryx/ops/stderr_log/data/"},
    {"namespace": "ops", "table": "stdout_log", "source_prefix": "opteryx/ops/stdout_log/data/"},
    {"namespace": "ops", "table": "varlog", "source_prefix": "opteryx/ops/varlog/data/"},
]

print("=" * 80)
print(f"Creating Iceberg tables in workspace: {workspace}")
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

    print(f"   Found {len(parquet_files)} Parquet files")

    # Read schema from first file (without loading data)
    try:
        first_file = parquet_files[0]
        input_file = io.new_input(first_file)
        with pq.ParquetFile(input_file.open()) as pf:
            schema = pf.schema_arrow

        print(
            f"   Schema: {schema.names[:5]}{'...' if len(schema.names) > 5 else ''} ({len(schema.names)} columns)"
        )
    except Exception as e:
        print(f"   ✗ Failed to read schema: {e}")
        continue

    # Create namespace if it doesn't exist
    try:
        catalog.create_namespace_if_not_exists(namespace)
    except Exception as e:
        print(f"   ⚠️  Namespace creation warning: {e}")

    # Check if table already exists
    identifier = (namespace, table_name)
    if catalog.table_exists(identifier):
        print("   ⚠️  Table already exists, dropping and recreating...")
        try:
            catalog.drop_table(identifier)
        except Exception as e:
            print(f"   ✗ Failed to drop existing table: {e}")
            continue

    # Create the Iceberg table
    try:
        iceberg_table = catalog.create_table(
            identifier=identifier,
            schema=schema,
            properties={
                "write.parquet.compression-codec": "snappy",
            },
        )
        print("   ✓ Table created")
    except Exception as e:
        print(f"   ✗ Failed to create table: {e}")
        continue

    # Register existing Parquet files
    try:
        iceberg_table.add_files(parquet_files)
        print(f"   ✓ Registered {len(parquet_files)} files")
        print(f"   ✅ {full_table_name} complete")
    except Exception as e:
        print(f"   ✗ Failed to add files: {e}")
        continue

print("\n" + "=" * 80)
print("Dataset creation complete!")
print("=" * 80)
