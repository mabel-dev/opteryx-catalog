"""
Script to create Iceberg tables for benchmark datasets.
This script creates Iceberg tables for:
- benchmarks.clickbench.hits
- benchmarks.tpc_h.customer
- benchmarks.tpc_h.lineitem
- benchmarks.tpc_h.nation
- benchmarks.tpc_h.orders
- benchmarks.tpc_h.part
- benchmarks.tpc_h.partsupp
- benchmarks.tpc_h.region
- benchmarks.tpc_h.supplier

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


workspace = "benchmarks"  # Use benchmarks as the workspace

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
    {"namespace": "clickbench", "table": "hits", "source_prefix": "benchmarks/clickbench/hits/"},
    {"namespace": "tpc_h", "table": "customer", "source_prefix": "benchmarks/tpc_h/customer/"},
    {"namespace": "tpc_h", "table": "lineitem", "source_prefix": "benchmarks/tpc_h/lineitem/"},
    {"namespace": "tpc_h", "table": "nation", "source_prefix": "benchmarks/tpc_h/nation/"},
    {"namespace": "tpc_h", "table": "orders", "source_prefix": "benchmarks/tpc_h/orders/"},
    {"namespace": "tpc_h", "table": "part", "source_prefix": "benchmarks/tpc_h/part/"},
    {"namespace": "tpc_h", "table": "partsupp", "source_prefix": "benchmarks/tpc_h/partsupp/"},
    {"namespace": "tpc_h", "table": "region", "source_prefix": "benchmarks/tpc_h/region/"},
    {"namespace": "tpc_h", "table": "supplier", "source_prefix": "benchmarks/tpc_h/supplier/"},
]

# Create namespaces for clickbench and tpc_h
print("Creating namespace: clickbench...")
catalog.create_namespace_if_not_exists("clickbench")

print("Creating namespace: tpc_h...")
catalog.create_namespace_if_not_exists("tpc_h")

# Process each dataset
for dataset in datasets:
    namespace = dataset["namespace"]
    table_name = dataset["table"]
    source_prefix = dataset["source_prefix"]

    full_table_name = f"{namespace}.{table_name}"

    print(f"\nProcessing {full_table_name}...")
    print(f"  Source prefix: gs://opteryx_data/{source_prefix}")

    try:
        # List Parquet files in GCS
        print("  Listing Parquet files...")
        blobs = bucket.list_blobs(prefix=source_prefix)
        parquet_files = [
            f"gs://opteryx_data/{blob.name}" for blob in blobs if blob.name.endswith(".parquet")
        ]

        if not parquet_files:
            print(f"  ✗ No Parquet files found at {source_prefix}")
            continue

        print(f"  Found {len(parquet_files)} Parquet files")

        # Read schema from first Parquet file (no data reading)
        print("  Reading schema from Parquet metadata...")
        input_file = io.new_input(parquet_files[0])
        with input_file.open() as f:
            parquet_file = pq.ParquetFile(f)
            schema = parquet_file.schema_arrow

        print(f"  Schema: {schema}")

        # Drop table if it exists
        try:
            print("  Dropping existing table if it exists...")
            catalog.drop_table((namespace, table_name))
            print("  Dropped existing table")
        except Exception:
            print("  No existing table to drop (this is expected)")

        # Create the Iceberg table with the schema
        print("  Creating Iceberg table...")
        iceberg_table = catalog.create_table((namespace, table_name), schema)

        # Add the existing Parquet files to the table
        print(f"  Adding {len(parquet_files)} files to table...")
        iceberg_table.add_files(parquet_files)

        print(f"  ✓ Successfully created {full_table_name} with {len(parquet_files)} files")

    except Exception as e:
        print(f"  ✗ Error processing {full_table_name}: {e}")
        import traceback

        traceback.print_exc()

print("\n" + "=" * 80)
print("Done!")
print("=" * 80)
