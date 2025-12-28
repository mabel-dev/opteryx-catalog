#!/usr/bin/env python3
"""
Create REAL test data with actual Parquet files and proper Iceberg metadata.
This uses the actual PyIceberg write path, not fake metadata.
"""

import os
import sys
from datetime import datetime, timedelta
import pyarrow as pa

# Add local paths
sys.path.insert(0, os.path.join(sys.path[0], ".."))

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)
os.environ["GCP_PROJECT_ID"] = "mabeldev"
os.environ["FIRESTORE_DATABASE"] = "catalogs"
os.environ["GCS_BUCKET"] = "opteryx_data"

from pyiceberg_firestore_gcs import FirestoreCatalog

# Configuration
catalog_name = "prune_testing"
namespace = "default"
table_name = "events"

# Initialize catalog
catalog = FirestoreCatalog(
    catalog_name,
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
)

print(f"Creating real test data for {catalog_name}.{namespace}.{table_name}")

# Drop table if exists
try:
    catalog.drop_table(f"{namespace}.{table_name}")
    print("✓ Dropped existing table")
except:
    pass

# Create namespace
try:
    catalog.create_namespace(namespace)
    print(f"✓ Created namespace: {namespace}")
except:
    print(f"  Namespace {namespace} already exists")

# Define schema
schema = pa.schema(
    [
        ("id", pa.int64()),
        ("user_id", pa.string()),
        ("event_type", pa.string()),
        ("event_date", pa.date32()),
        ("event_timestamp", pa.timestamp("us")),
        ("amount", pa.float64()),
        ("quantity", pa.int32()),
    ]
)

print("\n✓ Defined schema with 7 columns")

# Create the Iceberg table
from pyiceberg.schema import Schema
from pyiceberg.types import (
    LongType,
    StringType,
    DateType,
    TimestampType,
    DoubleType,
    IntegerType,
    NestedField,
)

iceberg_schema = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "user_id", StringType(), required=False),
    NestedField(3, "event_type", StringType(), required=False),
    NestedField(4, "event_date", DateType(), required=False),
    NestedField(5, "event_timestamp", TimestampType(), required=False),
    NestedField(6, "amount", DoubleType(), required=False),
    NestedField(7, "quantity", IntegerType(), required=False),
)

table = catalog.create_table(
    f"{namespace}.{table_name}",
    schema=iceberg_schema,
)

print(f"✓ Created Iceberg table: {namespace}.{table_name}")
print(f"  Table location: {table.location()}")

# Generate and append data for each month
print("\nGenerating data for 12 months...")

total_records = 0
for month in range(1, 13):
    month_start = datetime(2024, month, 1)
    if month == 12:
        month_end = datetime(2024, 12, 31, 23, 59, 59)
    else:
        month_end = datetime(2024, month + 1, 1) - timedelta(seconds=1)

    # Determine event type by quarter
    quarter = (month - 1) // 3
    event_types = ["login", "purchase", "view", "logout"]
    event_type = event_types[quarter]

    # Generate records for this month
    num_records = (month + 9) * 1000  # Varying record counts

    # Create data
    ids = list(range(month * 10000, month * 10000 + num_records))
    user_ids = [f"user_{month:03d}_{i % 1000:03d}" for i in range(num_records)]
    event_types_list = [event_type] * num_records

    # Date range within the month
    date_range = (month_end - month_start).days + 1
    event_dates = [month_start.date() + timedelta(days=i % date_range) for i in range(num_records)]

    # Timestamp range within the month
    timestamp_range_seconds = int((month_end - month_start).total_seconds())
    event_timestamps = [
        month_start + timedelta(seconds=(i * 7919) % timestamp_range_seconds)
        for i in range(num_records)
    ]

    # Amount range varies by month
    amounts = [float(month * 100 + (i % 100)) for i in range(num_records)]

    # Quantity varies by month
    quantities = [month + (i % (month * 10)) for i in range(num_records)]

    # Create PyArrow table
    data_table = pa.table(
        {
            "id": ids,
            "user_id": user_ids,
            "event_type": event_types_list,
            "event_date": event_dates,
            "event_timestamp": event_timestamps,
            "amount": amounts,
            "quantity": quantities,
        },
        schema=schema,
    )

    # Append to Iceberg table (this will create actual Parquet files and Avro manifests)
    table.append(data_table)

    total_records += num_records
    print(
        f"  Month {month:2d}: {num_records:6d} records ({event_type:8s}) - "
        f"{month_start.date()} to {month_end.date()}"
    )

print(f"\n✓ Appended {total_records:,} total records across 12 months")

# Now the table has real data files and Avro manifests
# The Parquet manifest should be created automatically by commit_table
# Let's verify it exists
snapshot = table.current_snapshot()
if snapshot:
    print(f"\n✓ Current snapshot ID: {snapshot.snapshot_id}")
    print(f"  Manifest list: {snapshot.manifest_list}")

    # Check for Parquet manifest in Firestore
    from google.cloud import firestore

    db = firestore.Client(project="mabeldev", database="catalogs")

    snapshot_doc = (
        db.collection(catalog_name)
        .document(namespace)
        .collection("tables")
        .document(table_name)
        .collection("snapshots")
        .document(str(snapshot.snapshot_id))
        .get()
    )

    if snapshot_doc.exists:
        data = snapshot_doc.to_dict()
        parquet_manifest = data.get("parquet-manifest")
        if parquet_manifest:
            print(f"  Parquet manifest: {parquet_manifest}")
            print("\n✓ SUCCESS: Real data with Parquet manifest created!")
        else:
            print("  ⚠ No parquet-manifest field found in snapshot")
    else:
        print("  ⚠ Snapshot document not found in Firestore")

print("\n" + "=" * 70)
print("TEST DATA SUMMARY")
print("=" * 70)
print(f"Catalog: {catalog_name}")
print(f"Table: {namespace}.{table_name}")
print(f"Total records: {total_records:,}")
print("Data files: 12 (one per month)")
print("\nNow run: python tests/verify_pruning.py")
