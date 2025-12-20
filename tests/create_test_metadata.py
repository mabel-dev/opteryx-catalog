"""
Create test table metadata in Firestore without actual data files.
This creates a table with multiple data files with different date/timestamp ranges for pruning tests.
"""

import os
import uuid
from datetime import datetime, timedelta

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)

from google.cloud import firestore, storage
import pyarrow as pa
import pyarrow.parquet as pq

# Initialize Firestore and GCS
client = firestore.Client(project="mabeldev", database="catalogs")
gcs_client = storage.Client()
bucket = gcs_client.bucket("opteryx_data")

# Catalog and table info
catalog_name = "prune_testing"
namespace = "default"
table_name = "events"
table_path = f"{catalog_name}/{namespace}/{table_name}"

print(f"Creating test metadata for {table_path}")

# Create namespace document
namespace_ref = client.collection(catalog_name).document(namespace)
namespace_ref.set(
    {
        "created_at": firestore.SERVER_TIMESTAMP,
    }
)
print(f"✓ Created namespace: {namespace}")

# Define schema with various types
schema_json = {
    "type": "struct",
    "schema-id": 0,
    "fields": [
        {"id": 1, "name": "id", "required": False, "type": "long"},
        {"id": 2, "name": "user_id", "required": False, "type": "string"},
        {"id": 3, "name": "event_type", "required": False, "type": "string"},
        {"id": 4, "name": "event_date", "required": False, "type": "date"},
        {"id": 5, "name": "event_timestamp", "required": False, "type": "timestamp"},
        {"id": 6, "name": "amount", "required": False, "type": "double"},
        {"id": 7, "name": "quantity", "required": False, "type": "int"},
    ],
}

# Create base metadata structure
base_metadata = {
    "format-version": 2,
    "table-uuid": str(uuid.uuid4()),
    "location": f"gs://opteryx_data/{catalog_name}/{namespace}/{table_name}",
    "last-sequence-number": 0,
    "last-updated-ms": int(datetime.now().timestamp() * 1000),
    "last-column-id": 7,
    "schemas": [schema_json],
    "current-schema-id": 0,
    "partition-specs": [{"spec-id": 0, "fields": []}],
    "default-spec-id": 0,
    "last-partition-id": 0,
    "properties": {},
    "current-snapshot-id": None,
    "refs": {},
    "snapshots": [],
    "snapshot-log": [],
    "metadata-log": [],
    "sort-orders": [{"order-id": 0, "fields": []}],
    "default-sort-order-id": 0,
}

# Date ranges for test data files
# We'll create 12 files, one per month in 2024
base_date = datetime(2024, 1, 1)
data_files_info = []

for month in range(1, 13):
    month_start = datetime(2024, month, 1)
    if month == 12:
        month_end = datetime(2024, 12, 31)
    else:
        month_end = datetime(2024, month + 1, 1) - timedelta(days=1)

    # Convert to days since epoch for date type
    epoch = datetime(1970, 1, 1)
    min_date_days = (month_start - epoch).days
    max_date_days = (month_end - epoch).days

    # Timestamps in microseconds
    min_timestamp_us = int(month_start.timestamp() * 1_000_000)
    max_timestamp_us = int(month_end.timestamp() * 1_000_000)

    # String ranges (user IDs)
    min_user = f"user_{month:03d}_001"
    max_user = f"user_{month:03d}_999"

    # Event types for each quarter
    event_types = ["login", "purchase", "view", "logout"]
    event_type = event_types[(month - 1) // 3]

    file_info = {
        "month": month,
        "file_path": f"gs://opteryx_data/{catalog_name}/{namespace}/{table_name}/data/2024-{month:02d}.parquet",
        "record_count": 10000 + (month * 1000),  # Varying record counts
        "file_size": 5_000_000 + (month * 100_000),
        "column_stats": {
            # ID (long) - sequential ranges
            1: {
                "min": (month - 1) * 10000 + 1,
                "max": month * 10000,
            },
            # user_id (string)
            2: {
                "min": min_user.encode("utf-8"),
                "max": max_user.encode("utf-8"),
            },
            # event_type (string)
            3: {
                "min": event_type.encode("utf-8"),
                "max": event_type.encode("utf-8"),
            },
            # event_date (date as int32 - days since epoch)
            4: {
                "min": min_date_days,
                "max": max_date_days,
            },
            # event_timestamp (timestamp as int64 - microseconds)
            5: {
                "min": min_timestamp_us,
                "max": max_timestamp_us,
            },
            # amount (double)
            6: {
                "min": float(month * 10),
                "max": float(month * 10 + 100),
            },
            # quantity (int)
            7: {
                "min": month,
                "max": month * 100,
            },
        },
    }
    data_files_info.append(file_info)

print(f"\n✓ Generated metadata for {len(data_files_info)} data files")

# Create snapshot with manifest entries
snapshot_id = 1000000000000000001
timestamp_ms = int(datetime.now().timestamp() * 1000)

# Create table document with metadata
table_ref = (
    client.collection(catalog_name).document(namespace).collection("tables").document(table_name)
)

table_metadata = {
    **base_metadata,
    "name": table_name,
    "namespace": namespace,
    "workspace": catalog_name,
    "created_at": firestore.SERVER_TIMESTAMP,
    "updated_at": firestore.SERVER_TIMESTAMP,
    "current-snapshot-id": snapshot_id,
    "refs": {"main": {"snapshot-id": snapshot_id, "type": "branch"}},
}

table_ref.set(table_metadata)
print(f"✓ Created table document: {catalog_name}/{namespace}/tables/{table_name}")

# Create snapshot document
snapshot_ref = table_ref.collection("snapshots").document(str(snapshot_id))
snapshot_data = {
    "snapshot-id": snapshot_id,
    "timestamp-ms": timestamp_ms,
    "sequence-number": 1,
    "schema-id": 0,
    "manifest-list": f"gs://opteryx_data/{catalog_name}/{namespace}/{table_name}/metadata/snap-{snapshot_id}.avro",
    "summary": {
        "operation": "append",
        "total-data-files": str(len(data_files_info)),
        "total-records": str(sum(f["record_count"] for f in data_files_info)),
        "total-files-size": str(sum(f["file_size"] for f in data_files_info)),
    },
}

snapshot_ref.set(snapshot_data)
print(f"✓ Created snapshot: {snapshot_id}")

# Create snapshot log
snapshot_log_ref = table_ref.collection("snapshot_log").document(f"{snapshot_id}_{timestamp_ms}")
snapshot_log_ref.set(
    {
        "snapshot-id": snapshot_id,
        "timestamp-ms": timestamp_ms,
    }
)
print("✓ Created snapshot log entry")

# Store file info in a separate subcollection for the manifest
# This avoids Firestore's nested data limitations
manifest_files_collection = table_ref.collection("manifest_files")

for file_info in data_files_info:
    file_doc_id = f"file_{file_info['month']:02d}"
    file_ref = manifest_files_collection.document(file_doc_id)

    # Convert column stats to string keys for Firestore
    column_stats_firestore = {
        f"col_{col_id}": {
            "min": stats["min"]
            if not isinstance(stats["min"], bytes)
            else stats["min"].decode("utf-8"),
            "max": stats["max"]
            if not isinstance(stats["max"], bytes)
            else stats["max"].decode("utf-8"),
        }
        for col_id, stats in file_info["column_stats"].items()
    }

    file_ref.set(
        {
            "file_path": file_info["file_path"],
            "record_count": file_info["record_count"],
            "file_size_bytes": file_info["file_size"],
            "column_stats": column_stats_firestore,
            "month": file_info["month"],
        }
    )

# Update snapshot with manifest file count
snapshot_ref.update(
    {
        "manifest-file-count": len(data_files_info),
    }
)

print(f"✓ Created {len(data_files_info)} manifest file documents")

# Now create the actual Parquet manifest file with statistics
print("\nCreating Parquet manifest file...")

# Import to_int function for value conversion
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from pyiceberg_firestore_gcs.to_int import to_int
from pyiceberg.types import LongType, StringType, DateType, TimestampType, DoubleType, IntegerType

# Define the schema for the Parquet manifest - matches parquet_manifest.py
manifest_schema = pa.schema(
    [
        ("file_path", pa.string()),
        ("snapshot_id", pa.int64()),
        ("sequence_number", pa.int64()),
        ("file_sequence_number", pa.int64()),
        ("active", pa.bool_()),
        ("partition_spec_id", pa.int32()),
        ("partition_json", pa.string()),
        ("file_format", pa.string()),
        ("record_count", pa.int64()),
        ("file_size_bytes", pa.int64()),
        ("lower_bounds", pa.list_(pa.int64())),  # Array indexed by field_id
        ("upper_bounds", pa.list_(pa.int64())),  # Array indexed by field_id
        ("null_counts", pa.list_(pa.int64())),
        ("value_counts", pa.list_(pa.int64())),
        ("column_sizes", pa.list_(pa.int64())),
        ("nan_counts", pa.list_(pa.int64())),
        ("key_metadata", pa.binary()),
        ("split_offsets_json", pa.string()),
        ("equality_ids_json", pa.string()),
        ("sort_order_id", pa.int32()),
    ]
)

# Type mapping for to_int conversion
field_types = {
    1: LongType(),  # id
    2: StringType(),  # user_id
    3: StringType(),  # event_type
    4: DateType(),  # event_date
    5: TimestampType(),  # event_timestamp
    6: DoubleType(),  # amount
    7: IntegerType(),  # quantity
}


def encode_bounds_array(stats_dict, field_types):
    """Convert column statistics to array format for Parquet manifest."""
    # Find max field ID
    max_field_id = max(stats_dict.keys())

    # Create array with None for missing fields
    lower_array = [None] * (max_field_id + 1)
    upper_array = [None] * (max_field_id + 1)

    for field_id, stats in stats_dict.items():
        field_type = field_types[field_id]

        # Convert min/max values to int64 using to_int
        lower_array[field_id] = to_int(stats["min"], field_type)
        upper_array[field_id] = to_int(stats["max"], field_type)

    return lower_array, upper_array


# Build manifest data
manifest_data = {
    "file_path": [],
    "snapshot_id": [],
    "sequence_number": [],
    "file_sequence_number": [],
    "active": [],
    "partition_spec_id": [],
    "partition_json": [],
    "file_format": [],
    "record_count": [],
    "file_size_bytes": [],
    "lower_bounds": [],
    "upper_bounds": [],
    "null_counts": [],
    "value_counts": [],
    "column_sizes": [],
    "nan_counts": [],
    "key_metadata": [],
    "split_offsets_json": [],
    "equality_ids_json": [],
    "sort_order_id": [],
}

for file_info in data_files_info:
    lower_array, upper_array = encode_bounds_array(file_info["column_stats"], field_types)

    manifest_data["file_path"].append(file_info["file_path"])
    manifest_data["snapshot_id"].append(snapshot_id)
    manifest_data["sequence_number"].append(1)
    manifest_data["file_sequence_number"].append(1)
    manifest_data["active"].append(True)
    manifest_data["partition_spec_id"].append(0)
    manifest_data["partition_json"].append("{}")
    manifest_data["file_format"].append("PARQUET")
    manifest_data["record_count"].append(file_info["record_count"])
    manifest_data["file_size_bytes"].append(file_info["file_size"])
    manifest_data["lower_bounds"].append(lower_array)
    manifest_data["upper_bounds"].append(upper_array)
    # Empty statistics arrays for now
    empty_stats = [None] * 8  # 8 fields (0-7)
    manifest_data["null_counts"].append(empty_stats)
    manifest_data["value_counts"].append(empty_stats)
    manifest_data["column_sizes"].append(empty_stats)
    manifest_data["nan_counts"].append(empty_stats)
    manifest_data["key_metadata"].append(None)
    manifest_data["split_offsets_json"].append(None)
    manifest_data["equality_ids_json"].append(None)
    manifest_data["sort_order_id"].append(0)

# Create PyArrow table
manifest_table = pa.table(manifest_data, schema=manifest_schema)

# Write to GCS
manifest_path = f"{catalog_name}/{namespace}/{table_name}/metadata/manifest-{snapshot_id}.parquet"
manifest_blob = bucket.blob(manifest_path)

# Write to bytes buffer then upload
from io import BytesIO

buffer = BytesIO()
pq.write_table(manifest_table, buffer)
buffer.seek(0)
manifest_blob.upload_from_file(buffer, content_type="application/octet-stream")

manifest_gcs_path = f"gs://opteryx_data/{manifest_path}"
print(f"✓ Wrote Parquet manifest to {manifest_gcs_path}")

# Update snapshot with parquet-manifest path
snapshot_ref.update({"parquet-manifest": manifest_gcs_path})
print("✓ Updated snapshot with parquet-manifest reference")

print("\n" + "=" * 80)
print("TEST DATA SUMMARY")
print("=" * 80)
print(f"Catalog: {catalog_name}")
print(f"Table: {namespace}.{table_name}")
print(f"Files: {len(data_files_info)} (one per month of 2024)")
print(f"Total records: {sum(f['record_count'] for f in data_files_info):,}")
print("\nDate ranges by month:")
for file_info in data_files_info[:3]:  # Show first 3
    month = file_info["month"]
    min_date = datetime(2024, month, 1)
    if month == 12:
        max_date = datetime(2024, 12, 31)
    else:
        max_date = datetime(2024, month + 1, 1) - timedelta(days=1)
    print(
        f"  Month {month:2d}: {min_date.date()} to {max_date.date()} ({file_info['record_count']:,} records)"
    )
print(f"  ... ({len(data_files_info) - 3} more months)")

print("\nColumn types for pruning tests:")
print("  - id (long): Sequential ranges per month")
print("  - user_id (string): user_XXX_001 to user_XXX_999 per month")
print("  - event_type (string): Quarterly values (login/purchase/view/logout)")
print("  - event_date (date): Date ranges per month")
print("  - event_timestamp (timestamp): Timestamp ranges per month")
print("  - amount (double): Float ranges per month")
print("  - quantity (int): Integer ranges per month")
print("=" * 80)
