"""
Test file pruning with dates and timestamps using the fake metadata.
"""

import os
import sys
from datetime import datetime, date

sys.path.insert(0, os.path.join(sys.path[0], ".."))

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)

from google.cloud import firestore

# Query the test data structure
client = firestore.Client(project="mabeldev", database="catalogs")

catalog_name = "prune_testing"
namespace = "default"
table_name = "events"

print("=" * 80)
print("DATE AND TIMESTAMP PRUNING TEST")
print("=" * 80)

# Get table metadata
table_ref = (
    client.collection(catalog_name).document(namespace).collection("tables").document(table_name)
)
table_doc = table_ref.get()

if not table_doc.exists:
    print("❌ Table not found. Run create_test_metadata.py first.")
    sys.exit(1)

table_data = table_doc.to_dict()
print(f"\n✓ Found table: {catalog_name}.{namespace}.{table_name}")
print(f"  Current snapshot: {table_data.get('current-snapshot-id')}")

# Get snapshot
snapshot_id = table_data.get("current-snapshot-id")
snapshot_ref = table_ref.collection("snapshots").document(str(snapshot_id))
snapshot_data = snapshot_ref.get().to_dict()

print(
    f"  Total files in snapshot: {snapshot_data.get('summary', {}).get('total-data-files', 'unknown')}"
)

# Get manifest files
manifest_files = []
for file_doc in table_ref.collection("manifest_files").stream():
    file_data = file_doc.to_dict()
    manifest_files.append(file_data)

manifest_files.sort(key=lambda x: x["month"])
print(f"  Manifest files loaded: {len(manifest_files)}")

print("\n" + "=" * 80)
print("FILE STATISTICS")
print("=" * 80)

print("\nDate ranges per file:")
epoch = datetime(1970, 1, 1)
for file_data in manifest_files[:4]:  # Show first 4
    month = file_data["month"]
    col_stats = file_data["column_stats"]

    # Parse date (days since epoch)
    min_date_days = int(col_stats["col_4"]["min"])
    max_date_days = int(col_stats["col_4"]["max"])
    min_date = (epoch + __import__("datetime").timedelta(days=min_date_days)).date()
    max_date = (epoch + __import__("datetime").timedelta(days=max_date_days)).date()

    # Parse timestamp (microseconds since epoch)
    min_ts_us = int(col_stats["col_5"]["min"])
    max_ts_us = int(col_stats["col_5"]["max"])
    min_ts = datetime.fromtimestamp(min_ts_us / 1_000_000)
    max_ts = datetime.fromtimestamp(max_ts_us / 1_000_000)

    print(
        f"  File {month:2d}: dates {min_date} to {max_date}, timestamps {min_ts.date()} to {max_ts.date()}"
    )

print(f"  ... ({len(manifest_files) - 4} more files)")

print("\nString ranges per file:")
for file_data in manifest_files[:4]:
    month = file_data["month"]
    col_stats = file_data["column_stats"]
    min_user = col_stats["col_2"]["min"]
    max_user = col_stats["col_2"]["max"]
    event_type = col_stats["col_3"]["min"]
    print(f"  File {month:2d}: users {min_user} to {max_user}, event_type={event_type}")
print(f"  ... ({len(manifest_files) - 4} more files)")

# Now simulate pruning tests
print("\n" + "=" * 80)
print("SIMULATED PRUNING TESTS")
print("=" * 80)


def would_prune_date(file_data, filter_type, filter_value):
    """Check if a file would be pruned based on date filter."""
    col_stats = file_data["column_stats"]
    file_min = int(col_stats["col_4"]["min"])
    file_max = int(col_stats["col_4"]["max"])

    # Convert filter_value (date) to days since epoch
    value_days = (filter_value - date(1970, 1, 1)).days

    if filter_type == "EqualTo":
        # Prune if value is outside range
        return value_days < file_min or value_days > file_max
    elif filter_type == "LessThan":
        # Prune if file_min >= value
        return file_min >= value_days
    elif filter_type == "LessThanOrEqual":
        # Prune if file_min > value
        return file_min > value_days
    elif filter_type == "GreaterThan":
        # Prune if file_max <= value
        return file_max <= value_days
    elif filter_type == "GreaterThanOrEqual":
        # Prune if file_max < value
        return file_max < value_days
    return False


def would_prune_timestamp(file_data, filter_type, filter_value):
    """Check if a file would be pruned based on timestamp filter."""
    col_stats = file_data["column_stats"]
    file_min = int(col_stats["col_5"]["min"])
    file_max = int(col_stats["col_5"]["max"])

    # Convert filter_value (datetime) to microseconds since epoch
    value_us = int(filter_value.timestamp() * 1_000_000)

    if filter_type == "EqualTo":
        return value_us < file_min or value_us > file_max
    elif filter_type == "LessThan":
        return file_min >= value_us
    elif filter_type == "LessThanOrEqual":
        return file_min > value_us
    elif filter_type == "GreaterThan":
        return file_max <= value_us
    elif filter_type == "GreaterThanOrEqual":
        return file_max < value_us
    return False


def would_prune_string(file_data, filter_type, filter_value, col_key):
    """Check if a file would be pruned based on string filter."""
    col_stats = file_data["column_stats"]
    file_min = col_stats[col_key]["min"]
    file_max = col_stats[col_key]["max"]

    if filter_type == "EqualTo":
        return filter_value < file_min or filter_value > file_max
    elif filter_type == "LessThan":
        return file_min >= filter_value
    elif filter_type == "LessThanOrEqual":
        return file_min > filter_value
    elif filter_type == "GreaterThan":
        return file_max <= filter_value
    elif filter_type == "GreaterThanOrEqual":
        return file_max < filter_value
    return False


# Test cases
test_cases = [
    # DATE TESTS
    ("Date EqualTo March 15", "date", "EqualTo", date(2024, 3, 15), [3]),
    ("Date EqualTo July 4", "date", "EqualTo", date(2024, 7, 4), [7]),
    ("Date LessThan Feb 1", "date", "LessThan", date(2024, 2, 1), [1]),
    ("Date LessThanOrEqual Jan 31", "date", "LessThanOrEqual", date(2024, 1, 31), [1]),
    ("Date GreaterThan Nov 30", "date", "GreaterThan", date(2024, 11, 30), [12]),
    ("Date GreaterThanOrEqual Dec 1", "date", "GreaterThanOrEqual", date(2024, 12, 1), [12]),
    ("Date before all data", "date", "LessThan", date(2024, 1, 1), []),
    ("Date after all data", "date", "GreaterThan", date(2024, 12, 31), []),
    # TIMESTAMP TESTS
    ("Timestamp EqualTo April 15", "timestamp", "EqualTo", datetime(2024, 4, 15, 12, 0, 0), [4]),
    ("Timestamp LessThan March 1", "timestamp", "LessThan", datetime(2024, 3, 1, 0, 0, 0), [1, 2]),
    (
        "Timestamp GreaterThan Oct 31",
        "timestamp",
        "GreaterThan",
        datetime(2024, 10, 31, 23, 59, 59),
        [11, 12],
    ),
    # STRING TESTS
    ("String EqualTo user_005_500", "string", "EqualTo", "user_005_500", [5]),
    ("String LessThan user_003_000", "string", "LessThan", "user_003_000", [1, 2]),
    ("String GreaterThan user_010_999", "string", "GreaterThan", "user_010_999", [11, 12]),
]

print()
for description, data_type, filter_type, filter_value, expected_files in test_cases:
    if data_type == "date":
        pruned_files = [
            f["month"] for f in manifest_files if not would_prune_date(f, filter_type, filter_value)
        ]
    elif data_type == "timestamp":
        pruned_files = [
            f["month"]
            for f in manifest_files
            if not would_prune_timestamp(f, filter_type, filter_value)
        ]
    elif data_type == "string":
        pruned_files = [
            f["month"]
            for f in manifest_files
            if not would_prune_string(f, filter_type, filter_value, "col_2")
        ]

    kept_count = len(pruned_files)
    pruned_count = len(manifest_files) - kept_count
    matches_expected = set(pruned_files) == set(expected_files)

    status = "✅" if matches_expected else "❌"
    print(f"{status} {description}")
    print(f"   Filter: {filter_type}({filter_value})")
    print(f"   Pruned: {pruned_count}/{len(manifest_files)} files, Kept: {kept_count} files")
    if expected_files:
        print(f"   Expected months: {expected_files}, Got: {sorted(pruned_files)}")
    if not matches_expected:
        print("   ⚠️  Mismatch!")
    print()

print("=" * 80)
print("PRUNING EFFECTIVENESS SUMMARY")
print("=" * 80)
print(f"Total data files: {len(manifest_files)}")
print("Date range: 2024-01-01 to 2024-12-31 (12 months)")
print()
print("Key findings:")
print("  ✓ Date filters can prune files by month")
print("  ✓ Timestamp filters work similarly to dates")
print("  ✓ String filters (user_id) can prune by value ranges")
print("  ✓ Each month is a separate prunable unit")
print("=" * 80)
