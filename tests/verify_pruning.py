#!/usr/bin/env python3
"""
Verify that file pruning works with the test data.
Tests date, timestamp, int, float, and string filtering.
"""

import os
import sys

# Add local paths for pyiceberg_firestore_gcs
sys.path.insert(0, os.path.join(sys.path[0], ".."))

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)
os.environ["GCP_PROJECT_ID"] = "mabeldev"
os.environ["FIRESTORE_DATABASE"] = "catalogs"
os.environ["GCS_BUCKET"] = "opteryx_data"

from pyiceberg_firestore_gcs import FirestoreCatalog
from pyiceberg.expressions import (
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
)
from datetime import date, datetime

# Initialize catalog
catalog = FirestoreCatalog(
    "prune_testing",
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
)

# Load table
table = catalog.load_table("default.events")
print(f"Table location: {table.location()}")
print(f"Current snapshot: {table.current_snapshot()}")

# Test 1: Date filtering - should only need January file
print("\n" + "=" * 60)
print("TEST 1: Date range filter (January only)")
print("=" * 60)
scan = table.scan(row_filter=LessThan("event_date", date(2024, 2, 1)))
tasks = list(scan.plan_files())
print("Filter: event_date < 2024-02-01")
print(f"Files selected: {len(tasks)}")
print("Expected: 1 (January file only)")
if len(tasks) == 1:
    print("✓ PASS - Correctly pruned to 1 file")
else:
    print(f"✗ FAIL - Expected 1 file, got {len(tasks)}")
    for task in tasks:
        print(f"  - {task.file.file_path}")

# Test 2: Date range - should need March through June (Q2)
print("\n" + "=" * 60)
print("TEST 2: Date range filter (March-June)")
print("=" * 60)
scan = table.scan(
    row_filter=(
        GreaterThanOrEqual("event_date", date(2024, 3, 1))
        & LessThan("event_date", date(2024, 7, 1))
    )
)
tasks = list(scan.plan_files())
print("Filter: event_date >= 2024-03-01 AND event_date < 2024-07-01")
print(f"Files selected: {len(tasks)}")
print("Expected: 4 (March, April, May, June)")
if len(tasks) == 4:
    print("✓ PASS - Correctly pruned to 4 files")
else:
    print(f"✗ FAIL - Expected 4 files, got {len(tasks)}")

# Test 3: Timestamp filtering
print("\n" + "=" * 60)
print("TEST 3: Timestamp filter (after June)")
print("=" * 60)
scan = table.scan(row_filter=GreaterThan("event_timestamp", datetime(2024, 6, 30, 23, 59, 59)))
tasks = list(scan.plan_files())
print("Filter: event_timestamp > 2024-06-30 23:59:59")
print(f"Files selected: {len(tasks)}")
print("Expected: 6 (July through December)")
if len(tasks) == 6:
    print("✓ PASS - Correctly pruned to 6 files")
else:
    print(f"✗ FAIL - Expected 6 files, got {len(tasks)}")

# Test 4: Integer filtering - ID ranges
print("\n" + "=" * 60)
print("TEST 4: Integer filter (ID < 12000)")
print("=" * 60)
scan = table.scan(row_filter=LessThan("id", 12000))
tasks = list(scan.plan_files())
print("Filter: id < 12000")
print(f"Files selected: {len(tasks)}")
print("Expected: 1 (January: IDs 1000-11999)")
if len(tasks) == 1:
    print("✓ PASS - Correctly pruned to 1 file")
else:
    print(f"✗ FAIL - Expected 1 file, got {len(tasks)}")

# Test 5: Float filtering - amount ranges
print("\n" + "=" * 60)
print("TEST 5: Float filter (amount >= 600.0)")
print("=" * 60)
scan = table.scan(row_filter=GreaterThanOrEqual("amount", 600.0))
tasks = list(scan.plan_files())
print("Filter: amount >= 600.0")
print(f"Files selected: {len(tasks)}")
print("Expected: 6 (July-Dec: amounts 700-1200)")
if len(tasks) == 6:
    print("✓ PASS - Correctly pruned to 6 files")
else:
    print(f"✗ FAIL - Expected 6 files, got {len(tasks)}")

# Test 6: String filtering - user_id prefix
print("\n" + "=" * 60)
print("TEST 6: String filter (user_id = 'user_001_500')")
print("=" * 60)
scan = table.scan(row_filter=EqualTo("user_id", "user_001_500"))
tasks = list(scan.plan_files())
print("Filter: user_id = 'user_001_500'")
print(f"Files selected: {len(tasks)}")
print("Expected: 1 (January only)")
if len(tasks) == 1:
    print("✓ PASS - Correctly pruned to 1 file")
else:
    print(f"✗ FAIL - Expected 1 file, got {len(tasks)}")

# Test 7: String filtering - event_type
print("\n" + "=" * 60)
print("TEST 7: String filter (event_type = 'purchase')")
print("=" * 60)
scan = table.scan(row_filter=EqualTo("event_type", "purchase"))
tasks = list(scan.plan_files())
print("Filter: event_type = 'purchase'")
print(f"Files selected: {len(tasks)}")
print("Expected: 3 (April, May, June have 'purchase')")
if len(tasks) == 3:
    print("✓ PASS - Correctly pruned to 3 files")
else:
    print(f"✗ FAIL - Expected 3 files, got {len(tasks)}")

# Summary
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print("Test completed - review results above")
