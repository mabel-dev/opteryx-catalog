#!/usr/bin/env python3
"""
Comprehensive edge case tests for file pruning.
Focus heavily on date filtering since 80% of filters will be date-based.
"""

import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(sys.path[0], ".."))

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)

from pyiceberg_firestore_gcs import FirestoreCatalog
from pyiceberg.expressions import (
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
)

catalog = FirestoreCatalog(
    "prune_testing",
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
)

table = catalog.load_table("default.events")

# Data ranges (from create_real_test_data.py):
# Month 1 (Jan): 2024-01-01 to 2024-01-31, IDs 10000-19999
# Month 2 (Feb): 2024-02-01 to 2024-02-29, IDs 20000-30999
# Month 3 (Mar): 2024-03-01 to 2024-03-31, IDs 30000-41999
# ...
# Month 12 (Dec): 2024-12-01 to 2024-12-31, IDs 120000-140999


def test_filter(name, expected_files, row_filter, notes=""):
    """Test a filter and report results."""
    scan = table.scan(row_filter=row_filter)
    tasks = list(scan.plan_files())
    actual = len(tasks)

    status = "✓ PASS" if actual == expected_files else "✗ FAIL"
    print(f"{status} | {name:50s} | Expected: {expected_files:2d} | Got: {actual:2d}")

    if actual != expected_files:
        print(f"      Filter: {row_filter}")
        if notes:
            print(f"      Notes: {notes}")
        if actual > 0 and actual != expected_files:
            print(f"      Files: {[os.path.basename(t.file.file_path) for t in tasks[:3]]}")

    return actual == expected_files


print("=" * 100)
print("DATE FILTER EDGE CASES (Critical - 80% of real queries)")
print("=" * 100)

passed = 0
total = 0

# Test 1: Exact boundary - first day of month
total += 1
if test_filter(
    "Date = 2024-01-01 (exact first day)",
    1,
    EqualTo("event_date", date(2024, 1, 1)),
    "Should only match January file",
):
    passed += 1

# Test 2: Exact boundary - last day of month
total += 1
if test_filter(
    "Date = 2024-01-31 (exact last day)",
    1,
    EqualTo("event_date", date(2024, 1, 31)),
    "Should only match January file",
):
    passed += 1

# Test 3: Exact boundary - between months
total += 1
if test_filter(
    "Date = 2024-02-01 (boundary between Jan/Feb)",
    1,
    EqualTo("event_date", date(2024, 2, 1)),
    "Should only match February file",
):
    passed += 1

# Test 4: Less than first day (should exclude that month)
total += 1
if test_filter(
    "Date < 2024-02-01 (before Feb)",
    1,
    LessThan("event_date", date(2024, 2, 1)),
    "Should only include January",
):
    passed += 1

# Test 5: Less than or equal to last day
total += 1
if test_filter(
    "Date <= 2024-01-31 (through Jan)",
    1,
    LessThanOrEqual("event_date", date(2024, 1, 31)),
    "Should only include January",
):
    passed += 1

# Test 6: Greater than last day (should exclude that month)
total += 1
if test_filter(
    "Date > 2024-01-31 (after Jan)",
    11,
    GreaterThan("event_date", date(2024, 1, 31)),
    "Should include Feb-Dec (11 months)",
):
    passed += 1

# Test 7: Greater than or equal to first day
total += 1
if test_filter(
    "Date >= 2024-12-01 (from Dec)",
    1,
    GreaterThanOrEqual("event_date", date(2024, 12, 1)),
    "Should only include December",
):
    passed += 1

# Test 8: Greater than or equal on boundary (inclusive)
total += 1
if test_filter(
    "Date >= 2024-06-30 (inclusive end of June)",
    7,
    GreaterThanOrEqual("event_date", date(2024, 6, 30)),
    "Should include Jun 30 + Jul-Dec (7 files)",
):
    passed += 1

# Test 9: Less than or equal on boundary (inclusive)
total += 1
if test_filter(
    "Date <= 2024-02-01 (inclusive start of Feb)",
    2,
    LessThanOrEqual("event_date", date(2024, 2, 1)),
    "Should include Jan + Feb 1 (2 files)",
):
    passed += 1

# Test 10: Middle of month (no boundaries)
total += 1
if test_filter(
    "Date = 2024-06-15 (middle of June)",
    1,
    EqualTo("event_date", date(2024, 6, 15)),
    "Should only match June file",
):
    passed += 1

# Test 11: Range spanning multiple months
total += 1
if test_filter(
    "Date >= 2024-03-15 AND Date <= 2024-05-15",
    3,
    (
        GreaterThanOrEqual("event_date", date(2024, 3, 15))
        & LessThanOrEqual("event_date", date(2024, 5, 15))
    ),
    "Should include Mar, Apr, May",
):
    passed += 1

# Test 12: Date before all data
total += 1
if test_filter(
    "Date < 2024-01-01 (before all data)",
    0,
    LessThan("event_date", date(2024, 1, 1)),
    "Should prune all files",
):
    passed += 1

# Test 13: Date after all data
total += 1
if test_filter(
    "Date > 2024-12-31 (after all data)",
    0,
    GreaterThan("event_date", date(2024, 12, 31)),
    "Should prune all files",
):
    passed += 1

# Test 14: Date exactly on year boundary
total += 1
if test_filter(
    "Date >= 2024-01-01 (year start)",
    12,
    GreaterThanOrEqual("event_date", date(2024, 1, 1)),
    "Should include all 12 months",
):
    passed += 1

# Test 15: Last day of leap year February
total += 1
if test_filter(
    "Date = 2024-02-29 (leap day)",
    1,
    EqualTo("event_date", date(2024, 2, 29)),
    "Should only match February file",
):
    passed += 1

print("\n" + "=" * 100)
print("INTEGER FILTER EDGE CASES")
print("=" * 100)

# Test 16: ID exactly on file boundary (lower bound)
total += 1
if test_filter(
    "ID = 10000 (exact lower bound)", 1, EqualTo("id", 10000), "January IDs: 10000-19999"
):
    passed += 1

# Test 17: ID exactly on file boundary (upper bound)
total += 1
if test_filter(
    "ID = 19999 (exact upper bound)", 1, EqualTo("id", 19999), "January IDs: 10000-19999"
):
    passed += 1

# Test 18: ID less than lower bound (exclusive)
total += 1
if test_filter("ID < 10000 (below all data)", 0, LessThan("id", 10000), "Should prune all files"):
    passed += 1

# Test 19: ID greater than upper bound (exclusive)
total += 1
if test_filter(
    "ID > 140999 (above all data)", 0, GreaterThan("id", 140999), "December IDs: 120000-140999"
):
    passed += 1

# Test 20: ID on boundary between files
total += 1
if test_filter(
    "ID >= 20000 (Feb onwards)", 11, GreaterThanOrEqual("id", 20000), "Should include Feb-Dec"
):
    passed += 1

print("\n" + "=" * 100)
print("FLOAT/DOUBLE FILTER EDGE CASES")
print("=" * 100)

# Test 21: Float exact boundary
total += 1
if test_filter(
    "Amount = 100.0 (exact lower bound)", 1, EqualTo("amount", 100.0), "Jan amounts: 100-199"
):
    passed += 1

# Test 22: Float between boundaries
total += 1
if test_filter(
    "Amount >= 550.0 AND Amount < 650.0",
    2,
    (GreaterThanOrEqual("amount", 550.0) & LessThan("amount", 650.0)),
    "Should include May (500-599) and June (600-699)",
):
    passed += 1

# Test 23: Float slightly above boundary
total += 1
if test_filter(
    "Amount > 199.0 (just above Jan max)",
    11,
    GreaterThan("amount", 199.0),
    "Should exclude January",
):
    passed += 1

print("\n" + "=" * 100)
print("STRING FILTER EDGE CASES")
print("=" * 100)

# Test 24: String exact match (lexicographic)
total += 1
if test_filter(
    "event_type = 'login' (Q1)", 3, EqualTo("event_type", "login"), "Months 1-3 have 'login'"
):
    passed += 1

# Test 25: String exact match different value
total += 1
if test_filter(
    "event_type = 'logout' (Q4)", 3, EqualTo("event_type", "logout"), "Months 10-12 have 'logout'"
):
    passed += 1

# Test 26: String that doesn't exist
total += 1
if test_filter(
    "event_type = 'nonexistent'", 0, EqualTo("event_type", "nonexistent"), "Should prune all files"
):
    passed += 1

# Test 27: User ID with specific prefix (lexicographic ordering)
total += 1
scan = table.scan(row_filter=EqualTo("user_id", "user_012_500"))
actual = len(list(scan.plan_files()))
expected = 1  # Should only match December
# Note: This might not work perfectly due to lexicographic truncation to 7 bytes
if actual <= 3:  # Allow some tolerance for string matching
    print(
        f"✓ PASS | {'user_id = user_012_500 (string prefix)':50s} | Expected: ~{expected:2d} | Got: {actual:2d}"
    )
    passed += 1
else:
    print(
        f"✗ FAIL | {'user_id = user_012_500 (string prefix)':50s} | Expected: ~{expected:2d} | Got: {actual:2d}"
    )
total += 1

print("\n" + "=" * 100)
print("COMBINED FILTER EDGE CASES")
print("=" * 100)

# Test 28: AND with multiple fields
total += 1
if test_filter(
    "Date = 2024-06-15 AND Amount >= 600",
    1,
    (EqualTo("event_date", date(2024, 6, 15)) & GreaterThanOrEqual("amount", 600.0)),
    "Should only match June",
):
    passed += 1

# Test 29: Multiple boundaries simultaneously
total += 1
if test_filter(
    "Date >= 2024-06-01 AND Date <= 2024-06-30 AND ID >= 60000",
    1,
    (
        GreaterThanOrEqual("event_date", date(2024, 6, 1))
        & LessThanOrEqual("event_date", date(2024, 6, 30))
        & GreaterThanOrEqual("id", 60000)
    ),
    "Should only match June",
):
    passed += 1

print("\n" + "=" * 100)
print(f"SUMMARY: {passed}/{total} tests passed ({100 * passed // total}%)")
print("=" * 100)

if passed < total:
    print(f"\n⚠️  {total - passed} test(s) failed - review above for details")
    sys.exit(1)
else:
    print("\n✓ All tests passed!")
    sys.exit(0)
