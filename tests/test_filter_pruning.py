import os
import sys

# Add local paths to sys.path to use local code instead of installed packages
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
    NotEqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
)

workspace = "public"
schema_name = "space"
table = "planets"

# Create catalog
catalog = FirestoreCatalog(
    workspace,
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
    # iceberg_compatible=False,
)

# Load table
s = catalog.load_table(f"{schema_name}.{table}")

print("=" * 80)
print("FILE PRUNING TEST FOR SIMPLE COMPARISON OPERATORS")
print("=" * 80)

# Get baseline
scan = s.scan()
baseline_files = list(scan.plan_files())
baseline_count = len(baseline_files)
print(f"\n📊 Baseline: {baseline_count} file(s) without filter")

# Test cases: (description, filter, expected_rows, should_prune)
test_cases = [
    # STRING TESTS
    ("String EqualTo existing value", EqualTo("name", "Earth"), 1, True),
    ("String EqualTo non-existent", EqualTo("name", "Pluto"), 0, True),
    ("String NotEqualTo", NotEqualTo("name", "Earth"), 8, False),  # Should scan all
    # INTEGER TESTS
    ("Int EqualTo existing value", EqualTo("id", 3), 1, True),  # Earth
    ("Int EqualTo non-existent", EqualTo("id", 999), 0, True),
    ("Int GreaterThan", GreaterThan("id", 7), 1, True),  # Neptune (id=8)
    ("Int GreaterThanOrEqual", GreaterThanOrEqual("id", 8), 1, True),  # Neptune
    ("Int LessThan", LessThan("id", 2), 1, True),  # Mercury (id=1)
    ("Int LessThanOrEqual", LessThanOrEqual("id", 1), 1, True),  # Mercury
    ("Int GreaterThan (no match)", GreaterThan("id", 100), 0, True),
    ("Int LessThan (no match)", LessThan("id", 0), 0, True),
    # FLOAT TESTS
    ("Float EqualTo", EqualTo("mass", 0.815), 1, True),  # Venus
    ("Float GreaterThan", GreaterThan("mass", 300.0), 2, False),  # Jupiter, Saturn
    ("Float GreaterThanOrEqual", GreaterThanOrEqual("mass", 95.159), 2, False),  # Saturn, Jupiter
    ("Float LessThan", LessThan("mass", 0.1), 1, True),  # Mercury (0.055)
    ("Float LessThanOrEqual", LessThanOrEqual("mass", 0.055), 1, True),  # Mercury
    ("Float GreaterThan (no match)", GreaterThan("mass", 1000.0), 0, True),
]

results = {
    "string_prune": 0,
    "string_no_prune": 0,
    "int_prune": 0,
    "int_no_prune": 0,
    "float_prune": 0,
    "float_no_prune": 0,
}

print("\n" + "=" * 80)
for description, filter_expr, expected_rows, should_prune in test_cases:
    scan = s.scan(row_filter=filter_expr)
    files = list(scan.plan_files())
    file_count = len(files)

    # Read actual rows
    reader = scan.to_arrow_batch_reader()
    rows = []
    for batch in reader:
        rows.extend(batch.to_pylist())

    # Determine if pruning occurred
    pruned = file_count < baseline_count

    # Check correctness
    rows_match = len(rows) == expected_rows
    pruning_match = pruned == should_prune

    status = "✅" if (rows_match and pruning_match) else "❌"

    # Track results by type
    filter_type = description.split()[0].lower()
    if pruned and should_prune:
        results[f"{filter_type}_prune"] += 1
    elif not pruned and not should_prune:
        results[f"{filter_type}_no_prune"] += 1

    print(f"\n{status} {description}")
    print(f"   Filter: {filter_expr}")
    print(
        f"   Files: {file_count}/{baseline_count} (pruned={pruned}, expected_prune={should_prune})"
    )
    print(f"   Rows: {len(rows)} (expected={expected_rows}) {'✓' if rows_match else '✗'}")

    if not rows_match:
        print("   ⚠️  Row count mismatch!")
    if pruned != should_prune:
        if should_prune and not pruned:
            print("   ⚠️  Expected pruning but scanned all files")
        else:
            print("   ⚠️  Unexpected pruning behavior")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"String filters with pruning: {results['string_prune']}")
print(f"String filters without pruning: {results['string_no_prune']}")
print(f"Int filters with pruning: {results['int_prune']}")
print(f"Int filters without pruning: {results['int_no_prune']}")
print(f"Float filters with pruning: {results['float_prune']}")
print(f"Float filters without pruning: {results['float_no_prune']}")

# Overall assessment
total_pruning_tests = results["string_prune"] + results["int_prune"] + results["float_prune"]
print(f"\n🎯 Total successful pruning tests: {total_pruning_tests}")

if total_pruning_tests > 0:
    print("✅ File pruning IS working for simple comparisons!")
else:
    print("❌ File pruning NOT working")

print("=" * 80)
