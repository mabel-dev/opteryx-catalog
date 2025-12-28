import os
import sys
import time
import datetime

# Add local paths to sys.path to use local code instead of installed packages
sys.path.insert(0, os.path.join(sys.path[0], ".."))  # Add parent dir for pyiceberg_firestore_gcs
# sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)
os.environ["GCP_PROJECT_ID"] = "mabeldev"
os.environ["FIRESTORE_DATABASE"] = "catalogs"
os.environ["GCS_BUCKET"] = "opteryx_data"

from pyiceberg_firestore_gcs import FirestoreCatalog

# from opteryx.connectors.iceberg_connector import IcebergConnector
from pyiceberg.expressions import EqualTo, In

workspace = "public"
schema_name = "examples"
table = "vulnerabilities"

# Step 1: Create a local Iceberg catalog
catalog = FirestoreCatalog(
    workspace,
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
)


# opteryx.register_store(
#    prefix="_default",
#    connector=IcebergConnector,
#    remove_prefix=True,
#    catalog=FirestoreCatalog,
#    firestore_project="mabeldev",
#    firestore_database="catalogs",
#    gcs_bucket="opteryx_data",
# )

# catalog.create_namespace_if_not_exists(schema_name, properties={"iceberg_compatible": "false"})

# df = opteryx.query_to_arrow("SELECT * FROM $planets")

# Drop table if it exists
# try:
#    catalog.drop_table(f"{schema_name}.{table}")
# except Exception:
#    pass

# s = catalog.create_table(f"{schema_name}.{table}", df.schema, properties={"iceberg_compatible": "false"})

s = catalog.load_table(f"{schema_name}.{table}")
# s.append(df)

ts = s.snapshots()[-1].timestamp_ms
print(f"Table last updated: {datetime.datetime.fromtimestamp(ts / 1000)}")
ts = s.snapshots()[0].timestamp_ms
print(f"Table last created: {datetime.datetime.fromtimestamp(ts / 1000)}")

print(dir(s.current_snapshot()))
print(s.current_snapshot().timestamp_ms)

quit()

print(f"Table format version: {s.metadata.format_version}")
print(f"Table location: {s.metadata.location}")

# Test 1: No filter (baseline)
print("\n=== Test 1: No filter (baseline) ===")
t = time.monotonic_ns()
scan = s.scan()
files = list(scan.plan_files())
print(f"✓ Planned {len(files)} files, took {(time.monotonic_ns() - t) / 1e6:.2f} ms")
assert len(files) > 0, "Should have at least 1 file without filter"
baseline_file_count = len(files)

# Test 2: EqualTo filter on 'name' = 'Earth'
print("\n=== Test 2: EqualTo filter (name = 'Earth') ===")
t = time.monotonic_ns()
scan_eq = s.scan(row_filter=EqualTo("name", "Earth"))
files_eq = list(scan_eq.plan_files())
print(f"✓ Planned {len(files_eq)} files, took {(time.monotonic_ns() - t) / 1e6:.2f} ms")
# Read and verify data
reader_eq = scan_eq.to_arrow_batch_reader()
rows_eq = []
for batch in reader_eq:
    rows_eq.extend(batch.to_pylist())
print(f"  Found {len(rows_eq)} rows")
assert len(rows_eq) == 1, f"Expected 1 row for Earth, got {len(rows_eq)}"
assert rows_eq[0]["name"] == "Earth", f"Expected Earth, got {rows_eq[0]['name']}"
print("  ✓ Verified: Only 'Earth' returned")


# Test 2: EqualTo filter on 'name' = 'Earth'
print("\n=== Test 2a: EqualTo filter (name = 'Xenon') ===")
t = time.monotonic_ns()
scan_eq = s.scan(row_filter=EqualTo("name", "Xenon"))
files_eq = list(scan_eq.plan_files())
print(f"✓ Planned {len(files_eq)} files, took {(time.monotonic_ns() - t) / 1e6:.2f} ms")
# Read and verify data
reader_eq = scan_eq.to_arrow_batch_reader()
rows_eq = []
for batch in reader_eq:
    rows_eq.extend(batch.to_pylist())
print(f"  Found {len(rows_eq)} rows")
assert len(rows_eq) == 0, f"Expected 0 rows for Xenon, got {len(rows_eq)}"
print("  ✓ Verified: No rows returned")

# Test 3: In filter with multiple values
print("\n=== Test 3: In filter (name IN ['Earth', 'Mars']) ===")
t = time.monotonic_ns()
scan_in = s.scan(row_filter=In("name", ["Earth", "Mars"]))
files_in = list(scan_in.plan_files())
print(f"✓ Planned {len(files_in)} files, took {(time.monotonic_ns() - t) / 1e6:.2f} ms")
# Read and verify data
reader_in = scan_in.to_arrow_batch_reader()
rows_in = []
for batch in reader_in:
    rows_in.extend(batch.to_pylist())
print(f"  Found {len(rows_in)} rows")
assert len(rows_in) == 2, f"Expected 2 rows for Earth and Mars, got {len(rows_in)}"
names = {row["name"] for row in rows_in}
assert names == {"Earth", "Mars"}, f"Expected Earth and Mars, got {names}"
print("  ✓ Verified: Only 'Earth' and 'Mars' returned")

# Test 4: In filter that should return no rows
print("\n=== Test 4: In filter (name IN ['NonExistent1', 'NonExistent2']) ===")
t = time.monotonic_ns()
scan_in_empty = s.scan(row_filter=In("name", ["NonExistent1", "NonExistent2"]))
files_in_empty = list(scan_in_empty.plan_files())
print(f"✓ Planned {len(files_in_empty)} files, took {(time.monotonic_ns() - t) / 1e6:.2f} ms")
# Read and verify data
reader_empty = scan_in_empty.to_arrow_batch_reader()
rows_empty = []
for batch in reader_empty:
    rows_empty.extend(batch.to_pylist())
print(f"  Found {len(rows_empty)} rows")
assert len(rows_empty) == 0, f"Expected 0 rows for non-existent names, got {len(rows_empty)}"
print("  ✓ Verified: No rows returned (filter works at row level)")
if len(files_in_empty) == baseline_file_count:
    print(
        f"  ⚠ Note: File pruning not working - still scanned {len(files_in_empty)} files (expected 0)"
    )

# Test 5: EqualTo on a different value
print("\n=== Test 5: EqualTo filter (name = 'Jupiter') ===")
t = time.monotonic_ns()
scan_jupiter = s.scan(row_filter=EqualTo("name", "Jupiter"))
files_jupiter = list(scan_jupiter.plan_files())
print(f"✓ Planned {len(files_jupiter)} files, took {(time.monotonic_ns() - t) / 1e6:.2f} ms")
reader_jupiter = scan_jupiter.to_arrow_batch_reader()
rows_jupiter = []
for batch in reader_jupiter:
    rows_jupiter.extend(batch.to_pylist())
print(f"  Found {len(rows_jupiter)} rows")
assert len(rows_jupiter) == 1, f"Expected 1 row for Jupiter, got {len(rows_jupiter)}"
assert rows_jupiter[0]["name"] == "Jupiter", f"Expected Jupiter, got {rows_jupiter[0]['name']}"
print("  ✓ Verified: Only 'Jupiter' returned")

# Test 6: In filter with single value (should behave like EqualTo)
print("\n=== Test 6: In filter (name IN ['Venus']) ===")
t = time.monotonic_ns()
scan_single = s.scan(row_filter=In("name", ["Venus"]))
files_single = list(scan_single.plan_files())
print(f"✓ Planned {len(files_single)} files, took {(time.monotonic_ns() - t) / 1e6:.2f} ms")
reader_single = scan_single.to_arrow_batch_reader()
rows_single = []
for batch in reader_single:
    rows_single.extend(batch.to_pylist())
print(f"  Found {len(rows_single)} rows")
assert len(rows_single) == 1, f"Expected 1 row for Venus, got {len(rows_single)}"
assert rows_single[0]["name"] == "Venus", f"Expected Venus, got {rows_single[0]['name']}"
print("  ✓ Verified: Only 'Venus' returned")

# Test 7: In filter with all planets
print("\n=== Test 7: In filter with all planet names ===")
all_planets = ["Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune"]
t = time.monotonic_ns()
scan_all = s.scan(row_filter=In("name", all_planets))
files_all = list(scan_all.plan_files())
print(f"✓ Planned {len(files_all)} files, took {(time.monotonic_ns() - t) / 1e6:.2f} ms")
reader_all = scan_all.to_arrow_batch_reader()
rows_all = []
for batch in reader_all:
    rows_all.extend(batch.to_pylist())
print(f"  Found {len(rows_all)} rows")
assert len(rows_all) == 8, f"Expected 8 rows for all planets, got {len(rows_all)}"
print("  ✓ Verified: All 8 planets returned")

# Test 8: Empty In filter
print("\n=== Test 8: In filter with empty list (name IN []) ===")
t = time.monotonic_ns()
scan_empty_list = s.scan(row_filter=In("name", []))
files_empty_list = list(scan_empty_list.plan_files())
print(f"✓ Planned {len(files_empty_list)} files, took {(time.monotonic_ns() - t) / 1e6:.2f} ms")
reader_empty_list = scan_empty_list.to_arrow_batch_reader()
rows_empty_list = []
for batch in reader_empty_list:
    rows_empty_list.extend(batch.to_pylist())
print(f"  Found {len(rows_empty_list)} rows")
assert len(rows_empty_list) == 0, f"Expected 0 rows for empty IN list, got {len(rows_empty_list)}"
print("  ✓ Verified: No rows returned for empty IN list")

print("\n" + "=" * 60)
print("SUMMARY OF FILTER PUSHDOWN TESTS")
print("=" * 60)
print("✅ All filter tests passed!")
print("✅ Filters work correctly at row level")
print(f"File counts: baseline={baseline_file_count}, with_filters={len(files_eq)}")
if len(files_in_empty) == baseline_file_count:
    print("⚠️  File-level pruning not working (scans all files regardless of filter)")
    print("    This is OK - filters are applied during reading, not during file planning")
else:
    print("✅ File-level pruning working (excludes files based on statistics)")
print("=" * 60)
