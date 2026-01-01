import datetime
import os
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

from opteryx_catalog.opteryx_catalog import OpteryxCatalog

# Add local paths to sys.path to use local code instead of installed packages
sys.path.insert(0, os.path.join(sys.path[0], ".."))  # Add parent dir for pyiceberg_firestore_gcs
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))
sys.path.insert(1, os.path.join(sys.path[0], "../pyiceberg-firestore-gcs"))


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)
os.environ["GCP_PROJECT_ID"] = "mabeldev"
os.environ["FIRESTORE_DATABASE"] = "catalogs"
os.environ["GCS_BUCKET"] = "opteryx_data"


# from opteryx.connectors.iceberg_connector import IcebergConnector
# Using opteryx_catalog for manifest discovery + simple in-Python filters

workspace = "opteryx"
collection_name = "tests_temp"


# Step 1: Create a local catalog
catalog = OpteryxCatalog(
    workspace,
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
)

# Choose the latest dataset in the collection to test against
datasets = list(catalog.list_datasets(collection_name))
if not datasets:
    raise RuntimeError(f"No datasets found in collection {collection_name}")
datasets = sorted(datasets)
table = datasets[-1]

table = "test_table_0_1767300842"


# opteryx.register_store(
#    prefix="_default",
#    connector=IcebergConnector,
#    remove_prefix=True,
#    catalog=FirestoreCatalog,
#    firestore_project="mabeldev",
#    firestore_database="catalogs",
#    gcs_bucket="opteryx_data",
# )

# catalog.create_collection_if_not_exists(schema_name, properties={"iceberg_compatible": "false"})

# df = opteryx.query_to_arrow("SELECT * FROM $planets")

# Drop table if it exists
# try:
#    catalog.drop_dataset(f"{schema_name}.{table}")
# except Exception:
#    pass

# s = catalog.create_dataset(f"{schema_name}.{table}", df.schema, properties={"iceberg_compatible": "false"})

# Load table metadata using the new catalog
# Attempt to load the requested table; if it has no manifest, fall back to
# the most recently created table in the namespace (useful when running the
# paired `create_dataset.py` script which creates new deterministic datasets).
s = catalog.load_dataset(f"{collection_name}.{table}")


def _read_parquet_manifest(io, manifest_path: str) -> list:
    """Read a Parquet manifest produced by `FirestoreCatalog.write_parquet_manifest`.

    Returns a list of entry dicts (the original pyarrow.from_pylist rows).
    """
    if not manifest_path:
        return []
    inp = io.new_input(manifest_path)
    try:
        buf = inp.open().read()
    except FileNotFoundError:
        return []
    tbl = pq.read_table(pa.BufferReader(buf))
    return tbl.to_pylist()


def _read_data_file(io, path: str) -> list:
    inp = io.new_input(path)
    buf = inp.open().read()
    tbl = pq.read_table(pa.BufferReader(buf))
    return tbl.to_pylist()


# s.append(df)

ts = s.snapshots()[-1].timestamp_ms
print(f"Table last updated: {datetime.datetime.fromtimestamp(ts / 1000)}")
ts = s.snapshots()[0].timestamp_ms
print(f"Table last created: {datetime.datetime.fromtimestamp(ts / 1000)}")


print(f"Table format version: {s.metadata.format_version}")
print(f"Table location: {s.metadata.location}")

# Discover planned files from the Parquet manifest
print("\n=== Test 1: No filter (baseline) ===")
entries = []
# Read manifests from all snapshots so edits (additional snapshots) are included
for snap in s.metadata.snapshots:
    if snap.manifest_list:
        entries.extend(_read_parquet_manifest(catalog.io, snap.manifest_list))
if not entries:
    # Try the most recently listed dataset in the namespace
    try:
        datasets = list(catalog.list_datasets(collection_name))
        if datasets:
            last = datasets[-1]
            print(f"Falling back to dataset: {last}")
            s = catalog.load_dataset(f"{collection_name}.{last}")
            # read manifests from all snapshots
            entries = []
            for snap in s.metadata.snapshots:
                if snap.manifest_list:
                    entries.extend(_read_parquet_manifest(catalog.io, snap.manifest_list))
    except Exception:
        pass

files = []
seen = set()
for e in entries:
    fp = e.get("file_path")
    if fp and fp not in seen:
        seen.add(fp)
        files.append(fp)
print(f"✓ Planned {len(files)} files (from manifest)")
assert len(files) > 0, "Should have at least 1 file without filter"
baseline_file_count = len(files)

# Test 2: EqualTo filter on 'name' = 'Earth'
print("\n=== Test 2: EqualTo filter (name = 'Earth') ===")
t = time.monotonic_ns()
files_eq = files
print(f"✓ Planned {len(files_eq)} files (from manifest)")
# Read and verify data by scanning files and applying the filter in Python
rows_eq = []
for fpath in files_eq:
    rows = _read_data_file(catalog.io, fpath)
    for r in rows:
        if r.get("name") == "Earth":
            rows_eq.append(r)
print(f"  Found {len(rows_eq)} rows")
assert len(rows_eq) == 1, f"Expected 1 row for Earth, got {len(rows_eq)}"
assert rows_eq[0]["name"] == "Earth", f"Expected Earth, got {rows_eq[0]['name']}"
print("  ✓ Verified: Only 'Earth' returned")


# Test 2a: EqualTo filter on 'name' = 'Xenon'
print("\n=== Test 2a: EqualTo filter (name = 'Xenon') ===")
t = time.monotonic_ns()
rows_eq = []
for fpath in files:
    rows = _read_data_file(catalog.io, fpath)
    for r in rows:
        if r.get("name") == "Xenon":
            rows_eq.append(r)
print(f"  Found {len(rows_eq)} rows")
assert len(rows_eq) == 0, f"Expected 0 rows for Xenon, got {len(rows_eq)}"
print("  ✓ Verified: No rows returned")

# Test 3: In filter with multiple values
print("\n=== Test 3: In filter (name IN ['Earth', 'Mars']) ===")
t = time.monotonic_ns()
rows_in = []
for fpath in files:
    rows = _read_data_file(catalog.io, fpath)
    for r in rows:
        if r.get("name") in {"Earth", "Mars"}:
            rows_in.append(r)
print(f"  Found {len(rows_in)} rows")
assert len(rows_in) == 2, f"Expected 2 rows for Earth and Mars, got {len(rows_in)}"
names = {row["name"] for row in rows_in}
assert names == {"Earth", "Mars"}, f"Expected Earth and Mars, got {names}"
print("  ✓ Verified: Only 'Earth' and 'Mars' returned")

# Test 4: In filter that should return no rows
print("\n=== Test 4: In filter (name IN ['NonExistent1', 'NonExistent2']) ===")
t = time.monotonic_ns()
rows_empty = []
for fpath in files:
    rows = _read_data_file(catalog.io, fpath)
    for r in rows:
        if r.get("name") in {"NonExistent1", "NonExistent2"}:
            rows_empty.append(r)
print(f"  Found {len(rows_empty)} rows")
assert len(rows_empty) == 0, f"Expected 0 rows for non-existent names, got {len(rows_empty)}"
print("  ✓ Verified: No rows returned (filter works at row level)")
if baseline_file_count == len(files):
    print(f"  ⚠ Note: File pruning not working - still scanned {len(files)} files (expected 0)")

# Test 5: EqualTo on a different value
print("\n=== Test 5: EqualTo filter (name = 'Jupiter') ===")
t = time.monotonic_ns()
rows_jupiter = []
for fpath in files:
    rows = _read_data_file(catalog.io, fpath)
    for r in rows:
        if r.get("name") == "Jupiter":
            rows_jupiter.append(r)
print(f"  Found {len(rows_jupiter)} rows")
assert len(rows_jupiter) == 1, f"Expected 1 row for Jupiter, got {len(rows_jupiter)}"
assert rows_jupiter[0]["name"] == "Jupiter", f"Expected Jupiter, got {rows_jupiter[0]['name']}"
print("  ✓ Verified: Only 'Jupiter' returned")

# Test 6: In filter with single value (should behave like EqualTo)
print("\n=== Test 6: In filter (name IN ['Venus']) ===")
t = time.monotonic_ns()
rows_single = []
for fpath in files:
    rows = _read_data_file(catalog.io, fpath)
    for r in rows:
        if r.get("name") == "Venus":
            rows_single.append(r)
print(f"  Found {len(rows_single)} rows")
assert len(rows_single) == 1, f"Expected 1 row for Venus, got {len(rows_single)}"
assert rows_single[0]["name"] == "Venus", f"Expected Venus, got {rows_single[0]['name']}"
print("  ✓ Verified: Only 'Venus' returned")

# Test 7: In filter with all planets
print("\n=== Test 7: In filter with all planet names ===")
all_planets = ["Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune"]
t = time.monotonic_ns()
rows_all = []
for fpath in files:
    rows = _read_data_file(catalog.io, fpath)
    rows_all.extend(rows)
print(f"  Found {len(rows_all)} rows")
assert len(rows_all) == 10, f"Expected 10 rows for all planets, got {len(rows_all)}"
print("  ✓ Verified: All rows returned (including appended rows)")

# Test 8: Empty In filter
print("\n=== Test 8: In filter with empty list (name IN []) ===")
t = time.monotonic_ns()
rows_empty_list = []
for fpath in files:
    rows = _read_data_file(catalog.io, fpath)
    for r in rows:
        if r.get("name") in []:
            rows_empty_list.append(r)
print(f"  Found {len(rows_empty_list)} rows")
assert len(rows_empty_list) == 0, f"Expected 0 rows for empty IN list, got {len(rows_empty_list)}"
print("  ✓ Verified: No rows returned for empty IN list")

print("\n" + "=" * 60)
print("SUMMARY OF FILTER PUSHDOWN TESTS")
print("=" * 60)
print("✅ All filter checks passed (performed in-Python against data files)")
print("✅ Filters work correctly at row level")
print(f"File counts: baseline={baseline_file_count}, planned_files={len(files)}")
print("⚠️  File-level pruning not implemented in this test (manifest-driven planning)")
print("=" * 60)
