"""Test BRIN-style pruning with predicates."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)

# Enable logging
import logging

logging.basicConfig(level=logging.INFO)

from pyiceberg_firestore_gcs import FirestoreCatalog

catalog = FirestoreCatalog(
    "opteryx",
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
)

# Load a table
table = catalog.load_table("ops.request_log")
print(f"\nTable: {table.name()}")
print(f"Table type: {type(table).__name__}")

# Print schema to see available fields
print("\nTable Schema:")
for field in table.schema().fields:
    print(f"  - {field.name} (id={field.field_id}, type={field.field_type})")

# Test 1: Scan without filter (baseline)
print("\n" + "=" * 80)
print("TEST 1: Scan without predicates (baseline)")
print("=" * 80)
scan = table.scan()
files = list(scan.plan_files())
print(f"Total files without filter: {len(files)}")

# Check if files have bounds
if files:
    sample_file = files[0].file
    print("\nSample file bounds check:")
    print(f"  Has lower_bounds: {sample_file.lower_bounds is not None}")
    print(f"  Has upper_bounds: {sample_file.upper_bounds is not None}")

    # Check severity bounds (field 6)
    if sample_file.lower_bounds and 6 in sample_file.lower_bounds:
        print("  Severity (field 6) bounds present: Yes")
        try:
            lower_bytes = sample_file.lower_bounds[6]
            upper_bytes = sample_file.upper_bounds[6]
            lower_severity = lower_bytes.decode("utf-8")
            upper_severity = upper_bytes.decode("utf-8")
            print(f"    Lower: '{lower_severity}'")
            print(f"    Upper: '{upper_severity}'")
        except Exception as e:
            print(f"    Could not decode: {e}")
    else:
        print("  Severity (field 6) bounds: NOT PRESENT")

    if sample_file.lower_bounds and 8 in sample_file.lower_bounds:
        print("  Timestamp (field 8) lower bound present: Yes")
        # Try to decode
        try:
            import struct
            import datetime

            lower_bytes = sample_file.lower_bounds[8]
            upper_bytes = sample_file.upper_bounds[8]
            lower_ts = struct.unpack(">q", lower_bytes)[0]
            upper_ts = struct.unpack(">q", upper_bytes)[0]
            print(f"  Lower timestamp (raw): {lower_ts}")
            print(f"  Upper timestamp (raw): {upper_ts}")
            # Try interpreting as microseconds since epoch
            print(
                f"  Lower as datetime (µs): {datetime.datetime.fromtimestamp(lower_ts / 1000000, datetime.timezone.utc)}"
            )
            print(
                f"  Upper as datetime (µs): {datetime.datetime.fromtimestamp(upper_ts / 1000000, datetime.timezone.utc)}"
            )
        except Exception as e:
            print(f"  Could not decode: {e}")
    else:
        print("  Timestamp (field 8) bounds: NOT PRESENT")

# Test 2: Scan with a simple predicate that should prune files
print("\n" + "=" * 80)
print("TEST 2: Scan with equality predicate (severity = 'WARNING')")
print("=" * 80)

# Test with severity field which should have better bounds for pruning
try:
    from pyiceberg.expressions import EqualTo

    # Filter for WARNING severity - should eliminate files with different severities
    scan_with_filter = table.scan(row_filter=EqualTo("severity", "WARNING"))
    filtered_files = list(scan_with_filter.plan_files())
    print(f"Files after filter: {len(filtered_files)}")

    if len(filtered_files) < len(files):
        pruned = len(files) - len(filtered_files)
        pruned_pct = (pruned / len(files)) * 100
        print(f"✓ BRIN pruning eliminated {pruned} files ({pruned_pct:.1f}%)!")
    else:
        print("Note: No files were pruned - possible reasons:")
        print("  - All files might contain mixed severities (overlapping ranges)")
        print("  - EqualTo predicate might not be implemented yet")
        print("  - Bounds might show all files could contain WARNING")

except Exception as e:
    print(f"Test skipped due to: {e}")
    print("Adjust the filter expression to match your table schema")

print("\n" + "=" * 80)
print("BRIN pruning test complete!")
print("=" * 80)
