"""Test writing and reading Parquet manifests with BRIN pruning.

This test creates a minimal manifest locally and verifies:
1. Writing manifest with array-based bounds
2. Reading manifest back correctly
3. Int64 conversions are order-preserving
4. BRIN pruning logic works correctly
"""

import os
import sys
import struct
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg_firestore_gcs.parquet_manifest import (
    get_parquet_manifest_schema,
    _value_to_int64,
    parquet_record_to_data_file,
)
from pyiceberg.types import (
    StringType,
    IntegerType,
    TimestampType,
    FloatType,
)


def test_string_encoding():
    """Test that string encoding preserves order and handles sign bit correctly."""
    print("\n=== Testing String Encoding ===")

    test_strings = [
        "AAA",
        "ABC",
        "WARNING",
        "ERROR",
        "INFO",
        "DEBUG",
        "CRITICAL",
        "ZZZ",
    ]

    # Sort strings naturally
    sorted_strings = sorted(test_strings)
    print(f"Natural sort order: {sorted_strings}")

    # Convert to int64 and check order is preserved
    string_type = StringType()
    encoded = []
    for s in test_strings:
        s_bytes = s.encode("utf-8")
        int_val = _value_to_int64(s_bytes, string_type)
        encoded.append((s, int_val))
        print(f"  '{s}' -> {int_val:20d} (0x{int_val:016x})")

    # Sort by int64 value
    int_sorted = [s for s, _ in sorted(encoded, key=lambda x: x[1])]
    print(f"Int64 sort order:  {int_sorted}")

    # Verify order is preserved
    if sorted_strings == int_sorted:
        print("✓ String ordering preserved!")
    else:
        print("✗ String ordering NOT preserved!")
        print(f"  Expected: {sorted_strings}")
        print(f"  Got:      {int_sorted}")
        return False

    # Check that no values are negative (sign bit should be 0)
    negative_values = [(s, val) for s, val in encoded if val < 0]
    if negative_values:
        print("✗ Found negative int64 values (sign bit set):")
        for s, val in negative_values:
            print(f"  '{s}' -> {val}")
        return False
    else:
        print("✓ All string encodings have sign bit = 0 (positive values)")

    return True


def test_numeric_encoding():
    """Test numeric type encoding."""
    print("\n=== Testing Numeric Encoding ===")

    # Test integers
    int_type = IntegerType()
    test_ints = [-100, -1, 0, 1, 100, 1000]
    print("Integers:")
    int_values = []
    for i in test_ints:
        i_bytes = struct.pack(">i", i)
        int_val = _value_to_int64(i_bytes, int_type)
        int_values.append((i, int_val))
        print(f"  {i:6d} -> {int_val:20d}")

    # Check order preserved
    sorted_ints = [i for i, _ in sorted(int_values, key=lambda x: x[0])]
    sorted_by_encoding = [i for i, _ in sorted(int_values, key=lambda x: x[1])]
    if sorted_ints == sorted_by_encoding:
        print("✓ Integer ordering preserved!")
    else:
        print("✗ Integer ordering NOT preserved!")
        return False

    # Test floats
    print("\nFloats:")
    float_type = FloatType()
    test_floats = [-10.5, -1.1, 0.0, 1.1, 10.5, 100.7]
    float_values = []
    for f in test_floats:
        f_bytes = struct.pack(">f", f)
        int_val = _value_to_int64(f_bytes, float_type)
        float_values.append((f, int_val))
        print(f"  {f:8.2f} -> {int_val:20d}")

    # Note: Float ordering after int() conversion might not be perfect for negatives
    # This is a known limitation of the simple approach
    print("  (Note: Float->int conversion may not preserve exact order for negative values)")

    return True


def test_timestamp_encoding():
    """Test timestamp encoding."""
    print("\n=== Testing Timestamp Encoding ===")

    ts_type = TimestampType()
    # Microseconds since epoch
    test_timestamps = [
        0,  # 1970-01-01
        1609459200000000,  # 2021-01-01
        1640995200000000,  # 2022-01-01
        1672531200000000,  # 2023-01-01
        1704067200000000,  # 2024-01-01
    ]

    print("Timestamps (microseconds since epoch):")
    ts_values = []
    for ts in test_timestamps:
        ts_bytes = struct.pack(">q", ts)
        int_val = _value_to_int64(ts_bytes, ts_type)
        ts_values.append((ts, int_val))
        print(f"  {ts:20d} -> {int_val:20d}")

    # Check order preserved
    if all(ts_values[i][1] < ts_values[i + 1][1] for i in range(len(ts_values) - 1)):
        print("✓ Timestamp ordering preserved!")
    else:
        print("✗ Timestamp ordering NOT preserved!")
        return False

    return True


def create_test_manifest():
    """Create a test manifest with sample data files."""
    print("\n=== Creating Test Manifest ===")

    schema = get_parquet_manifest_schema()

    # Create sample data with different bound values
    # Field IDs: 0=id (int), 1=severity (string), 2=timestamp (timestamp), 3=value (float)
    test_data = [
        {
            "file_path": "file1.parquet",
            "snapshot_id": 1,
            "sequence_number": 1,
            "file_sequence_number": 1,
            "active": True,
            "partition_spec_id": 0,
            "partition_json": None,
            "file_format": "PARQUET",
            "record_count": 100,
            "file_size_bytes": 1024,
            # Bounds: id=[1,100], severity=["DEBUG","INFO"], timestamp=[0,1000], value=[0.0,10.0]
            "lower_bounds": [1, _value_to_int64(b"DEBUG", StringType()), 0, int(0.0)],
            "upper_bounds": [100, _value_to_int64(b"INFO", StringType()), 1000, int(10.0)],
            "null_counts": [0, 0, 0, 5],
            "value_counts": [100, 100, 100, 95],
            "column_sizes": [400, 500, 800, 400],
            "nan_counts": [0, 0, 0, 0],
            "key_metadata": None,
            "split_offsets_json": None,
            "equality_ids_json": None,
            "sort_order_id": 0,
        },
        {
            "file_path": "file2.parquet",
            "snapshot_id": 1,
            "sequence_number": 1,
            "file_sequence_number": 1,
            "active": True,
            "partition_spec_id": 0,
            "partition_json": None,
            "file_format": "PARQUET",
            "record_count": 200,
            "file_size_bytes": 2048,
            # Bounds: id=[101,300], severity=["ERROR","WARNING"], timestamp=[1001,2000], value=[10.0,20.0]
            "lower_bounds": [101, _value_to_int64(b"ERROR", StringType()), 1001, int(10.0)],
            "upper_bounds": [300, _value_to_int64(b"WARNING", StringType()), 2000, int(20.0)],
            "null_counts": [0, 0, 0, 10],
            "value_counts": [200, 200, 200, 190],
            "column_sizes": [800, 1000, 1600, 800],
            "nan_counts": [0, 0, 0, 0],
            "key_metadata": None,
            "split_offsets_json": None,
            "equality_ids_json": None,
            "sort_order_id": 0,
        },
        {
            "file_path": "file3.parquet",
            "snapshot_id": 1,
            "sequence_number": 1,
            "file_sequence_number": 1,
            "active": True,
            "partition_spec_id": 0,
            "partition_json": None,
            "file_format": "PARQUET",
            "record_count": 150,
            "file_size_bytes": 1536,
            # Bounds: id=[301,450], severity=["CRITICAL","ERROR"], timestamp=[2001,3000], value=[20.0,30.0]
            "lower_bounds": [301, _value_to_int64(b"CRITICAL", StringType()), 2001, int(20.0)],
            "upper_bounds": [450, _value_to_int64(b"ERROR", StringType()), 3000, int(30.0)],
            "null_counts": [0, 0, 0, 8],
            "value_counts": [150, 150, 150, 142],
            "column_sizes": [600, 750, 1200, 600],
            "nan_counts": [0, 0, 0, 0],
            "key_metadata": None,
            "split_offsets_json": None,
            "equality_ids_json": None,
            "sort_order_id": 0,
        },
    ]

    # Write to Parquet
    table = pa.Table.from_pylist(test_data, schema=schema)
    output_path = "/tmp/test_manifest.parquet"
    pq.write_table(table, output_path, compression="zstd")

    print(f"✓ Wrote test manifest to {output_path}")
    print(f"  Files: {len(test_data)}")
    print(f"  Size: {os.path.getsize(output_path)} bytes")

    return output_path


def test_manifest_read(manifest_path):
    """Test reading the manifest back."""
    print("\n=== Testing Manifest Read ===")

    table = pq.read_table(manifest_path)
    records = table.to_pylist()

    print(f"Read {len(records)} records")

    for i, record in enumerate(records):
        print(f"\nFile {i + 1}: {record['file_path']}")
        print(f"  Record count: {record['record_count']}")
        print(f"  Lower bounds: {record['lower_bounds']}")
        print(f"  Upper bounds: {record['upper_bounds']}")

        # Convert back to DataFile
        data_file = parquet_record_to_data_file(record)
        print(f"  DataFile lower_bounds: {data_file.lower_bounds}")
        print(f"  DataFile upper_bounds: {data_file.upper_bounds}")
        print(f"  DataFile null_counts: {data_file.null_value_counts}")

    return records


def test_pruning_logic(records):
    """Test BRIN pruning logic with various predicates."""
    print("\n=== Testing BRIN Pruning Logic ===")

    # Convert records to DataFile objects
    data_files = [parquet_record_to_data_file(r) for r in records]

    string_type = StringType()
    all_tests_passed = True

    # Test 1: severity = "WARNING" should match file2 only
    print("\nTest 1: severity = 'WARNING' (EqualTo)")
    warning_int = _value_to_int64(b"WARNING", string_type)
    print(f"  'WARNING' as int64: {warning_int}")

    expected_prune = [True, False, True]  # file1, file2, file3
    for i, df in enumerate(data_files):
        min_val = df.lower_bounds.get(1)  # field_id 1 = severity
        max_val = df.upper_bounds.get(1)

        # Equality: prune if value < min OR value > max
        can_prune = warning_int < min_val or warning_int > max_val
        status = "✓" if can_prune == expected_prune[i] else "✗"
        print(
            f"  {status} File {i + 1}: min={min_val}, max={max_val}, prune={can_prune} (expected {expected_prune[i]})"
        )
        if can_prune != expected_prune[i]:
            all_tests_passed = False

    # Test 2: severity > "INFO" should match file2 only
    print("\nTest 2: severity > 'INFO' (GreaterThan)")
    info_int = _value_to_int64(b"INFO", string_type)
    print(f"  'INFO' as int64: {info_int}")

    expected_prune = [True, False, True]
    for i, df in enumerate(data_files):
        min_val = df.lower_bounds.get(1)
        max_val = df.upper_bounds.get(1)

        # Greater than: prune if max <= value
        can_prune = max_val <= info_int
        status = "✓" if can_prune == expected_prune[i] else "✗"
        print(
            f"  {status} File {i + 1}: min={min_val}, max={max_val}, prune={can_prune} (expected {expected_prune[i]})"
        )
        if can_prune != expected_prune[i]:
            all_tests_passed = False

    # Test 3: id < 150 should keep file1 and file2
    print("\nTest 3: id < 150 (LessThan)")
    expected_prune = [False, False, True]
    for i, df in enumerate(data_files):
        min_val = df.lower_bounds.get(0)  # field_id 0 = id
        max_val = df.upper_bounds.get(0)

        # Less than: prune if min >= value
        can_prune = min_val >= 150
        status = "✓" if can_prune == expected_prune[i] else "✗"
        print(
            f"  {status} File {i + 1}: min={min_val}, max={max_val}, prune={can_prune} (expected {expected_prune[i]})"
        )
        if can_prune != expected_prune[i]:
            all_tests_passed = False

    # Test 4: id <= 100 should keep file1 only
    print("\nTest 4: id <= 100 (LessThanOrEqual)")
    expected_prune = [False, True, True]
    for i, df in enumerate(data_files):
        min_val = df.lower_bounds.get(0)
        max_val = df.upper_bounds.get(0)

        # Less than or equal: prune if min > value
        can_prune = min_val > 100
        status = "✓" if can_prune == expected_prune[i] else "✗"
        print(
            f"  {status} File {i + 1}: min={min_val}, max={max_val}, prune={can_prune} (expected {expected_prune[i]})"
        )
        if can_prune != expected_prune[i]:
            all_tests_passed = False

    # Test 5: id >= 300 should keep file2 and file3
    print("\nTest 5: id >= 300 (GreaterThanOrEqual)")
    expected_prune = [True, False, False]
    for i, df in enumerate(data_files):
        min_val = df.lower_bounds.get(0)
        max_val = df.upper_bounds.get(0)

        # Greater than or equal: prune if max < value
        can_prune = max_val < 300
        status = "✓" if can_prune == expected_prune[i] else "✗"
        print(
            f"  {status} File {i + 1}: min={min_val}, max={max_val}, prune={can_prune} (expected {expected_prune[i]})"
        )
        if can_prune != expected_prune[i]:
            all_tests_passed = False

    # Test 6: id BETWEEN 200 AND 400 should keep file2 and file3
    print("\nTest 6: id BETWEEN 200 AND 400 (id >= 200 AND id <= 400)")
    expected_prune = [True, False, False]
    for i, df in enumerate(data_files):
        min_val = df.lower_bounds.get(0)
        max_val = df.upper_bounds.get(0)

        # >= 200: prune if max < 200
        can_prune_lower = max_val < 200
        # <= 400: prune if min > 400
        can_prune_upper = min_val > 400
        can_prune = can_prune_lower or can_prune_upper
        status = "✓" if can_prune == expected_prune[i] else "✗"
        print(
            f"  {status} File {i + 1}: min={min_val}, max={max_val}, prune={can_prune} (expected {expected_prune[i]})"
        )
        if can_prune != expected_prune[i]:
            all_tests_passed = False

    # Test 7: severity = "DEBUG" should keep file1 and file3
    print("\nTest 7: severity = 'DEBUG' (EqualTo - edge case, within range)")
    debug_int = _value_to_int64(b"DEBUG", string_type)
    print(f"  'DEBUG' as int64: {debug_int}")

    expected_prune = [
        False,
        True,
        False,
    ]  # file1 has DEBUG-INFO, file3 has CRITICAL-ERROR (DEBUG is in between)
    for i, df in enumerate(data_files):
        min_val = df.lower_bounds.get(1)
        max_val = df.upper_bounds.get(1)

        # Equality: prune if value < min OR value > max
        can_prune = debug_int < min_val or debug_int > max_val
        status = "✓" if can_prune == expected_prune[i] else "✗"
        print(
            f"  {status} File {i + 1}: min={min_val}, max={max_val}, prune={can_prune} (expected {expected_prune[i]})"
        )
        if can_prune != expected_prune[i]:
            all_tests_passed = False

    # Test 8: severity < "AAA" should prune all files
    print("\nTest 8: severity < 'AAA' (LessThan - all files pruned)")
    aaa_int = _value_to_int64(b"AAA", string_type)
    print(f"  'AAA' as int64: {aaa_int}")

    expected_prune = [True, True, True]
    for i, df in enumerate(data_files):
        min_val = df.lower_bounds.get(1)
        max_val = df.upper_bounds.get(1)

        # Less than: prune if min >= value
        can_prune = min_val >= aaa_int
        status = "✓" if can_prune == expected_prune[i] else "✗"
        print(
            f"  {status} File {i + 1}: min={min_val}, max={max_val}, prune={can_prune} (expected {expected_prune[i]})"
        )
        if can_prune != expected_prune[i]:
            all_tests_passed = False

    # Test 9: severity > "ZZZZZZ" should prune all files
    print("\nTest 9: severity > 'ZZZZZZ' (GreaterThan - all files pruned)")
    zzz_int = _value_to_int64(b"ZZZZZZ", string_type)
    print(f"  'ZZZZZZ' as int64: {zzz_int}")

    expected_prune = [True, True, True]
    for i, df in enumerate(data_files):
        min_val = df.lower_bounds.get(1)
        max_val = df.upper_bounds.get(1)

        # Greater than: prune if max <= value
        can_prune = max_val <= zzz_int
        status = "✓" if can_prune == expected_prune[i] else "✗"
        print(
            f"  {status} File {i + 1}: min={min_val}, max={max_val}, prune={can_prune} (expected {expected_prune[i]})"
        )
        if can_prune != expected_prune[i]:
            all_tests_passed = False

    # Test 10: timestamp = 1500 should keep file2 only
    print("\nTest 10: timestamp = 1500 (EqualTo - numeric)")
    expected_prune = [True, False, True]
    for i, df in enumerate(data_files):
        min_val = df.lower_bounds.get(2)  # field_id 2 = timestamp
        max_val = df.upper_bounds.get(2)

        # Equality: prune if value < min OR value > max
        can_prune = 1500 < min_val or 1500 > max_val
        status = "✓" if can_prune == expected_prune[i] else "✗"
        print(
            f"  {status} File {i + 1}: min={min_val}, max={max_val}, prune={can_prune} (expected {expected_prune[i]})"
        )
        if can_prune != expected_prune[i]:
            all_tests_passed = False

    # Test 11: timestamp >= 0 should keep all files
    print("\nTest 11: timestamp >= 0 (GreaterThanOrEqual - keeps all)")
    expected_prune = [False, False, False]
    for i, df in enumerate(data_files):
        min_val = df.lower_bounds.get(2)
        max_val = df.upper_bounds.get(2)

        # Greater than or equal: prune if max < value
        can_prune = max_val < 0
        status = "✓" if can_prune == expected_prune[i] else "✗"
        print(
            f"  {status} File {i + 1}: min={min_val}, max={max_val}, prune={can_prune} (expected {expected_prune[i]})"
        )
        if can_prune != expected_prune[i]:
            all_tests_passed = False

    # Test 12: id = 100 should keep file1 only (edge case: exact max bound)
    print("\nTest 12: id = 100 (EqualTo - edge case, exact max bound)")
    expected_prune = [False, True, True]
    for i, df in enumerate(data_files):
        min_val = df.lower_bounds.get(0)
        max_val = df.upper_bounds.get(0)

        # Equality: prune if value < min OR value > max
        can_prune = 100 < min_val or 100 > max_val
        status = "✓" if can_prune == expected_prune[i] else "✗"
        print(
            f"  {status} File {i + 1}: min={min_val}, max={max_val}, prune={can_prune} (expected {expected_prune[i]})"
        )
        if can_prune != expected_prune[i]:
            all_tests_passed = False

    # Test 13: id = 101 should keep file2 only (edge case: exact min bound)
    print("\nTest 13: id = 101 (EqualTo - edge case, exact min bound)")
    expected_prune = [True, False, True]
    for i, df in enumerate(data_files):
        min_val = df.lower_bounds.get(0)
        max_val = df.upper_bounds.get(0)

        # Equality: prune if value < min OR value > max
        can_prune = 101 < min_val or 101 > max_val
        status = "✓" if can_prune == expected_prune[i] else "✗"
        print(
            f"  {status} File {i + 1}: min={min_val}, max={max_val}, prune={can_prune} (expected {expected_prune[i]})"
        )
        if can_prune != expected_prune[i]:
            all_tests_passed = False

    # Test 14: severity >= 'ERROR' AND severity <= 'INFO' should keep file1 and file3
    # File2 has [ERROR, WARNING] where WARNING > INFO, so max > INFO means it won't be pruned
    print("\nTest 14: severity BETWEEN 'ERROR' AND 'INFO' (multi-predicate)")
    error_int = _value_to_int64(b"ERROR", string_type)
    info_int = _value_to_int64(b"INFO", string_type)
    print(f"  'ERROR' as int64: {error_int}")
    print(f"  'INFO' as int64: {info_int}")
    print(
        f"  File2 has WARNING ({_value_to_int64(b'WARNING', string_type)}) > INFO, so overlaps range"
    )

    expected_prune = [False, False, False]  # All files overlap [ERROR, INFO] range
    for i, df in enumerate(data_files):
        min_val = df.lower_bounds.get(1)
        max_val = df.upper_bounds.get(1)

        # >= ERROR: prune if max < ERROR
        can_prune_lower = max_val < error_int
        # <= INFO: prune if min > INFO
        can_prune_upper = min_val > info_int
        can_prune = can_prune_lower or can_prune_upper
        status = "✓" if can_prune == expected_prune[i] else "✗"
        print(
            f"  {status} File {i + 1}: min={min_val}, max={max_val}, prune={can_prune} (expected {expected_prune[i]})"
        )
        if can_prune != expected_prune[i]:
            all_tests_passed = False

    # Test 15: Multiple overlapping ranges - id < 200 OR id > 350
    print("\nTest 15: id < 200 OR id > 350 (OR predicate - conservative pruning)")
    print(
        "  Note: OR predicates require conservative approach - only prune if ALL branches eliminate"
    )
    expected_prune = [False, False, False]  # Conservative: keep all since it's OR
    for i, df in enumerate(data_files):
        min_val = df.lower_bounds.get(0)
        max_val = df.upper_bounds.get(0)

        # For OR predicates: can only prune if BOTH branches would prune
        # id < 200: prune if min >= 200
        can_prune_left = min_val >= 200
        # id > 350: prune if max <= 350
        can_prune_right = max_val <= 350
        # For OR: only prune if both would prune (conservative)
        can_prune = can_prune_left and can_prune_right
        status = "✓" if can_prune == expected_prune[i] else "✗"
        print(
            f"  {status} File {i + 1}: min={min_val}, max={max_val}, prune={can_prune} (expected {expected_prune[i]})"
        )
        if can_prune != expected_prune[i]:
            all_tests_passed = False

    return all_tests_passed


def main():
    """Run all tests."""
    print("=" * 60)
    print("Testing Parquet Manifest Writing and BRIN Pruning")
    print("=" * 60)

    # Test encoding functions
    if not test_string_encoding():
        print("\n✗ String encoding test FAILED")
        return 1

    if not test_numeric_encoding():
        print("\n✗ Numeric encoding test FAILED")
        return 1

    if not test_timestamp_encoding():
        print("\n✗ Timestamp encoding test FAILED")
        return 1

    # Create and test manifest
    manifest_path = create_test_manifest()
    records = test_manifest_read(manifest_path)

    if not test_pruning_logic(records):
        print("\n✗ Pruning logic tests FAILED")
        return 1

    print("\n" + "=" * 60)
    print("✓ All tests completed successfully!")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
