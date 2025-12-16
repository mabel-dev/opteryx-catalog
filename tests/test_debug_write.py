"""Quick test to check if None arrays cause issues with PyArrow."""

import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# Test if None values work with list fields
schema = pa.schema(
    [
        ("file_path", pa.string()),
        ("lower_bounds", pa.list_(pa.int64())),
        ("upper_bounds", pa.list_(pa.int64())),
    ]
)

# Test 1: All None
data1 = [
    {
        "file_path": "test1.parquet",
        "lower_bounds": None,
        "upper_bounds": None,
    }
]

print("Test 1: All None values")
try:
    table1 = pa.Table.from_pylist(data1, schema=schema)
    print(f"✓ Created table: {table1}")
    buffer = BytesIO()
    pq.write_table(table1, buffer)
    print(f"✓ Wrote to parquet, {len(buffer.getvalue())} bytes")
except Exception as e:
    print(f"✗ Failed: {e}")

# Test 2: Empty arrays
data2 = [
    {
        "file_path": "test2.parquet",
        "lower_bounds": [],
        "upper_bounds": [],
    }
]

print("\nTest 2: Empty arrays")
try:
    table2 = pa.Table.from_pylist(data2, schema=schema)
    print(f"✓ Created table: {table2}")
    buffer = BytesIO()
    pq.write_table(table2, buffer)
    print(f"✓ Wrote to parquet, {len(buffer.getvalue())} bytes")
except Exception as e:
    print(f"✗ Failed: {e}")

# Test 3: Arrays with None elements
data3 = [
    {
        "file_path": "test3.parquet",
        "lower_bounds": [None, 100, None, 200],
        "upper_bounds": [None, 150, None, 250],
    }
]

print("\nTest 3: Arrays with None elements")
try:
    table3 = pa.Table.from_pylist(data3, schema=schema)
    print(f"✓ Created table: {table3}")
    print(f"  lower_bounds: {table3['lower_bounds'].to_pylist()}")
    buffer = BytesIO()
    pq.write_table(table3, buffer)
    print(f"✓ Wrote to parquet, {len(buffer.getvalue())} bytes")
except Exception as e:
    print(f"✗ Failed: {e}")
