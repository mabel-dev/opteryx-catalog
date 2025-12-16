"""Test what PyArrow actually stores in Parquet."""

import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# Create a test with the timestamp value
test_value = 1765836905000000  # The correct Big Endian interpretation

# Store in PyArrow list
schema = pa.schema([("test_bounds", pa.list_(pa.int64()))])

data = [{"test_bounds": [None, test_value, None, test_value * 2]}]

table = pa.Table.from_pylist(data, schema=schema)
print(f"Created table with value: {test_value}")
print(f"Table:\n{table}")

# Write to Parquet
buffer = BytesIO()
pq.write_table(table, buffer)
buffer.seek(0)

# Read back
table2 = pq.read_table(buffer)
print(f"\nRead back table:\n{table2}")

# Get the actual values
values = table2["test_bounds"].to_pylist()[0]
print(f"\nValues as Python list: {values}")
print(f"First non-None value: {values[1]}")
print(f"Matches original: {values[1] == test_value}")

# Check if it's the wrong endian interpretation
wrong_endian = 4624199495910295040
print(f"\nWrong (Little Endian) value: {wrong_endian}")
print(f"Stored value matches wrong: {values[1] == wrong_endian}")

# Show bytes
if values[1] == test_value:
    print("\n✓ PyArrow correctly preserves the integer value")
    print("  The reader should get the correct value from PyArrow")
    print("  If it's seeing the wrong value, it might be reading raw Parquet bytes")
