"""Debug endianness issues with timestamp conversion."""

import struct
import datetime

# Test case: Big Endian bytes from Iceberg
iceberg_bytes = b"\x00\x06\x46\x04\xf1\x74\x2c\x40"  # From your diagnosis

# Read as Big Endian (Iceberg format)
value_big_endian = struct.unpack(">q", iceberg_bytes)[0]
print(f"Big Endian (Iceberg): {value_big_endian}")
print(
    f"  As timestamp: {datetime.datetime.fromtimestamp(value_big_endian / 1_000_000, tz=datetime.timezone.utc)}"
)

# Read as Little Endian (what might be happening incorrectly)
value_little_endian = struct.unpack("<q", iceberg_bytes)[0]
print(f"\nLittle Endian (wrong): {value_little_endian}")

# What PyArrow stores
print(f"\nPyArrow stores the Python int: {value_big_endian}")
print("When PyArrow writes to Parquet, it uses native byte order")

# The test: serialize a datetime to Big Endian
test_dt = datetime.datetime(2025, 12, 15, 22, 15, 5, tzinfo=datetime.timezone.utc)
test_micros = int(test_dt.timestamp() * 1_000_000)
test_bytes = struct.pack(">q", test_micros)

print(f"\nTest datetime: {test_dt}")
print(f"As microseconds: {test_micros}")
print(f"As Big Endian bytes: {test_bytes.hex()}")
print(f"Matches Iceberg bytes: {test_bytes == iceberg_bytes}")

# Verify round-trip
recovered = struct.unpack(">q", test_bytes)[0]
print(f"\nRound-trip value: {recovered}")
print(f"Matches original: {recovered == test_micros}")
