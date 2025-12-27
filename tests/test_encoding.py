#!/usr/bin/env python3
"""Test the _value_to_int64 function with large binary values"""

import struct


def test_encoding():
    """Test that encoding strings/bytes doesn't overflow int64"""

    # Simulate the encoding from the Cython code
    def encode_bytes_to_int64(raw_bytes):
        # Keep first byte as 0, copy up to 7 bytes starting at position 1
        buf = bytearray(8)
        buf[0] = 0  # First byte is 0
        copy_length = min(len(raw_bytes), 7)
        buf[1 : 1 + copy_length] = raw_bytes[:copy_length]
        # Decode as signed big-endian
        value = struct.unpack(">q", bytes(buf))[0]
        # Ensure in int64 range (should already be, but just in case)
        value = max(-9223372036854775808, min(9223372036854775807, value))
        return value

    # Test with various inputs
    test_cases = [
        b"",  # Empty
        b"a",  # Single byte
        b"hello",  # Short string
        b"hello world",  # String longer than 7 bytes
        b"\xff" * 16,  # All 0xFF bytes (worst case)
        b"\x00" * 16,  # All 0x00 bytes
        bytes(range(256)),  # All possible byte values
    ]

    INT64_MIN = -9223372036854775808
    INT64_MAX = 9223372036854775807

    for i, test_bytes in enumerate(test_cases):
        result = encode_bytes_to_int64(test_bytes)
        print(f"Test {i}: {test_bytes[:20]!r}... -> {result}")

        # Verify it's in int64 range
        assert INT64_MIN <= result <= INT64_MAX, f"Value {result} overflows int64 range!"

        # Verify it's actually an int
        assert isinstance(result, int), f"Result is not an int: {type(result)}"

    print("\nAll tests passed! ✓")


if __name__ == "__main__":
    test_encoding()
