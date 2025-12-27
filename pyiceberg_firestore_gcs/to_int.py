import datetime
import math
import struct
from decimal import Decimal
from typing import Any

NULL_FLAG = -(2**63)
MAX_SIGNED_64BIT = (2**63) - 1
MIN_SIGNED_64BIT = -(2**63) + 1


def _ensure_64bit_range(val):
    """Clamp a value to 64-bit signed range."""
    if val < MIN_SIGNED_64BIT:
        return MIN_SIGNED_64BIT
    if val > MAX_SIGNED_64BIT:
        return MAX_SIGNED_64BIT
    return val


def decode_signed_big_endian_bytes(buf):
    """Convert 8 bytes to signed 64-bit integer (big-endian)."""
    # Use struct to unpack 8 bytes as signed long long (q)
    return struct.unpack(">q", buf)[0]


def to_int(value: Any) -> int:
    """
    Convert a value to a signed 64-bit int for order-preserving comparisons.
    Returns MIN_SIGNED_64BIT to MAX_SIGNED_64BIT or raises ValueError.
    """
    value_type = type(value)

    if value_type is int:
        return _ensure_64bit_range(value)

    if value_type is float:
        if value == float("inf"):
            return MAX_SIGNED_64BIT
        if value == float("-inf"):
            return MIN_SIGNED_64BIT

        if math.isnan(value):
            return NULL_FLAG
        return _ensure_64bit_range(int(math.trunc(value)))

    if value_type is datetime.datetime:
        # Use timestamp method for datetime
        return _ensure_64bit_range(int(math.trunc(value.timestamp() * 1000)))

    if value_type is datetime.date:
        # Convert to days since epoch (1970-01-01)
        # Note: strftime("%s") is not portable; using a more robust method
        epoch = datetime.date(1970, 1, 1)
        delta = value - epoch
        return _ensure_64bit_range(int(delta.days) * 1000)

    if value_type is datetime.time:
        result = value.hour * 3600 + value.minute * 60 + value.second
        return _ensure_64bit_range(result)

    if value_type is Decimal:
        return _ensure_64bit_range(int(math.trunc(value)))

    if value_type is str:
        # Keep the first byte as 0 to ensure the order of the 64bit int
        # is preserved by creating negative numbers
        buf = bytearray(8)  # 8 zero bytes

        # Encode string to bytes
        encoded = value.encode("utf-8")
        length = min(len(encoded), 7)

        # Copy string bytes starting from position 1
        buf[1 : 1 + length] = encoded[:length]

        return _ensure_64bit_range(decode_signed_big_endian_bytes(bytes(buf)))

    if value_type is bytes:
        # Keep the first byte as 0 to ensure order is preserved
        buf = bytearray(8)  # 8 zero bytes

        length = min(len(value), 7)
        buf[1 : 1 + length] = value[:length]

        return _ensure_64bit_range(decode_signed_big_endian_bytes(bytes(buf)))

    return NULL_FLAG
