#!/usr/bin/env python3
"""Test date display values in manifest statistics."""

from datetime import date
from datetime import datetime

import pyarrow as pa

from opteryx_catalog.catalog.manifest import _compute_stats_for_arrow_column


def test_date_display():
    """Verify date columns have proper ISO format display values."""
    # Create a simple date column with dates in 2021-2026 range
    dates = [date(2021, 11, 13), date(2025, 6, 15), date(2026, 1, 24), date(2022, 3, 5)]
    arr = pa.array(dates, type=pa.date32())

    min_k, hist, col_min, col_max, min_display, max_display, null_count = (
        _compute_stats_for_arrow_column(arr, pa.date32(), "test_file.parquet")
    )

    print("Date column test:")
    print(f"  col_min (int64): {col_min}")
    print(f"  col_max (int64): {col_max}")
    print(f"  min_display: {min_display}")
    print(f"  max_display: {max_display}")
    print(f"  null_count: {null_count}")

    # Verify display values are ISO format strings
    assert isinstance(min_display, str), f"min_display should be string, got {type(min_display)}"
    assert isinstance(max_display, str), f"max_display should be string, got {type(max_display)}"

    # Verify they look like dates (YYYY-MM-DD)
    assert min_display.startswith("202"), f"min_display should look like ISO date: {min_display}"
    assert max_display.startswith("202"), f"max_display should look like ISO date: {max_display}"
    assert "-" in min_display and len(min_display) >= 10, (
        f"min_display formatting incorrect: {min_display}"
    )
    assert "-" in max_display and len(max_display) >= 10, (
        f"max_display formatting incorrect: {max_display}"
    )

    # col_min and col_max should be int64 values (compress() output)
    assert isinstance(col_min, int), f"col_min should be int, got {type(col_min)}"
    assert isinstance(col_max, int), f"col_max should be int, got {type(col_max)}"

    print("\n✓ Date display test passed!")


def test_timestamp_display():
    """Verify timestamp columns have proper ISO format display values."""
    timestamps = [
        datetime(2021, 11, 13, 10, 30, 45),
        datetime(2026, 1, 24, 14, 22, 30),
        datetime(2025, 6, 15, 9, 15, 0),
    ]
    arr = pa.array(timestamps, type=pa.timestamp("us"))

    min_k, hist, col_min, col_max, min_display, max_display, null_count = (
        _compute_stats_for_arrow_column(arr, pa.timestamp("us"), "test_file.parquet")
    )

    print("Timestamp column test:")
    print(f"  col_min (int64): {col_min}")
    print(f"  col_max (int64): {col_max}")
    print(f"  min_display: {min_display}")
    print(f"  max_display: {max_display}")

    # Verify display values are ISO format strings with timestamps
    assert isinstance(min_display, str), f"min_display should be string, got {type(min_display)}"
    assert isinstance(max_display, str), f"max_display should be string, got {type(max_display)}"
    assert "T" in min_display or ":" in min_display, (
        f"min_display should have time component: {min_display}"
    )
    assert "T" in max_display or ":" in max_display, (
        f"max_display should have time component: {max_display}"
    )

    print("\n✓ Timestamp display test passed!")


if __name__ == "__main__":
    test_date_display()
    test_timestamp_display()
    print("\n✅ All date/timestamp display tests passed!")
