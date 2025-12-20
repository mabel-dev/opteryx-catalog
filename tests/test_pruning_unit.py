#!/usr/bin/env python3
"""Unit tests for pruning logic that do not require Firestore."""

from datetime import date

from pyiceberg.expressions import EqualTo, GreaterThanOrEqual, And
from pyiceberg.expressions.visitors import bind
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, DoubleType, DateType

from pyiceberg_firestore_gcs.parquet_manifest import (
    parquet_record_to_data_file,
    prune_data_files_with_predicates,
)


def days_since_epoch(d: date) -> int:
    epoch = date(1970, 1, 1)
    return (d - epoch).days


def make_record(event_min_day, event_max_day, amount_min, amount_max):
    # Build arrays where index == field_id for simplicity
    # We'll create arrays with length 8 to include field ids up to 6
    lower = [None] * 8
    upper = [None] * 8

    # Field id 4 -> event_date, field id 6 -> amount
    lower[4] = event_min_day
    upper[4] = event_max_day
    lower[6] = amount_min
    upper[6] = amount_max

    return {
        "file_path": "dummy.parquet",
        "file_format": "PARQUET",
        "record_count": 100,
        "file_size_bytes": 1024,
        "lower_bounds": lower,
        "upper_bounds": upper,
        "null_counts": [None] * 8,
        "value_counts": [None] * 8,
        "column_sizes": [None] * 8,
        "nan_counts": [None] * 8,
        "split_offsets_json": None,
        "equality_ids_json": None,
    }


def test_and_pruning_keeps_only_matching_file():
    # Schema with field ids matching the arrays in make_record
    schema = Schema(
        NestedField(1, "id", DoubleType(), required=False),
        NestedField(2, "user_id", DoubleType(), required=False),
        NestedField(3, "event_type", DoubleType(), required=False),
        NestedField(4, "event_date", DateType(), required=False),
        NestedField(5, "event_timestamp", DoubleType(), required=False),
        NestedField(6, "amount", DoubleType(), required=False),
        NestedField(7, "quantity", DoubleType(), required=False),
    )

    # Create two records: January and June
    jan_min = days_since_epoch(date(2024, 1, 1))
    jan_max = days_since_epoch(date(2024, 1, 31))
    jan_amt_min = 100
    jan_amt_max = 199

    jun_min = days_since_epoch(date(2024, 6, 1))
    jun_max = days_since_epoch(date(2024, 6, 30))
    jun_amt_min = 600
    jun_amt_max = 699

    rec_jan = make_record(jan_min, jan_max, jan_amt_min, jan_amt_max)
    rec_jun = make_record(jun_min, jun_max, jun_amt_min, jun_amt_max)

    data_files = [parquet_record_to_data_file(rec_jan), parquet_record_to_data_file(rec_jun)]

    expr = And(EqualTo("event_date", date(2024, 6, 15)), GreaterThanOrEqual("amount", 600.0))
    bound = bind(schema, expr, case_sensitive=True)

    filtered, pruned = prune_data_files_with_predicates(data_files, bound, schema)

    # Only the June file should remain
    assert len(filtered) == 1
    assert pruned == 1


def test_date_after_all_prunes_everything():
    schema = Schema(
        NestedField(4, "event_date", DateType(), required=False),
        NestedField(6, "amount", DoubleType(), required=False),
    )

    jan_min = days_since_epoch(date(2024, 1, 1))
    jan_max = days_since_epoch(date(2024, 1, 31))
    rec = make_record(jan_min, jan_max, 100, 199)

    data_files = [parquet_record_to_data_file(rec)]

    from pyiceberg.expressions import GreaterThan

    expr = GreaterThan("event_date", date(9999, 12, 31))
    bound = bind(schema, expr, case_sensitive=True)

    filtered, pruned = prune_data_files_with_predicates(data_files, bound, schema)

    assert len(filtered) == 0
    assert pruned == 1
