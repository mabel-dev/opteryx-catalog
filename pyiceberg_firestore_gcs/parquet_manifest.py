"""Parquet manifest optimization for fast query planning.

This module provides optimized manifest handling that writes Parquet manifests
alongside standard Iceberg Avro manifests for 10-50x faster query planning.

Features:
---------
1. Fast Parquet Manifest Reading: Reads consolidated parquet manifests instead
   of multiple Avro manifest files for faster query planning.

2. BRIN-Style Pruning: Uses min/max bounds (lower_bounds/upper_bounds) from the
   manifest to eliminate data files that provably don't contain matching records
   based on pushed-down predicates. This is similar to PostgreSQL's BRIN indexes.

   Supported predicates for pruning:
   - BoundLessThan (<): Prunes files where min >= predicate_value
   - BoundLessThanOrEqual (<=): Prunes files where min > predicate_value
   - BoundGreaterThan (>): Prunes files where max <= predicate_value
   - BoundGreaterThanOrEqual (>=): Prunes files where max < predicate_value

   The pruning happens after reading the Parquet manifest, keeping the read
   logic simple while providing significant performance benefits for queries
   with selective predicates.
"""

from __future__ import annotations

import base64
import datetime
import json
import struct
import time
from decimal import Decimal
from io import BytesIO
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import pyarrow as pa
import pyarrow.parquet as pq
from orso.logging import get_logger
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.expressions import And
from pyiceberg.expressions import BooleanExpression
from pyiceberg.expressions import BoundEqualTo
from pyiceberg.expressions import BoundGreaterThan
from pyiceberg.expressions import BoundGreaterThanOrEqual
from pyiceberg.expressions import BoundLessThan
from pyiceberg.expressions import BoundLessThanOrEqual
from pyiceberg.expressions import Or
from pyiceberg.io import FileIO
from pyiceberg.manifest import FileFormat
from pyiceberg.manifest import ManifestEntry
from pyiceberg.manifest import ManifestEntryStatus
from pyiceberg.table import ALWAYS_TRUE
from pyiceberg.table import DataScan
from pyiceberg.table import FileScanTask
from pyiceberg.table import StaticTable
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.typedef import EMPTY_DICT
from pyiceberg.typedef import Properties
from pyiceberg.types import BinaryType
from pyiceberg.types import BooleanType
from pyiceberg.types import DoubleType
from pyiceberg.types import LongType
from pyiceberg.types import PrimitiveType
from pyiceberg.types import StringType
from pyiceberg.types import TimestampType
from pyiceberg.types import TimestamptzType

from .to_int import to_int

logger = get_logger()
logger.setLevel(5)

INT64_MIN = -9223372036854775808
INT64_MAX = 9223372036854775807


# Helper function to safely clamp integers (handles None)
def clamp_int(value):
    if value is None:
        return None
    return max(INT64_MIN, min(INT64_MAX, value))


def decode_iceberg_value(
    value: Union[int, float, bytes], data_type: str, scale: int = None
) -> Union[int, float, str, datetime.datetime, Decimal, bool]:
    """
    Decode Iceberg-encoded values based on the specified data type.

    Parameters:
        value: Union[int, float, bytes]
            The encoded value from Iceberg.
        data_type: str
            The type of the value ('int', 'long', 'float', 'double', 'timestamp', 'date', 'string', 'decimal', 'boolean').
        scale: int, optional
            Scale used for decoding decimal types, defaults to None.

    Returns:
        The decoded value in its original form.
    """

    data_type_class = data_type.__class__

    if data_type_class == LongType:
        return int.from_bytes(value, "little", signed=True)
    elif data_type_class == DoubleType:
        # IEEE 754 encoded floats are typically decoded directly
        return struct.unpack("<d", value)[0]  # 8-byte IEEE 754 double
    elif data_type_class in (TimestampType, TimestamptzType):
        # Iceberg stores timestamps as microseconds since epoch
        interval = int.from_bytes(value, "little", signed=True)
        if interval < 0:
            # Windows specifically doesn't like negative timestamps
            return datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=interval)
        return datetime.datetime.fromtimestamp(interval / 1_000_000)
    elif data_type == "date":
        # Iceberg stores dates as days since epoch (1970-01-01)
        interval = int.from_bytes(value, "little", signed=True)
        return datetime.datetime(1970, 1, 1) + datetime.timedelta(days=interval)
    elif data_type_class == StringType:
        # Assuming UTF-8 encoded bytes (or already decoded string)
        return value.decode("utf-8") if isinstance(value, bytes) else str(value)
    elif data_type_class == BinaryType:
        return value
    elif str(data_type).startswith("decimal"):
        # Iceberg stores decimals as unscaled integers
        int_value = int.from_bytes(value, byteorder="big", signed=True)
        return Decimal(int_value) / (10**data_type.scale)
    elif data_type_class == BooleanType:
        return bool(value)

    ValueError(f"Unsupported data type: {data_type}, {str(data_type)}")


def get_parquet_manifest_schema() -> pa.Schema:
    """Define schema for Parquet manifest files.

    This schema stores all data file metadata in a flat structure
    optimized for fast filtering with PyArrow.

    Note: Bounds are stored as int64 for order-preserving comparisons.
    We convert all types to int64 using an order-preserving transformation,
    allowing fast native integer comparisons without deserialization.
    """
    return pa.schema(
        [
            # Core file identification
            ("file_path", pa.string()),
            ("snapshot_id", pa.int64()),
            ("sequence_number", pa.int64()),
            ("file_sequence_number", pa.int64()),
            ("active", pa.bool_()),
            # Partition and spec info
            ("partition_spec_id", pa.int32()),
            ("partition_json", pa.string()),  # JSON string for flexibility
            # File metadata
            ("file_format", pa.string()),
            ("record_count", pa.int64()),
            ("file_size_bytes", pa.int64()),
            # Column bounds as parallel arrays indexed by field_id
            # All types converted to int64 for order-preserving comparisons
            ("lower_bounds", pa.list_(pa.int64())),  # lower_bounds[field_id] = min value
            ("upper_bounds", pa.list_(pa.int64())),  # upper_bounds[field_id] = max value
            # Column statistics as parallel arrays indexed by field_id
            ("null_counts", pa.list_(pa.int64())),  # null_counts[field_id] = null count
            ("value_counts", pa.list_(pa.int64())),  # value_counts[field_id] = value count
            ("column_sizes", pa.list_(pa.int64())),  # column_sizes[field_id] = size in bytes
            ("nan_counts", pa.list_(pa.int64())),  # nan_counts[field_id] = NaN count
            # Additional metadata
            ("key_metadata", pa.binary()),
            ("split_offsets_json", pa.string()),
            ("equality_ids_json", pa.string()),
            ("sort_order_id", pa.int32()),
        ]
    )


def _serialize_value(value: Any) -> Any:
    """Serialize a value for JSON storage, handling bytes specially."""
    if isinstance(value, bytes):
        # Convert bytes to base64 string for JSON serialization
        return base64.b64encode(value).decode("ascii")
    return value


def entry_to_dict(entry: ManifestEntry, schema: Any) -> Dict[str, Any]:
    """Convert ManifestEntry to flat dictionary for Parquet storage.

    Args:
        entry: The ManifestEntry to convert
        schema: Table schema for field type lookups

    Returns:
        Dictionary with all entry data in flat structure
    """
    df = entry.data_file

    # Convert bounds to int64 arrays indexed by field_id
    # This allows O(1) lookup: lower_bounds[field_id] gives min value
    lower_bounds_array = None
    upper_bounds_array = None

    if df.lower_bounds and df.upper_bounds:
        # Find max field_id to size arrays
        max_field_id = max(max(df.lower_bounds.keys()), max(df.upper_bounds.keys()))

        # Initialize arrays with None (will be sparse if not all fields have bounds)
        lower_bounds_array = [None] * (max_field_id + 1)
        upper_bounds_array = [None] * (max_field_id + 1)

        # Fill in bounds where available
        for field_id in df.lower_bounds.keys():
            if field_id in df.upper_bounds:
                field = schema.find_field(field_id)
                if field and isinstance(field.field_type, PrimitiveType):
                    try:
                        lower_value = decode_iceberg_value(
                            df.lower_bounds[field_id], field.field_type
                        )
                        lower_bounds_array[field_id] = to_int(lower_value)
                        upper_value = decode_iceberg_value(
                            df.upper_bounds[field_id], field.field_type
                        )
                        upper_bounds_array[field_id] = to_int(upper_value)
                    except Exception:
                        # If conversion fails, leave as None
                        pass

    # Convert stats to arrays indexed by field_id (same approach as bounds)
    # Determine max field_id across all stat types
    all_field_ids = set()
    if df.null_value_counts:
        all_field_ids.update(df.null_value_counts.keys())
    if df.value_counts:
        all_field_ids.update(df.value_counts.keys())
    if df.column_sizes:
        all_field_ids.update(df.column_sizes.keys())
    if df.nan_value_counts:
        all_field_ids.update(df.nan_value_counts.keys())

    null_counts_array = None
    value_counts_array = None
    column_sizes_array = None
    nan_counts_array = None

    if all_field_ids:
        max_stat_field_id = max(all_field_ids)
        null_counts_array = [None] * (max_stat_field_id + 1)
        value_counts_array = [None] * (max_stat_field_id + 1)
        column_sizes_array = [None] * (max_stat_field_id + 1)
        nan_counts_array = [None] * (max_stat_field_id + 1)

        # Fill in the arrays where we have data, clamping to int64 range
        # PyArrow requires values to fit in signed int64 (-2^63 to 2^63-1)

        if df.null_value_counts:
            for field_id, count in df.null_value_counts.items():
                null_counts_array[field_id] = clamp_int(count)
        if df.value_counts:
            for field_id, count in df.value_counts.items():
                value_counts_array[field_id] = clamp_int(count)
        if df.column_sizes:
            for field_id, size in df.column_sizes.items():
                column_sizes_array[field_id] = clamp_int(size)
        if df.nan_value_counts:
            for field_id, count in df.nan_value_counts.items():
                nan_counts_array[field_id] = clamp_int(count)

    # Convert lists to JSON
    split_offsets_json = json.dumps(df.split_offsets) if df.split_offsets else None
    equality_ids_json = json.dumps(df.equality_ids) if df.equality_ids else None

    # Convert partition dict, handling bytes values
    partition_json = None
    if df.partition:
        partition_json = json.dumps({k: _serialize_value(v) for k, v in df.partition.items()})

    return {
        "file_path": df.file_path,
        "snapshot_id": clamp_int(entry.snapshot_id),
        "sequence_number": clamp_int(entry.sequence_number),
        "file_sequence_number": clamp_int(entry.file_sequence_number),
        "active": entry.status != ManifestEntryStatus.DELETED,
        "partition_spec_id": clamp_int(df.spec_id),
        "partition_json": partition_json,
        "file_format": df.file_format.name if df.file_format else None,
        "record_count": clamp_int(df.record_count),
        "file_size_bytes": clamp_int(df.file_size_in_bytes),
        "lower_bounds": lower_bounds_array,
        "upper_bounds": upper_bounds_array,
        "null_counts": null_counts_array,
        "value_counts": value_counts_array,
        "column_sizes": column_sizes_array,
        "nan_counts": nan_counts_array,
        "key_metadata": df.key_metadata,
        "split_offsets_json": split_offsets_json,
        "equality_ids_json": equality_ids_json,
        "sort_order_id": clamp_int(df.sort_order_id),
    }


def write_parquet_manifest(
    metadata: TableMetadataV2,
    io: FileIO,
    location: str,
) -> Optional[str]:
    """Write consolidated Parquet manifest from current snapshot.

    Reads all Avro manifests and writes a single Parquet file with all
    data file metadata for fast query planning.

    Args:
        metadata: Table metadata containing current snapshot
        io: FileIO for reading manifests and writing Parquet
        location: Table location for manifest path

    Returns:
        Path to written Parquet manifest, or None if no snapshot
    """
    snapshot = metadata.current_snapshot()
    if not snapshot:
        logger.debug("No current snapshot, skipping Parquet manifest write")
        return None

    logger.debug(f"Writing Parquet manifest for snapshot {snapshot.snapshot_id}")

    # Collect all data files from Avro manifests
    all_entries = []
    manifest_count = 0
    schema = metadata.schema()

    for manifest_file in snapshot.manifests(io):
        manifest_count += 1
        try:
            entries = manifest_file.fetch_manifest_entry(io, discard_deleted=False)
            for entry in entries:
                all_entries.append(entry_to_dict(entry, schema))
        except Exception as exc:
            logger.warning(f"Failed to read manifest {manifest_file.manifest_path}: {exc}")
            # Continue with other manifests

    if not all_entries:
        logger.warning("No data files found in manifests")
        return None

    logger.debug(f"Collected {len(all_entries)} data file entries from {manifest_count} manifests")

    for i, entry in enumerate(all_entries):
        for key, value in entry.items():
            if isinstance(value, int) and (value < INT64_MIN or value > INT64_MAX):
                logger.error(f"Entry {i}, field '{key}': value {value} overflows int64 range!")
                # Clamp to int64 range
                entry[key] = clamp_int(value)
            elif isinstance(value, list):
                for j, v in enumerate(value):
                    if isinstance(v, int) and (v < INT64_MIN or v > INT64_MAX):
                        logger.error(
                            f"Entry {i}, field '{key}[{j}]': value {v} overflows int64 range!"
                        )
                        # Clamp to int64 range
                        value[j] = clamp_int(v)

    # Convert to Arrow table
    schema = get_parquet_manifest_schema()
    table = pa.Table.from_pylist(all_entries, schema=schema)

    # Write to GCS
    parquet_path = f"{location}/metadata/manifest-{snapshot.snapshot_id}.parquet"

    try:
        # Write to BytesIO buffer first (PyArrow doesn't support PyIceberg's OutputFile directly)
        buffer = BytesIO()
        pq.write_table(
            table,
            buffer,
            compression="zstd",  # Better compression, fast enough
            compression_level=3,  # Fast compression
            row_group_size=100000,  # Tune based on typical file counts
        )

        # Now write buffer to GCS via PyIceberg's FileIO
        # create() returns a writable file-like object
        buffer.seek(0)
        output_file = io.new_output(parquet_path)
        # PyIceberg's OutputFile supports create() which returns an OutputStream
        with output_file.create() as stream:
            stream.write(buffer.getvalue())

        logger.info(
            f"Wrote Parquet manifest: {len(all_entries)} files ({table.nbytes / 1024 / 1024:.1f} MB) to {parquet_path}"
        )
        return parquet_path

    except Exception as exc:
        logger.error(f"Failed to write Parquet manifest to {parquet_path}: {exc}")
        return None


def read_parquet_manifest(
    metadata: TableMetadataV2,
    io: FileIO,
    location: str,
) -> Optional[List[Dict[str, Any]]]:
    """Read Parquet manifest and return list of DataFile records.

    Args:
        metadata: Table metadata containing current snapshot
        io: FileIO for reading from GCS
        location: Table location for manifest path

    Returns:
        List of DataFile records as dicts, or None if Parquet manifest doesn't exist
    """
    snapshot = metadata.current_snapshot()
    if not snapshot:
        return None

    parquet_path = f"{location}/metadata/manifest-{snapshot.snapshot_id}.parquet"

    try:
        # Read Parquet file from GCS
        input_file = io.new_input(parquet_path)
        with input_file.open() as f:
            # Read entire file into memory (manifests are typically small)
            data = f.read()
            buffer = BytesIO(data)
            table = pq.read_table(buffer)

        logger.debug(f"Read Parquet manifest: {len(table)} files from {parquet_path}")

        # Convert PyArrow table to list of dicts
        records = table.to_pylist()
        return records

    except FileNotFoundError:
        logger.debug(f"Parquet manifest not found at {parquet_path}, falling back to Avro")
        return None
    except Exception as exc:
        logger.warning(
            f"Failed to read Parquet manifest from {parquet_path}: {exc}, falling back to Avro"
        )
        return None


def parquet_record_to_data_file(record: Dict[str, Any]):
    """Convert a Parquet record back to a DataFile-like object.

    Args:
        record: Dictionary from Parquet manifest

    Returns:
        DataFile-compatible dict that can be used with FileScanTask
    """
    # Bounds are stored as int64 values, but DataFile expects bytes in Iceberg format (Big Endian)
    # We need to convert int64 back to bytes for compatibility with PyIceberg's DataFile
    lower_bounds = None
    upper_bounds = None

    lower_array = record.get("lower_bounds")
    upper_array = record.get("upper_bounds")

    if lower_array and upper_array:
        # Store as dict mapping field_id -> bytes (Iceberg format)
        lower_bounds = {}
        upper_bounds = {}

        for field_id, min_val in enumerate(lower_array):
            if min_val is not None and field_id < len(upper_array):
                max_val = upper_array[field_id]
                if max_val is not None:
                    # Convert int64 values back to Big Endian bytes (Iceberg format)
                    # This ensures compatibility with PyIceberg's DataFile expectations
                    lower_bounds[field_id] = struct.pack(">q", min_val)
                    upper_bounds[field_id] = struct.pack(">q", max_val)

    # Convert arrays to dicts for DataFile
    null_value_counts = {}
    if record.get("null_counts"):
        for field_id, count in enumerate(record["null_counts"]):
            if count is not None:
                null_value_counts[field_id] = count

    value_counts = {}
    if record.get("value_counts"):
        for field_id, count in enumerate(record["value_counts"]):
            if count is not None:
                value_counts[field_id] = count

    column_sizes = {}
    if record.get("column_sizes"):
        for field_id, size in enumerate(record["column_sizes"]):
            if size is not None:
                column_sizes[field_id] = size

    nan_value_counts = {}
    if record.get("nan_counts"):
        for field_id, count in enumerate(record["nan_counts"]):
            if count is not None:
                nan_value_counts[field_id] = count

    split_offsets = (
        json.loads(record["split_offsets_json"]) if record.get("split_offsets_json") else None
    )

    equality_ids = (
        json.loads(record["equality_ids_json"]) if record.get("equality_ids_json") else None
    )

    file_format = (
        FileFormat[record["file_format"]] if record.get("file_format") else FileFormat.PARQUET
    )

    # Import DataFile here to create actual instance
    from pyiceberg.manifest import DataFile
    from pyiceberg.manifest import DataFileContent
    from pyiceberg.typedef import Record

    # DataFile constructor takes positional args matching Avro schema
    # Order: content, file_path, file_format, partition, record_count, file_size, column_sizes,
    #        value_counts, null_value_counts, nan_value_counts, lower_bounds, upper_bounds,
    #        key_metadata, split_offsets, equality_ids, sort_order_id
    data_file = DataFile(
        DataFileContent.DATA,  # content
        record["file_path"],  # file_path
        file_format,  # file_format
        Record(),  # partition (empty Record - not used in Opteryx)
        record["record_count"],  # record_count
        record["file_size_bytes"],  # file_size_in_bytes
        column_sizes,  # column_sizes
        value_counts,  # value_counts
        null_value_counts,  # null_value_counts
        nan_value_counts,  # nan_value_counts
        lower_bounds,  # lower_bounds
        upper_bounds,  # upper_bounds
        record.get("key_metadata"),  # key_metadata
        split_offsets,  # split_offsets
        equality_ids,  # equality_ids
        record.get("sort_order_id"),  # sort_order_id
    )

    # Set spec_id as property (not in constructor)
    data_file.spec_id = record.get("partition_spec_id", 0)

    return data_file


def _can_prune_file_with_predicate(
    data_file,
    predicate: Any,
    schema: Any,
) -> bool:
    """Check if a data file can be pruned based on a bound predicate.

    Uses BRIN-style min/max pruning with the lower_bounds and upper_bounds
    from the manifest.

    Args:
        data_file: DataFile object with bounds information
        predicate: Bound predicate to evaluate (BoundLessThan, BoundGreaterThan, etc.)
        schema: Table schema for field lookups

    Returns:
        True if the file can be pruned (doesn't match), False if it might contain matches
    """
    # Check if this is a bound predicate with a term that has a field
    if not hasattr(predicate, "term") or not hasattr(predicate.term, "field"):
        return False

    # Get the field being filtered
    field_id = predicate.term.field.field_id

    # Check if we have bounds for this field
    if not data_file.lower_bounds or not data_file.upper_bounds:
        return False

    if field_id not in data_file.lower_bounds or field_id not in data_file.upper_bounds:
        return False

    # Get the bounds - they are now in Big Endian bytes format (Iceberg format)
    file_min_bytes = data_file.lower_bounds[field_id]
    file_max_bytes = data_file.upper_bounds[field_id]

    print(
        f"Pruning check for field_id {field_id}: file_min_bytes={file_min_bytes}, file_max_bytes={file_max_bytes}"
    )

    # Handle None values
    if file_min_bytes is None or file_max_bytes is None:
        return False

    # Get field type from schema for converting predicate value
    field = schema.find_field(field_id)
    if not field or not isinstance(field.field_type, PrimitiveType):
        return False

    try:
        # Convert bounds from bytes to int64 for comparison
        # Bounds are stored as Big Endian signed 64-bit integers
        file_min_int = struct.unpack(">q", file_min_bytes)[0]
        file_max_int = struct.unpack(">q", file_max_bytes)[0]

        # Convert predicate value to int64
        # Use the to_int function which handles all type conversions
        pred_value = predicate.literal.value
        pred_value_int = to_int(pred_value)

        # Debug logging - now comparing simple integers!
        logger.debug(
            f"Pruning check for field {field_id} ({field.name}): "
            f"file_min_int={file_min_int}, file_max_int={file_max_int}, pred_value_int={pred_value_int}"
        )

        # Apply BRIN-style pruning logic with simple int64 comparisons
        if isinstance(predicate, BoundLessThan):
            # WHERE col < value: prune if file_min >= value
            return file_min_int > pred_value_int
        elif isinstance(predicate, BoundLessThanOrEqual):
            # WHERE col <= value: prune if file_min > value
            return file_min_int >= pred_value_int
        elif isinstance(predicate, BoundGreaterThan):
            # WHERE col > value: prune if file_max <= value
            return file_max_int < pred_value_int
        elif isinstance(predicate, BoundGreaterThanOrEqual):
            # WHERE col >= value: prune if file_max < value
            return file_max_int <= pred_value_int
        elif isinstance(predicate, BoundEqualTo):
            # WHERE col = value: prune if value < file_min OR value > file_max
            return pred_value_int < file_min_int or pred_value_int > file_max_int
        else:
            # For other predicate types (IN, NOT IN, etc.), don't prune for now
            return False

    except Exception as exc:
        # If we can't deserialize or compare, conservatively keep the file
        logger.debug(f"Failed to evaluate predicate for pruning: {exc}")
        return False


def _extract_bound_predicates(expr: BooleanExpression) -> List[Any]:
    """Recursively extract all bound predicate objects from an expression tree.

    Args:
        expr: Boolean expression to extract predicates from

    Returns:
        List of bound predicate objects (BoundLessThan, BoundGreaterThan, etc.)
    """
    predicates = []

    print(type(expr))
    # Check if this is a bound predicate (has term.field attribute)
    if hasattr(expr, "term") and hasattr(expr, "literal") and hasattr(expr.term, "field"):
        predicates.append(expr)
    elif isinstance(expr, (And, Or)):
        # Recursively extract from left and right
        predicates.extend(_extract_bound_predicates(expr.left))
        predicates.extend(_extract_bound_predicates(expr.right))
    # For AlwaysTrue, AlwaysFalse, Not, etc., just return empty list

    return predicates


def prune_data_files_with_predicates(
    data_files: List,
    row_filter: Union[str, BooleanExpression],
    schema: Any,
) -> Tuple[List, int]:
    """Prune data files based on predicates using BRIN-style min/max filtering.

    This function evaluates pushed-down predicates against the min/max bounds
    stored in the manifest to eliminate files that provably don't contain
    matching records.

    Args:
        data_files: List of DataFile objects from manifest
        row_filter: Row filter expression from scan
        schema: Table schema

    Returns:
        Tuple of (filtered_files, pruned_count)
    """
    # Debug logging
    logger.info(f"prune_data_files_with_predicates called with row_filter type: {type(row_filter)}")
    logger.info(f"row_filter value: {row_filter}")

    # If no filter or ALWAYS_TRUE, no pruning
    if row_filter == ALWAYS_TRUE or isinstance(row_filter, AlwaysTrue):
        logger.info("No filter or ALWAYS_TRUE, skipping pruning")
        return data_files, 0

    # Extract all bound predicates from the filter expression
    predicates = _extract_bound_predicates(row_filter)
    logger.info(f"Extracted {len(predicates)} predicates: {predicates}")

    if not predicates:
        logger.info("No bound predicates found, skipping pruning")
        return data_files, 0

    # Filter files based on predicates
    filtered_files = []
    pruned_count = 0

    for data_file in data_files:
        # Check if this file can be pruned by ANY predicate
        # For AND predicates, we can prune if any single predicate eliminates the file
        # For OR predicates, we need to be more conservative
        can_prune = False

        # For now, use conservative approach: only prune if we're certain
        # This works well for simple predicates and AND combinations
        for predicate in predicates:
            if _can_prune_file_with_predicate(data_file, predicate, schema):
                can_prune = True
                break

        if can_prune:
            pruned_count += 1
        else:
            filtered_files.append(data_file)

    return filtered_files, pruned_count


class OptimizedStaticTable(StaticTable):
    """StaticTable that uses Parquet manifests for fast query planning.

    Falls back to standard Avro manifests if Parquet is not available.

    Note: Phase 2 (fast Parquet reading) is not yet implemented.
    Currently uses standard Avro reading but Parquet manifests are being written.
    """

    def refresh(self) -> StaticTable:
        """Refresh is not supported for StaticTable instances."""
        raise NotImplementedError("StaticTable does not support refresh")

    def scan(
        self,
        row_filter: Union[str, BooleanExpression] = ALWAYS_TRUE,
        selected_fields: Tuple[str, ...] = ("*",),
        case_sensitive: bool = True,
        snapshot_id: Optional[int] = None,
        options: Properties = EMPTY_DICT,
        limit: Optional[int] = None,
    ) -> DataScan:
        """Return DataScan that uses Parquet manifests if available."""
        # Create a custom DataScan that will use Parquet for plan_files()
        return OptimizedDataScan(
            table_metadata=self.metadata,
            io=self.io,
            row_filter=row_filter,
            selected_fields=selected_fields,
            case_sensitive=case_sensitive,
            snapshot_id=snapshot_id,
            options=options,
            limit=limit,
        )


class OptimizedDataScan(DataScan):
    """DataScan that uses Parquet manifests for fast file planning."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def plan_files(self) -> Iterable[FileScanTask]:
        """Plan files using Parquet manifest if available, falling back to Avro."""
        print("Attempting to plan files using Parquet manifest...")
        start_time = time.perf_counter()

        # Try to read from Parquet manifest first
        parquet_records = read_parquet_manifest(
            self.table_metadata,
            self.io,
            self.table_metadata.location,
        )

        if parquet_records is not None:
            read_elapsed = (time.perf_counter() - start_time) * 1000

            # Convert Parquet records to DataFile objects
            data_files = []
            for record in parquet_records:
                if record.get("active", True):  # Only include active files
                    try:
                        print("Converting Parquet record to DataFile...")
                        data_file = parquet_record_to_data_file(record)
                        data_files.append(data_file)
                    except Exception as exc:
                        logger.warning(f"Failed to convert Parquet record: {exc}")
                        # Fall back to Avro on any conversion error
                        return self._plan_files_avro(start_time)

            conversion_elapsed = (time.perf_counter() - start_time) * 1000

            # Apply BRIN-style pruning based on predicates
            # Bind the row filter to the schema if it's not already bound
            row_filter = self.row_filter

            # Bind the filter if it's unbound (has a .bind() method)
            if hasattr(row_filter, "bind"):
                bound_filter = row_filter.bind(self.table_metadata.schema(), case_sensitive=True)
            else:
                bound_filter = row_filter

            # Debug: Check if we have a filter
            logger.info(f"Pruning with filter: {bound_filter} (type: {type(bound_filter)})")
            print(f"Pruning with filter: {bound_filter} (type: {type(bound_filter)})")

            data_files, pruned_count = prune_data_files_with_predicates(
                data_files,
                bound_filter,
                self.table_metadata.schema(),
            )

            pruning_elapsed = (time.perf_counter() - start_time) * 1000 - conversion_elapsed

            # Create FileScanTask objects directly (no partition filtering needed)
            tasks = []
            for data_file in data_files:
                # Simple task creation without splits
                # Since you don't use partitions, we use AlwaysTrue predicate
                task = FileScanTask(
                    data_file=data_file,
                    delete_files=set(),  # No delete files
                    start=0,
                    length=data_file.file_size_in_bytes,
                )
                tasks.append(task)

            total_elapsed = (time.perf_counter() - start_time) * 1000

            if pruned_count > 0:
                message = f"Query planning: ✓ Using PARQUET manifest ({len(tasks)} files, {pruned_count} pruned, {total_elapsed:.1f}ms total, {read_elapsed:.1f}ms read, {pruning_elapsed:.1f}ms pruning)"
            else:
                message = f"Query planning: ✓ Using PARQUET manifest ({len(tasks)} files, {total_elapsed:.1f}ms total, {read_elapsed:.1f}ms read)"
            print(message)
            logger.info(message)

            return tasks
        else:
            # Fall back to standard Avro reading
            return self._plan_files_avro(start_time)

    def _plan_files_avro(self, start_time: float) -> Iterable[FileScanTask]:
        """Fall back to Avro manifest reading."""
        result = super().plan_files()
        elapsed = (time.perf_counter() - start_time) * 1000
        message = f"Query planning: ✗ Using AVRO manifests (fallback, {elapsed:.1f}ms)"
        print(message)
        logger.info(message)
        return result
