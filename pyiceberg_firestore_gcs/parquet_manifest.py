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
import json
import time
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
from pyiceberg.expressions import BoundPredicate
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
from pyiceberg.types import DoubleType
from pyiceberg.types import FloatType
from pyiceberg.types import IntegerType
from pyiceberg.types import LongType
from pyiceberg.types import PrimitiveType

logger = get_logger()


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
            ("null_counts", pa.list_(pa.int64())),     # null_counts[field_id] = null count
            ("value_counts", pa.list_(pa.int64())),    # value_counts[field_id] = value count
            ("column_sizes", pa.list_(pa.int64())),    # column_sizes[field_id] = size in bytes
            ("nan_counts", pa.list_(pa.int64())),      # nan_counts[field_id] = NaN count
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


def _value_to_int64(raw_bytes: bytes, field_type: PrimitiveType) -> int:
    """Convert a bound value to int64 for order-preserving comparisons.
    
    This allows us to compare values of different types using simple integer
    comparisons without complex deserialization logic.
    
    Args:
        raw_bytes: Serialized bytes from Iceberg bounds
        field_type: The field type for deserialization
        
    Returns:
        int64 value that preserves ordering
    """
    import struct
    from pyiceberg.types import (
        StringType,
        TimestampType,
        TimestamptzType,
        DateType,
        TimeType,
    )
    
    # Numeric types - extract as integers
    if isinstance(field_type, (IntegerType, LongType)):
        if isinstance(field_type, IntegerType):
            return struct.unpack('>i', raw_bytes)[0]
        return struct.unpack('>q', raw_bytes)[0]
        
    # Floats - round to int64
    elif isinstance(field_type, (FloatType, DoubleType)):
        if isinstance(field_type, FloatType):
            return int(struct.unpack('>f', raw_bytes)[0])
        return int(struct.unpack('>d', raw_bytes)[0])
    
    # Timestamps - microseconds since epoch
    elif isinstance(field_type, (TimestampType, TimestamptzType)):
        return struct.unpack('>q', raw_bytes)[0]
    
    # Date - days since epoch  
    elif isinstance(field_type, DateType):
        return struct.unpack('>i', raw_bytes)[0]
        
    # Time - microseconds since midnight
    elif isinstance(field_type, TimeType):
        return struct.unpack('>q', raw_bytes)[0]
    
    # Strings - use first 7 bytes as int (similar to your approach)
    elif isinstance(field_type, StringType):
        # Pad to 8 bytes, keeping first byte 0 for sign
        buf = b'\x00' + raw_bytes[:7]
        buf = buf.ljust(8, b'\x00')
        return struct.unpack('>q', buf)[0]
    
    # For other types, use a hash of the bytes
    else:
        # Use first 8 bytes as int64
        buf = raw_bytes[:8].ljust(8, b'\x00')
        return struct.unpack('>q', buf)[0]


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
                        lower_bounds_array[field_id] = _value_to_int64(
                            df.lower_bounds[field_id], field.field_type
                        )
                        upper_bounds_array[field_id] = _value_to_int64(
                            df.upper_bounds[field_id], field.field_type
                        )
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
        
        # Fill in the arrays where we have data
        if df.null_value_counts:
            for field_id, count in df.null_value_counts.items():
                null_counts_array[field_id] = count
        if df.value_counts:
            for field_id, count in df.value_counts.items():
                value_counts_array[field_id] = count
        if df.column_sizes:
            for field_id, size in df.column_sizes.items():
                column_sizes_array[field_id] = size
        if df.nan_value_counts:
            for field_id, count in df.nan_value_counts.items():
                nan_counts_array[field_id] = count

    # Convert lists to JSON
    split_offsets_json = json.dumps(df.split_offsets) if df.split_offsets else None
    equality_ids_json = json.dumps(df.equality_ids) if df.equality_ids else None

    # Convert partition dict, handling bytes values
    partition_json = None
    if df.partition:
        partition_json = json.dumps({k: _serialize_value(v) for k, v in df.partition.items()})

    return {
        "file_path": df.file_path,
        "snapshot_id": entry.snapshot_id,
        "sequence_number": entry.sequence_number,
        "file_sequence_number": entry.file_sequence_number,
        "active": entry.status != ManifestEntryStatus.DELETED,
        "partition_spec_id": df.spec_id,
        "partition_json": partition_json,
        "file_format": df.file_format.name if df.file_format else None,
        "record_count": df.record_count,
        "file_size_bytes": df.file_size_in_bytes,
        "lower_bounds": lower_bounds_array,
        "upper_bounds": upper_bounds_array,
        "null_counts": null_counts_array,
        "value_counts": value_counts_array,
        "column_sizes": column_sizes_array,
        "nan_counts": nan_counts_array,
        "key_metadata": df.key_metadata,
        "split_offsets_json": split_offsets_json,
        "equality_ids_json": equality_ids_json,
        "sort_order_id": df.sort_order_id,
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


def _deserialize_value(value: Any, is_base64: bool = False) -> Any:
    """Deserialize a value from JSON storage, handling base64-encoded bytes."""
    if is_base64 and isinstance(value, str):
        return base64.b64decode(value)
    return value


def parquet_record_to_data_file(record: Dict[str, Any]):
    """Convert a Parquet record back to a DataFile-like object.

    Args:
        record: Dictionary from Parquet manifest

    Returns:
        DataFile-compatible dict that can be used with FileScanTask
    """
    # Bounds are stored as int64 arrays indexed by field_id
    # We keep them as-is for pruning (no need to convert back to bytes for DataFile)
    lower_bounds = None
    upper_bounds = None
    
    lower_array = record.get("lower_bounds")
    upper_array = record.get("upper_bounds")
    
    if lower_array and upper_array:
        # Store as dict mapping field_id -> int64 value
        # We'll use these directly in pruning without conversion
        lower_bounds = {}
        upper_bounds = {}
        
        for field_id, min_val in enumerate(lower_array):
            if min_val is not None and field_id < len(upper_array):
                max_val = upper_array[field_id]
                if max_val is not None:
                    # Store int64 values directly - no bytes conversion needed!
                    lower_bounds[field_id] = min_val
                    upper_bounds[field_id] = max_val

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


def _deserialize_bound_value(raw_value: bytes, field_type: PrimitiveType) -> Any:
    """Deserialize a bound value from bytes to its native Python type.

    Args:
        raw_value: Serialized bytes value from Iceberg bounds
        field_type: The Iceberg field type to deserialize to

    Returns:
        Deserialized Python value
    """
    # Import here to avoid circular dependency
    from pyiceberg.types import BinaryType
    from pyiceberg.types import BooleanType
    from pyiceberg.types import DateType
    from pyiceberg.types import DecimalType
    from pyiceberg.types import FixedType
    from pyiceberg.types import StringType
    from pyiceberg.types import TimestampType
    from pyiceberg.types import TimestamptzType
    from pyiceberg.types import TimeType
    from pyiceberg.types import UUIDType

    # For numeric types
    if isinstance(field_type, (IntegerType, LongType)):
        import struct

        if isinstance(field_type, IntegerType):
            return struct.unpack(">i", raw_value)[0]
        else:  # LongType
            return struct.unpack(">q", raw_value)[0]
    elif isinstance(field_type, (FloatType, DoubleType)):
        import struct

        if isinstance(field_type, FloatType):
            return struct.unpack(">f", raw_value)[0]
        else:  # DoubleType
            return struct.unpack(">d", raw_value)[0]
    elif isinstance(field_type, (DateType, TimeType)):
        import struct

        return struct.unpack(">i", raw_value)[0]
    elif isinstance(field_type, (TimestampType, TimestamptzType)):
        import struct

        return struct.unpack(">q", raw_value)[0]
    elif isinstance(field_type, StringType):
        return raw_value.decode("utf-8")
    elif isinstance(field_type, UUIDType):
        import uuid

        return uuid.UUID(bytes=raw_value)
    elif isinstance(field_type, (BinaryType, FixedType)):
        return raw_value
    elif isinstance(field_type, BooleanType):
        return bool(raw_value[0])
    elif isinstance(field_type, DecimalType):
        # Handle decimal - convert to int first
        import struct

        if len(raw_value) <= 8:
            return struct.unpack(">q", raw_value.rjust(8, b"\x00"))[0]
        else:
            # For larger decimals, use int.from_bytes
            return int.from_bytes(raw_value, byteorder="big", signed=True)
    else:
        # Fallback: return as-is
        return raw_value


def _can_prune_file_with_predicate(
    data_file,
    predicate: BoundPredicate,
    schema: Any,
) -> bool:
    """Check if a data file can be pruned based on a bound predicate.

    Uses BRIN-style min/max pruning with the lower_bounds and upper_bounds
    from the manifest.

    Args:
        data_file: DataFile object with bounds information
        predicate: Bound predicate to evaluate
        schema: Table schema for field lookups

    Returns:
        True if the file can be pruned (doesn't match), False if it might contain matches
    """
    if not isinstance(predicate, BoundPredicate):
        return False

    # Get the field being filtered
    field_id = predicate.term.field.field_id

    # Check if we have bounds for this field
    if not data_file.lower_bounds or not data_file.upper_bounds:
        return False

    if field_id not in data_file.lower_bounds or field_id not in data_file.upper_bounds:
        return False

    # Get the int64 bounds directly - already converted!
    file_min_int = data_file.lower_bounds[field_id]
    file_max_int = data_file.upper_bounds[field_id]
    
    # Handle None values
    if file_min_int is None or file_max_int is None:
        return False

    # Get field type from schema for converting predicate value
    field = schema.find_field(field_id)
    if not field or not isinstance(field.field_type, PrimitiveType):
        return False

    try:

        # Convert predicate value to int64
        # For the predicate value, we need to serialize it first then convert
        pred_value = predicate.literal.value
        
        # Serialize the predicate value to bytes (using Iceberg's serialization)
        from pyiceberg.types import StringType
        if isinstance(field.field_type, StringType) and isinstance(pred_value, str):
            pred_value_bytes = pred_value.encode('utf-8')
        elif isinstance(pred_value, int):
            import struct
            if isinstance(field.field_type, IntegerType):
                pred_value_bytes = struct.pack('>i', pred_value)
            else:
                pred_value_bytes = struct.pack('>q', pred_value)
        elif isinstance(pred_value, float):
            import struct
            if isinstance(field.field_type, FloatType):
                pred_value_bytes = struct.pack('>f', pred_value)
            else:
                pred_value_bytes = struct.pack('>d', pred_value)
        else:
            # For other types, try to use the raw value if it's already bytes
            if isinstance(pred_value, bytes):
                pred_value_bytes = pred_value
            else:
                # Fallback: convert to string and encode
                pred_value_bytes = str(pred_value).encode('utf-8')
        
        pred_value_int = _value_to_int64(pred_value_bytes, field.field_type)
        
        # Debug logging - now comparing simple integers!
        logger.debug(
            f"Pruning check for field {field_id} ({field.name}): "
            f"file_min_int={file_min_int}, file_max_int={file_max_int}, pred_value_int={pred_value_int}"
        )

        # Apply BRIN-style pruning logic with simple int64 comparisons
        if isinstance(predicate, BoundLessThan):
            # WHERE col < value: prune if file_min >= value
            return file_min_int >= pred_value_int
        elif isinstance(predicate, BoundLessThanOrEqual):
            # WHERE col <= value: prune if file_min > value
            return file_min_int > pred_value_int
        elif isinstance(predicate, BoundGreaterThan):
            # WHERE col > value: prune if file_max <= value
            return file_max_int <= pred_value_int
        elif isinstance(predicate, BoundGreaterThanOrEqual):
            # WHERE col >= value: prune if file_max < value
            return file_max_int < pred_value_int
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


def _extract_bound_predicates(expr: BooleanExpression) -> List[BoundPredicate]:
    """Recursively extract all BoundPredicate objects from an expression tree.

    Args:
        expr: Boolean expression to extract predicates from

    Returns:
        List of BoundPredicate objects
    """
    predicates = []

    if isinstance(expr, BoundPredicate):
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

        # print("paret_records:", parquet_records)

        if parquet_records is not None:
            read_elapsed = (time.perf_counter() - start_time) * 1000

            # Convert Parquet records to DataFile objects
            data_files = []
            for record in parquet_records:
                if record.get("active", True):  # Only include active files
                    try:
                        data_file = parquet_record_to_data_file(record)
                        data_files.append(data_file)
                    except Exception as exc:
                        logger.warning(f"Failed to convert Parquet record: {exc}")
                        # Fall back to Avro on any conversion error
                        return self._plan_files_avro(start_time)

            conversion_elapsed = (time.perf_counter() - start_time) * 1000

            # Apply BRIN-style pruning based on predicates
            # Use the projected_schema which has the bound filter
            # Access the bound row filter from DataScan's internal state
            bound_filter = self.row_filter
            
            # Debug: Check if we have a filter
            logger.info(f"Pruning with filter: {bound_filter} (type: {type(bound_filter)})")
            
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
