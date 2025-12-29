"""Parquet manifest optimization for fast query planning.

This module provides optimized manifest handling that writes consolidated
Parquet manifests for faster query planning and supports BRIN-style pruning
using stored lower/upper bounds.
"""

from __future__ import annotations

import base64
import datetime
import json
import struct
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
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
from pyiceberg.expressions import BoundIn
from pyiceberg.expressions import BoundLessThan
from pyiceberg.expressions import BoundLessThanOrEqual
from pyiceberg.expressions import NotNull
from pyiceberg.expressions import Or
from pyiceberg.expressions.visitors import bind
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
from pyiceberg.types import DateType
from pyiceberg.types import DoubleType
from pyiceberg.types import LongType
from pyiceberg.types import PrimitiveType
from pyiceberg.types import StringType
from pyiceberg.types import TimestampType
from pyiceberg.types import TimestamptzType

from .to_int import to_int

logger = get_logger()
logger.setLevel(5)

# Constants
INT64_MIN = -9223372036854775808
INT64_MAX = 9223372036854775807
MICROSECONDS_PER_SECOND = 1_000_000
BYTES_PER_MEGABYTE = 1024 * 1024
DEFAULT_ROW_GROUP_SIZE = 100000
DEFAULT_MAX_MANIFEST_SIZE_MB = 100


def _clean_empty_structs(obj: Any) -> Any:
    """
    Recursively remove empty dict/struct fields from nested structures.

    PyArrow cannot write structs with no child fields to Parquet.
    This function recursively traverses dicts and lists to remove any
    empty dicts (which represent empty structs in Avro).
    """
    if isinstance(obj, dict):
        cleaned = {}
        for key, value in obj.items():
            # Recursively clean the value
            cleaned_value = _clean_empty_structs(value)
            # Skip empty dicts entirely
            if isinstance(cleaned_value, dict) and len(cleaned_value) == 0:
                continue
            cleaned[key] = cleaned_value
        return cleaned
    elif isinstance(obj, list):
        return [_clean_empty_structs(item) for item in obj]
    else:
        return obj


def write_parquet_manifest_from_entries(entries: List[Dict[str, Any]]) -> bytes:
    """
    Write Parquet bytes from manifest entry dicts (for blackhole conversion).

    This is used by the FileIO blackhole hack to convert Avro manifest writes to Parquet.
    Takes raw entry dicts from Avro deserialization and converts to Parquet bytes.

    The key challenge: PyArrow cannot write structs with no child fields to Parquet.
    We handle this by recursively removing all empty dict/struct fields.

    Args:
        entries: List of manifest entry dicts (from Avro deserialization)

    Returns:
        Parquet-encoded bytes
    """
    if not entries:
        # Return empty Parquet file
        empty_table = pa.Table.from_pylist([])
        buffer = BytesIO()
        pq.write_table(empty_table, buffer, compression="zstd")
        return buffer.getvalue()

    logger.debug(f"Converting {len(entries)} entries to Parquet")

    # Recursively clean all empty structs from entries
    cleaned_entries = []
    for entry in entries:
        cleaned = _clean_empty_structs(entry)
        cleaned_entries.append(cleaned)

    logger.debug("Cleaned entries, creating Arrow table")

    # Convert to Arrow table
    table = pa.Table.from_pylist(cleaned_entries)

    logger.debug(f"Created Arrow table with {len(table)} rows")

    # Write to buffer
    buffer = BytesIO()
    pq.write_table(
        table,
        buffer,
        compression="zstd",
        compression_level=3,
        row_group_size=min(DEFAULT_ROW_GROUP_SIZE, len(cleaned_entries)),
    )

    return buffer.getvalue()


def write_parquet_manifest_raw(entries: List[Dict[str, Any]], avro_schema: Dict[str, Any]) -> bytes:
    """
    Write raw Parquet bytes from manifest entry dicts.

    This is used by the FileIO blackhole hack to convert Avro manifest writes to Parquet.
    Takes the raw entry dicts from Avro deserialization and converts to Parquet bytes.

    Args:
        entries: List of manifest entry dicts (from Avro deserialization)
        avro_schema: The Avro schema (unused for now, kept for future compatibility)

    Returns:
        Parquet-encoded bytes
    """
    # PyArrow cannot write structs with no child fields
    # We need to drop the 'partition' field entirely if it's empty
    # or find another workaround
    cleaned_entries = []
    for entry in entries:
        cleaned = {}

        for key, value in entry.items():
            # Skip partition if it's an empty dict
            if key == "partition" and isinstance(value, dict) and len(value) == 0:
                continue
            cleaned[key] = value

        cleaned_entries.append(cleaned)

    # Convert entries to Arrow table
    # We need to infer the schema from the entries since we don't have TableMetadata here
    table = pa.Table.from_pylist(cleaned_entries)

    # Write to BytesIO buffer
    buffer = BytesIO()
    pq.write_table(
        table,
        buffer,
        compression="zstd",
        compression_level=3,
        row_group_size=min(DEFAULT_ROW_GROUP_SIZE, len(entries)),
    )

    return buffer.getvalue()


class CompressionType(str, Enum):
    """Supported compression types for Parquet manifests."""

    ZSTD = "zstd"
    SNAPPY = "snappy"
    GZIP = "gzip"
    NONE = "none"


@dataclass
class ManifestOptimizationConfig:
    """Configuration for manifest optimization."""

    enabled: bool = True
    compression: CompressionType = CompressionType.ZSTD
    compression_level: int = 3
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE
    max_manifest_size_mb: int = DEFAULT_MAX_MANIFEST_SIZE_MB
    enable_pruning: bool = False
    cache_unpacked_bounds: bool = True
    enable_metrics: bool = True
    streaming_read: bool = True


class ManifestMetrics:
    """Metrics collector for manifest operations."""

    def __init__(self):
        self._lock = threading.RLock()
        self.read_times: List[float] = []
        self.write_times: List[float] = []
        self.pruning_stats: Dict[str, int] = defaultdict(int)
        self.file_counts: List[int] = []
        self.manifest_sizes: List[int] = []
        self.fallback_count: int = 0
        self.pruned_files_total: int = 0

    def record_read(self, duration_ms: float, file_count: int):
        """Record read operation metrics."""
        with self._lock:
            self.read_times.append(duration_ms)
            self.file_counts.append(file_count)

    def record_write(self, duration_ms: float, file_count: int, manifest_size: int):
        """Record write operation metrics."""
        with self._lock:
            self.write_times.append(duration_ms)
            self.file_counts.append(file_count)
            self.manifest_sizes.append(manifest_size)

    def record_prune(self, predicate_type: str, pruned_count: int):
        """Record pruning statistics."""
        with self._lock:
            self.pruning_stats[predicate_type] += pruned_count
            self.pruned_files_total += pruned_count

    def get_summary(self) -> Dict[str, Any]:
        """Get summary metrics."""
        with self._lock:
            return {
                "total_reads": len(self.read_times),
                "total_writes": len(self.write_times),
                "avg_read_time_ms": sum(self.read_times) / len(self.read_times)
                if self.read_times
                else 0,
                "avg_write_time_ms": sum(self.write_times) / len(self.write_times)
                if self.write_times
                else 0,
                "avg_file_count": sum(self.file_counts) / len(self.file_counts)
                if self.file_counts
                else 0,
                "avg_manifest_size_mb": (sum(self.manifest_sizes) / len(self.manifest_sizes))
                / BYTES_PER_MEGABYTE
                if self.manifest_sizes
                else 0,
                "pruned_files_total": self.pruned_files_total,
                "pruning_stats": dict(self.pruning_stats),
                "fallback_count": self.fallback_count,
            }


# Global metrics instance
_metrics = ManifestMetrics()


# Helper function to safely clamp integers (handles None)
def clamp_int(value: Optional[int]) -> Optional[int]:
    """Clamp integer value to int64 range."""
    if value is None:
        return None
    return max(INT64_MIN, min(INT64_MAX, value))


def decode_iceberg_value(
    value: Union[int, float, bytes], data_type: PrimitiveType, scale: Optional[int] = None
) -> Union[int, float, str, datetime.datetime, datetime.date, Decimal, bool, bytes]:
    """
    Decode Iceberg-encoded values based on the specified data type.

    Parameters:
        value: The encoded value from Iceberg
        data_type: The PrimitiveType of the value
        scale: Scale used for decoding decimal types

    Returns:
        The decoded value in its original form
    """
    try:
        if isinstance(data_type, LongType):
            return int.from_bytes(value, "little", signed=True)
        elif isinstance(data_type, DoubleType):
            return struct.unpack("<d", value)[0]  # 8-byte IEEE 754 double
        elif isinstance(data_type, (TimestampType, TimestamptzType)):
            interval = int.from_bytes(value, "little", signed=True)
            if interval < 0:
                return datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=interval)
            return datetime.datetime.fromtimestamp(interval / MICROSECONDS_PER_SECOND)
        elif isinstance(data_type, DateType):
            interval = int.from_bytes(value, "little", signed=True)
            return datetime.date(1970, 1, 1) + datetime.timedelta(days=interval)
        elif isinstance(data_type, StringType):
            return value.decode("utf-8") if isinstance(value, bytes) else str(value)
        elif isinstance(data_type, BinaryType):
            return value
        elif str(data_type).startswith("decimal"):
            int_value = int.from_bytes(value, byteorder="big", signed=True)
            return Decimal(int_value) / (10**data_type.scale)
        elif isinstance(data_type, BooleanType):
            return bool(value)
        else:
            raise ValueError(f"Unsupported data type: {data_type.__class__.__name__}")
    except Exception as e:
        logger.debug(f"Failed to decode value {value} for type {data_type}: {e}")
        raise


def get_parquet_manifest_schema(metadata: TableMetadataV2) -> pa.Schema:
    """Define schema for Parquet manifest files with version tracking.

    This schema stores all data file metadata in a flat structure
    optimized for fast filtering with PyArrow.

    Note: Bounds are stored as int64 for order-preserving comparisons.
    We convert all types to int64 using an order-preserving transformation,
    allowing fast native integer comparisons without deserialization.
    """
    # Create field ID to name mapping for schema evolution
    field_mapping = {}
    for field in metadata.schema().fields:
        field_mapping[field.field_id] = {
            "name": field.name,
            "type": str(field.field_type),
            "required": field.required,
        }

    base_schema = pa.schema(
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
            # Schema version and mapping for evolution
            ("schema_version", pa.int64()),
            ("field_id_mapping", pa.string()),  # JSON mapping of field IDs to names
        ]
    )

    return base_schema


def _serialize_value(value: Any) -> Any:
    """Serialize a value for JSON storage, handling bytes specially."""
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    return value


def _deserialize_value(value: Any, original_type: Optional[str] = None) -> Any:
    """Deserialize a value from JSON storage."""
    if isinstance(value, str) and original_type == "binary":
        try:
            return base64.b64decode(value)
        except Exception:
            return value
    return value


class DataFileWithCachedBounds:
    """Wrapper for DataFile with cached integer bounds for faster pruning."""

    __slots__ = ("data_file", "_min_int_cache", "_max_int_cache", "_field_mapping")

    def __init__(self, data_file, field_mapping: Optional[Dict[int, Dict]] = None):
        self.data_file = data_file
        self._min_int_cache: Dict[int, int] = {}
        self._max_int_cache: Dict[int, int] = {}
        self._field_mapping = field_mapping or {}

    def get_min_int(self, field_id: int) -> Optional[int]:
        """Get cached min value for field_id."""
        if field_id in self._min_int_cache:
            return self._min_int_cache[field_id]

        if (
            self.data_file.lower_bounds
            and field_id in self.data_file.lower_bounds
            and self.data_file.lower_bounds[field_id] is not None
        ):
            try:
                value = struct.unpack(">q", self.data_file.lower_bounds[field_id])[0]
                self._min_int_cache[field_id] = value
                return value
            except Exception:
                return None
        return None

    def get_max_int(self, field_id: int) -> Optional[int]:
        """Get cached max value for field_id."""
        if field_id in self._max_int_cache:
            return self._max_int_cache[field_id]

        if (
            self.data_file.upper_bounds
            and field_id in self.data_file.upper_bounds
            and self.data_file.upper_bounds[field_id] is not None
        ):
            try:
                value = struct.unpack(">q", self.data_file.upper_bounds[field_id])[0]
                self._max_int_cache[field_id] = value
                return value
            except Exception:
                return None
        return None

    def get_field_name(self, field_id: int) -> Optional[str]:
        """Get field name from mapping if available."""
        if field_id in self._field_mapping:
            return self._field_mapping[field_id].get("name")
        return None

    def __getattr__(self, name):
        """Delegate other attributes to the underlying data_file."""
        return getattr(self.data_file, name)


def entry_to_dict(
    entry: ManifestEntry, schema: Any, config: ManifestOptimizationConfig
) -> Dict[str, Any]:
    """Convert ManifestEntry to flat dictionary for Parquet storage.

    Args:
        entry: The ManifestEntry to convert
        schema: Table schema for field type lookups
        config: Optimization configuration

    Returns:
        Dictionary with all entry data in flat structure
    """
    df = entry.data_file

    # Convert bounds to int64 arrays indexed by field_id
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
                    except Exception as e:
                        logger.debug(f"Failed to convert bounds for field {field_id}: {e}")

    # Convert stats to arrays indexed by field_id
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

    # Create field ID mapping for schema evolution
    field_mapping = {}
    for field in schema.fields:
        field_mapping[field.field_id] = {
            "name": field.name,
            "type": str(field.field_type),
            "required": field.required,
        }

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
        "schema_version": getattr(schema, "schema_id", None),
        "field_id_mapping": json.dumps(field_mapping),
    }


def validate_manifest_data(all_entries: List[Dict[str, Any]]) -> bool:
    """Validate manifest data before writing."""
    if not all_entries:
        logger.warning("No data files found in manifests")
        return False

    for i, entry in enumerate(all_entries):
        for key, value in entry.items():
            if isinstance(value, int) and (value < INT64_MIN or value > INT64_MAX):
                logger.error(f"Entry {i}, field '{key}': value {value} overflows int64 range!")
                entry[key] = clamp_int(value)
            elif isinstance(value, list):
                for j, v in enumerate(value):
                    if isinstance(v, int) and (v < INT64_MIN or v > INT64_MAX):
                        logger.error(
                            f"Entry {i}, field '{key}[{j}]': value {v} overflows int64 range!"
                        )
                        value[j] = clamp_int(v)

    return True


def write_parquet_manifest(
    metadata: TableMetadataV2,
    io: FileIO,
    location: str,
    config: Optional[ManifestOptimizationConfig] = None,
    *,
    entries: Optional[List[Dict[str, Any]]] = None,
) -> Optional[str]:
    """Write consolidated Parquet manifest from current snapshot.

    Write a consolidated Parquet manifest from caller-provided entries.

    Args:
        metadata: Table metadata containing current snapshot
        io: FileIO for reading manifests and writing Parquet
        location: Table location for manifest path
        config: Optimization configuration

    Returns:
        Path to written Parquet manifest, or None if no snapshot
    """
    start_time = time.perf_counter()
    config = config or ManifestOptimizationConfig()

    if not config.enabled:
        logger.debug("Manifest optimization disabled, skipping Parquet manifest write")
        return None

    # Validate inputs
    if not metadata or not io or not location:
        logger.error("Missing required parameters for manifest writing")
        return None

    snapshot = metadata.current_snapshot()
    if not snapshot:
        logger.debug("No current snapshot, skipping Parquet manifest write")
        return None

    logger.debug(f"Writing Parquet manifest for snapshot {snapshot.snapshot_id}")

    # Check if manifest already exists (idempotency)
    parquet_path = f"{location}/metadata/manifest-{snapshot.snapshot_id}.parquet"
    try:
        # Check if file already exists
        input_file = io.new_input(parquet_path)
        with input_file.open() as f:
            # File exists and is readable
            logger.debug(f"Parquet manifest already exists at {parquet_path}, skipping write")
            return parquet_path
    except Exception:
        # File doesn't exist or isn't readable, proceed with writing
        pass

    # Collect all data files. Prefer caller-supplied `entries`.
    # IMPORTANT: We do not read Avro manifest-list/Avro manifests in the
    # hot path. If `entries` is not provided the function is a no-op and
    # returns None (no Parquet manifest written).
    all_entries: List[Dict[str, Any]] = []
    manifest_count = 0
    schema = metadata.schema()

    if entries is not None:
        all_entries = list(entries)
        manifest_count = 1
    else:
        logger.debug("No entries provided; skipping Parquet manifest write")
        return None

    if not validate_manifest_data(all_entries):
        return None

    logger.debug(f"Collected {len(all_entries)} data file entries from {manifest_count} manifests")

    # Convert to Arrow table
    schema = get_parquet_manifest_schema(metadata)
    table = pa.Table.from_pylist(all_entries, schema=schema)

    # Check if manifest size exceeds limit
    manifest_size_mb = table.nbytes / BYTES_PER_MEGABYTE
    if manifest_size_mb > config.max_manifest_size_mb:
        logger.warning(
            f"Manifest size {manifest_size_mb:.1f}MB exceeds limit {config.max_manifest_size_mb}MB, "
            f"splitting into multiple manifests"
        )
        # Implement splitting logic here if needed
        # For now, just log warning and proceed

    try:
        # Write to BytesIO buffer first
        buffer = BytesIO()

        # Configure compression
        compression = (
            config.compression.value if config.compression != CompressionType.NONE else None
        )

        pq.write_table(
            table,
            buffer,
            compression=compression,
            compression_level=config.compression_level if compression else None,
            row_group_size=min(config.row_group_size, len(all_entries)),
        )

        # Now write buffer to storage via PyIceberg's FileIO
        buffer.seek(0)
        output_file = io.new_output(parquet_path)
        with output_file.create() as stream:
            stream.write(buffer.getvalue())

        elapsed_ms = (time.perf_counter() - start_time) * 1000

        if config.enable_metrics:
            _metrics.record_write(elapsed_ms, len(all_entries), table.nbytes)

        logger.info(
            f"Wrote Parquet manifest: {len(all_entries)} files "
            f"({manifest_size_mb:.1f} MB) to {parquet_path} in {elapsed_ms:.1f}ms"
        )
        return parquet_path

    except Exception as exc:
        logger.error(f"Failed to write Parquet manifest to {parquet_path}: {exc}")
        return None


def write_parquet_manifest_direct(
    snapshot_id: int,
    entries: List[Dict[str, Any]],
    io: FileIO,
    location: str,
    config: ManifestOptimizationConfig | None = None,
) -> str | None:
    """Write a Parquet manifest directly from entries without metadata object.

    This is used when we have snapshot information from TableUpdates but the
    metadata hasn't been updated yet.

    Args:
        snapshot_id: Snapshot ID for the manifest filename
        entries: List of manifest entry dicts
        io: FileIO instance for writing
        location: Table location
        config: Optional manifest optimization configuration

    Returns:
        Path to written Parquet manifest, or None on failure
    """
    config = config or ManifestOptimizationConfig()

    if not entries:
        logger.debug("No entries provided; skipping Parquet manifest write")
        return None

    parquet_path = f"{location}/metadata/manifest-{snapshot_id}.parquet"

    # Check if file already exists
    try:
        input_file = io.new_input(parquet_path)
        with input_file.open() as f:
            logger.debug(f"Parquet manifest already exists at {parquet_path}, skipping write")
            return parquet_path
    except Exception:
        pass  # File doesn't exist, proceed with writing

    logger.info(f"Writing Parquet manifest for snapshot {snapshot_id} with {len(entries)} entries")

    if not validate_manifest_data(entries):
        return None

    # Validate that all referenced data files exist before writing the manifest.
    missing_files = []
    for i, rec in enumerate(entries):
        # Support both flat records and nested `data_file` shapes
        file_path = None
        if isinstance(rec, dict):
            if "file_path" in rec:
                file_path = rec.get("file_path")
            elif "data_file" in rec and isinstance(rec["data_file"], dict):
                file_path = rec["data_file"].get("file_path") or rec["data_file"].get("path")

        if not file_path:
            missing_files.append((i, None))
            continue

        try:
            input_file = io.new_input(file_path)
            # Try opening to confirm existence/readability
            with input_file.open():
                pass
        except Exception:
            missing_files.append((i, file_path))

    if missing_files:
        for idx, path in missing_files:
            if path:
                logger.error(
                    f"Parquet manifest write aborted: referenced data file missing: {path}"
                )
            else:
                logger.error(f"Parquet manifest write aborted: entry {idx} missing file_path")
        return None

    # Convert to Arrow table
    table = pa.Table.from_pylist(entries)

    try:
        # Write to BytesIO buffer first
        buffer = BytesIO()

        compression = (
            config.compression.value if config.compression != CompressionType.NONE else None
        )

        pq.write_table(
            table,
            buffer,
            compression=compression,
            compression_level=config.compression_level if compression else None,
            row_group_size=min(config.row_group_size, len(entries)),
        )

        # Write buffer to storage
        buffer.seek(0)
        output_file = io.new_output(parquet_path)
        with output_file.create() as stream:
            stream.write(buffer.getvalue())

        file_size_mb = buffer.tell() / BYTES_PER_MEGABYTE
        logger.info(
            f"Wrote Parquet manifest {parquet_path} ({len(entries)} entries, {file_size_mb:.2f}MB)"
        )

        return parquet_path

    except Exception as e:
        logger.error(f"Failed to write Parquet manifest: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return None


def read_parquet_manifest(
    metadata: TableMetadataV2,
    io: FileIO,
    location: str,
    config: Optional[ManifestOptimizationConfig] = None,
) -> Optional[List[Dict[str, Any]]]:
    """Read Parquet manifest and return list of DataFile records.

    try:
        # Check if file already exists
        input_file = io.new_input(parquet_path)
        with input_file.open():
            # File exists and is readable
            logger.debug(f"Parquet manifest already exists at {parquet_path}, skipping write")
            return parquet_path
        List of DataFile records as dicts, or None if Parquet manifest doesn't exist
    """
    start_time = time.perf_counter()
    config = config or ManifestOptimizationConfig()

    if not config.enabled:
        logger.debug("Manifest optimization disabled")
        return None

    snapshot = metadata.current_snapshot()
    if not snapshot:
        return None

    parquet_path = f"{location}/metadata/manifest-{snapshot.snapshot_id}.parquet"

    try:
        if config.streaming_read:
            # Use streaming read for better memory efficiency
            input_file = io.new_input(parquet_path)

            # Read metadata first to get row group count
            with input_file.open() as f:
                # Read just the footer to get metadata
                parquet_file = pq.ParquetFile(f)

                # Read in batches if configured for streaming
                batches = []
                for batch in parquet_file.iter_batches():
                    batches.append(batch)

                if batches:
                    table = pa.Table.from_batches(batches)
                else:
                    # Fall back to full read if no batches
                    f.seek(0)
                    table = pq.read_table(f)
        else:
            # Original approach: read entire file
            input_file = io.new_input(parquet_path)
            with input_file.open() as f:
                table = pq.read_table(f)

        read_elapsed = (time.perf_counter() - start_time) * 1000
        logger.debug(
            f"Read Parquet manifest: {len(table)} files from {parquet_path} in {read_elapsed:.1f}ms"
        )

        if config.enable_metrics:
            _metrics.record_read(read_elapsed, len(table))

        # Convert PyArrow table to list of dicts
        records = table.to_pylist()

        # Normalize records written by different code paths:
        # - Older writer (entry_to_dict) writes a flat schema
        # - The Avro->Parquet converter writes nested `data_file` structs
        # Detect nested `data_file` and flatten into the expected shape.
        from .to_int import to_int

        def _kv_list_to_dict(kv_list):
            if not kv_list:
                return None
            out = {}
            for item in kv_list:
                if not isinstance(item, dict):
                    continue
                k = item.get("key")
                v = item.get("value")
                if k is None:
                    continue
                out[int(k)] = v
            return out

        normalized = []
        for rec in records:
            # If the record already has top-level file_path, assume flat schema
            if isinstance(rec, dict) and "file_path" in rec:
                normalized.append(rec)
                continue

            # Handle nested data_file struct
            if isinstance(rec, dict) and "data_file" in rec and isinstance(rec["data_file"], dict):
                df = rec.get("data_file", {})

                # Convert kv-list structures to dicts
                lower_kv = _kv_list_to_dict(df.get("lower_bounds"))
                upper_kv = _kv_list_to_dict(df.get("upper_bounds"))
                nulls_kv = _kv_list_to_dict(df.get("null_value_counts")) or _kv_list_to_dict(
                    df.get("null_counts")
                )
                values_kv = _kv_list_to_dict(df.get("value_counts"))
                colsizes_kv = _kv_list_to_dict(df.get("column_sizes"))
                nans_kv = _kv_list_to_dict(df.get("nan_value_counts")) or _kv_list_to_dict(
                    df.get("nan_counts")
                )

                # Determine max field id
                all_ids = set()
                for d in (lower_kv, upper_kv, nulls_kv, values_kv, colsizes_kv, nans_kv):
                    if d:
                        all_ids.update(d.keys())
                max_id = max(all_ids) if all_ids else -1

                def _to_array(kv_dict):
                    if kv_dict is None:
                        return None
                    arr = [None] * (max_id + 1)
                    for k, v in kv_dict.items():
                        try:
                            arr[int(k)] = to_int(v) if v is not None else None
                        except Exception:
                            arr[int(k)] = None
                    return arr

                lower_array = _to_array(lower_kv)
                upper_array = _to_array(upper_kv)
                null_counts_array = _to_array(nulls_kv)
                value_counts_array = _to_array(values_kv)
                column_sizes_array = _to_array(colsizes_kv)
                nan_counts_array = _to_array(nans_kv)

                # Partition JSON
                partition_json = None
                if df.get("partition"):
                    try:
                        partition_json = json.dumps(
                            {k: _serialize_value(v) for k, v in df.get("partition").items()}
                        )
                    except Exception:
                        partition_json = None

                flat = {
                    "file_path": df.get("file_path") or df.get("path") or df.get("file_path"),
                    "snapshot_id": rec.get("snapshot_id"),
                    "sequence_number": rec.get("sequence_number"),
                    "file_sequence_number": rec.get("file_sequence_number"),
                    "active": (rec.get("status") != 2) if rec.get("status") is not None else True,
                    "partition_spec_id": df.get("spec_id") or df.get("partition_spec_id"),
                    "partition_json": partition_json,
                    "file_format": df.get("file_format"),
                    "record_count": df.get("record_count"),
                    "file_size_bytes": df.get("file_size_in_bytes") or df.get("file_size_bytes"),
                    "lower_bounds": lower_array,
                    "upper_bounds": upper_array,
                    "null_counts": null_counts_array,
                    "value_counts": value_counts_array,
                    "column_sizes": column_sizes_array,
                    "nan_counts": nan_counts_array,
                    "key_metadata": df.get("key_metadata"),
                    "split_offsets_json": json.dumps(df.get("split_offsets"))
                    if df.get("split_offsets")
                    else None,
                    "equality_ids_json": json.dumps(df.get("equality_ids"))
                    if df.get("equality_ids")
                    else None,
                    "sort_order_id": df.get("sort_order_id"),
                    "schema_version": rec.get("schema_version") or None,
                    "field_id_mapping": json.dumps({}),
                }
                normalized.append(flat)
            else:
                # Unknown shape, pass through
                normalized.append(rec)

        records = normalized

        # Validate schema version compatibility
        if records:
            first_record = records[0]
            stored_schema_version = first_record.get("schema_version")
            current_schema_version = metadata.schema().schema_id

            if stored_schema_version and stored_schema_version != current_schema_version:
                logger.warning(
                    f"Schema version mismatch: stored={stored_schema_version}, "
                    f"current={current_schema_version}. Some bounds may not be accurate."
                )

        return records

    except FileNotFoundError:
        logger.debug(f"Parquet manifest not found at {parquet_path}")
        return None
    except Exception as exc:
        logger.warning(f"Failed to read Parquet manifest from {parquet_path}: {exc}")
        return None


def parquet_record_to_data_file(
    record: Dict[str, Any], config: Optional[ManifestOptimizationConfig] = None
):
    """Convert a Parquet record back to a DataFile-like object.

    Args:
        record: Dictionary from Parquet manifest
        config: Optimization configuration

    Returns:
        DataFile or DataFileWithCachedBounds if caching enabled
    """
    config = config or ManifestOptimizationConfig()

    # Parse field mapping for schema evolution
    field_mapping = {}
    if "field_id_mapping" in record and record["field_id_mapping"]:
        try:
            field_mapping = json.loads(record["field_id_mapping"])
        except Exception:
            pass

    # Bounds are stored as int64 values
    lower_bounds = None
    upper_bounds = None

    lower_array = record.get("lower_bounds")
    upper_array = record.get("upper_bounds")

    if lower_array and upper_array:
        lower_bounds = {}
        upper_bounds = {}

        for field_id, min_val in enumerate(lower_array):
            if min_val is not None and field_id < len(upper_array):
                max_val = upper_array[field_id]
                if max_val is not None:
                    # Convert int64 values back to Big Endian bytes (Iceberg format)
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

    # Import DataFile here to avoid circular imports
    from pyiceberg.manifest import DataFile
    from pyiceberg.manifest import DataFileContent
    from pyiceberg.typedef import Record

    # DataFile constructor
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

    # Set spec_id as property
    data_file.spec_id = record.get("partition_spec_id", 0)

    # Wrap with caching if enabled
    if config.cache_unpacked_bounds:
        return DataFileWithCachedBounds(data_file, field_mapping)

    return data_file


def _can_prune_file_with_predicate(
    data_file,
    predicate: Any,
    schema: Any,
    config: ManifestOptimizationConfig,
) -> bool:
    """Check if a data file can be pruned based on a bound predicate.

    Uses BRIN-style min/max pruning with the lower_bounds and upper_bounds
    from the manifest.

    Args:
        data_file: DataFile or DataFileWithCachedBounds object
        predicate: Bound predicate to evaluate
        schema: Table schema for field lookups
        config: Optimization configuration

    Returns:
        True if the file can be pruned, False if it might contain matches
    """
    if not config.enable_pruning:
        return False

    # Check if this is a bound predicate with a term that has a field
    if not hasattr(predicate, "term") or not hasattr(predicate.term, "field"):
        return False

    # Get the field being filtered
    field_id = predicate.term.field.field_id

    print(data_file.get_min_int(field_id), "vs", predicate.literal.value)
    # Only perform numeric/order-preserving pruning for primitive types
    # where our `to_int` mapping preserves order (integers, timestamps, dates, floats).
    # Be conservative for strings and other types to avoid incorrect pruning
    try:
        field = schema.find_field(field_id)
        if field is None:
            return False
        from pyiceberg.types import DateType
        from pyiceberg.types import DoubleType
        from pyiceberg.types import LongType
        from pyiceberg.types import TimestampType
        from pyiceberg.types import TimestamptzType

        if not isinstance(
            field.field_type, (LongType, DoubleType, DateType, TimestampType, TimestamptzType)
        ):
            # Do not prune on string or complex types (case-sensitive ordering issues)
            return False
    except Exception:
        return False
    # Use cached bounds if available
    if hasattr(data_file, "get_min_int"):
        file_min_int = data_file.get_min_int(field_id)
        file_max_int = data_file.get_max_int(field_id)
    else:
        # Check if we have bounds for this field
        if not data_file.lower_bounds or not data_file.upper_bounds:
            return False

        if field_id not in data_file.lower_bounds or field_id not in data_file.upper_bounds:
            return False

        # Get the bounds from bytes
        file_min_bytes = data_file.lower_bounds[field_id]
        file_max_bytes = data_file.upper_bounds[field_id]

        if file_min_bytes is None or file_max_bytes is None:
            return False

        try:
            file_min_int = struct.unpack(">q", file_min_bytes)[0]
            file_max_int = struct.unpack(">q", file_max_bytes)[0]
        except Exception:
            return False

    if file_min_int is None or file_max_int is None:
        return False

    # Convert predicate value to int64
    try:
        if isinstance(
            predicate,
            (
                BoundLessThan,
                BoundLessThanOrEqual,
                BoundGreaterThan,
                BoundGreaterThanOrEqual,
                BoundEqualTo,
            ),
        ):
            pred_value = predicate.literal.value
            pred_value_int = to_int(pred_value)

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

        elif isinstance(predicate, BoundIn):
            # WHERE col IN (values): prune if none of the values overlap with bounds
            values_int = [to_int(v) for v in predicate.literals]
            if all(
                pred_value_int < file_min_int or pred_value_int > file_max_int
                for pred_value_int in values_int
            ):
                return True

        elif isinstance(predicate, NotNull):
            # WHERE col IS NOT NULL: prune if all values are null
            null_count = data_file.null_value_counts.get(field_id, 0)
            if null_count == data_file.record_count:
                return True

    except Exception as exc:
        logger.debug(f"Failed to evaluate predicate for pruning: {exc}")

    return False


def _extract_bound_predicates(expr: BooleanExpression) -> List[Any]:
    """Recursively extract all bound predicate objects from an expression tree.

    Args:
        expr: Boolean expression to extract predicates from

    Returns:
        List of bound predicate objects
    """
    predicates = []

    # Check if this is a bound predicate
    if hasattr(expr, "term") and hasattr(expr, "literal") and hasattr(expr.term, "field"):
        predicates.append(expr)
    elif isinstance(expr, (BoundIn, NotNull)):
        predicates.append(expr)
    elif isinstance(expr, (And, Or)):
        # Recursively extract from left and right
        predicates.extend(_extract_bound_predicates(expr.left))
        predicates.extend(_extract_bound_predicates(expr.right))

    return predicates


def prune_data_files_with_predicates(
    data_files: List,
    row_filter: Union[str, BooleanExpression],
    schema: Any,
    config: Optional[ManifestOptimizationConfig] = None,
) -> Tuple[List, int]:
    """Prune data files based on predicates using BRIN-style min/max filtering.

    Args:
        data_files: List of DataFile objects from manifest
        row_filter: Row filter expression from scan
        schema: Table schema
        config: Optimization configuration

    Returns:
        Tuple of (filtered_files, pruned_count)
    """
    config = config or ManifestOptimizationConfig()

    if not config.enable_pruning:
        return data_files, 0

    # If no filter or ALWAYS_TRUE, no pruning
    if row_filter == ALWAYS_TRUE or isinstance(row_filter, AlwaysTrue):
        return data_files, 0

    # Extract all bound predicates from the filter expression
    predicates = _extract_bound_predicates(row_filter)

    if not predicates:
        return data_files, 0

    # Filter files based on predicates
    filtered_files = []
    pruned_count = 0

    for data_file in data_files:
        can_prune = False

        # For now, use conservative approach: only prune if we're certain
        # This works well for simple predicates and AND combinations
        for predicate in predicates:
            if _can_prune_file_with_predicate(data_file, predicate, schema, config):
                can_prune = True
                predicate_type = predicate.__class__.__name__

                if config.enable_metrics:
                    _metrics.record_prune(predicate_type, 1)

                break

        if can_prune:
            pruned_count += 1
        else:
            filtered_files.append(data_file)

    return filtered_files, pruned_count


class OptimizedStaticTable(StaticTable):
    """StaticTable that uses Parquet manifests for fast query planning."""

    def __init__(self, *args, **kwargs):
        """Initialize with optional configuration."""
        self._manifest_config = kwargs.pop("manifest_config", ManifestOptimizationConfig())
        super().__init__(*args, **kwargs)

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
            full_history_loaded=getattr(self, "full_history_loaded", True),
            manifest_config=self._manifest_config,
        )


class OptimizedDataScan(DataScan):
    """DataScan that uses Parquet manifests for fast file planning."""

    def __init__(self, *args, **kwargs):
        self.full_history_loaded = kwargs.pop("full_history_loaded", True)
        self._manifest_config = kwargs.pop("manifest_config", ManifestOptimizationConfig())
        super().__init__(*args, **kwargs)

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get metrics summary for this scan."""
        if self._manifest_config.enable_metrics:
            return _metrics.get_summary()
        return {}

    def plan_files(self) -> Iterable[FileScanTask]:
        """Plan files using Parquet manifest if available; never fallback to Avro.

        If no Parquet manifest exists or conversion fails, return an empty list
        rather than attempting Avro reads.
        """
        start_time = time.perf_counter()

        # Warn if time travel is requested without full history loaded
        snapshot_id_requested = getattr(self, "snapshot_id", None)
        if snapshot_id_requested is not None and not self.full_history_loaded:
            logger.warning(
                f"Time travel requested (snapshot_id={snapshot_id_requested}) but table was opened "
                "without include_full_history=True; metadata may be incomplete."
            )

        # Try to read from Parquet manifest first
        parquet_records = read_parquet_manifest(
            self.table_metadata,
            self.io,
            self.table_metadata.location,
            self._manifest_config,
        )
        if parquet_records is not None:
            read_elapsed = (time.perf_counter() - start_time) * 1000

            # Convert Parquet records to DataFile objects
            data_files = []
            for record in parquet_records:
                if record.get("active", True):  # Only include active files
                    try:
                        data_file = parquet_record_to_data_file(record, self._manifest_config)
                        data_files.append(data_file)
                    except Exception as exc:
                        logger.warning(f"Failed to convert Parquet record: {exc}")
                        # Do not perform Avro reads; return no files
                        logger.info(
                            "Query planning: Parquet manifest conversion failed; Avro disabled"
                        )
                        return []

            conversion_elapsed = (time.perf_counter() - start_time) * 1000

            # Apply BRIN-style pruning based on predicates
            row_filter = self.row_filter

            # Bind the row filter to the schema
            if row_filter != ALWAYS_TRUE and not isinstance(row_filter, AlwaysTrue):
                bound_filter = bind(self.table_metadata.schema(), row_filter, case_sensitive=True)
            else:
                bound_filter = row_filter

            data_files, pruned_count = prune_data_files_with_predicates(
                data_files,
                bound_filter,
                self.table_metadata.schema(),
                self._manifest_config,
            )

            pruning_elapsed = (time.perf_counter() - start_time) * 1000 - conversion_elapsed

            # Create FileScanTask objects
            tasks = []
            for data_file in data_files:
                # Extract the actual data_file if it's wrapped
                actual_data_file = (
                    data_file.data_file if hasattr(data_file, "data_file") else data_file
                )

                # Verify the referenced data file exists before creating a scan task.
                try:
                    input_file = self.io.new_input(actual_data_file.file_path)
                    if not input_file.exists():
                        logger.warning(
                            f"Missing data file referenced in manifest: {actual_data_file.file_path}; skipping"
                        )
                        continue
                except Exception as e:
                    logger.warning(
                        f"Error checking existence of data file {getattr(actual_data_file, 'file_path', '<unknown>')}: {e}; skipping"
                    )
                    continue

                task = FileScanTask(
                    data_file=actual_data_file,
                    delete_files=set(),  # No delete files
                    start=0,
                    length=actual_data_file.file_size_in_bytes,
                )
                tasks.append(task)

            total_elapsed = (time.perf_counter() - start_time) * 1000

            if pruned_count > 0:
                message = (
                    f"Query planning: ✓ Using PARQUET manifest "
                    f"({len(tasks)} files, {pruned_count} pruned, "
                    f"{total_elapsed:.1f}ms total, {read_elapsed:.1f}ms read, "
                    f"{pruning_elapsed:.1f}ms pruning)"
                )
            else:
                message = (
                    f"Query planning: ✓ Using PARQUET manifest "
                    f"({len(tasks)} files, {total_elapsed:.1f}ms total, "
                    f"{read_elapsed:.1f}ms read)"
                )
            logger.info(message)

            return tasks

        # If we reach here, parquet_records is None or unavailable.
        # Per catalog policy, never fall back to Avro manifests. Return
        # an empty iterable so planning proceeds without reading Avro.
        logger.info(
            "Query planning: Parquet manifest unavailable; Avro disabled, returning no files"
        )
        return []


# Public API functions
def write_manifest(
    metadata: TableMetadataV2,
    io: FileIO,
    location: str,
    config: Optional[ManifestOptimizationConfig] = None,
) -> Optional[str]:
    """Public API for writing Parquet manifests."""
    return write_parquet_manifest(metadata, io, location, config)


def get_metrics_summary() -> Dict[str, Any]:
    """Get global metrics summary."""
    return _metrics.get_summary()


def reset_metrics():
    """Reset all metrics (for testing)."""
    global _metrics
    _metrics = ManifestMetrics()
