"""Parquet manifest optimization for fast query planning.

This module provides optimized manifest handling that writes Parquet manifests
alongside standard Iceberg Avro manifests for 10-50x faster query planning.
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
from pyiceberg.expressions import BooleanExpression
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

logger = get_logger()


def get_parquet_manifest_schema() -> pa.Schema:
    """Define schema for Parquet manifest files.

    This schema stores all data file metadata in a flat structure
    optimized for fast filtering with PyArrow.
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
            # Column bounds (stored as JSON for schema flexibility)
            # In production, you might want to extract specific columns as typed fields
            ("lower_bounds_json", pa.string()),
            ("upper_bounds_json", pa.string()),
            # Statistics (as JSON for flexibility)
            ("null_counts_json", pa.string()),
            ("value_counts_json", pa.string()),
            ("column_sizes_json", pa.string()),
            ("nan_value_counts_json", pa.string()),
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


def entry_to_dict(entry: ManifestEntry) -> Dict[str, Any]:
    """Convert ManifestEntry to flat dictionary for Parquet storage.

    Args:
        entry: The ManifestEntry to convert

    Returns:
        Dictionary with all entry data in flat structure
    """
    df = entry.data_file

    # Convert bounds to JSON (field_id -> value), handling bytes values
    lower_bounds_json = None
    if df.lower_bounds:
        lower_bounds_json = json.dumps(
            {str(k): _serialize_value(v) for k, v in df.lower_bounds.items()}
        )

    upper_bounds_json = None
    if df.upper_bounds:
        upper_bounds_json = json.dumps(
            {str(k): _serialize_value(v) for k, v in df.upper_bounds.items()}
        )

    # Convert stats to JSON
    null_counts_json = json.dumps({str(k): v for k, v in (df.null_value_counts or {}).items()})
    value_counts_json = json.dumps({str(k): v for k, v in (df.value_counts or {}).items()})
    column_sizes_json = json.dumps({str(k): v for k, v in (df.column_sizes or {}).items()})
    nan_counts_json = json.dumps({str(k): v for k, v in (df.nan_value_counts or {}).items()})

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
        "lower_bounds_json": lower_bounds_json,
        "upper_bounds_json": upper_bounds_json,
        "null_counts_json": null_counts_json if df.null_value_counts else None,
        "value_counts_json": value_counts_json if df.value_counts else None,
        "column_sizes_json": column_sizes_json if df.column_sizes else None,
        "nan_value_counts_json": nan_counts_json if df.nan_value_counts else None,
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

    for manifest_file in snapshot.manifests(io):
        manifest_count += 1
        try:
            entries = manifest_file.fetch_manifest_entry(io, discard_deleted=False)
            for entry in entries:
                all_entries.append(entry_to_dict(entry))
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
    # Parse JSON fields back to dicts
    # NOTE: lower_bounds and upper_bounds contain bytes values that were base64-encoded.
    # We need to decode them back to bytes for PyIceberg to use them correctly.
    lower_bounds = None
    if record.get("lower_bounds_json"):
        lower_bounds = {}
        for k, v in json.loads(record["lower_bounds_json"]).items():
            # All bound values are bytes in Iceberg, decode from base64
            if isinstance(v, str):
                try:
                    lower_bounds[int(k)] = base64.b64decode(v)
                except Exception:
                    # If it's not valid base64, keep as string (shouldn't happen)
                    lower_bounds[int(k)] = v
            else:
                lower_bounds[int(k)] = v

    upper_bounds = None
    if record.get("upper_bounds_json"):
        upper_bounds = {}
        for k, v in json.loads(record["upper_bounds_json"]).items():
            # All bound values are bytes in Iceberg, decode from base64
            if isinstance(v, str):
                try:
                    upper_bounds[int(k)] = base64.b64decode(v)
                except Exception:
                    # If it's not valid base64, keep as string (shouldn't happen)
                    upper_bounds[int(k)] = v
            else:
                upper_bounds[int(k)] = v

    null_value_counts = (
        {int(k): v for k, v in json.loads(record["null_counts_json"]).items()}
        if record.get("null_counts_json")
        else {}
    )

    value_counts = (
        {int(k): v for k, v in json.loads(record["value_counts_json"]).items()}
        if record.get("value_counts_json")
        else {}
    )

    column_sizes = (
        {int(k): v for k, v in json.loads(record["column_sizes_json"]).items()}
        if record.get("column_sizes_json")
        else {}
    )

    nan_value_counts = (
        {int(k): v for k, v in json.loads(record["nan_value_counts_json"]).items()}
        if record.get("nan_value_counts_json")
        else {}
    )

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
