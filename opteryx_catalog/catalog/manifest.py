from __future__ import annotations

import logging
import time
from collections import Counter
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict

NULL_FLAG = -(1 << 63)
MIN_K_HASHES = 32
HISTOGRAM_BINS = 32


@dataclass
class DataFile:
    file_path: str
    file_format: str = "PARQUET"
    record_count: int = 0
    file_size_in_bytes: int = 0
    partition: Dict[str, object] = field(default_factory=dict)
    lower_bounds: Dict[int, bytes] | None = None
    upper_bounds: Dict[int, bytes] | None = None


@dataclass
class ManifestEntry:
    snapshot_id: int
    data_file: DataFile
    status: str = "added"  # 'added' | 'deleted'


@dataclass
class ParquetManifestEntry:
    """Represents a single entry in a Parquet manifest with statistics."""

    file_path: str
    file_format: str
    record_count: int
    file_size_in_bytes: int
    uncompressed_size_in_bytes: int
    column_uncompressed_sizes_in_bytes: list[int]
    null_counts: list[int]
    min_k_hashes: list[list[int]]
    histogram_counts: list[list[int]]
    histogram_bins: int
    min_values: list
    max_values: list
    min_values_display: list
    max_values_display: list

    def to_dict(self) -> dict:
        return {
            "file_path": self.file_path,
            "file_format": self.file_format,
            "record_count": self.record_count,
            "file_size_in_bytes": self.file_size_in_bytes,
            "uncompressed_size_in_bytes": self.uncompressed_size_in_bytes,
            "column_uncompressed_sizes_in_bytes": self.column_uncompressed_sizes_in_bytes,
            "null_counts": self.null_counts,
            "min_k_hashes": self.min_k_hashes,
            "histogram_counts": self.histogram_counts,
            "histogram_bins": self.histogram_bins,
            "min_values": self.min_values,
            "max_values": self.max_values,
            "min_values_display": self.min_values_display,
            "max_values_display": self.max_values_display,
        }


logger = logging.getLogger(__name__)
_manifest_metrics = Counter()


def _compute_stats_for_arrow_column(col, field_type, file_path: str):
    """Compute statistics for a single PyArrow column (Array or ChunkedArray).

    Returns a tuple: (col_min_k, col_hist, col_min, col_max, min_display, max_display, null_count)
    """
    import heapq

    import opteryx.draken as draken  # type: ignore
    import pyarrow as pa

    # Ensure single contiguous array when possible
    if hasattr(col, "combine_chunks"):
        try:
            col = col.combine_chunks()
        except Exception:
            # leave as-is
            pass

    # Record compress/hash usage
    _manifest_metrics["hash_calls"] += 1
    _manifest_metrics["compress_calls"] += 1

    col_py = None
    try:
        vec = draken.Vector.from_arrow(col)
    except Exception:  # pragma: no cover - be robust
        raise

    hashes = set(vec.hash())

    # Decide whether to compute min-k/histogram for this column
    compute_min_k = False
    if (
        pa.types.is_integer(field_type)
        or pa.types.is_floating(field_type)
        or pa.types.is_decimal(field_type)
    ):
        compute_min_k = True
    elif (
        pa.types.is_timestamp(field_type)
        or pa.types.is_date(field_type)
        or pa.types.is_time(field_type)
    ):
        compute_min_k = True
    elif (
        pa.types.is_string(field_type)
        or pa.types.is_large_string(field_type)
        or pa.types.is_binary(field_type)
        or pa.types.is_large_binary(field_type)
    ):
        # For strings/binary we may need pylist for display
        try:
            col_py = col.to_pylist()
        except Exception:
            col_py = None
        compute_min_k = True

    if compute_min_k:
        smallest = heapq.nsmallest(MIN_K_HASHES, hashes)
        col_min_k = sorted(smallest)
    else:
        col_min_k = []

    import pyarrow as pa  # local import for types

    compute_hist = compute_min_k
    if pa.types.is_boolean(field_type):
        compute_hist = True

    # Use draken.compress() to get canonical int64 per value
    compressed = list(vec.compress())
    null_count = sum(1 for m in compressed if m == NULL_FLAG)

    non_nulls_compressed = [m for m in compressed if m != NULL_FLAG]
    if non_nulls_compressed:
        vmin = min(non_nulls_compressed)
        vmax = max(non_nulls_compressed)
        col_min = int(vmin)
        col_max = int(vmax)
        if compute_hist:
            # Special-case boolean histograms
            if pa.types.is_boolean(field_type):
                try:
                    if col_py is None:
                        try:
                            col_py = col.to_pylist()
                        except Exception:
                            col_py = None
                    if col_py is not None:
                        non_nulls_bool = [v for v in col_py if v is not None]
                        false_count = sum(1 for v in non_nulls_bool if v is False)
                        true_count = sum(1 for v in non_nulls_bool if v is True)
                    else:
                        # Fallback: infer from compressed mapping (assume 0/1)
                        false_count = sum(1 for m in non_nulls_compressed if m == 0)
                        true_count = sum(1 for m in non_nulls_compressed if m != 0)
                except Exception:
                    false_count = 0
                    true_count = 0

                col_hist = [int(true_count), int(false_count)]
            else:
                if vmin == vmax:
                    col_hist = []
                else:
                    col_hist = [0] * HISTOGRAM_BINS
                    span = float(vmax - vmin)
                    for m in non_nulls_compressed:
                        b = int(((float(m) - float(vmin)) / span) * (HISTOGRAM_BINS - 1))
                        if b < 0:
                            b = 0
                        if b >= HISTOGRAM_BINS:
                            b = HISTOGRAM_BINS - 1
                        col_hist[b] += 1
        else:
            col_hist = []
    else:
        # no non-null values
        col_min = NULL_FLAG
        col_max = NULL_FLAG
        col_hist = []

    # display values
    try:
        if pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
            if col_py is None:
                try:
                    col_py = col.to_pylist()
                except Exception:
                    col_py = None
            if col_py is not None:
                non_nulls_str = [x for x in col_py if x is not None]
                if non_nulls_str:
                    min_value = min(non_nulls_str)
                    max_value = max(non_nulls_str)
                    if len(min_value) > 16:
                        min_value = min_value[:16] + "..."
                    if len(max_value) > 16:
                        max_value = max_value[:16] + "..."
                    min_display = min_value
                    max_display = max_value
                else:
                    min_display = None
                    max_display = None
            else:
                min_display = None
                max_display = None
        elif pa.types.is_binary(field_type) or pa.types.is_large_binary(field_type):
            if col_py is None:
                try:
                    col_py = col.to_pylist()
                except Exception:
                    col_py = None
            if col_py is not None:
                non_nulls = [x for x in col_py if x is not None]
                if non_nulls:
                    min_value = min(non_nulls)
                    max_value = max(non_nulls)
                    if len(min_value) > 16:
                        min_value = min_value[:16] + "..."
                    if len(max_value) > 16:
                        max_value = max_value[:16] + "..."
                    if any(ord(b) < 32 or ord(b) > 126 for b in min_value):
                        min_value = min_value.hex()
                        min_value = min_value[:16] + "..."
                    if any(ord(b) < 32 or ord(b) > 126 for b in max_value):
                        max_value = max_value.hex()
                        max_value = max_value[:16] + "..."
                    min_display = min_value
                    max_display = max_value
                else:
                    min_display = None
                    max_display = None
            else:
                min_display = None
                max_display = None
        else:
            if col_py is None:
                try:
                    col_py = col.to_pylist()
                except Exception:
                    col_py = None
            if col_py is not None:
                non_nulls = [x for x in col_py if x is not None]
                if non_nulls:
                    min_display = min(non_nulls)
                    max_display = max(non_nulls)
                else:
                    min_display = None
                    max_display = None
            else:
                min_display = None
                max_display = None
    except Exception:
        min_display = None
        max_display = None

    return (
        col_min_k,
        col_hist,
        int(col_min),
        int(col_max),
        min_display,
        max_display,
        int(null_count),
    )


def build_parquet_manifest_entry_from_bytes(
    data_bytes: bytes,
    file_path: str,
    file_size_in_bytes: int | None = None,
    orig_table: Any | None = None,
) -> ParquetManifestEntry:
    """Build a manifest entry by reading a parquet file as bytes and scanning column-by-column.

    This reads the compressed file once and materializes one full column at a time
    (combine_chunks) which keeps peak memory low while letting per-column
    stat calculation (draken) operate on contiguous arrays.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    t_start = time.perf_counter()
    _manifest_metrics["files_read"] += 1
    _manifest_metrics["bytes_read"] += len(data_bytes)

    buf = pa.BufferReader(data_bytes)
    pf = pq.ParquetFile(buf)
    meta = pf.metadata

    # Try to read rugo metadata early so we can compute sizes without
    # materializing the table later. This is zero-copy and fast.
    try:
        from opteryx.rugo.parquet import read_metadata_from_memoryview

        rmeta = read_metadata_from_memoryview(memoryview(data_bytes))
    except Exception:
        rmeta = None

    # Prepare result containers
    min_k_hashes: list[list[int]] = []
    histograms: list[list[int]] = []
    min_values: list[int] = []
    null_counts: list[int] = []
    max_values: list[int] = []
    min_values_display: list = []
    max_values_display: list = []

    # iterate schema fields and process each column independently
    schema = pf.schema_arrow
    for col_idx, field in enumerate(schema):
        col_name = field.name
        try:
            col_table = pf.read(columns=[col_name])
            col = col_table.column(0).combine_chunks()
        except Exception:
            # fallback: try reading the row group column (more granular)
            try:
                tbl = pf.read_row_group(0, columns=[col_name])
                col = tbl.column(0).combine_chunks()
            except Exception:
                # Last resort: read entire file and then take the column
                tbl = pf.read()
                col = tbl.column(col_idx).combine_chunks()

        # compute stats using existing logic encapsulated in helper
        (
            col_min_k,
            col_hist,
            col_min,
            col_max,
            col_min_display,
            col_max_display,
            null_count,
        ) = _compute_stats_for_arrow_column(col, field.type, file_path)

        # free the table-level reference if present so memory can be reclaimed
        try:
            del col_table
        except Exception:
            pass
        try:
            del tbl
        except Exception:
            pass

        min_k_hashes.append(col_min_k)
        histograms.append(col_hist)
        min_values.append(col_min)
        max_values.append(col_max)
        min_values_display.append(col_min_display)
        max_values_display.append(col_max_display)
        null_counts.append(null_count)

    # Calculate uncompressed sizes. When the original in-memory table is
    # available (we just wrote it), prefer using it so sizes match the
    # table-based builder exactly. Otherwise materialize the table from
    # bytes and compute sizes the same way.
    import pyarrow as pa
    import pyarrow.parquet as pq

    column_uncompressed: list[int] = []
    uncompressed_size = 0

    # Free references to large objects we no longer need so memory can be reclaimed
    try:
        del buf
    except Exception:
        pass
    try:
        del pf
    except Exception:
        pass
    try:
        del data_bytes
    except Exception:
        pass

    if orig_table is not None:
        # Use the original table buffers so results match the table-based route
        for col in orig_table.columns:
            col_total = 0
            for chunk in col.chunks:
                try:
                    buffs = chunk.buffers()
                except Exception as exc:
                    raise RuntimeError(
                        f"Unable to access chunk buffers to calculate uncompressed size for {file_path}: {exc}"
                    ) from exc
                for buffer in buffs:
                    if buffer is not None:
                        col_total += buffer.size
            column_uncompressed.append(int(col_total))
            uncompressed_size += col_total
    else:
        # Use rugo metadata (if available) to compute per-column uncompressed sizes
        if rmeta:
            rgs = rmeta.get("row_groups", [])
            if rgs:
                ncols = len(rgs[0].get("columns", []))
                for cidx in range(ncols):
                    col_total = 0
                    for rg in rgs:
                        cols = rg.get("columns", [])
                        if cidx < len(cols):
                            col_total += int(cols[cidx].get("total_byte_size", 0) or 0)
                    column_uncompressed.append(int(col_total))
                    uncompressed_size += col_total
                _manifest_metrics["sizes_from_rugo"] += 1
            else:
                column_uncompressed = [0] * len(schema)
                uncompressed_size = 0
                _manifest_metrics["sizes_from_rugo_missing"] += 1
        else:
            # If rugo metadata isn't available, avoid materializing the table;
            # emit zero sizes (safe and memory-light) and track that we lacked
            # metadata for sizes.
            column_uncompressed = [0] * len(schema)
            uncompressed_size = 0
            _manifest_metrics["sizes_from_rugo_unavailable"] += 1
            logger.debug(
                "rugo metadata unavailable for %s; emitting zero column sizes to avoid materializing table",
                file_path,
            )

    entry = ParquetManifestEntry(
        file_path=file_path,
        file_format="parquet",
        record_count=int(meta.num_rows),
        file_size_in_bytes=int(file_size_in_bytes or len(data_bytes)),
        uncompressed_size_in_bytes=uncompressed_size,
        column_uncompressed_sizes_in_bytes=column_uncompressed,
        null_counts=null_counts,
        min_k_hashes=min_k_hashes,
        histogram_counts=histograms,
        histogram_bins=HISTOGRAM_BINS,
        min_values=min_values,
        max_values=max_values,
        min_values_display=min_values_display,
        max_values_display=max_values_display,
    )

    logger.debug(
        "build_parquet_manifest_entry_from_bytes %s files=%d dur=%.3fs",
        file_path,
        _manifest_metrics["files_read"],
        time.perf_counter() - t_start,
    )
    return entry


# Backwards-compatible wrapper that keeps the original calling convention
# when a pyarrow Table is already provided (tests and some scripts rely on it).
def build_parquet_manifest_entry(
    table: Any, file_path: str, file_size_in_bytes: int | None = None
) -> ParquetManifestEntry:
    """DEPRECATED: explicit table-based manifest building is removed.

    The implementation previously accepted a PyArrow ``table`` and performed
    the same per-column statistics calculation. That behavior hid a different
    IO/scan path and led to inconsistent performance characteristics.

    Use ``build_parquet_manifest_entry_from_bytes(data_bytes, file_path, file_size_in_bytes, orig_table=None)``
    instead. If you have an in-memory table you can serialize it and call the
    bytes-based builder, or pass ``orig_table`` to preserve exact uncompressed
    size calculations.

    This function now fails fast to avoid silently using the removed path.
    """
    raise RuntimeError(
        "table-based manifest builder removed: use build_parquet_manifest_entry_from_bytes(data_bytes, file_path, file_size_in_bytes, orig_table=table) instead"
    )


def get_manifest_metrics() -> dict:
    """Return a snapshot of manifest instrumentation counters (for tests/benchmarks)."""
    return dict(_manifest_metrics)


def reset_manifest_metrics() -> None:
    """Reset the manifest metrics counters to zero."""
    _manifest_metrics.clear()
