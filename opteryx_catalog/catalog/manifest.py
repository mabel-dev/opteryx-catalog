from __future__ import annotations

import logging
import time
from collections import Counter, OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict

NULL_FLAG = -(1 << 63)
MIN_K_HASHES = 32
HISTOGRAM_BINS = 32

# Performance tuning parameters
ENABLE_BATCH_COLUMN_READS = True  # Read all columns at once before falling back
PYLIST_CONVERSION_CACHE = True  # Cache to_pylist() results per column

# Manifest retrieval optimization
ENABLE_LAZY_MANIFEST = (
    True  # Use Arrow format for planning instead of converting to Python
)
MANIFEST_COLUMNS_FOR_PLANNING = [  # Only read these columns for query planning
    "file_path",
    "record_count",
    "null_counts",
    "min_k_hashes",
    "histogram_counts",
    "histogram_bins",
    "min_values",
    "max_values",
    "min_values_display",
    "max_values_display",
    "column_uncompressed_sizes_in_bytes",
    "min_lengths",
    "max_lengths",
]

# Parsed manifest cache (LRU) for Arrow tables (faster than Python dicts)
_arrow_manifest_cache: "OrderedDict[str, Any]" = OrderedDict()


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
    min_lengths: list[int]
    max_lengths: list[int]

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
            "min_lengths": self.min_lengths,
            "max_lengths": self.max_lengths,
        }


logger = logging.getLogger(__name__)
_manifest_metrics = Counter()

# Parsed-manifest cache (LRU): store parsed Python representation (list[dict])
# to avoid repeated pyarrow parsing and expensive to_pylist() conversions.
# Entries are "frozen" for memory efficiency (inner lists -> tuples).
PARSED_MANIFEST_CACHE_SIZE: int = 32
_parsed_manifest_cache: "OrderedDict[str, list]" = OrderedDict()


def _freeze_for_cache(value):
    """Recursively freeze lists to tuples and convert byte-like to bytes.

    Keeps top-level entries as dicts (callers expect Mapping access) but
    replaces inner mutable lists with tuples to reduce memory overhead and
    prevent accidental mutation of cached data.
    """
    # Primitive/bytes
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value

    # Lists -> tuples (recursive)
    if isinstance(value, list):
        return tuple(_freeze_for_cache(v) for v in value)

    # Dicts -> keep as dict but freeze values
    if isinstance(value, dict):
        return {k: _freeze_for_cache(v) for k, v in value.items()}

    # Fallback: return as-is
    return value


def get_parsed_manifest(io, manifest_path: str) -> list:
    """Return a cached Python representation (list[dict]) of the Parquet manifest.

    - Uses an in-memory LRU cache keyed by `manifest_path`.
    - Cached entries are frozen (inner lists -> tuples) to reduce memory.
    - Callers MUST treat returned lists/dicts as read-only.
    - Optimized: uses selective column reading and lazy conversion when available.
    """

    if not manifest_path:
        return []

    # Fast path: cache hit
    if manifest_path in _parsed_manifest_cache:
        _parsed_manifest_cache.move_to_end(manifest_path)
        _manifest_metrics["parsed_cache_hits"] += 1
        return _parsed_manifest_cache[manifest_path]

    # Miss: read bytes -> parse -> freeze -> cache
    import pyarrow as pa
    import pyarrow.parquet as pq

    inp = io.new_input(manifest_path)
    try:
        with inp.open() as f:
            data = f.read()
    except FileNotFoundError:
        # keep behavior consistent with callers
        raise

    if not data:
        _manifest_metrics["parsed_cache_misses"] += 1
        _parsed_manifest_cache[manifest_path] = []
        # Evict oldest if needed
        if len(_parsed_manifest_cache) > PARSED_MANIFEST_CACHE_SIZE:
            _parsed_manifest_cache.popitem(last=False)
        return []

    # Optimized: try selective column reading first
    frozen_rows = _parse_manifest_optimized(data)

    _parsed_manifest_cache[manifest_path] = frozen_rows
    _manifest_metrics["parsed_cache_misses"] += 1

    # Evict oldest if cache exceeds size
    if len(_parsed_manifest_cache) > PARSED_MANIFEST_CACHE_SIZE:
        _parsed_manifest_cache.popitem(last=False)

    return _parsed_manifest_cache[manifest_path]


def _parse_manifest_optimized(data: bytes) -> list:
    """Parse manifest parquet bytes with selective column reading.

    Reads only columns needed for planning, reducing conversion overhead.
    Falls back to full read if selective read fails.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    buf = pa.BufferReader(data)
    pf = pq.ParquetFile(buf)
    schema = pf.schema_arrow

    # Get available column names
    available_cols = {field.name for field in schema}

    # Determine which columns to read (only read what planning needs)
    cols_to_read = [
        col for col in MANIFEST_COLUMNS_FOR_PLANNING if col in available_cols
    ]

    if not cols_to_read:
        # Fallback: read all columns
        cols_to_read = None

    try:
        # Read only the needed columns - significantly faster for wide manifests
        if cols_to_read:
            table = pq.read_table(buf, columns=cols_to_read)
            _manifest_metrics["selective_column_reads"] += 1
        else:
            table = pq.read_table(buf)
            _manifest_metrics["full_column_reads"] += 1

        # Minimize to_pylist() overhead by batch converting
        rows = table.to_pylist()

        # Freeze inner mutable structures to tuples
        frozen_rows: list = []
        for r in rows:
            if isinstance(r, dict):
                fr = {k: _freeze_for_cache(v) for k, v in r.items()}
                frozen_rows.append(fr)
            else:
                frozen_rows.append(r)

        return frozen_rows
    except Exception as e:
        # Fallback: try reading entire manifest without column selection
        logger.debug(
            f"Selective column read failed for manifest: {e}, falling back to full read"
        )
        try:
            buf = pa.BufferReader(data)
            table = pq.read_table(buf)
            rows = table.to_pylist()
            frozen_rows: list = []
            for r in rows:
                if isinstance(r, dict):
                    fr = {k: _freeze_for_cache(v) for k, v in r.items()}
                    frozen_rows.append(fr)
                else:
                    frozen_rows.append(r)
            return frozen_rows
        except Exception:
            # Last resort: return empty
            return []


def invalidate_parsed_manifest(manifest_path: str) -> None:
    """Remove a manifest from the parsed-manifest cache (if present)."""
    _parsed_manifest_cache.pop(manifest_path, None)


def clear_parsed_manifest_cache() -> None:
    """Clear the entire parsed-manifest cache (tests / admin use)."""
    _parsed_manifest_cache.clear()


def _compute_stats_for_arrow_column(col, field_type, file_path: str):
    """Compute statistics for a single PyArrow column (Array or ChunkedArray).

    Returns a tuple: (col_min_k, col_hist, col_min, col_max, min_display, max_display, null_count, min_len, max_len)

    Optimized to call to_pylist() only once if needed, caching the result for all uses.
    """
    import heapq

    import draken  # type: ignore
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

    if hasattr(col, "combine_chunks"):
        col = col.combine_chunks()
    vec = draken.Vector.from_arrow(col)

    hashes = set(vec.hash())

    # Decide whether to compute min-k/histogram and if we need pylist upfront
    needs_pylist = False
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
        needs_pylist = True
    elif (
        pa.types.is_string(field_type)
        or pa.types.is_large_string(field_type)
        or pa.types.is_binary(field_type)
        or pa.types.is_large_binary(field_type)
    ):
        compute_min_k = True
        needs_pylist = True

    if pa.types.is_boolean(field_type):
        needs_pylist = True

    # Single to_pylist() call if needed (major optimization)
    col_py = None
    if needs_pylist:
        try:
            col_py = col.to_pylist()
        except Exception:
            col_py = None

    if compute_min_k:
        smallest = heapq.nsmallest(MIN_K_HASHES, hashes)
        col_min_k = sorted(smallest)
    else:
        col_min_k = []

    # Use draken.compress() to get canonical int64 per value
    compressed = list(vec.compress())
    null_count = sum(1 for m in compressed if m == NULL_FLAG)

    non_nulls_compressed = [m for m in compressed if m != NULL_FLAG]
    if non_nulls_compressed:
        vmin = min(non_nulls_compressed)
        vmax = max(non_nulls_compressed)
        col_min = int(vmin)
        col_max = int(vmax)

        # Compute histogram if needed
        compute_hist = compute_min_k
        if pa.types.is_boolean(field_type):
            compute_hist = True

        if compute_hist:
            # Special-case boolean histograms
            if pa.types.is_boolean(field_type):
                try:
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
                        b = int(
                            ((float(m) - float(vmin)) / span) * (HISTOGRAM_BINS - 1)
                        )
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

    # Compute display values using cached col_py
    min_display = None
    max_display = None
    try:
        if pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
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
        elif pa.types.is_binary(field_type) or pa.types.is_large_binary(field_type):
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
        elif (
            pa.types.is_date(field_type)
            or pa.types.is_timestamp(field_type)
            or pa.types.is_time(field_type)
        ):
            if col_py is not None:
                non_nulls = [x for x in col_py if x is not None]
                if non_nulls:
                    min_val = min(non_nulls)
                    max_val = max(non_nulls)
                    # Convert to ISO format strings for proper display
                    try:
                        min_display = (
                            min_val.isoformat()
                            if hasattr(min_val, "isoformat")
                            else str(min_val)
                        )
                        max_display = (
                            max_val.isoformat()
                            if hasattr(max_val, "isoformat")
                            else str(max_val)
                        )
                    except Exception:
                        min_display = str(min_val)
                        max_display = str(max_val)
    except Exception:
        min_display = None
        max_display = None

    # Compute min/max lengths for variable-width types
    min_len = 0
    max_len = 0
    is_variable_width = (
        pa.types.is_string(field_type)
        or pa.types.is_large_string(field_type)
        or pa.types.is_binary(field_type)
        or pa.types.is_large_binary(field_type)
        or pa.types.is_list(field_type)
        or pa.types.is_large_list(field_type)
    )

    if is_variable_width and col_py is not None:
        try:
            lengths = [len(v) for v in col_py if v is not None]
            if lengths:
                min_len = min(lengths)
                max_len = max(lengths)
        except Exception:
            min_len = 0
            max_len = 0

    return (
        col_min_k,
        col_hist,
        int(col_min),
        int(col_max),
        min_display,
        max_display,
        int(null_count),
        min_len,
        max_len,
    )


def build_parquet_manifest_entry_from_bytes(
    data_bytes: bytes,
    file_path: str,
    file_size_in_bytes: int | None = None,
    orig_table: Any | None = None,
) -> ParquetManifestEntry:
    """Build a manifest entry by reading a parquet file as bytes and scanning column-by-column.

    This reads the compressed file once and processes one full column at a time.
    Columns stay chunked here so very large binary/string columns do not overflow
    Arrow's 32-bit offsets during eager concatenation; the stats helper still
    attempts a best-effort ``combine_chunks()`` when that is safe.

    Optimized with:
    - Batch column reading (all columns at once) before fallback
    - Early memory release of large objects
    - Reduced to_pylist() calls in stats computation
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    t_start = time.perf_counter()
    _manifest_metrics["files_read"] += 1
    _manifest_metrics["bytes_read"] += len(data_bytes)
    data_bytes_len = len(data_bytes)

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
    min_lengths_list: list[int] = []
    max_lengths_list: list[int] = []

    # Optimized: Try batch read of all columns first (faster for small column counts)
    schema = pf.schema_arrow
    batch_table = None
    use_batch = ENABLE_BATCH_COLUMN_READS and len(schema) <= 128

    if use_batch:
        try:
            batch_table = pf.read()
            _manifest_metrics["batch_reads"] += 1
        except Exception:
            batch_table = None

    # iterate schema fields and process each column independently
    for col_idx, col_field in enumerate(schema):
        col_name = col_field.name
        col = None
        col_table = None
        tbl = None

        try:
            if batch_table is not None:
                # Fast path: column already in memory from batch read
                col = batch_table.column(col_idx)
            else:
                # Original fallback strategy
                try:
                    col_table = pf.read(columns=[col_name])
                    col = col_table.column(0)
                except Exception:
                    # fallback: try reading the row group column (more granular)
                    try:
                        tbl = pf.read_row_group(0, columns=[col_name])
                        col = tbl.column(0)
                    except Exception:
                        # Last resort: read entire file and then take the column
                        tbl = pf.read()
                        col = tbl.column(col_idx)

            # compute stats using existing logic encapsulated in helper
            (
                col_min_k,
                col_hist,
                col_min,
                col_max,
                col_min_display,
                col_max_display,
                null_count,
                col_min_len,
                col_max_len,
            ) = _compute_stats_for_arrow_column(col, col_field.type, file_path)

            min_k_hashes.append(col_min_k)
            histograms.append(col_hist)
            min_values.append(col_min)
            max_values.append(col_max)
            min_values_display.append(col_min_display)
            max_values_display.append(col_max_display)
            null_counts.append(null_count)
            min_lengths_list.append(col_min_len)
            max_lengths_list.append(col_max_len)
        finally:
            # free the table-level reference if present so memory can be reclaimed
            try:
                del col_table
            except Exception:
                pass
            try:
                del tbl
            except Exception:
                pass
            try:
                del col
            except Exception:
                pass

    # Calculate uncompressed sizes. When the original in-memory table is
    # available (we just wrote it), prefer using it so sizes match the
    # table-based builder exactly. Otherwise use rugo metadata or batch_table.
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
    elif batch_table is not None:
        # If we already have the batch table loaded, compute sizes from it
        for col in batch_table.columns:
            col_total = 0
            for chunk in col.chunks:
                try:
                    buffs = chunk.buffers()
                except Exception:
                    # If buffers unavailable, fall through to rugo/zero
                    col_total = 0
                    break
                for buffer in buffs:
                    if buffer is not None:
                        col_total += buffer.size
            column_uncompressed.append(int(col_total))
            uncompressed_size += col_total
        _manifest_metrics["sizes_from_batch_table"] += 1
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

    # Free batch table if it was loaded
    try:
        del batch_table
    except Exception:
        pass

    entry = ParquetManifestEntry(
        file_path=file_path,
        file_format="parquet",
        record_count=int(meta.num_rows),
        file_size_in_bytes=int(file_size_in_bytes or data_bytes_len),
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
        min_lengths=min_lengths_list,
        max_lengths=max_lengths_list,
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
