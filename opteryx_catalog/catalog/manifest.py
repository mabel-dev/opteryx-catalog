from __future__ import annotations

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


def build_parquet_manifest_entry(
    table: Any, file_path: str, file_size_in_bytes: int
) -> ParquetManifestEntry:
    """Build a Parquet manifest entry with statistics for a PyArrow table.

    Args:
        table: PyArrow table to analyze
        file_path: Path where the file is stored
        file_size_in_bytes: Size of the parquet file in bytes

    Returns:
        ParquetManifestEntry with computed statistics
    """
    import pyarrow as pa

    min_k_hashes: list[list[int]] = []
    histograms: list[list[int]] = []
    min_values: list[int] = []
    null_counts: list[int] = []
    max_values: list[int] = []
    min_values_display: list = []
    max_values_display: list = []

    # Use draken for efficient hashing and compression when available.
    import heapq

    # Try to compute additional per-column statistics when draken is available.
    try:
        import opteryx.draken as draken  # type: ignore

        for col_idx, col in enumerate(table.columns):
            col_py = None
            # hash column values to 64-bit via draken (new cpdef API)
            vec = draken.Vector.from_arrow(col)
            hashes = list(vec.hash())

            # Decide whether to compute min-k/histogram for this column based
            # on field type and, for strings, average length of values.
            field_type = table.schema.field(col_idx).type
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
            elif pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
                # compute average length from non-null values; only allow
                # min-k/histogram for short strings (avg <= 16)
                col_py = None
                try:
                    col_py = col.to_pylist()
                except Exception:
                    col_py = None

                if col_py is not None:
                    lens = [len(x) for x in col_py if x is not None]
                    if lens:
                        avg_len = sum(lens) / len(lens)
                        if avg_len <= 16:
                            compute_min_k = True

            # KMV: take K smallest unique hashes when allowed; otherwise
            # store an empty list for this column. Deduplicate hashes so
            # the KMV sketch contains unique hashes (avoids duplicates
            # skewing cardinality estimates).
            if compute_min_k:
                unique_hashes = set(hashes)
                smallest = heapq.nsmallest(MIN_K_HASHES, unique_hashes)
                col_min_k = sorted(smallest)
            else:
                col_min_k = []

            # For histogram decisions follow the same rule as min-k
            compute_hist = compute_min_k

            # Use draken.compress() to get canonical int64 per value
            mapped = list(vec.compress())
            # Compute null count from compressed representation
            null_count = sum(1 for m in mapped if m == NULL_FLAG)
            null_counts.append(int(null_count))
            non_nulls_mapped = [m for m in mapped if m != NULL_FLAG]
            if non_nulls_mapped:
                vmin = min(non_nulls_mapped)
                vmax = max(non_nulls_mapped)
                col_min = int(vmin)
                col_max = int(vmax)
                if compute_hist:
                    if vmin == vmax:
                        col_hist = [0] * HISTOGRAM_BINS
                        col_hist[-1] = len(non_nulls_mapped)
                    else:
                        col_hist = [0] * HISTOGRAM_BINS
                        span = float(vmax - vmin)
                        for m in non_nulls_mapped:
                            b = int(((float(m) - float(vmin)) / span) * (HISTOGRAM_BINS - 1))
                            if b < 0:
                                b = 0
                            if b >= HISTOGRAM_BINS:
                                b = HISTOGRAM_BINS - 1
                            col_hist[b] += 1
                else:
                    col_hist = [0] * HISTOGRAM_BINS
            else:
                # no non-null values; histogram via hash buckets
                col_min = NULL_FLAG
                col_max = NULL_FLAG
                if compute_hist:
                    col_hist = [0] * HISTOGRAM_BINS
                    for h in hashes:
                        b = (h >> (64 - 5)) & 0x1F
                        col_hist[b] += 1
                else:
                    col_hist = [0] * HISTOGRAM_BINS

            min_k_hashes.append(col_min_k)
            histograms.append(col_hist)
            # Store compressed int64 for all column types via draken.compress()
            min_values.append(col_min)
            max_values.append(col_max)
            # For display: try to get human-readable representation
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
                            min_values_display.append(min(non_nulls_str))
                            max_values_display.append(max(non_nulls_str))
                        else:
                            min_values_display.append(None)
                            max_values_display.append(None)
                    else:
                        min_values_display.append(None)
                        max_values_display.append(None)
                else:
                    min_values_display.append(None)
                    max_values_display.append(None)
            except Exception:
                min_values_display.append(None)
                max_values_display.append(None)
        # end for
    except Exception:
        # Draken not available or failed; leave min_k_hashes/histograms empty
        min_k_hashes = [[] for _ in table.columns]
        histograms = [[] for _ in table.columns]
        # Attempt to compute per-column min/max from the table directly
        # Store as int64 hashes for consistency with the Draken path
        try:
            import hashlib

            def compress_value_fallback(v):
                """Hash a value to int64 for storage consistency with Draken."""
                if v is None:
                    return NULL_FLAG
                s = str(v)
                h = hashlib.sha256(s.encode("utf-8")).digest()
                # Take first 8 bytes and interpret as signed int64
                return int.from_bytes(h[:8], byteorder="big", signed=True)

            for col in table.columns:
                try:
                    col_py = col.to_pylist()
                    non_nulls = [v for v in col_py if v is not None]
                    null_count = len(col_py) - len(non_nulls)
                    null_counts.append(int(null_count))
                    if non_nulls:
                        try:
                            # Store compressed int64 values
                            non_nulls_compressed = [compress_value_fallback(v) for v in non_nulls]
                            col_min = min(non_nulls_compressed)
                            col_max = max(non_nulls_compressed)
                            min_values.append(col_min)
                            max_values.append(col_max)
                            # preserve textual display when possible
                            try:
                                min_values_display.append(min(non_nulls))
                                max_values_display.append(max(non_nulls))
                            except Exception:
                                min_values_display.append(None)
                                max_values_display.append(None)
                        except Exception:
                            min_values.append(None)
                            max_values.append(None)
                            min_values_display.append(None)
                            max_values_display.append(None)
                    else:
                        min_values.append(None)
                        max_values.append(None)
                        min_values_display.append(None)
                        max_values_display.append(None)
                except Exception:
                    min_values.append(None)
                    max_values.append(None)
                    min_values_display.append(None)
                    max_values_display.append(None)
                    # If we couldn't introspect column values, assume 0 nulls
                    null_counts.append(0)
        except Exception:
            # If even direct inspection fails, ensure lists lengths match
            min_values = [None] * len(table.columns)
            max_values = [None] * len(table.columns)
            min_values_display = [None] * len(table.columns)
            max_values_display = [None] * len(table.columns)
            null_counts = [0] * len(table.columns)

    # Calculate uncompressed size from table buffers — must be accurate.
    column_uncompressed: list[int] = []
    uncompressed_size = 0
    for col in table.columns:
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

    return ParquetManifestEntry(
        file_path=file_path,
        file_format="parquet",
        record_count=int(table.num_rows),
        file_size_in_bytes=file_size_in_bytes,
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
