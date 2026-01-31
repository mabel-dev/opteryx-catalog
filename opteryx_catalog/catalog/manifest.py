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
    import heapq

    import opteryx.draken as draken  # type: ignore
    import pyarrow as pa

    min_k_hashes: list[list[int]] = []
    histograms: list[list[int]] = []
    min_values: list[int] = []
    null_counts: list[int] = []
    max_values: list[int] = []
    min_values_display: list = []
    max_values_display: list = []

    # Try to compute additional per-column statistics when draken is available.
    try:
        for col_idx, col in enumerate(table.columns):
            col_py = None
            # hash column values to 64-bit via draken (new cpdef API)
            if hasattr(col, "combine_chunks"):
                col = col.combine_chunks()
            vec = draken.Vector.from_arrow(col)
            hashes = set(vec.hash())

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
            elif (
                pa.types.is_string(field_type)
                or pa.types.is_large_string(field_type)
                or pa.types.is_binary(field_type)
                or pa.types.is_large_binary(field_type)
            ):
                # Compute min-k for string and binary columns unconditionally
                # (removed previous average length restriction).
                col_py = None
                try:
                    col_py = col.to_pylist()
                except Exception:
                    col_py = None

                # Always allow min-k/histogram for these types
                compute_min_k = True

            # KMV: take K smallest unique hashes when allowed; otherwise
            # store an empty list for this column. Deduplicate hashes so
            # the KMV sketch contains unique hashes (avoids duplicates
            # skewing cardinality estimates).
            if compute_min_k:
                smallest = heapq.nsmallest(MIN_K_HASHES, hashes)
                col_min_k = sorted(smallest)
            else:
                col_min_k = []

            # For histogram decisions follow the same rule as min-k
            compute_hist = compute_min_k
            # Booleans should always compute histograms even when min-k is not used
            import pyarrow as pa

            if pa.types.is_boolean(field_type):
                compute_hist = True

            # Use draken.compress() to get canonical int64 per value
            compressed = list(vec.compress())
            # Compute null count from compressed representation
            null_count = sum(1 for m in compressed if m == NULL_FLAG)
            null_counts.append(int(null_count))
            non_nulls_compressed = [m for m in compressed if m != NULL_FLAG]
            if non_nulls_compressed:
                vmin = min(non_nulls_compressed)
                vmax = max(non_nulls_compressed)
                col_min = int(vmin)
                col_max = int(vmax)
                if compute_hist:
                    # Special-case boolean histograms: use true/false counts
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
                # no non-null values; histogram via hash buckets
                col_min = NULL_FLAG
                col_max = NULL_FLAG
                col_hist = []

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
                            min_value = min(non_nulls_str)
                            max_value = max(non_nulls_str)
                            if len(min_value) > 16:
                                min_value = min_value[:16] + "..."
                            if len(max_value) > 16:
                                max_value = max_value[:16] + "..."
                            min_values_display.append(min_value)
                            max_values_display.append(max_value)
                        else:
                            min_values_display.append(None)
                            max_values_display.append(None)
                    else:
                        min_values_display.append(None)
                        max_values_display.append(None)
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
                            min_values_display.append(min_value)
                            max_values_display.append(max_value)
                        else:
                            min_values_display.append(None)
                            max_values_display.append(None)
                    else:
                        min_values_display.append(None)
                        max_values_display.append(None)
                else:
                    if col_py is None:
                        try:
                            col_py = col.to_pylist()
                        except Exception:
                            col_py = None
                    if col_py is not None:
                        non_nulls = [x for x in col_py if x is not None]
                        if non_nulls:
                            min_values_display.append(min(non_nulls))
                            max_values_display.append(max(non_nulls))
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
    except Exception as exc:
        print(f"Warning: Unable to compute per-column statistics for {file_path}: {exc}")
        # Do not populate any per-column
        # statistics. The manifest entry will include empty lists so the
        # rest of the system treats the file as having no statistics.
        min_k_hashes = []
        histograms = []
        min_values = []
        max_values = []
        min_values_display = []
        max_values_display = []
        null_counts = []

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

    entry = ParquetManifestEntry(
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
    return entry
