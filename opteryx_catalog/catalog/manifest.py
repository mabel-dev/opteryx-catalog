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
    min_k_hashes: list[list[int]]
    histogram_counts: list[list[int]]
    histogram_bins: int
    min_values: list
    max_values: list

    def to_dict(self) -> dict:
        return {
            "file_path": self.file_path,
            "file_format": self.file_format,
            "record_count": self.record_count,
            "file_size_in_bytes": self.file_size_in_bytes,
            "uncompressed_size_in_bytes": self.uncompressed_size_in_bytes,
            "column_uncompressed_sizes_in_bytes": self.column_uncompressed_sizes_in_bytes,
            "min_k_hashes": self.min_k_hashes,
            "histogram_counts": self.histogram_counts,
            "histogram_bins": self.histogram_bins,
            "min_values": self.min_values,
            "max_values": self.max_values,
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
    max_values: list[int] = []

    # Use draken for efficient hashing and compression when available.
    import heapq

    # Try to compute additional per-column statistics when draken is available.
    try:
        import opteryx.draken as draken  # type: ignore

        for col_idx, col in enumerate(table.columns):
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

            # KMV: take K smallest hashes when allowed; otherwise store an
            # empty list for this column.
            if compute_min_k:
                smallest = heapq.nsmallest(MIN_K_HASHES, hashes)
                col_min_k = sorted(smallest)
            else:
                col_min_k = []

            # For histogram decisions follow the same rule as min-k
            compute_hist = compute_min_k

            # Use draken.compress() to get canonical int64 per value
            mapped = list(vec.compress())
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
            min_values.append(col_min)
            max_values.append(col_max)
    except Exception:
        # Draken not available or failed; leave min_k_hashes/histograms empty
        min_k_hashes = [[] for _ in table.columns]
        histograms = [[] for _ in table.columns]
        # Attempt to compute per-column min/max from the table directly
        try:
            for col in table.columns:
                try:
                    col_py = col.to_pylist()
                    non_nulls = [v for v in col_py if v is not None]
                    if non_nulls:
                        try:
                            min_values.append(min(non_nulls))
                            max_values.append(max(non_nulls))
                        except Exception:
                            min_values.append(None)
                            max_values.append(None)
                    else:
                        min_values.append(None)
                        max_values.append(None)
                except Exception:
                    min_values.append(None)
                    max_values.append(None)
        except Exception:
            # If even direct inspection fails, ensure lists lengths match
            min_values = [None] * len(table.columns)
            max_values = [None] * len(table.columns)

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
        min_k_hashes=min_k_hashes,
        histogram_counts=histograms,
        histogram_bins=HISTOGRAM_BINS,
        min_values=min_values,
        max_values=max_values,
    )


def build_parquet_manifest_minmax_entry(data: bytes, file_path: str) -> ParquetManifestEntry:
    """Build a Parquet manifest entry with min/max statistics using fast rugo reader.

    This is much faster than build_parquet_manifest_entry (microseconds per file)
    and is suitable for bulk file operations where full statistics are not needed.

    Args:
        data: Raw parquet file bytes
        file_path: Path where the file is stored

    Returns:
        ParquetManifestEntry with min/max statistics only (no histograms or k-hashes)
    """
    file_size = len(data)

    # Prefer rugo fast metadata reader when available, otherwise fall back
    # to pyarrow ParquetFile to extract row-group statistics.
    try:
        import opteryx.rugo.parquet as parquet_meta
        from opteryx.compiled.structures.relation_statistics import to_int

        if isinstance(data, memoryview):
            metadata = parquet_meta.read_metadata_from_memoryview(data, include_statistics=True)
        else:
            metadata = parquet_meta.read_metadata_from_memoryview(
                memoryview(data), include_statistics=True
            )

        record_count = metadata["num_rows"]
    except ImportError:
        # Fallback: use pyarrow to read Parquet metadata
        import pyarrow as pa
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(pa.BufferReader(data))
        record_count = int(pf.metadata.num_rows or 0)

        # Construct minimal metadata structure compatible with expected shape
        metadata = {"num_rows": record_count, "row_groups": []}
        for rg in range(pf.num_row_groups):
            rg_entry = {"columns": []}
            for ci in range(pf.metadata.num_columns):
                col_meta = pf.metadata.row_group(rg).column(ci)
                col_entry = {"name": pf.schema.names[ci]}
                stats = getattr(col_meta, "statistics", None)
                if stats:
                    col_entry["min"] = getattr(stats, "min", None)
                    col_entry["max"] = getattr(stats, "max", None)
                rg_entry["columns"].append(col_entry)
            # total_byte_size may not be available; leave out to trigger full-table calculation later
            metadata["row_groups"].append(rg_entry)

        # Define a simple to_int fallback for the pyarrow path
        def to_int(v: object) -> int:
            try:
                return int(v)
            except Exception:
                try:
                    if isinstance(v, (bytes, bytearray)):
                        s = v.decode("utf-8", errors="ignore")
                        return int(float(s)) if s else 0
                    return int(float(v))
                except Exception:
                    return 0

    # Gather min/max per column across all row groups
    column_stats = {}
    for row_group in metadata["row_groups"]:
        for column in row_group["columns"]:
            column_name = column["name"]

            if column_name not in column_stats:
                column_stats[column_name] = {"min": None, "max": None}

            min_value = column.get("min")
            if min_value is not None:
                # Compress value to int using to_int
                min_compressed = to_int(min_value)
                if column_stats[column_name]["min"] is None:
                    column_stats[column_name]["min"] = min_compressed
                else:
                    column_stats[column_name]["min"] = min(
                        column_stats[column_name]["min"], min_compressed
                    )

            max_value = column.get("max")
            if max_value is not None:
                # Compress value to int using to_int
                max_compressed = to_int(max_value)
                if column_stats[column_name]["max"] is None:
                    column_stats[column_name]["max"] = max_compressed
                else:
                    column_stats[column_name]["max"] = max(
                        column_stats[column_name]["max"], max_compressed
                    )

    # Extract min/max values (filter out None)
    min_values = [stats["min"] for stats in column_stats.values() if stats["min"] is not None]
    max_values = [stats["max"] for stats in column_stats.values() if stats["max"] is not None]

    # Get uncompressed size from metadata; if missing, read full table and
    # compute accurate uncompressed size from buffers. Also attempt to
    # compute per-column uncompressed byte counts when reading the table.
    uncompressed_size = 0
    column_uncompressed: list[int] = []
    missing = False
    for row_group in metadata["row_groups"]:
        v = row_group.get("total_byte_size", None)
        if v is None:
            missing = True
            break
        uncompressed_size += v

    if missing or uncompressed_size == 0:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pq.read_table(pa.BufferReader(data))
            uncompressed_size = 0
            for col in table.columns:
                col_total = 0
                for chunk in col.chunks:
                    for buffer in chunk.buffers():
                        if buffer is not None:
                            col_total += buffer.size
                column_uncompressed.append(int(col_total))
                uncompressed_size += col_total
        except Exception as exc:
            raise RuntimeError(
                f"Unable to determine uncompressed size for {file_path}: {exc}"
            ) from exc

    return ParquetManifestEntry(
        file_path=file_path,
        file_format="parquet",
        record_count=int(record_count),
        file_size_in_bytes=file_size,
        uncompressed_size_in_bytes=uncompressed_size,
        column_uncompressed_sizes_in_bytes=column_uncompressed,
        min_k_hashes=[],
        histogram_counts=[],
        histogram_bins=0,
        min_values=min_values,
        max_values=max_values,
    )
