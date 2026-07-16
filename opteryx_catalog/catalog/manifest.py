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
# to avoid repeated rugo parsing and expensive to_pylist() conversions.
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


# Columns that write_parquet_manifest() comma-encodes into ARRAY<VARCHAR>
# because rugo's writer doesn't support nested ARRAY<ARRAY<...>> — decoded
# back to list[list[int]] here so callers see the original shape. Manifests
# written before this encoding existed store these as raw nested int lists
# directly (pyarrow's list<list<uint64/int64>>) — pass those through as-is.
_NESTED_INT_LIST_COLUMNS = ("min_k_hashes", "histogram_counts")


def _decode_nested_int_list_column(rows: list) -> list:
    def _decode_cell(s):
        if not isinstance(s, str):
            return s
        return [int(h) for h in s.split(",")] if s else []

    return [None if row is None else [_decode_cell(s) for s in row] for row in rows]


def read_manifest_columns(data: bytes, keep_native: tuple = ()) -> tuple:
    """Decode manifest parquet bytes into ``({column_name: [values...]}, row_count, native)``.

    The native (no-pyarrow) read path, backed by rugo. The manifest schema
    contains dictionary-encoded strings and nested ``list<list<...>>``
    statistics columns, all of which are materialized directly into Python
    values matching the previous pyarrow ``to_pylist()`` output.

    Internal API: returns a 3-tuple ``(columns, row_count, native)``. ``keep_native``
    names columns whose whole-column native draken Vector is retained (in addition
    to the boxed lists) and returned in the third element ``{column_name: Vector}``. Consumers that reduce a column with native
    kernels (e.g. the planner's KMV/histogram over ``min_k_hashes`` /
    ``histogram_counts``) take the vector and skip re-boxing. The morsel owns its
    buffers, so a retained vector outlives the reader context. For a
    multi-row-group manifest the per-morsel vectors are concatenated
    (``Morsel.combine``) into one whole-column vector.
    """
    if not data:
        return {}, 0, {}

    from rugo import parquet as _rugo_parquet

    column_data: dict[str, list] = {}
    row_count = 0
    kept_morsels: list = []
    with _rugo_parquet.read_parquet(bytes(data)) as reader:
        for morsel in reader:
            row_count += morsel.num_rows
            if keep_native:
                kept_morsels.append(morsel)
            for name_b in morsel.column_names:
                name = (
                    name_b.decode("utf-8")
                    if isinstance(name_b, (bytes, bytearray))
                    else name_b
                )
                column_data.setdefault(name, []).extend(morsel.column(name_b).to_pylist())

    for name in _NESTED_INT_LIST_COLUMNS:
        if name in column_data:
            column_data[name] = _decode_nested_int_list_column(column_data[name])

    native: dict = {}
    if kept_morsels:
        combined = kept_morsels[0] if len(kept_morsels) == 1 else kept_morsels[0].combine(kept_morsels)
        for name in keep_native:
            name_b = name.encode("utf-8")
            if name_b in combined.column_names or name in combined.column_names:
                native[name] = combined.column(name_b)

    return column_data, row_count, native


def read_manifest_rows(data: bytes) -> list:
    """Decode manifest parquet bytes into a list of row dicts using rugo."""
    column_data, row_count, _native = read_manifest_columns(data)
    if not column_data:
        return []
    names = list(column_data.keys())
    return [{name: column_data[name][i] for name in names} for i in range(row_count)]


def _parse_manifest_optimized(data: bytes) -> list:
    """Parse manifest parquet bytes into frozen row dicts for caching."""
    rows = read_manifest_rows(data)
    _manifest_metrics["full_column_reads"] += 1
    return [{k: _freeze_for_cache(v) for k, v in r.items()} for r in rows]


def invalidate_parsed_manifest(manifest_path: str) -> None:
    """Remove a manifest from the parsed-manifest cache (if present)."""
    _parsed_manifest_cache.pop(manifest_path, None)


def clear_parsed_manifest_cache() -> None:
    """Clear the entire parsed-manifest cache (tests / admin use)."""
    _parsed_manifest_cache.clear()


import datetime
import heapq
import re

_COMPRESSIBLE_CATEGORIES = {
    "INT8",
    "INT16",
    "INT32",
    "INT64",
    "DECIMAL",
    "DECIMAL128",
    "FLOAT32",
    "FLOAT64",
    "DATE32",
    "TIMESTAMP64",
    "TIME32",
    "TIME64",
    "INTERVAL",
    "BOOL",
}
_VARIABLE_WIDTH_CATEGORIES = {"VARCHAR", "NVARCHAR", "VARBINARY", "ARRAY"}

# Maps a rugo ParquetMetadata SchemaColumn.logical_type string (e.g.
# "date32[day]", "timestamp[ms,UTC]", "decimal(10, 2)", "varchar") to the same
# category names used by draken's Morsel.schema (DrakenType.name), so a single
# stats path works whether the vector came from a live in-memory Morsel or
# from re-reading a parquet file's bytes.
_LOGICAL_TYPE_ALIASES = {
    "varchar": "VARCHAR",
    "nvarchar": "NVARCHAR",
    "varbinary": "VARBINARY",
    "boolean": "BOOL",
    "int8": "INT8",
    "int16": "INT16",
    "int32": "INT32",
    "int64": "INT64",
    "float": "FLOAT32",
    "double": "FLOAT64",
    "array": "ARRAY",
    "interval": "INTERVAL",
}


def _category_from_logical_type(logical_type: str) -> tuple:
    """Return ``(category, decimal_scale)`` from a rugo/parquet logical-type
    string. ``decimal_scale`` is only set for DECIMAL columns (needed to
    rescale the raw unscaled integer back into a display value).
    """
    lt = (logical_type or "").lower()
    if lt.startswith("decimal"):
        m = re.match(r"decimal\((\d+)\s*,\s*(\d+)\)", lt)
        return "DECIMAL", (int(m.group(2)) if m else 0)
    if lt.startswith("timestamp"):
        return "TIMESTAMP64", None
    if lt.startswith("date"):
        return "DATE32", None
    if lt.startswith("time"):
        return "TIME64", None
    return _LOGICAL_TYPE_ALIASES.get(lt, lt.upper()), None


def _display_value(value, category: str, decimal_scale=None):
    """Render a decoded column value as a display string.

    Handles two shapes of ``value``: a proper Python object (``datetime.date``,
    ``decimal.Decimal``, ...) as produced by a live Morsel's ``to_pylist()``,
    or a raw physical int as produced by re-reading a parquet file (Parquet
    round-trips DATE/TIMESTAMP/TIME/DECIMAL columns down to plain physical
    ints — draken's Vector doesn't carry the logical annotation back).
    """
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if category == "DECIMAL" and decimal_scale is not None and isinstance(value, int):
        import decimal

        return str(decimal.Decimal(value).scaleb(-decimal_scale))
    if category == "DATE32" and isinstance(value, int):
        return (datetime.date(1970, 1, 1) + datetime.timedelta(days=value)).isoformat()
    if category == "TIMESTAMP64" and isinstance(value, int):
        return (
            datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=value)
        ).isoformat()
    if category in ("TIME32", "TIME64") and isinstance(value, int):
        return str(datetime.timedelta(microseconds=value))
    if isinstance(value, str):
        return value[:16] + "..." if len(value) > 16 else value
    if isinstance(value, (bytes, bytearray, memoryview)):
        b = bytes(value)
        if any(c < 32 or c > 126 for c in b):
            hexed = b.hex()
            return hexed[:16] + "..." if len(hexed) > 16 else hexed
        s = b.decode("latin-1", errors="replace")
        return s[:16] + "..." if len(s) > 16 else s
    return str(value)


def _compute_column_stats(vec, category: str, decimal_scale=None) -> tuple:
    """Compute statistics for a single column from its native draken Vector.

    ``vec`` may come from a live in-memory Morsel (correct semantic type) or
    from re-reading a parquet file's bytes (temporal/decimal columns flattened
    to plain physical ints by the round-trip — ``category``/``decimal_scale``
    carry the true semantic type so display values still render correctly).

    Returns: (min_k, histogram, min_value, max_value, min_display, max_display,
    null_count, min_length, max_length)
    """
    try:
        # ARRAY (and possibly other nested/complex types) don't support
        # native hashing — no min-k sketch for those, everything else works.
        hashes = vec.hash()
    except ValueError:
        hashes = []
    null_count = int(sum(vec.is_null()))
    is_compressible = category in _COMPRESSIBLE_CATEGORIES
    is_boolean = category == "BOOL"
    is_variable_width = category in _VARIABLE_WIDTH_CATEGORIES

    # Native uint64: .hash() returns true unsigned 64-bit values (up to 2**64-1).
    # rugo's parquet writer now stores nested ARRAY<ARRAY<UINT64>> with an
    # unsigned leaf annotation, so these are kept as plain ints (no decimal-string
    # workaround) — write_parquet_manifest builds the UINT64 vector directly.
    col_min_k = sorted(heapq.nsmallest(MIN_K_HASHES, set(hashes)))
    col_hist: list = []
    col_min = NULL_FLAG
    col_max = NULL_FLAG
    min_display = None
    max_display = None
    min_len = 0
    max_len = 0

    values = vec.to_pylist()
    non_null_values = [v for v in values if v is not None]

    if is_compressible:
        compressed = [c for c in vec.compress() if c != NULL_FLAG]
        if compressed:
            vmin, vmax = min(compressed), max(compressed)
            col_min, col_max = int(vmin), int(vmax)
            if is_boolean:
                true_count = sum(1 for v in non_null_values if v is True)
                false_count = sum(1 for v in non_null_values if v is False)
                col_hist = [int(true_count), int(false_count)]
            elif vmax > vmin:
                col_hist = [0] * HISTOGRAM_BINS
                span = float(vmax - vmin)
                for c in compressed:
                    b = int(((float(c) - float(vmin)) / span) * (HISTOGRAM_BINS - 1))
                    col_hist[max(0, min(HISTOGRAM_BINS - 1, b))] += 1
        if non_null_values:
            min_display = _display_value(min(non_null_values), category, decimal_scale)
            max_display = _display_value(max(non_null_values), category, decimal_scale)
    elif non_null_values:
        min_display = _display_value(min(non_null_values), category, decimal_scale)
        max_display = _display_value(max(non_null_values), category, decimal_scale)

    if is_variable_width:
        lengths = [len(v) for v in non_null_values]
        if lengths:
            min_len, max_len = min(lengths), max(lengths)

    return (
        col_min_k,
        col_hist,
        col_min,
        col_max,
        min_display,
        max_display,
        null_count,
        min_len,
        max_len,
    )


def _column_uncompressed_estimate(values: list) -> int:
    """Rough uncompressed-size estimate for one column's decoded values.

    Used only for reporting/summary purposes (dataset ``describe()`` and
    snapshot size totals) — not correctness-critical, so a plain per-value
    ``sys.getsizeof`` sum is good enough and avoids depending on parquet
    row-group byte metadata that isn't exposed by rugo's public API.
    """
    import sys

    return sum(sys.getsizeof(v) for v in values if v is not None)


def morsel_schema_dict(morsel: Any) -> dict:
    """Return ``{name: DrakenType}`` for a Morsel, across draken versions.

    Newer draken exposes ``Morsel.schema`` directly. Older versions (draken
    0.4.2, as pinned by at least one real consumer app) have no ``.schema``
    property at all — only the separate ``column_names``/``column_types``
    lists. Always go through this helper rather than ``morsel.schema``
    directly so both versions work.
    """
    schema = getattr(morsel, "schema", None)
    if schema is not None:
        return schema
    names = morsel.column_names
    types = morsel.column_types
    return {
        (n.decode("utf-8") if isinstance(n, (bytes, bytearray)) else n): t
        for n, t in zip(names, types)
    }


def build_parquet_manifest_entry_from_morsel(
    morsel: Any,
    data_bytes: bytes,
    file_path: str,
    file_size_in_bytes: int | None = None,
) -> ParquetManifestEntry:
    """Build a manifest entry from the in-memory Morsel that was just written.

    Stats are computed from ``morsel`` directly (not by re-reading
    ``data_bytes``) because Parquet round-trips temporal/decimal columns down
    to plain physical ints — re-reading would lose the semantic type needed
    for correct display values.
    """
    t_start = time.perf_counter()
    _manifest_metrics["files_read"] += 1
    _manifest_metrics["bytes_read"] += len(data_bytes)

    schema = morsel_schema_dict(morsel)
    col_names = list(schema.keys())

    min_k_hashes: list = []
    histograms: list = []
    min_values: list = []
    max_values: list = []
    min_values_display: list = []
    max_values_display: list = []
    null_counts: list = []
    min_lengths_list: list = []
    max_lengths_list: list = []
    column_uncompressed: list = []
    uncompressed_size = 0

    for name in col_names:
        # draken 0.4.2's Morsel.column() requires bytes; newer versions accept
        # either, so bytes is the universally-safe choice here.
        vec = morsel.column(name.encode("utf-8"))
        category = schema[name].name
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
        ) = _compute_column_stats(vec, category)

        min_k_hashes.append(col_min_k)
        histograms.append(col_hist)
        min_values.append(col_min)
        max_values.append(col_max)
        min_values_display.append(col_min_display)
        max_values_display.append(col_max_display)
        null_counts.append(null_count)
        min_lengths_list.append(col_min_len)
        max_lengths_list.append(col_max_len)

        col_bytes = _column_uncompressed_estimate(vec.to_pylist())
        column_uncompressed.append(col_bytes)
        uncompressed_size += col_bytes

    entry = ParquetManifestEntry(
        file_path=file_path,
        file_format="parquet",
        record_count=int(morsel.num_rows),
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
        min_lengths=min_lengths_list,
        max_lengths=max_lengths_list,
    )

    logger.debug(
        "build_parquet_manifest_entry_from_morsel %s files=%d dur=%.3fs",
        file_path,
        _manifest_metrics["files_read"],
        time.perf_counter() - t_start,
    )
    return entry


def build_parquet_manifest_entry_from_bytes(
    data_bytes: bytes,
    file_path: str,
    file_size_in_bytes: int | None = None,
    orig_morsel: Any | None = None,
) -> ParquetManifestEntry:
    """Build a manifest entry by reading a parquet file's bytes.

    Used when there's no live in-memory Morsel to hand (rescanning an
    existing file during ``add_files``/``refresh_manifest``/compaction, or
    from a standalone script). Pass ``orig_morsel`` when you do have the
    original in-memory Morsel (e.g. right after writing it) to skip the
    re-read and get exact stats via :func:`build_parquet_manifest_entry_from_morsel`.
    """
    if orig_morsel is not None:
        return build_parquet_manifest_entry_from_morsel(
            orig_morsel, data_bytes, file_path, file_size_in_bytes
        )

    from rugo.parquet import read_metadata_from_memoryview
    from rugo.parquet import read_parquet

    t_start = time.perf_counter()
    _manifest_metrics["files_read"] += 1
    _manifest_metrics["bytes_read"] += len(data_bytes)

    meta = read_metadata_from_memoryview(memoryview(data_bytes))
    # name -> (category, decimal_scale) from Parquet's own logical-type
    # annotations, since a re-read Vector's own .type is the flattened
    # physical storage type (e.g. a DATE column reads back as plain INT64).
    col_info = {
        c.name: _category_from_logical_type(c.logical_type) for c in meta.schema_columns
    }
    col_names = list(col_info.keys())

    min_k_hashes: list = []
    histograms: list = []
    min_values: list = []
    max_values: list = []
    min_values_display: list = []
    max_values_display: list = []
    null_counts: list = []
    min_lengths_list: list = []
    max_lengths_list: list = []
    column_uncompressed = [0] * len(col_names)
    uncompressed_size = 0
    record_count = 0

    # Accumulate across row groups (read_parquet yields one Morsel per
    # surviving row group).
    accum: dict = {name: {"hashes": set(), "compressed": [], "values": []} for name in col_names}

    with read_parquet(bytes(data_bytes)) as reader:
        for morsel in reader:
            record_count += morsel.num_rows
            for name_b in morsel.column_names:
                name = name_b.decode("utf-8") if isinstance(name_b, (bytes, bytearray)) else name_b
                if name not in accum:
                    continue
                vec = morsel.column(name_b)
                category, _ = col_info[name]
                acc = accum[name]
                try:
                    acc["hashes"].update(vec.hash())
                except ValueError:
                    pass
                acc["values"].extend(vec.to_pylist())
                if category in _COMPRESSIBLE_CATEGORIES:
                    acc["compressed"].extend(vec.compress())

    for name in col_names:
        category, decimal_scale = col_info[name]
        acc = accum[name]
        values = acc["values"]
        non_null_values = [v for v in values if v is not None]
        null_count = sum(1 for v in values if v is None)

        # Native uint64 (see _compute_column_stats): kept as ints, not strings —
        # rugo's writer stores them as an unsigned nested array.
        col_min_k = sorted(heapq.nsmallest(MIN_K_HASHES, acc["hashes"]))
        col_hist: list = []
        col_min = NULL_FLAG
        col_max = NULL_FLAG
        min_display = None
        max_display = None
        min_len = 0
        max_len = 0

        if category in _COMPRESSIBLE_CATEGORIES:
            compressed = [c for c in acc["compressed"] if c != NULL_FLAG]
            if compressed:
                vmin, vmax = min(compressed), max(compressed)
                col_min, col_max = int(vmin), int(vmax)
                if category == "BOOL":
                    true_count = sum(1 for v in non_null_values if v is True)
                    false_count = sum(1 for v in non_null_values if v is False)
                    col_hist = [int(true_count), int(false_count)]
                elif vmax > vmin:
                    col_hist = [0] * HISTOGRAM_BINS
                    span = float(vmax - vmin)
                    for c in compressed:
                        b = int(((float(c) - float(vmin)) / span) * (HISTOGRAM_BINS - 1))
                        col_hist[max(0, min(HISTOGRAM_BINS - 1, b))] += 1
            if non_null_values:
                min_display = _display_value(min(non_null_values), category, decimal_scale)
                max_display = _display_value(max(non_null_values), category, decimal_scale)
        elif non_null_values:
            min_display = _display_value(min(non_null_values), category, decimal_scale)
            max_display = _display_value(max(non_null_values), category, decimal_scale)

        if category in _VARIABLE_WIDTH_CATEGORIES:
            lengths = [len(v) for v in non_null_values]
            if lengths:
                min_len, max_len = min(lengths), max(lengths)

        min_k_hashes.append(col_min_k)
        histograms.append(col_hist)
        min_values.append(col_min)
        max_values.append(col_max)
        min_values_display.append(min_display)
        max_values_display.append(max_display)
        null_counts.append(null_count)
        min_lengths_list.append(min_len)
        max_lengths_list.append(max_len)

        col_bytes = _column_uncompressed_estimate(values)
        column_uncompressed[col_names.index(name)] = col_bytes
        uncompressed_size += col_bytes

    entry = ParquetManifestEntry(
        file_path=file_path,
        file_format="parquet",
        record_count=int(record_count or meta.num_rows),
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


def get_manifest_metrics() -> dict:
    """Return a snapshot of manifest instrumentation counters (for tests/benchmarks)."""
    return dict(_manifest_metrics)


def reset_manifest_metrics() -> None:
    """Reset the manifest metrics counters to zero."""
    _manifest_metrics.clear()
