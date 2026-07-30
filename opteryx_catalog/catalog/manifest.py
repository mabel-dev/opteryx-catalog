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
    min_lengths: list[int]
    max_lengths: list[int]
    # Stable per-column field-id, same order/index as every list above (e.g.
    # min_values[i] is field_ids[i]'s min). Lets readers key statistics by a
    # schema-stable id instead of assuming array position equals column
    # position in some other schema snapshot. Empty for entries built before
    # field-ids existed or for schemas with no catalog-assigned ids.
    field_ids: list[int] = field(default_factory=list)
    # Per-column byte-class histogram (8 fixed classes: upper, lower, digit,
    # whitespace, punct_text, semantic, extended, control) and total byte
    # count, VARCHAR/NVARCHAR/VARBINARY columns only (empty list / 0 for
    # everything else) -- backs the LIKE '%needle%' selectivity char-class
    # estimator. See _compute_column_stats / Vector.char_class_stats().
    char_class_counts: list[list[int]] = field(default_factory=list)
    char_total_bytes: list[int] = field(default_factory=list)

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
            "min_lengths": self.min_lengths,
            "max_lengths": self.max_lengths,
            "field_ids": self.field_ids,
            "char_class_counts": self.char_class_counts,
            "char_total_bytes": self.char_total_bytes,
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


import heapq

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
# The subset of _VARIABLE_WIDTH_CATEGORIES Vector.char_class_stats() accepts
# (see draken_native.cpp) -- ARRAY has no byte-class concept, stays on the
# boxed to_pylist()-length fallback below.
_STRING_CATEGORIES = {"VARCHAR", "NVARCHAR", "VARBINARY"}

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


def _category_from_logical_type(logical_type: str) -> str:
    """Return ``category`` from a rugo/parquet logical-type string."""
    lt = (logical_type or "").lower()
    if lt.startswith("decimal"):
        return "DECIMAL"
    if lt.startswith("timestamp"):
        return "TIMESTAMP64"
    if lt.startswith("date"):
        return "DATE32"
    if lt.startswith("time"):
        return "TIME64"
    return _LOGICAL_TYPE_ALIASES.get(lt, lt.upper())


def _compute_column_stats(vec, category: str) -> tuple:
    """Compute statistics for a single column from its native draken Vector.

    ``vec`` may come from a live in-memory Morsel or from re-reading a parquet
    file's bytes; ``category`` selects the compression/histogram/length
    handling for the column's logical type.

    Every reduction below is a native draken kernel over the whole column --
    no Python-level min()/max()/loop over per-row values (previously: a
    Python list-comprehension filter, then Python min()/max(), then a Python
    histogram-bucketing loop -- a real cost at the Tb-scale row counts this
    catalog runs against). The one remaining Python-level pass is BOOL's
    true/false histogram, which has no native equivalent (Vector.sum()
    doesn't support BOOL) and touches only however many rows one column has,
    not a Tb-scale reduction.

    Returns: (min_k, histogram, min_value, max_value, null_count, min_length,
    max_length, char_class_counts, char_total_bytes)
    """
    try:
        # ARRAY (and possibly other nested/complex types) don't support
        # native hashing — no min-k sketch for those, everything else works.
        hashes = vec.hash()
    except ValueError:
        hashes = []
    null_count = vec.null_count()
    is_compressible = category in _COMPRESSIBLE_CATEGORIES
    is_boolean = category == "BOOL"
    is_variable_width = category in _VARIABLE_WIDTH_CATEGORIES
    is_string = category in _STRING_CATEGORIES

    # Native uint64: .hash() returns true unsigned 64-bit values (up to 2**64-1).
    # rugo's parquet writer now stores nested ARRAY<ARRAY<UINT64>> with an
    # unsigned leaf annotation, so these are kept as plain ints (no decimal-string
    # workaround) — write_parquet_manifest builds the UINT64 vector directly.
    col_min_k = sorted(heapq.nsmallest(MIN_K_HASHES, set(hashes)))
    col_hist: list = []
    col_min = NULL_FLAG
    col_max = NULL_FLAG
    min_len = 0
    max_len = 0
    char_class_counts: list = []
    char_total_bytes = 0

    if is_compressible:
        # draken 2026-07-30: Vector.compress() was renamed to .ordinalize()
        # (disambiguated from the unrelated native .dictionary_encode()/
        # .drop_nulls() split on draken.draken_native.Vector -- this is the
        # draken.vectors.vector shim's int64 sort-key producer), and as of the
        # kernel relocation below it is fully native end to end: .ordinalize()
        # produces an INT64 Vector, .ordinal_min_max()/.histogram_bucket() are
        # native reductions over it that correctly exclude the ORDINAL_NULL
        # sentinel ordinalize() bakes into null rows (see draken_native.cpp's
        # ordinal_min_max/histogram_bucket bindings) -- NOT draken's generic
        # .min()/.max(), which would trust the (absent) validity bitmap on an
        # ordinalized column and treat the sentinel as real data.
        # ordinalize() doesn't support ARRAY/VECTOR_FP16/DECIMAL128 (see
        # draken/ops/ordinalize.h) -- no min/max/histogram for those
        # specific columns rather than crashing the whole stats pass. Every
        # OTHER _COMPRESSIBLE_CATEGORIES member (including, as of this
        # session, VARCHAR/NVARCHAR/VARBINARY) is ordinalize-supported.
        try:
            ordinal = vec.ordinalize()
        except ValueError:
            ordinal = None
        if ordinal is not None:
            min_max = ordinal.ordinal_min_max()
            if min_max is not None:
                vmin, vmax = min_max
                col_min, col_max = int(vmin), int(vmax)
                if is_boolean:
                    # No native bool-count kernel (Vector.sum() doesn't support
                    # BOOL) -- bounded, per-column Python pass, not a Tb-scale one.
                    values = vec.to_pylist()
                    true_count = sum(1 for v in values if v is True)
                    false_count = sum(1 for v in values if v is False)
                    col_hist = [int(true_count), int(false_count)]
                elif vmax > vmin:
                    col_hist = ordinal.histogram_bucket(vmin, vmax, HISTOGRAM_BINS)

    if is_string:
        # One native pass: byte-class counts, total bytes, AND min/max length
        # together (see draken_native.cpp's char_class_stats binding).
        char_class_counts, char_total_bytes, length_range = vec.char_class_stats()
        if length_range is not None:
            min_len, max_len = length_range
    elif is_variable_width:
        # ARRAY: char_class_stats() is string-only; no native length reduction
        # exists for it, so this one category keeps the boxed length path.
        lengths = [len(v) for v in vec.to_pylist() if v is not None]
        if lengths:
            min_len, max_len = min(lengths), max(lengths)

    return (
        col_min_k,
        col_hist,
        col_min,
        col_max,
        null_count,
        min_len,
        max_len,
        char_class_counts,
        char_total_bytes,
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
    field_id_by_name: Dict[str, int] | None = None,
) -> ParquetManifestEntry:
    """Build a manifest entry from the in-memory Morsel that was just written.

    Stats are computed from ``morsel`` directly (not by re-reading
    ``data_bytes``) because Parquet round-trips temporal/decimal columns down
    to plain physical ints — re-reading would lose the semantic type needed
    for correct display values.

    ``field_id_by_name``, when provided, is the dataset's current
    name->field_id mapping (from its schema doc). ``field_ids`` on the
    resulting entry is a list parallel to every other per-column stats list
    (``field_ids[i]`` is the field-id for whichever column produced
    ``min_values[i]``/``max_values[i]``/etc.); a column absent from the
    mapping (e.g. a stale/dropped column) gets ``None`` in that slot so
    readers can tell "no usable field-id for this position" from "not
    computed at all".
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
    null_counts: list = []
    min_lengths_list: list = []
    max_lengths_list: list = []
    column_uncompressed: list = []
    field_ids: list = []
    char_class_counts: list = []
    char_total_bytes_list: list = []
    uncompressed_size = 0

    for name in col_names:
        field_ids.append(field_id_by_name.get(name) if field_id_by_name else None)
        # draken 0.4.2's Morsel.column() requires bytes; newer versions accept
        # either, so bytes is the universally-safe choice here.
        vec = morsel.column(name.encode("utf-8"))
        category = schema[name].name
        (
            col_min_k,
            col_hist,
            col_min,
            col_max,
            null_count,
            col_min_len,
            col_max_len,
            col_char_class_counts,
            col_char_total_bytes,
        ) = _compute_column_stats(vec, category)

        min_k_hashes.append(col_min_k)
        histograms.append(col_hist)
        min_values.append(col_min)
        max_values.append(col_max)
        null_counts.append(null_count)
        min_lengths_list.append(col_min_len)
        max_lengths_list.append(col_max_len)
        char_class_counts.append(col_char_class_counts)
        char_total_bytes_list.append(col_char_total_bytes)

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
        min_lengths=min_lengths_list,
        max_lengths=max_lengths_list,
        field_ids=field_ids,
        char_class_counts=char_class_counts,
        char_total_bytes=char_total_bytes_list,
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
    field_id_by_name: Dict[str, int] | None = None,
) -> ParquetManifestEntry:
    """Build a manifest entry by reading a parquet file's bytes.

    Used when there's no live in-memory Morsel to hand (rescanning an
    existing file during ``add_files``/``refresh_manifest``/compaction, or
    from a standalone script). Pass ``orig_morsel`` when you do have the
    original in-memory Morsel (e.g. right after writing it) to skip the
    re-read and get exact stats via :func:`build_parquet_manifest_entry_from_morsel`.

    ``field_id_by_name``: see :func:`build_parquet_manifest_entry_from_morsel`.
    """
    if orig_morsel is not None:
        return build_parquet_manifest_entry_from_morsel(
            orig_morsel, data_bytes, file_path, file_size_in_bytes, field_id_by_name
        )

    from rugo.parquet import read_metadata_from_memoryview
    from rugo.parquet import read_parquet

    t_start = time.perf_counter()
    _manifest_metrics["files_read"] += 1
    _manifest_metrics["bytes_read"] += len(data_bytes)

    meta = read_metadata_from_memoryview(memoryview(data_bytes))
    # name -> category from Parquet's own logical-type annotations, since a
    # re-read Vector's own .type is the flattened physical storage type (e.g.
    # a DATE column reads back as plain INT64).
    col_info = {
        c.name: _category_from_logical_type(c.logical_type) for c in meta.schema_columns
    }
    col_names = list(col_info.keys())

    min_k_hashes: list = []
    histograms: list = []
    min_values: list = []
    max_values: list = []
    null_counts: list = []
    min_lengths_list: list = []
    max_lengths_list: list = []
    column_uncompressed = [0] * len(col_names)
    char_class_counts: list = []
    char_total_bytes_list: list = []
    uncompressed_size = 0
    record_count = 0

    # Accumulate across row groups (read_parquet yields one Morsel per
    # surviving row group). min/max/histogram need the FILE-WIDE ordinal
    # range before any row can be bucketed, so each row group's ordinalized
    # column is buffered (a compact INT64 vector, not the raw column) rather
    # than re-reading the file a second time -- one pass over the on-disk
    # data, min/max derived natively from the buffered vectors, then
    # histogram bucketing natively against that range. Every per-row
    # reduction (hash, null count, ordinalize, char-class counts, min/max,
    # histogram) is a native kernel; "values"/"bool_values" are the two
    # documented exceptions with no native equivalent (see
    # _compute_column_stats and _column_uncompressed_estimate).
    accum: dict = {
        name: {
            "hashes": set(),
            "null_count": 0,
            "ordinal_vecs": [],
            "char_counts": [0] * 8,
            "char_total_bytes": 0,
            "length_range": None,
            "bool_values": [],
            "values": [],
        }
        for name in col_names
    }

    with read_parquet(bytes(data_bytes)) as reader:
        for morsel in reader:
            record_count += morsel.num_rows
            for name_b in morsel.column_names:
                name = name_b.decode("utf-8") if isinstance(name_b, (bytes, bytearray)) else name_b
                if name not in accum:
                    continue
                vec = morsel.column(name_b)
                category = col_info[name]
                acc = accum[name]
                try:
                    acc["hashes"].update(vec.hash())
                except ValueError:
                    pass
                acc["null_count"] += vec.null_count()
                # Kept only for _column_uncompressed_estimate below -- no
                # longer used for null count / lengths / bool histogram.
                acc["values"].extend(vec.to_pylist())

                if category in _COMPRESSIBLE_CATEGORIES:
                    # ordinalize() doesn't support ARRAY/VECTOR_FP16/DECIMAL128
                    # -- see the identical guard in _compute_column_stats.
                    try:
                        acc["ordinal_vecs"].append(vec.ordinalize())
                    except ValueError:
                        pass
                    if category == "BOOL":
                        # No native bool-count kernel -- see _compute_column_stats.
                        values = vec.to_pylist()
                        acc["bool_values"].append(
                            (
                                sum(1 for v in values if v is True),
                                sum(1 for v in values if v is False),
                            )
                        )

                if category in _STRING_CATEGORIES:
                    counts, total_bytes, length_range = vec.char_class_stats()
                    for i in range(8):
                        acc["char_counts"][i] += counts[i]
                    acc["char_total_bytes"] += total_bytes
                    if length_range is not None:
                        lo, hi = length_range
                        cur = acc["length_range"]
                        acc["length_range"] = (
                            (lo, hi) if cur is None else (min(cur[0], lo), max(cur[1], hi))
                        )
                elif category in _VARIABLE_WIDTH_CATEGORIES:
                    # ARRAY: no native length reduction: see _compute_column_stats.
                    lengths = [len(v) for v in vec.to_pylist() if v is not None]
                    if lengths:
                        lo, hi = min(lengths), max(lengths)
                        cur = acc["length_range"]
                        acc["length_range"] = (
                            (lo, hi) if cur is None else (min(cur[0], lo), max(cur[1], hi))
                        )

    field_ids: list = []
    for name in col_names:
        field_ids.append(field_id_by_name.get(name) if field_id_by_name else None)
        category = col_info[name]
        acc = accum[name]

        # Native uint64 (see _compute_column_stats): kept as ints, not strings —
        # rugo's writer stores them as an unsigned nested array.
        col_min_k = sorted(heapq.nsmallest(MIN_K_HASHES, acc["hashes"]))
        col_hist: list = []
        col_min = NULL_FLAG
        col_max = NULL_FLAG
        min_len = 0
        max_len = 0

        if category in _COMPRESSIBLE_CATEGORIES:
            vecs = acc["ordinal_vecs"]
            pairs = [p for p in (v.ordinal_min_max() for v in vecs) if p is not None]
            if pairs:
                vmin = min(p[0] for p in pairs)
                vmax = max(p[1] for p in pairs)
                col_min, col_max = int(vmin), int(vmax)
                if category == "BOOL":
                    true_count = sum(t for t, _ in acc["bool_values"])
                    false_count = sum(f for _, f in acc["bool_values"])
                    col_hist = [int(true_count), int(false_count)]
                elif vmax > vmin:
                    bins = [0] * HISTOGRAM_BINS
                    for v in vecs:
                        per = v.histogram_bucket(vmin, vmax, HISTOGRAM_BINS)
                        for i in range(HISTOGRAM_BINS):
                            bins[i] += per[i]
                    col_hist = bins

        length_range = acc["length_range"]
        if length_range is not None:
            min_len, max_len = length_range

        min_k_hashes.append(col_min_k)
        histograms.append(col_hist)
        min_values.append(col_min)
        max_values.append(col_max)
        null_counts.append(acc["null_count"])
        min_lengths_list.append(min_len)
        max_lengths_list.append(max_len)
        char_class_counts.append(acc["char_counts"] if category in _STRING_CATEGORIES else [])
        char_total_bytes_list.append(acc["char_total_bytes"] if category in _STRING_CATEGORIES else 0)

        col_bytes = _column_uncompressed_estimate(acc["values"])
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
        min_lengths=min_lengths_list,
        max_lengths=max_lengths_list,
        field_ids=field_ids,
        char_class_counts=char_class_counts,
        char_total_bytes=char_total_bytes_list,
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
