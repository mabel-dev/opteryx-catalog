from __future__ import annotations

import logging
import time
from collections import Counter
from collections import OrderedDict
from dataclasses import dataclass
from dataclasses import field
from typing import Any

NULL_FLAG = -(1 << 63)
MIN_K_HASHES = 32
HISTOGRAM_BINS = 32

# Performance tuning parameters
ENABLE_BATCH_COLUMN_READS = True  # Read all columns at once before falling back
PYLIST_CONVERSION_CACHE = True  # Cache to_pylist() results per column

# Manifest retrieval optimization
ENABLE_LAZY_MANIFEST = True  # Use Arrow format for planning instead of converting to Python

# Parsed manifest cache (LRU) for Arrow tables (faster than Python dicts)
_arrow_manifest_cache: OrderedDict[str, Any] = OrderedDict()


@dataclass
class DataFile:
    file_path: str
    file_format: str = "PARQUET"
    record_count: int = 0
    file_size_in_bytes: int = 0
    partition: dict[str, object] = field(default_factory=dict)
    lower_bounds: dict[int, bytes] | None = None
    upper_bounds: dict[int, bytes] | None = None


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
    # ARRAY columns only: statistics over the flat CHILD vector -- the elements
    # themselves, pooled across every row's list. An ARRAY has no ordinal
    # encoding of its own, so the three lists above are the sentinel/empty for
    # it and an array column could be pruned on nothing at all; its elements,
    # however, are an ordinary vector and take the ordinary kernels.
    #
    # `element_min_values`/`element_max_values` are the child's ordinal bounds,
    # which is what lets `ARRAY_CONTAINS(tags, 'x')` skip a file whose elements
    # cannot include 'x'. `element_min_k_hashes` is the same KMV sketch every
    # other column gets, over elements rather than rows, which answers "how
    # many distinct tags are in this column" -- a different question from how
    # many distinct lists there are. NULL_FLAG / empty for non-ARRAY columns.
    element_min_values: list = field(default_factory=list)
    element_max_values: list = field(default_factory=list)
    element_min_k_hashes: list[list[int]] = field(default_factory=list)

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
            "element_min_values": self.element_min_values,
            "element_max_values": self.element_max_values,
            "element_min_k_hashes": self.element_min_k_hashes,
        }


logger = logging.getLogger(__name__)
_manifest_metrics = Counter()

# Parsed-manifest cache (LRU): store parsed Python representation (list[dict])
# to avoid repeated rugo parsing and expensive to_pylist() conversions.
# Entries are "frozen" for memory efficiency (inner lists -> tuples).
PARSED_MANIFEST_CACHE_SIZE: int = 32
_parsed_manifest_cache: OrderedDict[str, list] = OrderedDict()


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
    # A missing manifest propagates as FileNotFoundError: callers distinguish
    # "no manifest" from "empty manifest", and caching a [] for a path that
    # merely failed to read would serve that emptiness to every later caller.
    inp = io.new_input(manifest_path)
    with inp.open() as f:
        data = f.read()

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
                name = name_b.decode("utf-8") if isinstance(name_b, (bytes, bytearray)) else name_b
                column_data.setdefault(name, []).extend(morsel.column(name_b).to_pylist())

    for name in _NESTED_INT_LIST_COLUMNS:
        if name in column_data:
            column_data[name] = _decode_nested_int_list_column(column_data[name])

    native: dict = {}
    if kept_morsels:
        combined = (
            kept_morsels[0] if len(kept_morsels) == 1 else kept_morsels[0].combine(kept_morsels)
        )
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

# Categories that get ordinalize()-based min/max and a value histogram.
# VARCHAR/NVARCHAR/VARBINARY are in this set as of draken's 2026-07-30
# ordinalize rewrite, which added string support (an 8-byte big-endian
# content prefix, monotonic but not a total order — see draken/ops/
# ordinalize.h). Before that they had no min/max at all and every string
# column's bounds were the NULL_FLAG sentinel, so a string predicate could
# never prune and opteryx-core's local ANALYZE path (which does compute
# them) disagreed with this one about the same data.
#
# UINT8/UINT16/UINT32 are here for the same reason: ordinalize() maps each of
# them onto int64 by value (0 and 4294967295 come back as 0 and 4294967295 —
# every unsigned value below 2**63 IS its own ordinal), so they bound and
# prune exactly like the signed widths, and leaving them out cost every
# unsigned column — including IPv4, which is physically a UINT32 — its bounds,
# its histogram, and any chance of being pruned on.
#
# UINT64 is deliberately still absent, and not only because its ordinal is
# the value offset by 2**63 (so 5 ordinalizes to -9223372036854775803, which
# is not what a consumer reading min_values for display expects). The offset
# puts UINT64's ZERO exactly on ordinalize()'s ORDINAL_NULL sentinel, and
# ordinal_min_max() excludes that row as null: a column holding 0 and 5
# reports BOTH bounds as ordinal(5), and a column holding only 0 reports no
# bounds at all. Those bounds are wrong, not merely offset -- a `= 0`
# predicate would prune away the very file that holds the match. Including
# UINT64 needs a sentinel-free encoding first; until then it keeps NULL_FLAG,
# which readers already handle.
_COMPRESSIBLE_CATEGORIES = {
    "INT8",
    "INT16",
    "INT32",
    "INT64",
    "UINT8",
    "UINT16",
    "UINT32",
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
    "VARCHAR",
    "NVARCHAR",
    "VARBINARY",
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


def _min_k_smallest_distinct(hashes, k: int) -> list:
    """Return the ``k`` smallest distinct values from ``hashes``, ascending.

    Python fallback only (see ``_native_min_k_smallest``) for draken builds
    without ``Vector.unique()``. Single Python pass with a bounded (size
    <= k) max-heap + companion membership set, instead of
    ``sorted(heapq.nsmallest(k, set(hashes)))``: once the heap has k
    entries, a new hash needs only one comparison against the current max
    to be rejected, so a large/high-cardinality column's hashes mostly
    never reach a set insertion or heap operation at all. ``present``
    always mirrors the heap's contents (not every hash ever seen), so it
    stays bounded to k entries rather than growing with the column.
    """
    heap: list = []
    present: set = set()
    push = heapq.heappush
    replace = heapq.heapreplace
    n = 0  # == len(heap), tracked locally so the hot loop isn't calling len() every row
    for h in hashes:
        if h in present:
            continue
        if n < k:
            push(heap, -h)
            present.add(h)
            n += 1
        elif h < -heap[0]:
            evicted = -replace(heap, -h)
            present.discard(evicted)
            present.add(h)
    return sorted(-x for x in heap)


def _native_min_k_smallest(hash_vec, k: int) -> list:
    """K smallest distinct values of a ``hash_shaped()`` Vector, natively.

    ``Vector.unique()`` (draken_native.cpp) is a first-occurrence-index
    permutation computed via the same Parvi (<=16 distinct, zero-alloc) ->
    Carchar (SIMD-probed hash set) promotion path that already drives
    DISTINCT/GROUP BY -- or, for an already dict-shaped hash vector (a
    low-cardinality source column), a direct O(n) scan with no hashing at
    all. Either way it touches Python only for the column's DISTINCT
    count, never its row count -- the row-count-scale Python loop this
    replaces (see ``_min_k_smallest_distinct``) is gone entirely for the
    common case where distinct count is already <= k (idx.length <= k
    below), and bounded by distinct count otherwise.
    """
    idx = hash_vec.unique()
    if idx.length == 0:
        return []
    # hash_shaped() is tagged DRAKEN_INT64 and to_pylist() boxes its bits as
    # SIGNED Python ints, but .hash()/the min_k_hashes contract (and downstream
    # KMV consumers) use the true UNSIGNED 64-bit value -- mask back or "smallest"
    # silently means "smallest by signed comparison", a different, wrong set
    # for any hash >= 2**63 (about half of them).
    distinct_vals = [v & 0xFFFFFFFFFFFFFFFF for v in hash_vec.take(idx.to_pylist()).to_pylist()]
    if len(distinct_vals) <= k:
        return sorted(distinct_vals)
    return heapq.nsmallest(k, distinct_vals)


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
    max_length, char_class_counts, char_total_bytes, element_min, element_max,
    element_min_k)
    """
    try:
        # ARRAY (and possibly other nested/complex types) don't support
        # native hashing — no min-k sketch for those, everything else works.
        hash_vec = vec.hash_shaped()
    except ValueError:
        hash_vec = None
    null_count = vec.null_count()
    is_compressible = category in _COMPRESSIBLE_CATEGORIES
    is_boolean = category == "BOOL"
    is_variable_width = category in _VARIABLE_WIDTH_CATEGORIES
    is_string = category in _STRING_CATEGORIES

    # Native uint64: .hash() returns true unsigned 64-bit values (up to 2**64-1).
    # rugo's parquet writer now stores nested ARRAY<ARRAY<UINT64>> with an
    # unsigned leaf annotation, so these are kept as plain ints (no decimal-string
    # workaround) — write_parquet_manifest builds the UINT64 vector directly.
    col_min_k = [] if hash_vec is None else _native_min_k_smallest(hash_vec, MIN_K_HASHES)
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
                    # BOOL's ordinal domain is always exactly {0, 1} (False,
                    # True) -- bucketing the already-computed ordinal vector
                    # against that FIXED range (not this column's own
                    # ordinal_min_max(), which degenerates to a single bucket
                    # when every non-null value is the same) gives an exact
                    # native [false_count, true_count], replacing what used
                    # to be a to_pylist() decode of the whole column plus two
                    # Python-level sum() passes over it.
                    false_count, true_count = ordinal.histogram_bucket(0, 1, 2)
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

    # ARRAY elements. `array_child` is the flat vector of every element in the
    # column, lists concatenated -- an ordinary vector, so the ordinary kernels
    # apply to it even though they refuse the ARRAY that owns it. This is the
    # only statistic an array column can be pruned on, and the only distinct
    # count that answers the question a reader actually has ("how many distinct
    # tags?", not "how many distinct lists?").
    element_min = NULL_FLAG
    element_max = NULL_FLAG
    element_min_k: list = []
    if category == "ARRAY":
        child = getattr(vec, "array_child", None)
        if child is not None:
            # Each step is independently optional: a child type with no hash
            # kernel still gets bounds, one with no ordinalize kernel (an ARRAY
            # of ARRAY) still gets a sketch, and neither failing costs the
            # column anything it has today.
            try:
                element_min_k = _native_min_k_smallest(child.hash_shaped(), MIN_K_HASHES)
            except (ValueError, AttributeError):
                element_min_k = []
            try:
                child_min_max = child.ordinalize().ordinal_min_max()
                if child_min_max is not None:
                    element_min, element_max = (int(v) for v in child_min_max)
            except (ValueError, AttributeError):
                pass

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
        element_min,
        element_max,
        element_min_k,
    )


def _column_uncompressed_estimate(values: list) -> int:
    """Rough uncompressed-size estimate for one column's decoded values.

    Fallback path only (see ``_column_nbytes_estimate``) for draken builds
    that don't expose ``Morsel.select()``/``.nbytes`` -- a plain per-value
    ``sys.getsizeof`` sum, which requires the caller to have already decoded
    the column to a Python list.
    """
    import sys

    return sum(sys.getsizeof(v) for v in values if v is not None)


def _column_nbytes_estimate(morsel: Any, name: str, vec: Any) -> int:
    """In-memory byte footprint for one column: validity bitmap + payload
    (offsets for ARRAY, string arena for the string family), read natively
    off the Morsel.

    ``Morsel.nbytes`` does this accounting via draken's native
    ``draken_vector_nbytes``/``draken_vector_owner_nbytes`` helpers -- summed
    here over a single-column selection rather than decoding every value to a
    Python object and summing ``sys.getsizeof()`` over them. Falls back to the
    old estimate if the running draken build doesn't expose
    ``select()``/``nbytes`` (older pinned versions -- see
    ``morsel_schema_dict`` for the same kind of cross-version split).

    Requires a draken/rugo build with the DRAKEN_ARRAY nbytes fix (buffers.h /
    vector_owner.h / cxx_morsel.h / _morsel_shim.pyx): earlier builds silently
    undercounted ARRAY columns to 0 bytes whenever the column happened to have
    no nulls (no validity bitmap, and the child subtree was unreachable from a
    bare DrakenVector -- see buffers.h's now-resolved KNOWN LIMITATION note).
    That fix isn't reflected in this project's ``rugo`` version pin, so an
    environment installing a real (not locally rebuilt) rugo release could
    still hit the old bug here.
    """
    try:
        return int(morsel.select([name]).nbytes)
    except AttributeError:
        return _column_uncompressed_estimate(vec.to_pylist())


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
    field_id_by_name: dict[str, int] | None = None,
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
    element_min_values: list = []
    element_max_values: list = []
    element_min_k_hashes: list = []
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
            col_element_min,
            col_element_max,
            col_element_min_k,
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
        element_min_values.append(col_element_min)
        element_max_values.append(col_element_max)
        element_min_k_hashes.append(col_element_min_k)

        col_bytes = _column_nbytes_estimate(morsel, name, vec)
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
        element_min_values=element_min_values,
        element_max_values=element_max_values,
        element_min_k_hashes=element_min_k_hashes,
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
    field_id_by_name: dict[str, int] | None = None,
    footer_only: bool = False,
) -> ParquetManifestEntry:
    """Build a manifest entry by reading a parquet file's bytes.

    Used when there's no live in-memory Morsel to hand (rescanning an
    existing file during ``add_files``/``refresh_manifest``/compaction, or
    from a standalone script). Pass ``orig_morsel`` when you do have the
    original in-memory Morsel (e.g. right after writing it) to skip the
    re-read and get exact stats via :func:`build_parquet_manifest_entry_from_morsel`.

    ``field_id_by_name``: see :func:`build_parquet_manifest_entry_from_morsel`.

    ``footer_only``: skip decoding row-group data entirely and build the
    entry from the parquet footer alone (``record_count``/schema only — one
    small metadata parse over ``data_bytes``, not a decode of the file's
    column data). This is a CPU/time saving on bytes already in hand, NOT a
    network-egress saving — ``data_bytes`` must already be the full file
    (this function has no way to fetch less; callers reading from remote
    storage still transfer the whole object before calling it). rugo's
    ``read_metadata_from_memoryview`` doesn't expose per-column-chunk footer
    statistics (min/max/null-count) the way Parquet's own footer format
    carries them, only ``num_rows``/schema, so a footer-only entry has no
    min/max/null-count/histogram/min-k stats at all — every per-column list
    is left empty, same sentinel already used for the empty-file case in
    ``add_files``. Callers get a registered, queryable file with none of the
    file-pruning stats; those columns just never prune. Ignored when
    ``orig_morsel`` is given, since that path is already free.
    """
    if orig_morsel is not None:
        return build_parquet_manifest_entry_from_morsel(
            orig_morsel, data_bytes, file_path, file_size_in_bytes, field_id_by_name
        )

    from rugo.parquet import read_metadata_from_memoryview

    t_start = time.perf_counter()
    _manifest_metrics["files_read"] += 1
    _manifest_metrics["bytes_read"] += len(data_bytes)

    meta = read_metadata_from_memoryview(memoryview(data_bytes))

    if footer_only:
        entry = ParquetManifestEntry(
            file_path=file_path,
            file_format="parquet",
            record_count=int(meta.num_rows),
            file_size_in_bytes=int(file_size_in_bytes or len(data_bytes)),
            uncompressed_size_in_bytes=0,
            column_uncompressed_sizes_in_bytes=[],
            null_counts=[],
            min_k_hashes=[],
            histogram_counts=[],
            histogram_bins=0,
            min_values=[],
            max_values=[],
            min_lengths=[],
            max_lengths=[],
        )
        logger.debug(
            "build_parquet_manifest_entry_from_bytes(footer_only) %s files=%d dur=%.3fs",
            file_path,
            _manifest_metrics["files_read"],
            time.perf_counter() - t_start,
        )
        return entry

    from rugo.parquet import read_parquet
    # name -> category from Parquet's own logical-type annotations, since a
    # re-read Vector's own .type is the flattened physical storage type (e.g.
    # a DATE column reads back as plain INT64).
    col_info = {c.name: _category_from_logical_type(c.logical_type) for c in meta.schema_columns}
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
    element_min_values: list = []
    element_max_values: list = []
    element_min_k_hashes: list = []
    uncompressed_size = 0
    record_count = 0

    # Accumulate across row groups (read_parquet yields one Morsel per
    # surviving row group). min/max/histogram need the FILE-WIDE ordinal
    # range before any row can be bucketed, so each row group's ordinalized
    # column is buffered (a compact INT64 vector, not the raw column) rather
    # than re-reading the file a second time -- one pass over the on-disk
    # data, min/max derived natively from the buffered vectors, then
    # histogram bucketing natively against that range. Every per-row
    # reduction (hash/min-k, null count, nbytes, ordinalize, char-class
    # counts, min/max, histogram) is a native kernel; "bool_values" is the
    # one documented exception with no native equivalent (see
    # _compute_column_stats).
    accum: dict = {
        name: {
            # Bounded to <= MIN_K_HASHES candidates per row group (see
            # _native_min_k_smallest below), not a full-cardinality set of
            # every hash in the column -- for a multi-million-row column
            # that set was the dominant cost (CPU and, worse, retained
            # memory) of this whole function.
            "min_k_candidates": [],
            "null_count": 0,
            "ordinal_vecs": [],
            "char_counts": [0] * 8,
            "char_total_bytes": 0,
            "length_range": None,
            "nbytes": 0,
            # ARRAY only: the same two accumulators as above, but over the flat
            # child vector -- see the element block in _compute_column_stats.
            "element_min_k_candidates": [],
            "element_ordinal_vecs": [],
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
                    hash_vec = vec.hash_shaped()
                except ValueError:
                    hash_vec = None
                if hash_vec is not None:
                    # Per-row-group min-k (native, see _native_min_k_smallest),
                    # merged into a small cross-group candidate pool below --
                    # never a Python set sized to the column's row count.
                    acc["min_k_candidates"].extend(_native_min_k_smallest(hash_vec, MIN_K_HASHES))
                acc["null_count"] += vec.null_count()
                # Native byte accounting (see _column_nbytes_estimate) -- not
                # a to_pylist() decode of every row held for the whole file.
                acc["nbytes"] += _column_nbytes_estimate(morsel, name, vec)

                if category in _COMPRESSIBLE_CATEGORIES:
                    # ordinalize() doesn't support ARRAY/VECTOR_FP16/DECIMAL128
                    # -- see the identical guard in _compute_column_stats.
                    # BOOL true/false counts are derived from this same
                    # buffered ordinal vector at merge time below (see
                    # _compute_column_stats for why the range is fixed
                    # (0, 1) rather than this group's own min/max) --
                    # no separate to_pylist() pass needed here.
                    try:
                        acc["ordinal_vecs"].append(vec.ordinalize())
                    except ValueError:
                        pass

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

                if category == "ARRAY":
                    child = getattr(vec, "array_child", None)
                    if child is not None:
                        try:
                            acc["element_min_k_candidates"].extend(
                                _native_min_k_smallest(child.hash_shaped(), MIN_K_HASHES)
                            )
                        except (ValueError, AttributeError):
                            pass
                        try:
                            acc["element_ordinal_vecs"].append(child.ordinalize())
                        except (ValueError, AttributeError):
                            pass

    field_ids: list = []
    for name in col_names:
        field_ids.append(field_id_by_name.get(name) if field_id_by_name else None)
        category = col_info[name]
        acc = accum[name]

        # Merge each row group's own (already <= MIN_K_HASHES, already-unsigned)
        # min-k into one file-wide min-k -- the candidate pool is bounded by
        # MIN_K_HASHES * row_group_count, never by the column's row count, so
        # this dedupe+sort is cheap regardless of file size.
        candidates = acc["min_k_candidates"]
        distinct = set(candidates)
        col_min_k = (
            sorted(distinct)
            if len(distinct) <= MIN_K_HASHES
            else heapq.nsmallest(MIN_K_HASHES, distinct)
        )
        col_hist: list = []
        col_min = NULL_FLAG
        col_max = NULL_FLAG
        min_len = 0
        max_len = 0

        # ARRAY element stats, merged across row groups exactly as their
        # whole-column counterparts are.
        element_candidates = set(acc["element_min_k_candidates"])
        col_element_min_k = (
            sorted(element_candidates)
            if len(element_candidates) <= MIN_K_HASHES
            else heapq.nsmallest(MIN_K_HASHES, element_candidates)
        )
        col_element_min = NULL_FLAG
        col_element_max = NULL_FLAG
        element_pairs = [
            p
            for p in (v.ordinal_min_max() for v in acc["element_ordinal_vecs"])
            if p is not None
        ]
        if element_pairs:
            col_element_min = int(min(p[0] for p in element_pairs))
            col_element_max = int(max(p[1] for p in element_pairs))

        if category in _COMPRESSIBLE_CATEGORIES:
            vecs = acc["ordinal_vecs"]
            pairs = [p for p in (v.ordinal_min_max() for v in vecs) if p is not None]
            if pairs:
                vmin = min(p[0] for p in pairs)
                vmax = max(p[1] for p in pairs)
                col_min, col_max = int(vmin), int(vmax)
                if category == "BOOL":
                    # Fixed (0, 1) range, not (vmin, vmax) -- see the identical
                    # reasoning in _compute_column_stats. Native per-group
                    # bucketing on the already-buffered ordinal vectors, no
                    # to_pylist() decode.
                    false_count = 0
                    true_count = 0
                    for v in vecs:
                        b0, b1 = v.histogram_bucket(0, 1, 2)
                        false_count += b0
                        true_count += b1
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
        char_total_bytes_list.append(
            acc["char_total_bytes"] if category in _STRING_CATEGORIES else 0
        )
        element_min_values.append(col_element_min)
        element_max_values.append(col_element_max)
        element_min_k_hashes.append(col_element_min_k)

        col_bytes = acc["nbytes"]
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
        element_min_values=element_min_values,
        element_max_values=element_max_values,
        element_min_k_hashes=element_min_k_hashes,
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
