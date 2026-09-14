from __future__ import annotations

import logging
import time
from collections import Counter
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

# There is no row-dict cache. Row dicts are DERIVED from the columnar cache on
# every call (see ``get_parsed_manifest``), because they cannot be cached
# safely: their cells are shared with the decoded columns, so a retained row
# list retains the whole decode. A count-bounded row cache therefore held
# manifests that the columnar cache's byte budget had already evicted and
# stopped accounting for - 32 entries at the ~90MB a gdelt_events manifest
# inflates to, in a 1GiB container, which is what OOMed the platform-wide
# expiration sweep nightly from 2026-08-27. One decode, one budget, one place
# to evict from: manifest_arrow.MANIFEST_CACHE_BYTES.


def get_parsed_manifest(io, manifest_path: str) -> list:
    """Return a Python representation (list[dict]) of the Parquet manifest.

    Read through the COLUMNAR cache (manifest_arrow) rather than decoding
    independently: that cache is the single decode, and the row dicts built
    here are views over its (immutable) cells.

    The rows themselves are NOT cached, and must not be. Sharing cells is what
    makes building them cheap, and it is also what makes retaining them
    expensive: a row list holds the entire decoded column set alive, so a
    row-dict cache kept manifests resident that the columnar cache had already
    evicted to honour its byte budget - the budget freed the bookkeeping and
    not the bytes. Deriving them per call costs the boxing of N dicts over
    columns already in memory; the storage read and the parquet decode stay
    cached, which is where the real cost was.

    A missing manifest propagates as FileNotFoundError: callers distinguish
    "no manifest" from "empty manifest".

    - Callers MUST treat returned lists/dicts as read-only.
    """

    if not manifest_path:
        return []

    from .manifest_arrow import get_arrow_manifest

    manifest = get_arrow_manifest(io, manifest_path)
    rows = _rows_from_columns(manifest._columns, len(manifest))
    _manifest_metrics["parsed_rows_materialized"] += len(rows)
    return rows


# Columns that write_parquet_manifest() comma-encodes into ARRAY<VARCHAR>
# because rugo's writer doesn't support nested ARRAY<ARRAY<...>> — decoded
# back to list[list[int]] here so callers see the original shape. Manifests
# written before this encoding existed store these as raw nested int lists
# directly (pyarrow's list<list<uint64/int64>>) — pass those through as-is.
_NESTED_INT_LIST_COLUMNS = ("min_k_hashes", "histogram_counts")


def _decode_nested_int_list_column(rows: list) -> list:
    """Decode to TUPLES of tuples, not lists: cells are shared between the
    columnar cache and every row dict built over it (see
    ``get_parsed_manifest``), and immutability is what makes that sharing
    safe."""

    def _decode_cell(s):
        if isinstance(s, str):
            return tuple(int(h) for h in s.split(",")) if s else ()
        if isinstance(s, list):  # native nested read: list[int] per column
            return tuple(s)
        return s

    return [None if row is None else tuple(_decode_cell(s) for s in row) for row in rows]


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

    # Freeze every remaining list cell (the generic ARRAY columns) to a tuple.
    # Cells are shared between the columnar cache, its row views, and the
    # row-dict cache built over it - immutable cells are what make one decoded
    # copy safe to hand to all of them.
    for name, col in column_data.items():
        if any(isinstance(c, list) for c in col):
            column_data[name] = [tuple(c) if isinstance(c, list) else c for c in col]

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


def _rows_from_columns(column_data: dict, row_count: int) -> list:
    """Row dicts over already-decoded columns. Cells are shared, not copied -
    they are immutable by construction (see ``read_manifest_columns``)."""
    if not column_data:
        return []
    names = list(column_data.keys())
    return [{name: column_data[name][i] for name in names} for i in range(row_count)]


def read_manifest_rows(data: bytes) -> list:
    """Decode manifest parquet bytes into a list of row dicts using rugo."""
    column_data, row_count, _native = read_manifest_columns(data)
    return _rows_from_columns(column_data, row_count)


def seed_parsed_manifest(manifest_path: str, data: bytes) -> None:
    """Populate the parsed-manifest cache from manifest bytes just WRITTEN.

    The write path holds the manifest's exact bytes at the moment it uploads
    them; parsing those into the cache means the next commit's
    ``_parent_manifest_entries`` is a cache hit instead of a re-download and
    re-parse of a file this process created moments ago. For a steady append
    stream that read was the dominant recurring cost of every commit.

    Parsing the written bytes rather than caching the caller's entry dicts is
    deliberate: the cache's contract is "exactly what a read of this path
    returns", and only the parquet round trip (numeric NULL handling, nested
    list decode, freezing) guarantees that.

    Replaces any stale entry at the same path, so it also subsumes the
    invalidation the write path used to do.
    """
    from .manifest_arrow import seed_arrow_manifest

    seed_arrow_manifest(manifest_path, data)


def invalidate_parsed_manifest(manifest_path: str) -> None:
    """Remove a manifest from the parsed-manifest cache (if present).

    Row dicts are derived per call and hold nothing between calls, so the
    columnar entry is the only thing there is to drop."""
    from .manifest_arrow import invalidate_arrow_manifest

    invalidate_arrow_manifest(manifest_path)


def clear_parsed_manifest_cache() -> None:
    """Drop every decoded manifest (tests / admin use).

    Kept under its original name: callers outside this package (opteryx-core's
    integration tests) use it to force a re-read, and that is still exactly
    what it does now that the columnar cache is the only one.
    """
    from .manifest_arrow import clear_arrow_manifest_cache

    clear_arrow_manifest_cache()


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


def _row_histogram_bins(histograms: list) -> int:
    """The row-level bin count, or 0 when the columns do not share one width.

    Widths legitimately vary per column: a BOOLEAN column gets an exact two-bin
    true/false histogram (and any low-cardinality column is entitled to exact
    bins) while the rest get HISTOGRAM_BINS. One row carries one
    `histogram_bins`, so a mixed file is described by 0 — the "no single width"
    marker readers normalize to None before falling back to each column's own
    slice length, which is the real width in every case. Stamping
    HISTOGRAM_BINS unconditionally instead makes the row claim 32 bins for a
    histogram that holds 2.
    """
    widths = {len(bins) for bins in histograms if bins}
    return widths.pop() if len(widths) == 1 else 0


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
        histogram_bins=_row_histogram_bins(histograms),
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


# Per-row-group histogram resolution in BOUNDED mode (see
# ParquetManifestEntryAccumulator). Each row group is bucketed at this width
# over its OWN range, and the file-wide HISTOGRAM_BINS histogram is assembled
# from those by proportional redistribution. Error per source bin is bounded by
# one fine-bin width of that row group's range: 1/256 of it.
FINE_HISTOGRAM_BINS = HISTOGRAM_BINS * 8


def _merge_min_k(candidates: list) -> list:
    """One file-wide min-k from per-row-group candidate pools.

    Each pool is already <= MIN_K_HASHES and already unsigned, so the merge is
    bounded by MIN_K_HASHES * row_group_count, never by the column's row count.
    """
    distinct = set(candidates)
    if len(distinct) <= MIN_K_HASHES:
        return sorted(distinct)
    return heapq.nsmallest(MIN_K_HASHES, distinct)


def _redistribute_histograms(groups: list, vmin: int, vmax: int) -> list:
    """Assemble a HISTOGRAM_BINS histogram over [vmin, vmax] from per-row-group
    fine histograms, each taken over its own [gmin, gmax].

    `groups` holds (gmin, gmax, fine_counts | None, non_null_count) per row
    group; `fine_counts` is None for a single-valued group. Mirrors the bucket
    mapping of Vector.histogram_bucket - value -> int(frac * (n_bins - 1)),
    clamped - so a single-group file lands its mass where the exact kernel
    would. Counts are conserved exactly: the result sums to the total non-null
    count, integer rounding settled by largest remainder.
    """
    bins = HISTOGRAM_BINS
    span = vmax - vmin
    acc = [0.0] * bins
    total = 0

    def _target(value: float) -> float:
        return (value - vmin) / span * (bins - 1)

    def _clamp(index: float) -> int:
        b = int(index)
        return 0 if b < 0 else (bins - 1 if b >= bins else b)

    for gmin, gmax, fine, non_null in groups:
        total += non_null
        if fine is None:
            acc[_clamp(_target(gmin))] += non_null
            continue
        fine_bins = len(fine)
        gspan = gmax - gmin
        for j, count in enumerate(fine):
            if count == 0:
                continue
            if j == fine_bins - 1:
                # The last fine bin holds only values exactly at gmax.
                acc[_clamp(_target(gmax))] += count
                continue
            v_lo = gmin + gspan * j / (fine_bins - 1)
            v_hi = gmin + gspan * (j + 1) / (fine_bins - 1)
            t_lo = _target(v_lo)
            t_hi = _target(v_hi)
            if t_hi <= t_lo:
                acc[_clamp(t_lo)] += count
                continue
            width = t_hi - t_lo
            b = int(t_lo)
            while b < bins and b <= t_hi:
                seg_lo = max(t_lo, b)
                seg_hi = min(t_hi, b + 1)
                if seg_hi > seg_lo:
                    acc[b] += count * (seg_hi - seg_lo) / width
                b += 1

    floors = [int(x) for x in acc]
    remainder = total - sum(floors)
    if remainder > 0:
        by_fraction = sorted(range(bins), key=lambda i: acc[i] - floors[i], reverse=True)
        for i in by_fraction[:remainder]:
            floors[i] += 1
    return floors


class ParquetManifestEntryAccumulator:
    """Build a ParquetManifestEntry one row group at a time.

    The producer-side twin of ``build_parquet_manifest_entry_from_bytes``: the
    same per-column kernels, run over each morsel as it is WRITTEN rather than
    over a re-read of the finished file. A writer that hands every row group
    through ``add`` gets, from ``finish``, the entry the catalog would
    otherwise have to download and decode the whole file to compute. That
    download and decode was a full extra pass over every byte compaction
    wrote, on top of the read, the sort and the write.

    Every statistic but one is merged exactly across row groups: min-k pools,
    null counts, byte counts, min/max, string byte classes, length ranges and
    ARRAY element bounds are all additive or order-free. The histogram is not:
    equi-width bins need the FILE-WIDE range before any row can be bucketed.

    ``exact_histograms`` chooses how that is paid for.
      True   buffers each row group's ordinalized column (an INT64 vector, 8
             bytes a row) until ``finish`` and buckets them all against the
             final range. Exact, and what the bytes path has always done -
             but it retains rows * compressible-columns * 8 bytes, which for a
             4 GB data file is gigabytes.
      False  buckets each row group at FINE_HISTOGRAM_BINS over its own range
             and redistributes into the file-wide bins at ``finish`` (see
             ``_redistribute_histograms``). Memory per row group per column is
             FINE_HISTOGRAM_BINS integers. Bin placement can be off by at most
             one fine-bin width of a row group's range; totals, bounds and
             BOOL true/false counts stay exact.
    The streaming writer uses False - constant memory is its whole point - and
    the bytes path keeps True so its output is unchanged.

    ``categories`` maps column name -> category (DrakenType name). When absent
    it is taken from the first morsel's schema, which is the exact-type source
    the morsel builder uses; the bytes path passes the categories it derives
    from the parquet logical types instead, because a re-read vector's own type
    is the flattened physical one.
    """

    def __init__(
        self,
        field_id_by_name: dict[str, int] | None = None,
        categories: dict[str, str] | None = None,
        exact_histograms: bool = False,
    ):
        self._field_id_by_name = field_id_by_name or {}
        self._categories: dict[str, str] | None = dict(categories) if categories else None
        self._exact = exact_histograms
        self._col_names: list[str] | None = None
        self._accum: dict = {}
        self._record_count = 0
        self._row_group_count = 0
        self._uncompressed = 0
        if self._categories is not None:
            # Known columns up front: the entry is describable even if no row
            # group ever arrives (a file with no row groups, in the bytes path).
            self._init_columns()

    @property
    def record_count(self) -> int:
        return self._record_count

    @property
    def row_group_count(self) -> int:
        return self._row_group_count

    @property
    def uncompressed_size_in_bytes(self) -> int:
        """Running in-memory byte footprint of everything added so far - the
        manifest's own size unit, so a writer rolling files on it produces
        files the planner's selection rules measure the same way."""
        return self._uncompressed

    def _start(self, morsel: Any) -> None:
        schema = morsel_schema_dict(morsel)
        self._categories = {name: schema[name].name for name in schema}
        self._init_columns()

    def _init_columns(self) -> None:
        self._col_names = list(self._categories.keys())
        self._accum = {
            name: {
                "min_k_candidates": [],
                "null_count": 0,
                "nbytes": 0,
                "ordinal_vecs": [],  # exact mode only, non-BOOL compressible columns
                "groups": [],  # bounded mode: (gmin, gmax, fine | None, non_null)
                "min": None,
                "max": None,
                "true_count": 0,
                "false_count": 0,
                "char_counts": [0] * 8,
                "char_total_bytes": 0,
                "length_range": None,
                "element_min_k_candidates": [],
                "element_min": None,
                "element_max": None,
            }
            for name in self._col_names
        }

    def add(self, morsel: Any) -> None:
        """Fold one row group in. Every morsel must carry every column."""
        if self._col_names is None:
            self._start(morsel)

        present = {
            (n.decode("utf-8") if isinstance(n, (bytes, bytearray)) else n)
            for n in morsel.column_names
        }
        missing = [name for name in self._col_names if name not in present]
        if missing:
            raise ValueError(
                f"row group is missing column(s) {missing}; every row group of a data "
                "file must carry the same columns"
            )

        self._record_count += int(morsel.num_rows)
        self._row_group_count += 1

        for name in self._col_names:
            # `_cxx_column`, not `column`: the engine hands its sinks morsels
            # on the C++ substrate, where PyObject column access is refused by
            # design (only the cursor materializes). `_cxx_column` is draken's
            # accessor for exactly this consumer - it reads the substrate
            # column when there is one and falls through to `column` when the
            # morsel is PyObject-backed - so one accumulator serves both the
            # streaming writer and the bytes path.
            vec = morsel._cxx_column(name.encode("utf-8"))
            category = self._categories[name]
            acc = self._accum[name]

            try:
                hash_vec = vec.hash_shaped()
            except ValueError:
                hash_vec = None
            if hash_vec is not None:
                acc["min_k_candidates"].extend(_native_min_k_smallest(hash_vec, MIN_K_HASHES))
            acc["null_count"] += vec.null_count()
            col_bytes = _column_nbytes_estimate(morsel, name, vec)
            acc["nbytes"] += col_bytes
            self._uncompressed += col_bytes

            if category in _COMPRESSIBLE_CATEGORIES:
                # ordinalize() refuses ARRAY/VECTOR_FP16/DECIMAL128 - no
                # bounds or histogram for those, same as every other builder.
                try:
                    ordinal = vec.ordinalize()
                except ValueError:
                    ordinal = None
                if ordinal is not None:
                    self._fold_ordinal(acc, ordinal, category)

            if category in _STRING_CATEGORIES:
                counts, total_bytes, length_range = vec.char_class_stats()
                for i in range(8):
                    acc["char_counts"][i] += counts[i]
                acc["char_total_bytes"] += total_bytes
                if length_range is not None:
                    self._fold_length(acc, length_range)
            elif category in _VARIABLE_WIDTH_CATEGORIES:
                # ARRAY: no native length reduction - see _compute_column_stats.
                lengths = [len(v) for v in vec.to_pylist() if v is not None]
                if lengths:
                    self._fold_length(acc, (min(lengths), max(lengths)))

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
                        pair = child.ordinalize().ordinal_min_max()
                    except (ValueError, AttributeError):
                        pair = None
                    if pair is not None:
                        lo, hi = int(pair[0]), int(pair[1])
                        acc["element_min"] = lo if acc["element_min"] is None else min(acc["element_min"], lo)
                        acc["element_max"] = hi if acc["element_max"] is None else max(acc["element_max"], hi)

    @staticmethod
    def _fold_length(acc: dict, length_range) -> None:
        lo, hi = length_range
        cur = acc["length_range"]
        acc["length_range"] = (lo, hi) if cur is None else (min(cur[0], lo), max(cur[1], hi))

    def _fold_ordinal(self, acc: dict, ordinal: Any, category: str) -> None:
        pair = ordinal.ordinal_min_max()
        if pair is None:
            return  # every row null: nothing to bound
        gmin, gmax = int(pair[0]), int(pair[1])
        acc["min"] = gmin if acc["min"] is None else min(acc["min"], gmin)
        acc["max"] = gmax if acc["max"] is None else max(acc["max"], gmax)
        if category == "BOOL":
            # Fixed (0, 1) domain, exact in every mode - see
            # _compute_column_stats for why the range is not this group's own.
            b0, b1 = ordinal.histogram_bucket(0, 1, 2)
            acc["false_count"] += int(b0)
            acc["true_count"] += int(b1)
            return
        if self._exact:
            acc["ordinal_vecs"].append(ordinal)
            return
        if gmax > gmin:
            fine = [int(c) for c in ordinal.histogram_bucket(gmin, gmax, FINE_HISTOGRAM_BINS)]
            acc["groups"].append((gmin, gmax, fine, sum(fine)))
        else:
            (non_null,) = ordinal.histogram_bucket(gmin, gmax, 1)
            acc["groups"].append((gmin, gmax, None, int(non_null)))

    def finish(self, file_path: str, file_size_in_bytes: int) -> ParquetManifestEntry:
        """The entry for everything added. Callable once the file is complete."""
        if self._col_names is None:
            raise ValueError("no row groups were added and no columns were declared; nothing to describe")

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

        for name in self._col_names:
            field_ids.append(self._field_id_by_name.get(name))
            category = self._categories[name]
            acc = self._accum[name]

            col_hist: list = []
            col_min = NULL_FLAG
            col_max = NULL_FLAG
            if acc["min"] is not None:
                col_min, col_max = acc["min"], acc["max"]
                if category == "BOOL":
                    col_hist = [acc["true_count"], acc["false_count"]]
                elif col_max > col_min:
                    if self._exact:
                        bins = [0] * HISTOGRAM_BINS
                        for v in acc["ordinal_vecs"]:
                            per = v.histogram_bucket(col_min, col_max, HISTOGRAM_BINS)
                            for i in range(HISTOGRAM_BINS):
                                bins[i] += per[i]
                        col_hist = bins
                    else:
                        col_hist = _redistribute_histograms(acc["groups"], col_min, col_max)

            min_len, max_len = acc["length_range"] if acc["length_range"] is not None else (0, 0)

            min_k_hashes.append(_merge_min_k(acc["min_k_candidates"]))
            histograms.append(col_hist)
            min_values.append(col_min)
            max_values.append(col_max)
            null_counts.append(acc["null_count"])
            min_lengths_list.append(min_len)
            max_lengths_list.append(max_len)
            is_string = category in _STRING_CATEGORIES
            char_class_counts.append(acc["char_counts"] if is_string else [])
            char_total_bytes_list.append(acc["char_total_bytes"] if is_string else 0)
            element_min_values.append(
                NULL_FLAG if acc["element_min"] is None else acc["element_min"]
            )
            element_max_values.append(
                NULL_FLAG if acc["element_max"] is None else acc["element_max"]
            )
            element_min_k_hashes.append(_merge_min_k(acc["element_min_k_candidates"]))
            column_uncompressed.append(acc["nbytes"])

        return ParquetManifestEntry(
            file_path=file_path,
            file_format="parquet",
            record_count=int(self._record_count),
            file_size_in_bytes=int(file_size_in_bytes),
            uncompressed_size_in_bytes=self._uncompressed,
            column_uncompressed_sizes_in_bytes=column_uncompressed,
            null_counts=null_counts,
            min_k_hashes=min_k_hashes,
            histogram_counts=histograms,
            histogram_bins=_row_histogram_bins(histograms),
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
    categories = {c.name: _category_from_logical_type(c.logical_type) for c in meta.schema_columns}

    # One pass over the on-disk data, one row group at a time, through the
    # same accumulator the streaming writer feeds as it writes. EXACT
    # histograms here: this path has the whole file in hand and has always
    # bucketed against the file-wide range, so its output does not change.
    accumulator = ParquetManifestEntryAccumulator(
        field_id_by_name=field_id_by_name, categories=categories, exact_histograms=True
    )
    with read_parquet(bytes(data_bytes)) as reader:
        for morsel in reader:
            accumulator.add(morsel)

    entry = accumulator.finish(file_path, int(file_size_in_bytes or len(data_bytes)))

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
