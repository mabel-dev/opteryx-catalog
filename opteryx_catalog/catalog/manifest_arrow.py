"""Native manifest retrieval for efficient query planning.

This module reads manifest parquet files via rugo (no pyarrow) and keeps the
data in a column-oriented form, avoiding per-cell scalar conversion.

Key properties:
- Columns are materialized once (bulk) and shared across row views.
- Column-oriented cache keyed by manifest path.
- Dict-like row interface for compatibility with existing code.

The module name is retained for backwards compatibility; the data is no longer
held as an Arrow table.
"""

from __future__ import annotations

import logging
import os
import time
from collections import OrderedDict
from collections.abc import Iterable
from collections.abc import Iterator
from typing import Any

from .manifest import read_manifest_columns

logger = logging.getLogger(__name__)

# Columns whose whole-column native draken Vector the planner reduces with
# native kernels (KMV NDV, histogram fold, exact-set membership, char-class
# fold) rather than the per-file boxed lists. Retained by get_arrow_manifest
# alongside the boxed data.
_SKETCH_VECTOR_COLUMNS = ("min_k_hashes", "histogram_counts", "char_class_counts")

# Cache configuration. Each entry is ``({column_name: [values...]}, row_count, sketch_vectors)``.
# Note: entries now also pin the native sketch vectors (min_k_hashes /
# histogram_counts / char_class_counts) per manifest, so a cached entry holds
# that manifest's sketch buffers alongside the boxed columns until evicted.
#
# The cache is bounded by BYTES as well as by entry count. The count bound
# alone let 32 whole parsed manifests pin memory: a ~1MB manifest parquet
# inflates to ~90MB resident once boxed to Python lists and its sketch
# vectors are pinned (measured against public.geopolitics.gdelt_events,
# 2026-08-27), so 32 entries exceeded a 1GiB container on their own and the
# platform-wide expiration sweep OOMed. The native sketch Vectors expose no
# byte-size API, so an entry's cost is ESTIMATED as the raw manifest size
# times the measured inflation factor. Entries whose estimated cost exceeds
# the whole budget are served but never cached.
ARROW_MANIFEST_CACHE_SIZE: int = 32
# Measured resident-set inflation of a parsed+pinned manifest over its raw
# parquet bytes (~92x for gdelt_events). Deliberately rounded up.
MANIFEST_CACHE_INFLATION: int = 100
MANIFEST_CACHE_BYTES: int = int(os.environ.get("OPTERYX_MANIFEST_CACHE_MB") or 128) * 1024 * 1024
_arrow_manifest_cache: OrderedDict[str, tuple] = OrderedDict()
# path -> estimated resident bytes for that entry; total tracked alongside.
_arrow_manifest_cache_costs: dict[str, int] = {}
_arrow_manifest_cache_total: int = 0


def _cache_store(manifest_path: str, entry: tuple, raw_byte_len: int) -> None:
    """Insert a parsed entry, evicting LRU entries to hold the byte budget.

    ``raw_byte_len`` is the size of the manifest's raw parquet bytes; the
    resident cost is estimated from it (see MANIFEST_CACHE_INFLATION).
    """
    global _arrow_manifest_cache_total

    cost = raw_byte_len * MANIFEST_CACHE_INFLATION
    if cost > MANIFEST_CACHE_BYTES:
        # Too large to ever fit: serve it, but don't let one entry own the
        # budget. Drop any stale entry at the same path.
        invalidate_arrow_manifest(manifest_path)
        return

    if manifest_path in _arrow_manifest_cache:
        _arrow_manifest_cache_total -= _arrow_manifest_cache_costs.get(manifest_path, 0)
    _arrow_manifest_cache[manifest_path] = entry
    _arrow_manifest_cache.move_to_end(manifest_path)
    _arrow_manifest_cache_costs[manifest_path] = cost
    _arrow_manifest_cache_total += cost

    while _arrow_manifest_cache and (
        _arrow_manifest_cache_total > MANIFEST_CACHE_BYTES
        or len(_arrow_manifest_cache) > ARROW_MANIFEST_CACHE_SIZE
    ):
        evicted_path, _ = _arrow_manifest_cache.popitem(last=False)
        _arrow_manifest_cache_total -= _arrow_manifest_cache_costs.pop(evicted_path, 0)


# Metrics
_manifest_retrieval_metrics = {
    "arrow_cache_hits": 0,
    "arrow_cache_misses": 0,
    "selective_reads": 0,
    "full_reads": 0,
    "total_retrieval_time_ms": 0,
    "total_conversion_time_ms": 0,
}


class ArrowManifestRow:
    """Dict-like view over one row of a manifest whose columns have already
    been bulk-materialized to Python lists.

    Field access is a plain list index — no per-cell scalar conversion.
    """

    __slots__ = ("_columns", "_row_idx")

    def __init__(self, columns: dict, row_idx: int):
        self._columns = columns
        self._row_idx = row_idx

    def get(self, key: str, default: Any = None) -> Any:
        col = self._columns.get(key)
        if col is None:
            return default
        return col[self._row_idx]

    def __getitem__(self, key: str) -> Any:
        return self._columns[key][self._row_idx]

    def __contains__(self, key: str) -> bool:
        return key in self._columns

    def keys(self):
        return self._columns.keys()

    def items(self):
        idx = self._row_idx
        for key, col in self._columns.items():
            yield key, col[idx]

    def __repr__(self) -> str:
        return f"ArrowManifestRow(row={self._row_idx})"


class ArrowManifest(Iterable):
    """Iterable wrapper over a manifest's column data.

    Columns are already materialized as Python lists and shared across all row
    views, so iterating N rows costs no per-cell conversion.
    """

    __slots__ = ("_columns", "_row_count", "sketch_vectors")

    def __init__(self, columns: dict, row_count: int, sketch_vectors: dict | None = None):
        self._columns = columns
        self._row_count = row_count
        # Whole-column native draken Vectors for sketch columns (min_k_hashes /
        # histogram_counts), retained so the planner reduces them with native
        # kernels instead of re-boxing. Empty on the boxing-only path.
        self.sketch_vectors = sketch_vectors or {}

    def __iter__(self) -> Iterator[ArrowManifestRow]:
        """Iterate over rows as dict-like objects."""
        columns = self._columns
        for i in range(self._row_count):
            yield ArrowManifestRow(columns, i)

    def __len__(self) -> int:
        """Get number of rows."""
        return self._row_count

    def to_pylist(self) -> list[dict]:
        """Convert to a list of row dicts."""
        columns = self._columns
        names = list(columns.keys())
        return [{name: columns[name][i] for name in names} for i in range(self._row_count)]


def get_arrow_manifest(io: Any, manifest_path: str) -> ArrowManifest:
    """Get manifest column data via rugo, keeping it column-oriented for planning."""

    if not manifest_path:
        return ArrowManifest({}, 0)

    # Check cache
    if manifest_path in _arrow_manifest_cache:
        _arrow_manifest_cache.move_to_end(manifest_path)
        _manifest_retrieval_metrics["arrow_cache_hits"] += 1
        columns, row_count, sketch_vectors = _arrow_manifest_cache[manifest_path]
        return ArrowManifest(columns, row_count, sketch_vectors)

    _manifest_retrieval_metrics["arrow_cache_misses"] += 1
    start_time = time.perf_counter()

    # Read bytes from storage
    inp = io.new_input(manifest_path)
    with inp.open() as f:
        data = f.read()

    columns, row_count, sketch_vectors = read_manifest_columns(
        data, keep_native=_SKETCH_VECTOR_COLUMNS
    )
    _manifest_retrieval_metrics["full_reads"] += 1

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    _manifest_retrieval_metrics["total_retrieval_time_ms"] += elapsed_ms

    _cache_store(manifest_path, (columns, row_count, sketch_vectors), len(data))

    logger.debug(f"Loaded manifest {manifest_path} in {elapsed_ms:.1f}ms ({row_count} rows)")

    return ArrowManifest(columns, row_count, sketch_vectors)


def seed_arrow_manifest(manifest_path: str, data: bytes) -> ArrowManifest:
    """Populate the columnar cache from manifest bytes already in hand (the
    write path holds them at upload time). One decode serves the planner's
    columnar reads AND, via ``manifest.seed_parsed_manifest``, the commit
    path's row dicts. Replaces any stale entry at the same path."""
    columns, row_count, sketch_vectors = read_manifest_columns(
        data, keep_native=_SKETCH_VECTOR_COLUMNS
    )
    _cache_store(manifest_path, (columns, row_count, sketch_vectors), len(data))
    return ArrowManifest(columns, row_count, sketch_vectors)


def get_parsed_manifest(io: Any, manifest_path: str) -> list:
    """Compatibility wrapper: returns list of row dicts."""
    start_time = time.perf_counter()
    result = get_arrow_manifest(io, manifest_path).to_pylist()
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    _manifest_retrieval_metrics["total_conversion_time_ms"] += elapsed_ms
    return result


def get_arrow_manifest_rows(io: Any, manifest_path: str) -> Iterator[ArrowManifestRow]:
    """Iterate manifest rows without building a list of dicts."""
    manifest = get_arrow_manifest(io, manifest_path)
    return iter(manifest)


def invalidate_arrow_manifest(manifest_path: str) -> None:
    """Remove manifest from cache."""
    global _arrow_manifest_cache_total
    if _arrow_manifest_cache.pop(manifest_path, None) is not None:
        _arrow_manifest_cache_total -= _arrow_manifest_cache_costs.pop(manifest_path, 0)


def clear_arrow_manifest_cache() -> None:
    """Clear entire manifest cache."""
    global _arrow_manifest_cache_total
    _arrow_manifest_cache.clear()
    _arrow_manifest_cache_costs.clear()
    _arrow_manifest_cache_total = 0


def get_retrieval_metrics() -> dict:
    """Get manifest retrieval performance metrics."""
    metrics = dict(_manifest_retrieval_metrics)
    metrics["manifest_cache_entries"] = len(_arrow_manifest_cache)
    metrics["manifest_cache_estimated_bytes"] = _arrow_manifest_cache_total
    return metrics


def reset_retrieval_metrics() -> None:
    """Reset retrieval metrics."""
    _manifest_retrieval_metrics.update(
        {
            "arrow_cache_hits": 0,
            "arrow_cache_misses": 0,
            "selective_reads": 0,
            "full_reads": 0,
            "total_retrieval_time_ms": 0,
            "total_conversion_time_ms": 0,
        }
    )


# For backwards compatibility with existing code
def get_manifest_rows_as_dicts(io: Any, manifest_path: str) -> list[dict]:
    """Get manifest as list of dicts (old API)."""
    return get_parsed_manifest(io, manifest_path)
