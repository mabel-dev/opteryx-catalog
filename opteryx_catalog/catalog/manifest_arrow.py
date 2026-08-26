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
ARROW_MANIFEST_CACHE_SIZE: int = 32
_arrow_manifest_cache: OrderedDict[str, tuple] = OrderedDict()

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

    _arrow_manifest_cache[manifest_path] = (columns, row_count, sketch_vectors)
    if len(_arrow_manifest_cache) > ARROW_MANIFEST_CACHE_SIZE:
        _arrow_manifest_cache.popitem(last=False)

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
    _arrow_manifest_cache[manifest_path] = (columns, row_count, sketch_vectors)
    _arrow_manifest_cache.move_to_end(manifest_path)
    if len(_arrow_manifest_cache) > ARROW_MANIFEST_CACHE_SIZE:
        _arrow_manifest_cache.popitem(last=False)
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
    _arrow_manifest_cache.pop(manifest_path, None)


def clear_arrow_manifest_cache() -> None:
    """Clear entire manifest cache."""
    _arrow_manifest_cache.clear()


def get_retrieval_metrics() -> dict:
    """Get manifest retrieval performance metrics."""
    return dict(_manifest_retrieval_metrics)


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
