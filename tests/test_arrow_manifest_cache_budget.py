"""The columnar manifest cache is bounded in BYTES, not just entries.

The 32-entry count bound alone let the cache pin ~1GiB: a ~1MB manifest
parquet inflates to ~90MB resident once its columns are boxed and its sketch
vectors pinned, which OOMed the platform-wide expiration sweep in a 1GiB
container (2026-08-27). The native sketch Vectors expose no byte-size API, so
entry cost is estimated as raw-manifest bytes x MANIFEST_CACHE_INFLATION.
"""

import os
import sys

# Ensure local package imports during test runs
sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog.catalog import manifest_arrow


def setup_function(function):
    manifest_arrow.clear_arrow_manifest_cache()


def _store(path: str, raw_len: int):
    manifest_arrow._cache_store(path, ({}, 0, {}), raw_len)


def _budget_in_raw_bytes() -> int:
    return manifest_arrow.MANIFEST_CACHE_BYTES // manifest_arrow.MANIFEST_CACHE_INFLATION


def test_byte_budget_evicts_lru_before_count_bound():
    # Three entries that each claim just under half the budget: storing the
    # third must evict the least-recently-used first entry, long before the
    # 32-entry count bound is reached.
    raw_len = int(_budget_in_raw_bytes() * 0.45)
    _store("m1", raw_len)
    _store("m2", raw_len)
    _store("m3", raw_len)

    assert "m1" not in manifest_arrow._arrow_manifest_cache
    assert "m2" in manifest_arrow._arrow_manifest_cache
    assert "m3" in manifest_arrow._arrow_manifest_cache
    assert manifest_arrow._arrow_manifest_cache_total <= manifest_arrow.MANIFEST_CACHE_BYTES


def test_oversized_entry_is_never_cached():
    _store("small", 1024)
    _store("huge", _budget_in_raw_bytes() + 1)

    assert "huge" not in manifest_arrow._arrow_manifest_cache
    # ... and it must not have evicted anything to make room it can't use.
    assert "small" in manifest_arrow._arrow_manifest_cache


def test_oversized_replacement_drops_stale_entry():
    # A path re-seeded with bytes now too large to cache must not leave the
    # stale small entry serving reads.
    _store("m1", 1024)
    _store("m1", _budget_in_raw_bytes() + 1)

    assert "m1" not in manifest_arrow._arrow_manifest_cache
    assert manifest_arrow._arrow_manifest_cache_total == 0


def test_replacing_entry_does_not_leak_accounted_bytes():
    for _ in range(5):
        _store("m1", 1024)

    assert len(manifest_arrow._arrow_manifest_cache) == 1
    assert (
        manifest_arrow._arrow_manifest_cache_total == 1024 * manifest_arrow.MANIFEST_CACHE_INFLATION
    )


def test_invalidate_and_clear_release_accounted_bytes():
    _store("m1", 2048)
    _store("m2", 2048)

    manifest_arrow.invalidate_arrow_manifest("m1")
    assert (
        manifest_arrow._arrow_manifest_cache_total == 2048 * manifest_arrow.MANIFEST_CACHE_INFLATION
    )

    manifest_arrow.clear_arrow_manifest_cache()
    assert manifest_arrow._arrow_manifest_cache_total == 0
    assert manifest_arrow._arrow_manifest_cache_costs == {}


def test_count_bound_still_applies_to_tiny_entries():
    for i in range(manifest_arrow.ARROW_MANIFEST_CACHE_SIZE + 5):
        _store(f"m{i}", 16)

    assert len(manifest_arrow._arrow_manifest_cache) == manifest_arrow.ARROW_MANIFEST_CACHE_SIZE
