"""Row dicts from `get_parsed_manifest` must not outlive the columnar cache.

Row dicts share the decoded column cells, so anything that RETAINS a row list
retains the whole decode. The row-dict cache that used to sit in front of the
columnar one was bounded by entry count only, so it kept ~90MB manifests
resident that the columnar cache's byte budget had already evicted and stopped
accounting for - the budget freed the bookkeeping and not the bytes, and the
platform-wide expiration sweep OOMed nightly in its 1GiB container from
2026-08-27. Rows are derived per call now; these tests hold that line.
"""

import gc
import os
import sys
import weakref

# Ensure local package imports during test runs
sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog.catalog import manifest
from opteryx_catalog.catalog import manifest_arrow


class _Columns(dict):
    """A dict that can be weak-referenced, so a test can prove it was freed."""


def setup_function(function):
    manifest_arrow.clear_arrow_manifest_cache()


def _seed(path: str, rows: int = 2, raw_len: int = 1024) -> _Columns:
    """Put one decoded manifest in the columnar cache, bypassing IO."""
    columns = _Columns(
        {
            "file_path": [f"{path}/data-{i}.parquet" for i in range(rows)],
            "file_size_in_bytes": [100 + i for i in range(rows)],
        }
    )
    manifest_arrow._cache_store(path, (columns, rows, {}), raw_len)
    return columns


def _budget_in_raw_bytes() -> int:
    return manifest_arrow.MANIFEST_CACHE_BYTES // manifest_arrow.MANIFEST_CACHE_INFLATION


def test_there_is_no_row_dict_cache():
    # The regression guard proper: a module-level store of row lists is the
    # bug, whatever it ends up being called.
    assert not hasattr(manifest, "_parsed_manifest_cache")


def test_rows_are_derived_per_call_not_handed_back_from_a_store():
    _seed("m1")

    first = manifest.get_parsed_manifest(None, "m1")
    second = manifest.get_parsed_manifest(None, "m1")

    assert first == second
    assert first is not second


def test_rows_read_correctly_through_the_columnar_cache():
    _seed("m1", rows=3)

    rows = manifest.get_parsed_manifest(None, "m1")

    assert [r["file_path"] for r in rows] == [
        "m1/data-0.parquet",
        "m1/data-1.parquet",
        "m1/data-2.parquet",
    ]
    assert rows[1]["file_size_in_bytes"] == 101


def test_evicted_manifest_is_actually_freed():
    # The whole point: once the columnar cache evicts an entry and the caller
    # drops the rows it was handed, the decode must be collectable. While the
    # row cache existed this assertion failed - the columns stayed alive,
    # unaccounted, for as long as the row list was cached.
    columns = _seed("m1")
    watch = weakref.ref(columns)
    del columns

    rows = manifest.get_parsed_manifest(None, "m1")
    assert len(rows) == 2
    del rows

    # Push it out on the byte budget, the way a sweep over many datasets does.
    _seed("m2", raw_len=int(_budget_in_raw_bytes() * 0.9))
    _seed("m3", raw_len=int(_budget_in_raw_bytes() * 0.9))
    assert "m1" not in manifest_arrow._arrow_manifest_cache

    gc.collect()
    assert watch() is None


def test_seeding_from_written_bytes_leaves_no_row_list_behind():
    # `seed_parsed_manifest` is the write path's fast path; it must seed the
    # decode only, never a second retained shape of it.
    columns = _Columns({"file_path": ["m1/data-0.parquet"]})
    manifest_arrow._cache_store("m1", (columns, 1, {}), 1024)
    watch = weakref.ref(columns)
    del columns

    manifest_arrow.invalidate_arrow_manifest("m1")
    gc.collect()
    assert watch() is None


def test_clear_parsed_manifest_cache_still_forces_a_reread():
    # Name kept for opteryx-core's integration tests, which call it to force a
    # re-read after writing behind the catalog's back.
    _seed("m1")
    assert "m1" in manifest_arrow._arrow_manifest_cache

    manifest.clear_parsed_manifest_cache()

    assert "m1" not in manifest_arrow._arrow_manifest_cache
