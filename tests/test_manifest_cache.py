import os
import sys

# Ensure local package imports during test runs
sys.path.insert(0, os.path.join(sys.path[0], ".."))

import io

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache
from opteryx_catalog.catalog.manifest import get_parsed_manifest
from opteryx_catalog.catalog.manifest import invalidate_parsed_manifest
from opteryx_catalog.catalog.manifest import reset_manifest_metrics
from opteryx_catalog.catalog.manifest_arrow import get_retrieval_metrics
from opteryx_catalog.catalog.manifest_arrow import reset_retrieval_metrics
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot


class _MemInput:
    def __init__(self, data: bytes):
        self._data = data

    def open(self):
        return io.BytesIO(self._data)


class _MemIO:
    def __init__(self, mapping: dict):
        self._mapping = mapping
        self.reads = 0

    def new_input(self, path: str):
        self.reads += 1
        return _MemInput(self._mapping[path])


def _build_manifest_bytes():
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    columns = {
        "file_path": (["f1.parquet"], "VARCHAR"),
        "file_format": (["parquet"], "VARCHAR"),
        "record_count": ([10], "INTEGER"),
        "file_size_in_bytes": ([100], "INTEGER"),
        "uncompressed_size_in_bytes": ([1000], "INTEGER"),
        "column_uncompressed_sizes_in_bytes": ([[100, 400]], "ARRAY"),
        "null_counts": ([[0, 0]], "ARRAY"),
        # min_k_hashes/histogram_counts store each per-column hash/bucket list
        # comma-encoded (see write_parquet_manifest) since rugo can't write
        # ARRAY<ARRAY<...>> — one string per schema column, per row.
        "min_k_hashes": ([["1,2"]], "ARRAY"),
        "histogram_counts": ([["1,2"]], "ARRAY"),
        "histogram_bins": ([32], "INTEGER"),
        "min_values": ([[10, 20]], "ARRAY"),
        "max_values": ([[100, 400]], "ARRAY"),
        "min_values_display": ([[None, None]], "ARRAY"),
        "max_values_display": ([[None, None]], "ARRAY"),
    }

    m = Morsel()
    for name, (values, dtype) in columns.items():
        m.append_vector(name, vector_from_sequence(values, dtype=dtype))

    return write_parquet(m)


def test_manifest_reads_are_cached_and_invalidation_forces_a_reread():
    """A repeat read costs no IO and no decode; invalidation restores both.

    Asserted on reads through the IO layer rather than on cache counters: the
    row dicts `get_parsed_manifest` returns are derived per call now (they
    share the decoded cells, so caching them pinned whatever the columnar
    budget had evicted - see test_parsed_manifest_not_retained), and what must
    stay cached is the expensive half, the download and the parquet decode.
    """
    manifest_bytes = _build_manifest_bytes()
    manifest_path = "mem://manifest-cache-test"

    mapping = {manifest_path: manifest_bytes}
    ds_meta = DatasetMetadata(
        dataset_identifier="tests.test", location="mem://", schema=None, properties={}
    )
    snap = Snapshot(snapshot_id=1, timestamp_ms=1, manifest_list=manifest_path)
    ds_meta.snapshots.append(snap)
    ds_meta.current_snapshot_id = 1

    ds = SimpleDataset(identifier="tests.test", _metadata=ds_meta)
    ds.io = _MemIO(mapping)

    # Clear any previous state
    clear_parsed_manifest_cache()
    reset_manifest_metrics()
    reset_retrieval_metrics()

    # First read -> miss, one trip to storage
    rows1 = get_parsed_manifest(ds.io, manifest_path)
    assert ds.io.reads == 1
    assert get_retrieval_metrics().get("arrow_cache_misses", 0) == 1
    assert isinstance(rows1, list)

    # Inner list fields are frozen to tuples: the cells are shared with the
    # cached columns and with every other row dict built over them, and
    # immutability is what makes that sharing safe.
    ent = rows1[0]
    assert isinstance(ent.get("column_uncompressed_sizes_in_bytes"), tuple)
    assert isinstance(ent.get("min_k_hashes"), tuple)

    # Second read -> hit, no further IO
    rows2 = get_parsed_manifest(ds.io, manifest_path)
    assert ds.io.reads == 1
    assert get_retrieval_metrics().get("arrow_cache_hits", 0) >= 1
    assert rows2 == rows1

    # Invalidate and force re-read -> back to storage
    invalidate_parsed_manifest(manifest_path)
    get_parsed_manifest(ds.io, manifest_path)
    assert ds.io.reads == 2
    assert get_retrieval_metrics().get("arrow_cache_misses", 0) == 2
