import os
import sys

# Ensure local package imports during test runs
sys.path.insert(0, os.path.join(sys.path[0], ".."))

import io

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache
from opteryx_catalog.catalog.manifest import get_manifest_metrics
from opteryx_catalog.catalog.manifest import get_parsed_manifest
from opteryx_catalog.catalog.manifest import invalidate_parsed_manifest
from opteryx_catalog.catalog.manifest import reset_manifest_metrics
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

    def new_input(self, path: str):
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


def test_parsed_manifest_cache_hits_and_invalidation():
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

    # First read -> miss
    rows1 = get_parsed_manifest(ds.io, manifest_path)
    m1 = get_manifest_metrics()
    assert m1.get("parsed_cache_misses", 0) >= 1
    assert m1.get("parsed_cache_hits", 0) == 0
    assert isinstance(rows1, list)

    # Inner list fields should have been frozen to tuples
    ent = rows1[0]
    assert isinstance(ent.get("column_uncompressed_sizes_in_bytes"), tuple)
    assert isinstance(ent.get("min_k_hashes"), tuple)

    # Second read -> hit
    get_parsed_manifest(ds.io, manifest_path)
    m2 = get_manifest_metrics()
    assert m2.get("parsed_cache_hits", 0) >= 1

    # Invalidate and force re-read -> miss increments
    invalidate_parsed_manifest(manifest_path)
    get_parsed_manifest(ds.io, manifest_path)
    m3 = get_manifest_metrics()
    assert m3.get("parsed_cache_misses", 0) >= 2
