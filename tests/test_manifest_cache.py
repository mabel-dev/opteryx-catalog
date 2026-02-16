import os
import sys

# Ensure local package imports during test runs
sys.path.insert(0, os.path.join(sys.path[0], ".."))

import io

import pyarrow as pa
import pyarrow.parquet as pq

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.metadata import DatasetMetadata, Snapshot

from opteryx_catalog.catalog.manifest import (
    get_parsed_manifest,
    invalidate_parsed_manifest,
    clear_parsed_manifest_cache,
    get_manifest_metrics,
    reset_manifest_metrics,
)


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
    schema = pa.schema(
        [
            ("file_path", pa.string()),
            ("file_format", pa.string()),
            ("record_count", pa.int64()),
            ("file_size_in_bytes", pa.int64()),
            ("uncompressed_size_in_bytes", pa.int64()),
            ("column_uncompressed_sizes_in_bytes", pa.list_(pa.int64())),
            ("null_counts", pa.list_(pa.int64())),
            ("min_k_hashes", pa.list_(pa.list_(pa.uint64()))),
            ("histogram_counts", pa.list_(pa.list_(pa.int64()))),
            ("histogram_bins", pa.int64()),
            ("min_values", pa.list_(pa.int64())),
            ("max_values", pa.list_(pa.int64())),
            ("min_values_display", pa.list_(pa.string())),
            ("max_values_display", pa.list_(pa.string())),
        ]
    )

    file_path = pa.array(["f1.parquet"], type=pa.string())
    file_format = pa.array(["parquet"], type=pa.string())
    record_count = pa.array([10], type=pa.int64())
    file_size_in_bytes = pa.array([100], type=pa.int64())
    uncompressed_size_in_bytes = pa.array([1000], type=pa.int64())
    column_uncompressed_sizes_in_bytes = pa.array([[100, 400]], type=pa.list_(pa.int64()))
    null_counts = pa.array([[0, 0]], type=pa.list_(pa.int64()))
    min_k_hashes = pa.array([[[1, 2]]], type=pa.list_(pa.list_(pa.uint64())))
    histogram_counts = pa.array([[[1, 2]]], type=pa.list_(pa.list_(pa.int64())))
    histogram_bins = pa.array([32], type=pa.int64())
    min_values = pa.array([[10, 20]], type=pa.list_(pa.int64()))
    max_values = pa.array([[100, 400]], type=pa.list_(pa.int64()))
    min_values_display = pa.array([[None, None]], type=pa.list_(pa.string()))
    max_values_display = pa.array([[None, None]], type=pa.list_(pa.string()))

    table = pa.Table.from_arrays(
        [
            file_path,
            file_format,
            record_count,
            file_size_in_bytes,
            uncompressed_size_in_bytes,
            column_uncompressed_sizes_in_bytes,
            null_counts,
            min_k_hashes,
            histogram_counts,
            histogram_bins,
            min_values,
            max_values,
            min_values_display,
            max_values_display,
        ],
        schema=schema,
    )

    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


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
    rows2 = get_parsed_manifest(ds.io, manifest_path)
    m2 = get_manifest_metrics()
    assert m2.get("parsed_cache_hits", 0) >= 1

    # Invalidate and force re-read -> miss increments
    invalidate_parsed_manifest(manifest_path)
    rows3 = get_parsed_manifest(ds.io, manifest_path)
    m3 = get_manifest_metrics()
    assert m3.get("parsed_cache_misses", 0) >= 2
