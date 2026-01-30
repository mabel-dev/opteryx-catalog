import io

import pyarrow as pa
import pyarrow.parquet as pq

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.metadata import DatasetMetadata, Snapshot


class _MemInput:
    def __init__(self, data: bytes):
        self._data = data

    def open(self):
        # Provide a file-like BytesIO which .read() returns the bytes
        return io.BytesIO(self._data)


class _MemIO:
    def __init__(self, mapping: dict):
        self._mapping = mapping

    def new_input(self, path: str):
        return _MemInput(self._mapping[path])


def _build_manifest_bytes():
    # Construct a parquet manifest with two entries, two columns per file
    schema = pa.schema(
        [
            ("file_path", pa.string()),
            ("file_format", pa.string()),
            ("record_count", pa.int64()),
            ("file_size_in_bytes", pa.int64()),
            ("uncompressed_size_in_bytes", pa.int64()),
            ("column_uncompressed_sizes_in_bytes", pa.list_(pa.int64())),
            ("null_counts", pa.list_(pa.int64())),
            ("min_k_hashes", pa.list_(pa.int64())),
            ("histogram_counts", pa.list_(pa.int64())),
            ("histogram_bins", pa.int64()),
            ("min_values", pa.list_(pa.int64())),
            ("max_values", pa.list_(pa.int64())),
            ("min_values_display", pa.list_(pa.string())),
            ("max_values_display", pa.list_(pa.string())),
        ]
    )

    file_path = pa.array(["f1.parquet", "f2.parquet"], type=pa.string())
    file_format = pa.array(["parquet", "parquet"], type=pa.string())
    record_count = pa.array([10, 20], type=pa.int64())
    file_size_in_bytes = pa.array([100, 200], type=pa.int64())
    uncompressed_size_in_bytes = pa.array([1000, 2000], type=pa.int64())
    column_uncompressed_sizes_in_bytes = pa.array(
        [[100, 400], [300, 200]], type=pa.list_(pa.int64())
    )
    null_counts = pa.array([[0, 0], [0, 0]], type=pa.list_(pa.int64()))
    min_k_hashes = pa.array([[1, 2], [1]], type=pa.list_(pa.int64()))
    histogram_counts = pa.array([[1, 2], [3, 4]], type=pa.list_(pa.int64()))
    histogram_bins = pa.array([32, 32], type=pa.int64())
    min_values = pa.array([[10, 20], [5, 30]], type=pa.list_(pa.int64()))
    max_values = pa.array([[100, 400], [300, 200]], type=pa.list_(pa.int64()))
    min_values_display = pa.array([[None, None], [None, None]], type=pa.list_(pa.string()))
    max_values_display = pa.array([[None, None], [None, None]], type=pa.list_(pa.string()))

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


def test_describe_includes_uncompressed_bytes():
    manifest_bytes = _build_manifest_bytes()
    manifest_path = "mem://manifest"

    meta = DatasetMetadata(
        dataset_identifier="tests_temp.test",
        location="mem://",
        schema=None,
        properties={},
    )

    # Add a schema with two columns so describe() can map names -> indices
    meta.schemas.append(
        {"schema_id": "s1", "columns": [{"name": "a"}, {"name": "b"}]}
    )
    meta.current_schema_id = "s1"

    # Prepare snapshot referencing our in-memory manifest
    snap = Snapshot(
        snapshot_id=1,
        timestamp_ms=1,
        manifest_list=manifest_path,
    )
    meta.snapshots.append(snap)
    meta.current_snapshot_id = 1

    ds = SimpleDataset(identifier="tests_temp.test", _metadata=meta)

    # Inject our in-memory IO mapping
    ds.io = _MemIO({manifest_path: manifest_bytes})

    desc = ds.describe()

    assert "a" in desc
    assert "b" in desc

    # Column 'a' should have uncompressed bytes = 100 + 300 = 400
    assert desc["a"]["uncompressed_bytes"] == 400
    # Column 'b' should have uncompressed bytes = 400 + 200 = 600
    assert desc["b"]["uncompressed_bytes"] == 600
