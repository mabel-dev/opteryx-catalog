import io

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
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    columns = {
        "file_path": (["f1.parquet", "f2.parquet"], "VARCHAR"),
        "file_format": (["parquet", "parquet"], "VARCHAR"),
        "record_count": ([10, 20], "INTEGER"),
        "file_size_in_bytes": ([100, 200], "INTEGER"),
        "uncompressed_size_in_bytes": ([1000, 2000], "INTEGER"),
        "column_uncompressed_sizes_in_bytes": ([[100, 400], [300, 200]], "ARRAY"),
        "null_counts": ([[0, 0], [0, 0]], "ARRAY"),
        "min_k_hashes": ([[1, 2], [1]], "ARRAY"),
        "histogram_counts": ([[1, 2], [3, 4]], "ARRAY"),
        "histogram_bins": ([32, 32], "INTEGER"),
        "min_values": ([[10, 20], [5, 30]], "ARRAY"),
        "max_values": ([[100, 400], [300, 200]], "ARRAY"),
        "min_values_display": ([[None, None], [None, None]], "ARRAY"),
        "max_values_display": ([[None, None], [None, None]], "ARRAY"),
    }

    m = Morsel()
    for name, (values, dtype) in columns.items():
        m.append_vector(name, vector_from_sequence(values, dtype=dtype))

    return write_parquet(m)


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
    meta.schemas.append({"schema_id": "s1", "columns": [{"name": "a"}, {"name": "b"}]})
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
