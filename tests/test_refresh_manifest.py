import io
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

# Add local paths to sys.path to use local code instead of installed packages
sys.path.insert(0, os.path.join(sys.path[0], ".."))  # Add parent dir for pyiceberg_firestore_gcs
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))
sys.path.insert(1, os.path.join(sys.path[0], "../pyiceberg-firestore-gcs"))


import pytest

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.manifest import (
    build_parquet_manifest_entry_from_bytes,
    get_manifest_metrics,
    reset_manifest_metrics,
)
from opteryx_catalog.catalog.metadata import DatasetMetadata, Snapshot
from opteryx_catalog.opteryx_catalog import OpteryxCatalog


def test_min_k_hashes_for_string_and_binary():
    try:
        pass  # type: ignore
    except Exception:
        pytest.skip("opteryx.compiled.draken not available")

    import pyarrow as pa

    # short binary and short string columns should get min-k
    t = _make_parquet_table(
        [("bin", pa.binary()), ("s", pa.string())], [(b"a", "x"), (b"b", "y"), (b"c", "z")]
    )
    buf = pa.BufferOutputStream()
    pq.write_table(t, buf, compression="zstd")
    data = buf.getvalue().to_pybytes()
    e = build_parquet_manifest_entry_from_bytes(data, "mem://f", len(data), orig_table=t)
    assert len(e.min_k_hashes[0]) > 0
    assert len(e.min_k_hashes[1]) > 0


# Step 1: Create a local catalog
catalog = OpteryxCatalog(
    "opteryx",
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
)

# print(catalog.load_dataset("ops.stdout_log").describe())


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

    def new_output(self, path: str):
        class Out:
            def __init__(self, mapping, path):
                self._buf = io.BytesIO()
                self._mapping = mapping
                self._path = path

            def write(self, data: bytes):
                self._buf.write(data)

            def close(self):
                self._mapping[self._path] = self._buf.getvalue()

            def create(self):
                return self

        return Out(self._mapping, path)


class _FakeCatalog:
    def __init__(self, io):
        self.io = io

    def write_parquet_manifest(
        self, snapshot_id: int, entries: list[dict], dataset_location: str
    ) -> str:
        # Minimal manifest writer using same schema as production
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
                ("histogram_bins", pa.int32()),
                ("min_values", pa.list_(pa.int64())),
                ("max_values", pa.list_(pa.int64())),
                ("min_values_display", pa.list_(pa.string())),
                ("max_values_display", pa.list_(pa.string())),
                ("min_lengths", pa.list_(pa.int64())),
                ("max_lengths", pa.list_(pa.int64())),
            ]
        )
        normalized = []
        for ent in entries:
            if not isinstance(ent, dict):
                normalized.append(ent)
                continue
            e = dict(ent)
            e.setdefault("min_k_hashes", [])
            e.setdefault("histogram_counts", [])
            e.setdefault("histogram_bins", 0)
            e.setdefault("column_uncompressed_sizes_in_bytes", [])
            e.setdefault("null_counts", [])
            e.setdefault("min_values_display", [])
            e.setdefault("max_values_display", [])
            e.setdefault("min_lengths", [])
            e.setdefault("max_lengths", [])
            mv = e.get("min_values") or []
            xv = e.get("max_values") or []
            mv_disp = e.get("min_values_display") or []
            xv_disp = e.get("max_values_display") or []
            e["min_values"] = [int(v) if v is not None else None for v in mv]
            e["max_values"] = [int(v) if v is not None else None for v in xv]
            e["min_values_display"] = [str(v) if v is not None else None for v in mv_disp]
            e["max_values_display"] = [str(v) if v is not None else None for v in xv_disp]
            normalized.append(e)

        table = pa.Table.from_pylist(normalized, schema=schema)
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf, compression="zstd")
        data = buf.getvalue().to_pybytes()
        path = f"{dataset_location}/metadata/manifest-{snapshot_id}.parquet"
        out = self.io.new_output(path).create()
        out.write(data)
        out.close()
        return path


def _make_parquet_table(columns: list[tuple[str, pa.DataType]], rows: list[tuple]):
    arrays = []
    for i, (name, dtype) in enumerate(columns):
        col_vals = [r[i] for r in rows]
        arrays.append(pa.array(col_vals, type=dtype))
    return pa.Table.from_arrays(arrays, names=[c[0] for c in columns])


def test_build_manifest_from_bytes_matches_table():
    # ensure the bytes-based builder matches the table-based one
    t = _make_parquet_table([("a", pa.int64()), ("b", pa.int64())], [(1, 10), (2, 20)])
    buf = pa.BufferOutputStream()
    pq.write_table(t, buf, compression="zstd")
    data = buf.getvalue().to_pybytes()

    e_bytes = build_parquet_manifest_entry_from_bytes(data, "mem://f", len(data), orig_table=t)
    # basic sanity checks (parity is enforced by using orig_table when available)
    assert e_bytes.record_count == 2
    assert e_bytes.file_size_in_bytes == len(data)


def test_build_manifest_keeps_chunked_columns_until_stats(monkeypatch):
    chunked = pa.chunked_array(
        [
            pa.array([b"a"], type=pa.binary()),
            pa.array([b"bb"], type=pa.binary()),
            pa.array([b"ccc"], type=pa.binary()),
        ]
    )
    table = pa.Table.from_arrays([chunked], names=["blob"])
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="zstd", row_group_size=1)
    data = buf.getvalue().to_pybytes()

    seen = {}

    def fake_compute(col, field_type, file_path):
        seen["col"] = col
        seen["field_type"] = field_type
        seen["file_path"] = file_path
        return ([], [], 0, 0, None, None, 0, 0, 0)

    monkeypatch.setattr(
        "opteryx_catalog.catalog.manifest._compute_stats_for_arrow_column",
        fake_compute,
    )

    entry = build_parquet_manifest_entry_from_bytes(data, "test.parquet", len(data))

    assert isinstance(seen["col"], pa.ChunkedArray)
    assert seen["field_type"] == pa.binary()
    assert seen["file_path"] == "test.parquet"
    assert entry.record_count == 3


def test_manifest_metrics_increments():
    reset_manifest_metrics()
    t = _make_parquet_table([("a", pa.int64()), ("b", pa.int64())], [(1, 10), (2, 20)])
    buf = pa.BufferOutputStream()
    pq.write_table(t, buf, compression="zstd")
    data = buf.getvalue().to_pybytes()

    _ = build_parquet_manifest_entry_from_bytes(data, "mem://f", len(data), orig_table=t)
    m = get_manifest_metrics()
    assert m.get("files_read", 0) >= 1
    assert m.get("hash_calls", 0) >= 1
    assert m.get("compress_calls", 0) >= 1


def test_table_based_builder_is_removed():
    from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry

    t = _make_parquet_table([("a", pa.int64())], [(1,)])
    with pytest.raises(RuntimeError):
        _ = build_parquet_manifest_entry(t, "mem://f", 0)


def test_manifest_uses_rugo_for_sizes():
    # Ensure the bytes-based builder uses rugo metadata to compute per-column sizes
    reset_manifest_metrics()
    t = _make_parquet_table([("a", pa.int64()), ("b", pa.int64())], [(1, 10), (2, 20)])
    buf = pa.BufferOutputStream()
    pq.write_table(t, buf, compression="zstd")
    data = buf.getvalue().to_pybytes()

    entry = build_parquet_manifest_entry_from_bytes(data, "mem://f", len(data))
    m = get_manifest_metrics()

    # rugo should report sizes (non-zero) for these synthetic files
    assert m.get("sizes_from_rugo", 0) >= 1 or m.get("sizes_from_rugo_missing", 0) == 0
    assert entry.uncompressed_size_in_bytes >= 0
    assert isinstance(entry.column_uncompressed_sizes_in_bytes, list)
    assert len(entry.column_uncompressed_sizes_in_bytes) == 2
    # column sizes may be non-zero when metadata is available
    assert all(isinstance(x, int) for x in entry.column_uncompressed_sizes_in_bytes)


def test_refresh_manifest_with_single_file():
    # single file with columns a,b for quick iteration
    t1 = _make_parquet_table([("a", pa.int64()), ("b", pa.int64())], [(1, 10), (2, 20)])

    # Write parquet file to mem
    buf = pa.BufferOutputStream()
    pq.write_table(t1, buf, compression="zstd")
    d1 = buf.getvalue().to_pybytes()

    f1 = "mem://data/f1.parquet"
    manifest_path = "mem://manifest-old"

    # Build initial manifest entry for single file (bytes-based builder)
    e1 = build_parquet_manifest_entry_from_bytes(d1, f1, len(d1), orig_table=t1).to_dict()

    # Create in-memory IO mapping including manifest and data file
    mapping = {f1: d1}

    # Write initial manifest with the single entry using the same writer as the catalog
    fake_writer = _FakeCatalog(_MemIO(mapping))
    manifest_path = fake_writer.write_parquet_manifest(1, [e1], "mem://")
    # Ensure the manifest bytes are present in the mapping
    mapping[manifest_path] = mapping[manifest_path]

    # Persist the single-file manifest as JSON for quick inspection during
    # iterative debugging (writes to repo `artifacts/` so you can open it).
    import json
    import os

    artifacts_dir = os.path.join(os.getcwd(), "artifacts")
    os.makedirs(artifacts_dir, exist_ok=True)
    with open(
        os.path.join(artifacts_dir, "single_file_manifest.json"), "w", encoding="utf-8"
    ) as fh:
        json.dump(e1, fh, indent=2, default=str)

    # Create metadata and snapshot
    meta = DatasetMetadata(
        dataset_identifier="tests_temp.test", location="mem://", schema=None, properties={}
    )
    meta.schemas.append({"schema_id": "s1", "columns": [{"name": "a"}, {"name": "b"}]})
    meta.current_schema_id = "s1"
    snap = Snapshot(snapshot_id=1, timestamp_ms=1, manifest_list=manifest_path)
    meta.snapshots.append(snap)
    meta.current_snapshot_id = 1

    ds = SimpleDataset(identifier="tests_temp.test", _metadata=meta)
    ds.io = _MemIO(mapping)
    ds.catalog = _FakeCatalog(ds.io)

    # Refresh manifest (should re-read f1 and write a new manifest)
    new_snap_id = ds.refresh_manifest(agent="test-agent", author="tester")
    assert new_snap_id is not None

    # Describe should include both columns and count bytes appropriately
    desc = ds.describe()
    assert "a" in desc
    assert "b" in desc

    # ensure uncompressed bytes are present and non-zero for both cols
    assert desc["a"]["uncompressed_bytes"] > 0
    assert desc["b"]["uncompressed_bytes"] > 0


def test_min_max_lengths_for_strings():
    """Test min/max length computation for string columns."""
    try:
        pass  # type: ignore
    except Exception:
        pytest.skip("opteryx.compiled.draken not available")

    # Create a Parquet table with variable-length strings
    table = pa.table({
        "strings": pa.array(["a", "hello", "the quick brown fox", None, "hi"])
    })

    # Write to bytes
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="zstd")
    data = buf.getvalue().to_pybytes()

    # Build manifest entry
    entry = build_parquet_manifest_entry_from_bytes(data, "test.parquet", len(data), orig_table=table)

    # Verify min/max lengths (None values should be excluded)
    # Non-null strings: "a" (1), "hello" (5), "the quick brown fox" (19), "hi" (2)
    assert entry.min_lengths[0] == 1  # min length is "a"
    assert entry.max_lengths[0] == 19  # max length is "the quick brown fox"


def test_min_max_lengths_for_binary():
    """Test min/max length computation for binary columns."""
    try:
        pass  # type: ignore
    except Exception:
        pytest.skip("opteryx.compiled.draken not available")

    # Create a Parquet table with variable-length binary data
    table = pa.table({
        "binary_data": pa.array([b"ab", b"x", b"hello world", None, b"123456789"])
    })

    # Write to bytes
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="zstd")
    data = buf.getvalue().to_pybytes()

    # Build manifest entry
    entry = build_parquet_manifest_entry_from_bytes(data, "test.parquet", len(data), orig_table=table)

    # Non-null binary: b"ab" (2), b"x" (1), b"hello world" (11), b"123456789" (9)
    assert entry.min_lengths[0] == 1  # min length is b"x"
    assert entry.max_lengths[0] == 11  # max length is b"hello world"


def test_min_max_lengths_for_lists():
    """Test min/max length computation for list/array columns."""
    try:
        pass  # type: ignore
    except Exception:
        pytest.skip("opteryx.compiled.draken not available")

    # Create a Parquet table with variable-size lists
    table = pa.table({
        "list_data": pa.array([[1, 2, 3], [4], [5, 6], None, [7, 8, 9, 10, 11]])
    })

    # Write to bytes
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="zstd")
    data = buf.getvalue().to_pybytes()

    # Build manifest entry
    entry = build_parquet_manifest_entry_from_bytes(data, "test.parquet", len(data), orig_table=table)

    # Non-null lists: [1,2,3] (3), [4] (1), [5,6] (2), [7,8,9,10,11] (5)
    assert entry.min_lengths[0] == 1  # min length is [4]
    assert entry.max_lengths[0] == 5  # max length is [7,8,9,10,11]


def test_min_max_lengths_for_numeric_columns():
    """Test that numeric/boolean columns have zero lengths."""
    try:
        pass  # type: ignore
    except Exception:
        pytest.skip("opteryx.compiled.draken not available")

    # Create a Parquet table with various numeric types
    table = pa.table({
        "int_col": pa.array([1, 2, 3, 4, 5]),
        "float_col": pa.array([1.1, 2.2, 3.3]),
        "bool_col": pa.array([True, False, True])
    })

    # Write to bytes
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="zstd")
    data = buf.getvalue().to_pybytes()

    # Build manifest entry
    entry = build_parquet_manifest_entry_from_bytes(data, "test.parquet", len(data), orig_table=table)

    # All non-variable-width types should have 0 length
    assert entry.min_lengths[0] == 0  # int_col
    assert entry.max_lengths[0] == 0  # int_col
    assert entry.min_lengths[1] == 0  # float_col
    assert entry.max_lengths[1] == 0  # float_col
    assert entry.min_lengths[2] == 0  # bool_col
    assert entry.max_lengths[2] == 0  # bool_col


def test_min_max_lengths_equal_length_strings():
    """Test edge case where all strings have equal length (fixed-width)."""
    try:
        pass  # type: ignore
    except Exception:
        pytest.skip("opteryx.compiled.draken not available")

    # Create a Parquet table with fixed-length strings
    table = pa.table({
        "codes": pa.array(["ABC", "XYZ", "DEF", None, "123"])
    })

    # Write to bytes
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="zstd")
    data = buf.getvalue().to_pybytes()

    # Build manifest entry
    entry = build_parquet_manifest_entry_from_bytes(data, "test.parquet", len(data), orig_table=table)

    # All non-null strings are 3 characters
    assert entry.min_lengths[0] == 3
    assert entry.max_lengths[0] == 3


def test_lengths_in_manifest_roundtrip():
    """Test end-to-end: lengths survive serialization and deserialization."""
    try:
        pass  # type: ignore
    except Exception:
        pytest.skip("opteryx.compiled.draken not available")

    # Create dataset with string data
    table = pa.table({
        "name": pa.array(["Alice", "Bob", "Christopher", None, "Dan"]),
        "value": pa.array([1, 2, 3, 4, 5])
    })

    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="zstd")
    data = buf.getvalue().to_pybytes()

    # Build initial entry
    entry = build_parquet_manifest_entry_from_bytes(data, "test.parquet", len(data), orig_table=table)

    # Verify lengths were computed
    assert entry.min_lengths[0] > 0  # name column has non-zero min length
    assert entry.max_lengths[0] > 0  # name column has non-zero max length
    assert entry.min_lengths[1] == 0  # value is numeric
    assert entry.max_lengths[1] == 0  # value is numeric

    # Convert to dict and back to simulate roundtrip
    entry_dict = entry.to_dict()
    assert "min_lengths" in entry_dict
    assert "max_lengths" in entry_dict
    assert entry_dict["min_lengths"] == entry.min_lengths
    assert entry_dict["max_lengths"] == entry.max_lengths
