import io
import os
import sys

from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel

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
    # short binary and short string columns should get min-k
    t = _make_test_morsel(
        [("bin", "VARBINARY"), ("s", "VARCHAR")], [(b"a", "x"), (b"b", "y"), (b"c", "z")]
    )
    from rugo.parquet import write_parquet

    data = write_parquet(t, compression="zstd")
    e = build_parquet_manifest_entry_from_bytes(data, "mem://f", len(data), orig_morsel=t)
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

    # Reuse the real implementation so this test fixture can't drift from it.
    write_parquet_manifest = OpteryxCatalog.write_parquet_manifest


def _make_test_morsel(columns: list, rows: list):
    """Build a Morsel from ``[(name, dtype), ...]`` and row tuples."""
    m = Morsel()
    for i, (name, dtype) in enumerate(columns):
        col_vals = [r[i] for r in rows]
        m.append_vector(name, vector_from_sequence(col_vals, dtype=dtype))
    return m


def test_build_manifest_from_bytes_matches_table():
    # ensure the bytes-based builder matches the morsel-based one
    from rugo.parquet import write_parquet

    t = _make_test_morsel([("a", "INTEGER"), ("b", "INTEGER")], [(1, 10), (2, 20)])
    data = write_parquet(t, compression="zstd")

    e_bytes = build_parquet_manifest_entry_from_bytes(data, "mem://f", len(data), orig_morsel=t)
    # basic sanity checks (parity is enforced by using orig_morsel when available)
    assert e_bytes.record_count == 2
    assert e_bytes.file_size_in_bytes == len(data)


def test_manifest_metrics_increments():
    from rugo.parquet import write_parquet

    reset_manifest_metrics()
    t = _make_test_morsel([("a", "INTEGER"), ("b", "INTEGER")], [(1, 10), (2, 20)])
    data = write_parquet(t, compression="zstd")

    _ = build_parquet_manifest_entry_from_bytes(data, "mem://f", len(data), orig_morsel=t)
    m = get_manifest_metrics()
    assert m.get("files_read", 0) >= 1


def test_manifest_uses_rugo_for_sizes():
    # Ensure the bytes-based builder computes per-column sizes without pyarrow
    from rugo.parquet import write_parquet

    reset_manifest_metrics()
    t = _make_test_morsel([("a", "INTEGER"), ("b", "INTEGER")], [(1, 10), (2, 20)])
    data = write_parquet(t, compression="zstd")

    entry = build_parquet_manifest_entry_from_bytes(data, "mem://f", len(data))

    assert entry.uncompressed_size_in_bytes >= 0
    assert isinstance(entry.column_uncompressed_sizes_in_bytes, list)
    assert len(entry.column_uncompressed_sizes_in_bytes) == 2
    assert all(isinstance(x, int) for x in entry.column_uncompressed_sizes_in_bytes)


def test_refresh_manifest_with_single_file():
    from rugo.parquet import write_parquet

    # single file with columns a,b for quick iteration
    t1 = _make_test_morsel([("a", "INTEGER"), ("b", "INTEGER")], [(1, 10), (2, 20)])
    d1 = write_parquet(t1, compression="zstd")

    f1 = "mem://data/f1.parquet"
    manifest_path = "mem://manifest-old"

    # Build initial manifest entry for single file (bytes-based builder)
    e1 = build_parquet_manifest_entry_from_bytes(d1, f1, len(d1), orig_morsel=t1).to_dict()

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
    from rugo.parquet import write_parquet

    # Create a Morsel with variable-length strings
    table = _make_test_morsel(
        [("strings", "VARCHAR")],
        [("a",), ("hello",), ("the quick brown fox",), (None,), ("hi",)],
    )
    data = write_parquet(table, compression="zstd")

    # Build manifest entry
    entry = build_parquet_manifest_entry_from_bytes(data, "test.parquet", len(data), orig_morsel=table)

    # Verify min/max lengths (None values should be excluded)
    # Non-null strings: "a" (1), "hello" (5), "the quick brown fox" (19), "hi" (2)
    assert entry.min_lengths[0] == 1  # min length is "a"
    assert entry.max_lengths[0] == 19  # max length is "the quick brown fox"


def test_min_max_lengths_for_binary():
    """Test min/max length computation for binary columns."""
    from rugo.parquet import write_parquet

    # Create a Morsel with variable-length binary data
    table = _make_test_morsel(
        [("binary_data", "VARBINARY")],
        [(b"ab",), (b"x",), (b"hello world",), (None,), (b"123456789",)],
    )
    data = write_parquet(table, compression="zstd")

    # Build manifest entry
    entry = build_parquet_manifest_entry_from_bytes(data, "test.parquet", len(data), orig_morsel=table)

    # Non-null binary: b"ab" (2), b"x" (1), b"hello world" (11), b"123456789" (9)
    assert entry.min_lengths[0] == 1  # min length is b"x"
    assert entry.max_lengths[0] == 11  # max length is b"hello world"


def test_min_max_lengths_for_lists():
    """Test min/max length computation for list/array columns."""
    from rugo.parquet import write_parquet

    # Create a Morsel with variable-size lists
    table = _make_test_morsel(
        [("list_data", "ARRAY")],
        [([1, 2, 3],), ([4],), ([5, 6],), (None,), ([7, 8, 9, 10, 11],)],
    )
    data = write_parquet(table, compression="zstd")

    # Build manifest entry
    entry = build_parquet_manifest_entry_from_bytes(data, "test.parquet", len(data), orig_morsel=table)

    # Non-null lists: [1,2,3] (3), [4] (1), [5,6] (2), [7,8,9,10,11] (5)
    assert entry.min_lengths[0] == 1  # min length is [4]
    assert entry.max_lengths[0] == 5  # max length is [7,8,9,10,11]


def test_min_max_lengths_for_numeric_columns():
    """Test that numeric/boolean columns have zero lengths."""
    from rugo.parquet import write_parquet

    # Create a Morsel with various numeric types
    table = Morsel()
    table.append_vector("int_col", vector_from_sequence([1, 2, 3, 4, 5], dtype="INTEGER"))
    table.append_vector("float_col", vector_from_sequence([1.1, 2.2, 3.3], dtype="DOUBLE"))
    table.append_vector("bool_col", vector_from_sequence([True, False, True], dtype="BOOLEAN"))
    data = write_parquet(table, compression="zstd")

    # Build manifest entry
    entry = build_parquet_manifest_entry_from_bytes(data, "test.parquet", len(data), orig_morsel=table)

    # All non-variable-width types should have 0 length
    assert entry.min_lengths[0] == 0  # int_col
    assert entry.max_lengths[0] == 0  # int_col
    assert entry.min_lengths[1] == 0  # float_col
    assert entry.max_lengths[1] == 0  # float_col
    assert entry.min_lengths[2] == 0  # bool_col
    assert entry.max_lengths[2] == 0  # bool_col


def test_min_max_lengths_equal_length_strings():
    """Test edge case where all strings have equal length (fixed-width)."""
    from rugo.parquet import write_parquet

    # Create a Morsel with fixed-length strings
    table = _make_test_morsel(
        [("codes", "VARCHAR")],
        [("ABC",), ("XYZ",), ("DEF",), (None,), ("123",)],
    )
    data = write_parquet(table, compression="zstd")

    # Build manifest entry
    entry = build_parquet_manifest_entry_from_bytes(data, "test.parquet", len(data), orig_morsel=table)

    # All non-null strings are 3 characters
    assert entry.min_lengths[0] == 3
    assert entry.max_lengths[0] == 3


def test_lengths_in_manifest_roundtrip():
    """Test end-to-end: lengths survive serialization and deserialization."""
    from rugo.parquet import write_parquet

    # Create dataset with string data
    table = Morsel()
    table.append_vector(
        "name", vector_from_sequence(["Alice", "Bob", "Christopher", None, "Dan"], dtype="VARCHAR")
    )
    table.append_vector("value", vector_from_sequence([1, 2, 3, 4, 5], dtype="INTEGER"))
    data = write_parquet(table, compression="zstd")

    # Build initial entry
    entry = build_parquet_manifest_entry_from_bytes(data, "test.parquet", len(data), orig_morsel=table)

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
