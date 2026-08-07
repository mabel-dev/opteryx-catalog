import io
import os
import sys

from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel

# Add local paths to sys.path to use local code instead of installed packages
sys.path.insert(0, os.path.join(sys.path[0], ".."))  # Add parent dir for opteryx_catalog
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-catalog"))


import pytest

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry_from_bytes
from opteryx_catalog.catalog.manifest import get_manifest_metrics
from opteryx_catalog.catalog.manifest import reset_manifest_metrics
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot
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
    entry = build_parquet_manifest_entry_from_bytes(
        data, "test.parquet", len(data), orig_morsel=table
    )

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
    entry = build_parquet_manifest_entry_from_bytes(
        data, "test.parquet", len(data), orig_morsel=table
    )

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
    entry = build_parquet_manifest_entry_from_bytes(
        data, "test.parquet", len(data), orig_morsel=table
    )

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
    entry = build_parquet_manifest_entry_from_bytes(
        data, "test.parquet", len(data), orig_morsel=table
    )

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
    entry = build_parquet_manifest_entry_from_bytes(
        data, "test.parquet", len(data), orig_morsel=table
    )

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
    entry = build_parquet_manifest_entry_from_bytes(
        data, "test.parquet", len(data), orig_morsel=table
    )

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


# ── refresh_manifest fails whole rather than committing partial statistics ──
#
# refresh_manifest used to swallow per-file re-read/stats failures
# (`except Exception: dent = ent`) and fall back to the file's PREVIOUS
# manifest entry, then commit a snapshot as if everything had succeeded. A
# manifest mixing freshly-computed statistics with silently-retained stale
# ones is indistinguishable downstream from a fully-successful refresh, so a
# refresh that cannot recompute every file now raises and commits nothing.


# `get_arrow_manifest` memoises by manifest PATH, and that path is derived
# from (snapshot_id, dataset location) — so every fixture built at
# snapshot_id=1 under "mem://" resolves to ONE cache key and tests silently
# read each other's manifests. Each fixture below gets its own snapshot id.
_FIXTURE_SNAPSHOT_ID = iter(range(1000, 100000))


def _dataset_with_manifest(mapping, manifest_path, snapshot_id):
    """A SimpleDataset wired to in-memory IO, sharing this file's fixtures."""
    meta = DatasetMetadata(
        dataset_identifier="tests_temp.test", location="mem://", schema=None, properties={}
    )
    meta.schemas.append({"schema_id": "s1", "columns": [{"name": "a"}, {"name": "b"}]})
    meta.current_schema_id = "s1"
    meta.snapshots.append(
        Snapshot(snapshot_id=snapshot_id, timestamp_ms=1, manifest_list=manifest_path)
    )
    meta.current_snapshot_id = snapshot_id

    ds = SimpleDataset(identifier="tests_temp.test", _metadata=meta)
    ds.io = _MemIO(mapping)
    ds.catalog = _FakeCatalog(ds.io)
    return ds, meta


def _two_file_fixture():
    """Two data files + a manifest describing both, all in memory."""
    from rugo.parquet import write_parquet

    snapshot_id = next(_FIXTURE_SNAPSHOT_ID)

    t1 = _make_test_morsel([("a", "INTEGER"), ("b", "INTEGER")], [(1, 10), (2, 20)])
    t2 = _make_test_morsel([("a", "INTEGER"), ("b", "INTEGER")], [(3, 30), (4, 40)])
    d1 = write_parquet(t1, compression="zstd")
    d2 = write_parquet(t2, compression="zstd")

    f1 = f"mem://data/{snapshot_id}/f1.parquet"
    f2 = f"mem://data/{snapshot_id}/f2.parquet"

    e1 = build_parquet_manifest_entry_from_bytes(d1, f1, len(d1), orig_morsel=t1).to_dict()
    e2 = build_parquet_manifest_entry_from_bytes(d2, f2, len(d2), orig_morsel=t2).to_dict()

    mapping = {f1: d1, f2: d2}
    manifest_path = _FakeCatalog(_MemIO(mapping)).write_parquet_manifest(
        snapshot_id, [e1, e2], "mem://"
    )
    return mapping, manifest_path, f1, f2, snapshot_id


def test_refresh_manifest_raises_when_a_file_cannot_be_read():
    from opteryx_catalog.exceptions import ManifestRefreshError

    mapping, manifest_path, f1, f2, snapshot_id = _two_file_fixture()
    ds, meta = _dataset_with_manifest(mapping, manifest_path, snapshot_id)

    # f2's bytes disappear (deleted/unreadable object) AFTER the manifest was written.
    del mapping[f2]
    mapping_before = dict(mapping)

    with pytest.raises(ManifestRefreshError) as exc:
        ds.refresh_manifest(agent="test-agent", author="tester")

    # The failing file is named — an operator shouldn't have to guess which.
    assert f2 in str(exc.value)
    # No partial commit: the snapshot pointer and stored objects are untouched.
    assert meta.current_snapshot_id == snapshot_id
    assert mapping == mapping_before


def test_refresh_manifest_reports_every_failed_file_not_just_the_first():
    # A bad batch write / bucket issue usually affects many files at once;
    # surfacing one per re-run would take N runs to discover N bad files.
    from opteryx_catalog.exceptions import ManifestRefreshError

    mapping, manifest_path, f1, f2, snapshot_id = _two_file_fixture()
    ds, meta = _dataset_with_manifest(mapping, manifest_path, snapshot_id)

    del mapping[f1]
    del mapping[f2]

    with pytest.raises(ManifestRefreshError) as exc:
        ds.refresh_manifest(agent="test-agent", author="tester")

    message = str(exc.value)
    assert f1 in message
    assert f2 in message
    assert "2 of 2" in message
    assert meta.current_snapshot_id == snapshot_id


def test_refresh_manifest_raises_when_the_manifest_itself_is_unreadable():
    # Previously this degraded to `prev_rows = []`, "refreshed" zero files,
    # and committed a snapshot describing nothing at all.
    from opteryx_catalog.exceptions import ManifestRefreshError

    mapping, manifest_path, _f1, _f2, snapshot_id = _two_file_fixture()
    ds, meta = _dataset_with_manifest(mapping, manifest_path, snapshot_id)

    del mapping[manifest_path]

    with pytest.raises(ManifestRefreshError):
        ds.refresh_manifest(agent="test-agent", author="tester")

    assert meta.current_snapshot_id == snapshot_id


def test_refresh_manifest_succeeds_when_every_file_is_readable():
    # The positive control for the three failure tests above: the same
    # two-file fixture, nothing removed, commits a new snapshot.
    mapping, manifest_path, _f1, _f2, snapshot_id = _two_file_fixture()
    ds, meta = _dataset_with_manifest(mapping, manifest_path, snapshot_id)

    new_snapshot_id = ds.refresh_manifest(agent="test-agent", author="tester")

    assert new_snapshot_id != snapshot_id
    assert meta.current_snapshot_id == new_snapshot_id


def _entry_from_morsel(morsel):
    """Manifest entry for an in-memory morsel, via the same bytes-based
    builder the rest of this file uses (orig_morsel keeps the semantic types
    Parquet would otherwise flatten)."""
    from rugo.parquet import write_parquet

    data = write_parquet(morsel, compression="zstd")
    return build_parquet_manifest_entry_from_bytes(data, "f.parquet", len(data), orig_morsel=morsel)


# ── string columns get real min/max + histograms ────────────────────────────
#
# Before draken's 2026-07-30 ordinalize rewrite added string support, strings
# were excluded from _COMPRESSIBLE_CATEGORIES, so every VARCHAR column's
# bounds were the NULL_FLAG sentinel: a string predicate could never prune,
# and opteryx-core's local ANALYZE path (which DID compute them) disagreed
# with this one about the same data. These bounds are ordinalize() keys (an
# 8-byte content prefix), which is what the reader ordinalizes literals into.


def test_string_columns_get_ordinal_min_max_and_histogram():
    from draken.draken_native import DrakenType

    from opteryx_catalog.catalog.manifest import NULL_FLAG

    morsel = _make_test_morsel(
        [("id", "INTEGER"), ("name", "VARCHAR")],
        [(1, "apple"), (2, "pear"), (3, None)],
    )
    entry = _entry_from_morsel(morsel)

    assert entry.min_values[1] != NULL_FLAG, "string column left without bounds"
    assert entry.min_values[1] == DrakenType.VARCHAR.ordinalize("apple")
    assert entry.max_values[1] == DrakenType.VARCHAR.ordinalize("pear")
    assert len(entry.histogram_counts[1]) == 32


def test_string_bounds_are_monotonic_in_value_order():
    # The property pruning depends on: a value inside the real range must have
    # an ordinal key inside the ordinal range, or a file gets wrongly skipped.
    from draken.draken_native import DrakenType

    morsel = _make_test_morsel([("name", "VARCHAR")], [("apple",), ("mango",), ("pear",)])
    entry = _entry_from_morsel(morsel)
    lo, hi = entry.min_values[0], entry.max_values[0]
    assert lo <= DrakenType.VARCHAR.ordinalize("mango") <= hi
    assert DrakenType.VARCHAR.ordinalize("aaaa") < lo
    assert DrakenType.VARCHAR.ordinalize("zebra") > hi


def test_binary_columns_also_get_bounds():
    from opteryx_catalog.catalog.manifest import NULL_FLAG

    morsel = _make_test_morsel([("b", "VARBINARY")], [(b"aa",), (b"zz",)])
    entry = _entry_from_morsel(morsel)
    assert entry.min_values[0] != NULL_FLAG
    assert entry.min_values[0] < entry.max_values[0]


def test_array_columns_still_have_no_bounds():
    # ARRAY has no ordinalize kernel; it must degrade to "no stats", not crash.
    from opteryx_catalog.catalog.manifest import NULL_FLAG

    morsel = _make_test_morsel([("tags", "ARRAY")], [(["a", "b"],), (["c"],)])
    entry = _entry_from_morsel(morsel)
    assert entry.min_values[0] == NULL_FLAG
    assert entry.histogram_counts[0] == []
