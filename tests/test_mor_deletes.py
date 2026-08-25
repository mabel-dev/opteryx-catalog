"""Merge-on-read deletes: bitmap encoding, sidecar IO, and the commit paths.

See catalog/deletes.py and MOR_DELETES_DESIGN.md.
"""

import copy
import io
import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

import pytest
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo.parquet import write_parquet

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.deletes import decode_positions
from opteryx_catalog.catalog.deletes import encode_positions
from opteryx_catalog.catalog.deletes import is_delete_vector_path
from opteryx_catalog.catalog.deletes import read_delete_vector_file
from opteryx_catalog.catalog.deletes import read_delete_vectors_for_entries
from opteryx_catalog.catalog.deletes import write_delete_vector_file
from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry_from_bytes
from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot
from opteryx_catalog.opteryx_catalog import OpteryxCatalog

LOCATION = "mem://ws/mor"


# ---------------------------------------------------------------------------
# Fixtures / fakes (same shapes as test_refresh_manifest.py)
# ---------------------------------------------------------------------------


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
        self.saved_snapshots = []
        self.saved_metadata = []

    # Reuse the real implementation so the fixture can't drift from it.
    write_parquet_manifest = OpteryxCatalog.write_parquet_manifest

    def save_snapshot(self, identifier, snapshot):
        self.saved_snapshots.append((identifier, snapshot))

    def save_dataset_metadata(self, identifier, metadata):
        self.saved_metadata.append((identifier, copy.deepcopy(metadata)))


def _make_morsel(values):
    m = Morsel()
    m.append_vector("a", vector_from_sequence(values, dtype="INTEGER"))
    return m


def _seed_dataset(files: dict[str, list[int]]):
    """A dataset whose current snapshot has one data file per `files` entry."""
    clear_parsed_manifest_cache()
    storage: dict[str, bytes] = {}
    mem_io = _MemIO(storage)
    catalog = _FakeCatalog(mem_io)

    entries = []
    for path, values in files.items():
        data = write_parquet(_make_morsel(values), compression="zstd")
        storage[path] = data
        entry = build_parquet_manifest_entry_from_bytes(data, path, len(data))
        entries.append(entry.to_dict())

    snapshot_id = 1000
    manifest_path = catalog.write_parquet_manifest(snapshot_id, entries, LOCATION)

    meta = DatasetMetadata(
        dataset_identifier="col.mor",
        location=LOCATION,
        schema=None,
        properties={},
    )
    snap = Snapshot(
        snapshot_id=snapshot_id,
        timestamp_ms=snapshot_id,
        author="seed",
        sequence_number=1,
        user_created=True,
        operation_type="append",
        manifest_list=manifest_path,
    )
    meta.snapshots.append(snap)
    meta.current_snapshot_id = snapshot_id

    ds = SimpleDataset(identifier="col.mor", _metadata=meta)
    ds.io = mem_io
    ds.catalog = catalog
    return ds, storage


def _current_entries(ds):
    from opteryx_catalog.catalog.manifest import read_manifest_rows

    snap = ds.snapshot(None)
    with ds.io.new_input(snap.manifest_list).open() as f:
        return read_manifest_rows(f.read())


# ---------------------------------------------------------------------------
# Bitmap encoding
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "positions, record_count",
    [
        ([], 100),
        ([0], 1),
        ([0, 1, 2], 3),
        ([5], 1000),
        ([0, 7, 8, 63, 64, 999], 1000),
        (list(range(0, 1000, 3)), 1000),
        (list(range(1000)), 1000),  # everything deleted
        ([999_999_999], 1_000_000_000),  # sparse in a huge file
    ],
)
def test_bitmap_roundtrip(positions, record_count):
    blob = encode_positions(positions, record_count)
    assert decode_positions(blob) == sorted(set(positions))


def test_bitmap_dense_wins_for_heavy_deletes():
    # ~all rows deleted: dense bitset must beat per-row varints.
    blob = encode_positions(list(range(10_000)), 10_000)
    assert blob[0] == 0x01
    assert len(blob) <= 1 + (10_000 + 7) // 8


def test_bitmap_sparse_wins_for_light_deletes():
    blob = encode_positions([17], 10_000_000)
    assert blob[0] == 0x00
    assert len(blob) < 10  # one varint, not a megabyte of bitset


def test_bitmap_rejects_out_of_range():
    with pytest.raises(ValueError):
        encode_positions([100], 100)
    with pytest.raises(ValueError):
        encode_positions([-1], 100)


def test_bitmap_rejects_unknown_encoding():
    with pytest.raises(ValueError):
        decode_positions(bytes([0x7F, 0x00]))


# ---------------------------------------------------------------------------
# Sidecar IO
# ---------------------------------------------------------------------------


def test_sidecar_roundtrip():
    clear_parsed_manifest_cache()
    storage = {}
    mem_io = _MemIO(storage)
    path = f"{LOCATION}/metadata/deletes-123.parquet"
    vectors = {"mem://f1.parquet": [0, 5, 9], "mem://f2.parquet": [1]}
    write_delete_vector_file(mem_io, path, vectors)
    assert path in storage
    assert read_delete_vector_file(mem_io, path) == vectors


def test_sidecar_rejects_empty_vector():
    with pytest.raises(ValueError):
        write_delete_vector_file(_MemIO({}), "mem://x", {"f": []})


def test_is_delete_vector_path():
    assert is_delete_vector_path(f"{LOCATION}/metadata/deletes-1755000000000.parquet")
    assert not is_delete_vector_path(f"{LOCATION}/metadata/manifest-1755000000000.parquet")
    assert not is_delete_vector_path(f"{LOCATION}/data/deletes.parquet")


def test_read_vectors_for_entries_missing_vector_raises():
    clear_parsed_manifest_cache()
    storage = {}
    mem_io = _MemIO(storage)
    path = f"{LOCATION}/metadata/deletes-9.parquet"
    write_delete_vector_file(mem_io, path, {"mem://present.parquet": [0]})
    entries = [
        {"file_path": "mem://absent.parquet", "delete_file_path": path, "deleted_record_count": 2}
    ]
    with pytest.raises(ValueError):
        read_delete_vectors_for_entries(mem_io, entries)


# ---------------------------------------------------------------------------
# delete_rows commit path
# ---------------------------------------------------------------------------


def test_delete_rows_commits_sidecar_and_manifest():
    ds, storage = _seed_dataset({"mem://f1.parquet": [10, 20, 30, 40], "mem://f2.parquet": [1, 2]})

    snap = ds.delete_rows({"mem://f1.parquet": [1, 3]}, author="tester")

    assert snap.operation_type == "delete"
    assert snap.summary["deleted-records"] == 2
    assert snap.summary["total-records"] == 6  # physical, unchanged
    assert snap.summary["total-deleted-records"] == 2
    assert snap.summary["deleted-data-files"] == 0

    entries = {e["file_path"]: e for e in _current_entries(ds)}
    f1, f2 = entries["mem://f1.parquet"], entries["mem://f2.parquet"]
    assert f1["deleted_record_count"] == 2
    assert f1["delete_file_path"] == f"{LOCATION}/metadata/deletes-{snap.snapshot_id}.parquet"
    assert f1["record_count"] == 4  # physical rows untouched
    assert not f2["deleted_record_count"]
    assert f2["delete_file_path"] is None

    # And the read-side view resolves the ordinals.
    assert ds.delete_vectors() == {"mem://f1.parquet": [1, 3]}

    # The commit persisted both the snapshot and the dataset pointer.
    catalog = ds.catalog
    assert catalog.saved_snapshots and catalog.saved_metadata
    assert catalog.saved_metadata[-1][1].current_snapshot_id == snap.snapshot_id


def test_delete_rows_merges_into_existing_vector():
    ds, _ = _seed_dataset({"mem://f1.parquet": [10, 20, 30, 40]})
    ds.delete_rows({"mem://f1.parquet": [0]}, author="tester")
    snap2 = ds.delete_rows({"mem://f1.parquet": [2]}, author="tester")

    assert ds.delete_vectors() == {"mem://f1.parquet": [0, 2]}
    entries = _current_entries(ds)
    assert entries[0]["deleted_record_count"] == 2
    # The merged state lives in the NEW snapshot's own sidecar.
    assert entries[0]["delete_file_path"].endswith(f"deletes-{snap2.snapshot_id}.parquet")
    # deleted-records counts only the NEWLY deleted row.
    assert snap2.summary["deleted-records"] == 1


def test_delete_rows_idempotent_redelete_rejected_when_nothing_new():
    ds, _ = _seed_dataset({"mem://f1.parquet": [10, 20]})
    ds.delete_rows({"mem://f1.parquet": [0]}, author="tester")
    with pytest.raises(ValueError, match="already deleted"):
        ds.delete_rows({"mem://f1.parquet": [0]}, author="tester")


def test_delete_rows_fully_deleted_file_leaves_manifest():
    ds, _ = _seed_dataset({"mem://f1.parquet": [10, 20], "mem://f2.parquet": [1]})
    snap = ds.delete_rows({"mem://f1.parquet": [0, 1]}, author="tester")

    entries = _current_entries(ds)
    assert [e["file_path"] for e in entries] == ["mem://f2.parquet"]
    assert snap.summary["deleted-data-files"] == 1
    assert snap.summary["deleted-records"] == 2
    assert snap.summary["total-records"] == 1
    # No surviving deletes -> no sidecar written for this snapshot.
    assert ds.delete_vectors() == {}


def test_delete_rows_validates_positions_and_paths():
    ds, _ = _seed_dataset({"mem://f1.parquet": [10, 20]})
    with pytest.raises(ValueError, match="not in the current manifest"):
        ds.delete_rows({"mem://nope.parquet": [0]}, author="tester")
    with pytest.raises(ValueError, match="out of range"):
        ds.delete_rows({"mem://f1.parquet": [2]}, author="tester")
    with pytest.raises(ValueError, match="author"):
        ds.delete_rows({"mem://f1.parquet": [0]})


def test_append_carries_delete_columns_forward():
    ds, _ = _seed_dataset({"mem://f1.parquet": [10, 20, 30]})
    del_snap = ds.delete_rows({"mem://f1.parquet": [1]}, author="tester")

    ds.append(_make_morsel([7, 8]), author="tester")

    entries = {e["file_path"]: e for e in _current_entries(ds)}
    f1 = entries["mem://f1.parquet"]
    # The append copied the parent rows forward verbatim: f1 still points at
    # the delete snapshot's sidecar and the read side still resolves it.
    assert f1["deleted_record_count"] == 1
    assert f1["delete_file_path"].endswith(f"deletes-{del_snap.snapshot_id}.parquet")
    assert ds.delete_vectors() == {"mem://f1.parquet": [1]}


# ---------------------------------------------------------------------------
# delete_files commit path
# ---------------------------------------------------------------------------


def test_delete_files_drops_entries():
    ds, _ = _seed_dataset({"mem://f1.parquet": [10, 20], "mem://f2.parquet": [1, 2, 3]})
    snap = ds.delete_files(["mem://f1.parquet"], author="tester")

    entries = _current_entries(ds)
    assert [e["file_path"] for e in entries] == ["mem://f2.parquet"]
    assert snap.summary["deleted-data-files"] == 1
    assert snap.summary["deleted-records"] == 2
    assert snap.summary["total-records"] == 3


def test_delete_files_carries_survivor_vectors_forward():
    ds, _ = _seed_dataset({"mem://f1.parquet": [10, 20], "mem://f2.parquet": [1, 2, 3]})
    ds.delete_rows({"mem://f2.parquet": [0]}, author="tester")
    snap = ds.delete_files(["mem://f1.parquet"], author="tester")

    entries = _current_entries(ds)
    assert [e["file_path"] for e in entries] == ["mem://f2.parquet"]
    assert entries[0]["deleted_record_count"] == 1
    assert entries[0]["delete_file_path"].endswith(f"deletes-{snap.snapshot_id}.parquet")
    assert ds.delete_vectors() == {"mem://f2.parquet": [0]}


def test_delete_files_unknown_path_rejected():
    ds, _ = _seed_dataset({"mem://f1.parquet": [10]})
    with pytest.raises(ValueError, match="not in the current manifest"):
        ds.delete_files(["mem://nope.parquet"], author="tester")


# ---------------------------------------------------------------------------
# Time travel
# ---------------------------------------------------------------------------


def test_time_travel_sees_per_snapshot_delete_state():
    ds, _ = _seed_dataset({"mem://f1.parquet": [10, 20, 30]})
    base = ds.snapshot(None).snapshot_id
    s1 = ds.delete_rows({"mem://f1.parquet": [0]}, author="tester")
    s2 = ds.delete_rows({"mem://f1.parquet": [2]}, author="tester")

    assert ds.delete_vectors(base) == {}
    assert ds.delete_vectors(s1.snapshot_id) == {"mem://f1.parquet": [0]}
    assert ds.delete_vectors(s2.snapshot_id) == {"mem://f1.parquet": [0, 2]}


# ---------------------------------------------------------------------------
# GC protection
# ---------------------------------------------------------------------------


def test_deep_clean_protects_sidecar():
    from opteryx_catalog.catalog.deep_clean import DatasetDeepClean

    ds, _ = _seed_dataset({"mem://f1.parquet": [10, 20, 30]})
    ds.delete_rows({"mem://f1.parquet": [1]}, author="tester")

    class _Cat:
        io = ds.io

    protected = DatasetDeepClean(_Cat()).get_all_manifest_files(ds.metadata.snapshots)
    sidecars = {p for p in protected if "/metadata/deletes-" in p}
    assert sidecars, "the delete sidecar must be in the protected set"


def test_expiration_collector_protects_sidecar():
    from opteryx_catalog.catalog.expiration import SnapshotExpiration

    ds, _ = _seed_dataset({"mem://f1.parquet": [10, 20, 30]})
    ds.delete_rows({"mem://f1.parquet": [1]}, author="tester")

    class _Cat:
        io = ds.io

    mgr = SnapshotExpiration.__new__(SnapshotExpiration)
    mgr.catalog = _Cat()
    files = mgr._get_file_sizes_in_snapshots(ds.metadata.snapshots, required=True)
    assert any("/metadata/deletes-" in p for p in files)


# ---------------------------------------------------------------------------
# Compaction safety
# ---------------------------------------------------------------------------


def _decode_values(io, file_path):
    from rugo.parquet import read_parquet

    with io.new_input(file_path).open() as f:
        data = f.read()
    values = []
    with read_parquet(bytes(data)) as reader:
        for morsel in reader:
            values.extend(morsel.column(b"a").to_pylist())
    return values


def test_drop_deleted_rows_from_morsels_spans_row_groups():
    from opteryx_catalog.catalog.deletes import drop_deleted_rows_from_morsels

    # Two "row groups" of 3 rows each; file-global ordinals 0..5.
    m1 = _make_morsel([10, 11, 12])
    m2 = _make_morsel([13, 14, 15])
    out = drop_deleted_rows_from_morsels([m1, m2], [1, 3, 5])
    values = [v for m in out for v in m.column(b"a").to_pylist()]
    assert values == [10, 12, 14]
    # A fully-deleted group emits nothing.
    out = drop_deleted_rows_from_morsels([m1, m2], [0, 1, 2])
    values = [v for m in out for v in m.column(b"a").to_pylist()]
    assert values == [13, 14, 15]
    assert len(out) == 1
    # No positions: morsels pass through untouched.
    assert drop_deleted_rows_from_morsels([m1], []) == [m1]


def test_materialise_live_parquet():
    from opteryx_catalog.catalog.deletes import materialise_live_parquet
    from rugo.parquet import read_parquet

    data = write_parquet(_make_morsel([10, 20, 30, 40]), compression="zstd")
    live = materialise_live_parquet(data, [0, 2])
    with read_parquet(bytes(live)) as reader:
        values = [v for m in reader for v in m.column(b"a").to_pylist()]
    assert values == [20, 40]
    with pytest.raises(ValueError, match="every row"):
        materialise_live_parquet(data, [0, 1, 2, 3])


def test_compaction_materialises_deletes():
    """A merge that touches a delete-bearing file drops its deleted rows and
    emits a vector-free output."""
    from opteryx_catalog.catalog.compaction import DatasetCompactor

    ds, storage = _seed_dataset(
        {
            "mem://ws/mor/data/f1.parquet": [10, 20, 30],
            "mem://ws/mor/data/f2.parquet": [1, 2, 3],
            "mem://ws/mor/data/f3.parquet": [4, 5, 6],
        }
    )
    ds.delete_rows({"mem://ws/mor/data/f1.parquet": [1]}, author="tester")  # delete 20

    compactor = DatasetCompactor(ds, strategy="brute", author="tester", agent="test")
    snap = compactor.compact()
    assert snap is not None, compactor._last_error
    assert snap.operation_type == "compact"

    entries = _current_entries(ds)
    # Outputs carry no delete debt.
    for e in entries:
        assert not (e.get("deleted_record_count") or 0)
        assert e.get("delete_file_path") is None
    # The merged data holds exactly the live rows — 20 is gone.
    merged_values = []
    for e in entries:
        merged_values.extend(_decode_values(ds.io, e["file_path"]))
    assert sorted(merged_values) == [1, 2, 3, 4, 5, 6, 10, 30]
    # Physical totals agree with the live merge.
    assert snap.summary["total-records"] == 8


def test_delete_debt_rule_selects_heavy_debt():
    from opteryx_catalog.catalog.compaction import DatasetCompactor

    ds, _ = _seed_dataset({"mem://ws/mor/data/f1.parquet": list(range(100, 110))})
    # 2/10 deleted = 20% >= 10% default threshold.
    ds.delete_rows({"mem://ws/mor/data/f1.parquet": [0, 1]}, author="tester")

    compactor = DatasetCompactor(ds, strategy="brute", author="tester", agent="test")
    plan = compactor.compact(dry_run=True, rule="debt")
    assert plan is not None
    assert plan["mode"] == "brute"
    assert [e["file_path"] for e in plan["files"]] == ["mem://ws/mor/data/f1.parquet"]
    assert "delete-debt" in plan["reason"]

    # Executing the plan rewrites the file without its deleted rows.
    snap = compactor.compact(rule="debt")
    assert snap is not None, compactor._last_error
    entries = _current_entries(ds)
    assert len(entries) == 1
    assert not (entries[0].get("deleted_record_count") or 0)
    assert sorted(_decode_values(ds.io, entries[0]["file_path"])) == list(range(102, 110))
    assert ds.delete_vectors() == {}


def test_delete_debt_below_threshold_not_selected():
    from opteryx_catalog.catalog.compaction import DatasetCompactor

    ds, _ = _seed_dataset({"mem://ws/mor/data/f1.parquet": list(range(100))})
    ds.delete_rows({"mem://ws/mor/data/f1.parquet": [0]}, author="tester")  # 1% < 10%

    compactor = DatasetCompactor(ds, strategy="brute", author="tester", agent="test")
    assert compactor.compact(dry_run=True, rule="debt") is None


def test_delete_debt_threshold_override():
    from opteryx_catalog.catalog.compaction import DatasetCompactor

    ds, _ = _seed_dataset({"mem://ws/mor/data/f1.parquet": list(range(10))})
    ds.delete_rows({"mem://ws/mor/data/f1.parquet": [0, 1]}, author="tester")  # 20%
    ds.metadata.maintenance_policy["delete-debt-threshold"] = 0.5

    compactor = DatasetCompactor(ds, strategy="brute", author="tester", agent="test")
    assert compactor.compact(dry_run=True, rule="debt") is None
    ds.metadata.maintenance_policy["delete-debt-threshold"] = 0.15
    assert compactor.compact(dry_run=True, rule="debt") is not None


def test_source_cache_serves_live_bytes():
    """The streaming merge's one read point: a delete-bearing file comes out
    of the source cache already materialised, so every downstream consumer
    (predicate reads, sort-column projection, chunk streaming) sees live rows."""
    import tempfile

    from rugo.parquet import read_parquet

    from opteryx_catalog.catalog.compaction import _SourceFileCache

    storage = {}
    mem_io = _MemIO(storage)
    data = write_parquet(_make_morsel([10, 20, 30, 40]), compression="zstd")
    storage["mem://f1.parquet"] = data

    with tempfile.TemporaryDirectory() as tmpdir:
        cache = _SourceFileCache(
            mem_io, tmpdir, delete_vectors={"mem://f1.parquet": [1, 3]}
        )
        src = cache.source("mem://f1.parquet")
        with read_parquet(src) as reader:
            values = [v for m in reader for v in m.column(b"a").to_pylist()]
        assert values == [10, 30]

        # A file with no vector passes through byte-identical.
        storage["mem://f2.parquet"] = data
        src2 = cache.source("mem://f2.parquet")
        with read_parquet(src2) as reader:
            values = [v for m in reader for v in m.column(b"a").to_pylist()]
        assert values == [10, 20, 30, 40]


def test_row_counts_balance_uses_live_rows():
    from opteryx_catalog.catalog.compaction import DatasetCompactor

    ds, _ = _seed_dataset({"mem://ws/mor/data/f1.parquet": [1]})
    compactor = DatasetCompactor(ds, strategy="brute", author="tester", agent="test")
    inputs = [
        {"file_path": "a", "record_count": 10, "deleted_record_count": 3},
        {"file_path": "b", "record_count": 5},
    ]
    # Outputs holding the LIVE rows balance…
    assert compactor._row_counts_balance(inputs, [{"record_count": 12}])
    # …outputs holding the PHYSICAL rows (resurrected deletes) do not.
    assert not compactor._row_counts_balance(inputs, [{"record_count": 15}])
    # …and losing live rows does not.
    assert not compactor._row_counts_balance(inputs, [{"record_count": 11}])
