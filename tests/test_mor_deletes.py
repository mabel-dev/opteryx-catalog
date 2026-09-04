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

    def save_dataset_metadata(self, identifier, metadata, **kwargs):
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
    # Both shapes must be recognised: the nonce form written now, and the bare
    # form written before the nonce existed. A sweep that stopped matching the
    # legacy names would leak every one of those files forever.
    assert is_delete_vector_path(f"{LOCATION}/metadata/deletes-1755000000000.parquet")
    assert is_delete_vector_path(f"{LOCATION}/metadata/deletes-1755000000000-a1b2c3d4e5f6.parquet")
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
    # The name carries a per-write nonce (see delete_vector_path), so this
    # asserts the entry points at THIS snapshot's sidecar rather than at an
    # exact byte string the naming scheme is free to extend.
    assert f1["delete_file_path"].startswith(f"{LOCATION}/metadata/deletes-{snap.snapshot_id}-")
    assert is_delete_vector_path(f1["delete_file_path"])
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
    assert f"deletes-{snap2.snapshot_id}-" in entries[0]["delete_file_path"]
    assert is_delete_vector_path(entries[0]["delete_file_path"])
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
    assert f"deletes-{del_snap.snapshot_id}-" in f1["delete_file_path"]
    assert is_delete_vector_path(f1["delete_file_path"])
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
    assert f"deletes-{snap.snapshot_id}-" in entries[0]["delete_file_path"]
    assert is_delete_vector_path(entries[0]["delete_file_path"])
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


def test_compaction_commit_row_count_invariant_uses_live_rows():
    """The one check standing between a bug and silent data loss.

    Compaction only ever rewrites rows, so the outputs must hold exactly as many
    LIVE records as the inputs did — record_count minus deleted_record_count,
    because a rewrite MATERIALISES merge-on-read deletes and the output carries
    no delete vector. Outputs holding the PHYSICAL count would mean resurrected
    deleted rows; holding fewer would mean lost ones. Both are refused.

    The compaction executor moved to the engine; this invariant deliberately did
    not follow it. It lives with the commit so the catalog enforces it on any
    caller, rather than trusting each one to check itself.
    """
    from opteryx_catalog.exceptions import CompactionInvariantError

    ds, storage = _seed_dataset({"mem://ws/mor/data/f1.parquet": list(range(10))})
    ds.delete_rows({"mem://ws/mor/data/f1.parquet": [0, 1]}, author="tester")

    # A commit naming a file that is not in the manifest is refused before any
    # row counting - it cannot be replacing what it claims to replace.
    with pytest.raises(CompactionInvariantError, match="not in the current manifest"):
        ds.compaction_commit(
            files=[], retired_files=["mem://ws/mor/data/nope.parquet"], author="tester"
        )

    # Retiring nothing is a caller error, not a no-op to absorb quietly.
    with pytest.raises(ValueError, match="retires no files"):
        ds.compaction_commit(files=[], retired_files=[], author="tester")

    # --- The case the invariant exists for: a genuine live-row mismatch. ---
    # f1 holds 10 physical rows, 2 of them deleted, so 8 LIVE rows go in.
    src = "mem://ws/mor/data/f1.parquet"
    assert [e["record_count"] for e in _current_entries(ds)] == [10]
    assert [e["deleted_record_count"] for e in _current_entries(ds)] == [2]

    # An output carrying the PHYSICAL count is a compactor that rewrote the file
    # without applying its delete vector: the two deleted rows come back to life.
    resurrected = "mem://ws/mor/data/out-physical.parquet"
    storage[resurrected] = write_parquet(_make_morsel(list(range(10))), compression="zstd")
    with pytest.raises(CompactionInvariantError, match="8 live rows in, 10 rows out"):
        ds.compaction_commit(files=[resurrected], retired_files=[src], author="tester")

    # An output short of the live count is rows dropped on the floor - the same
    # refusal, from the other side.
    lossy = "mem://ws/mor/data/out-lossy.parquet"
    storage[lossy] = write_parquet(_make_morsel([2, 3, 4]), compression="zstd")
    with pytest.raises(CompactionInvariantError, match="8 live rows in, 3 rows out"):
        ds.compaction_commit(files=[lossy], retired_files=[src], author="tester")

    # Both refusals leave the inputs untouched, exactly as the message promises.
    assert [e["file_path"] for e in _current_entries(ds)] == [src]
    assert ds.delete_vectors() == {src: [0, 1]}

    # The honest output - deletes materialised, 8 live rows - commits, and the
    # delete vector is gone because the rewrite consumed it.
    good = "mem://ws/mor/data/out-live.parquet"
    storage[good] = write_parquet(_make_morsel(list(range(2, 10))), compression="zstd")
    ds.compaction_commit(files=[good], retired_files=[src], author="tester")

    entries = _current_entries(ds)
    assert [e["file_path"] for e in entries] == [good]
    assert entries[0]["record_count"] == 8
    assert not (entries[0].get("deleted_record_count") or 0)
    assert sorted(_decode_values(ds.io, good)) == list(range(2, 10))
    assert ds.delete_vectors() == {}
