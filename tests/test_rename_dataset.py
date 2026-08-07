from __future__ import annotations

import json

import pytest

from opteryx_catalog.exceptions import DatasetAlreadyExists
from opteryx_catalog.exceptions import DatasetLocked
from opteryx_catalog.exceptions import DatasetNotFound
from opteryx_catalog.opteryx_catalog import OpteryxCatalog


class _Doc:
    def __init__(self, data=None, exists=True, doc_id=None):
        self.exists = exists
        self._data = data or {}
        self.id = doc_id

    def to_dict(self):
        return self._data


class _DocRef:
    def __init__(self, data=None, exists=True, subcollections=None, doc_id=None):
        self._doc = _Doc(data, exists, doc_id)
        self.id = doc_id
        self._subcollections = subcollections or {}
        self.written = None
        self.deleted = False

    def get(self):
        return self._doc

    def set(self, data, merge=False):
        self.written = data
        self._doc._data = {**self._doc._data, **data} if merge else dict(data)
        self._doc.exists = True

    def update(self, data):
        self._doc._data = {**self._doc._data, **data}

    def delete(self):
        self.deleted = True
        self._doc.exists = False

    def collection(self, name):
        return self._subcollections.setdefault(name, _Collection())


class _Collection:
    def __init__(self, docs=None):
        self._docs = dict(docs or {})

    def document(self, doc_id):
        if doc_id not in self._docs:
            self._docs[doc_id] = _DocRef(exists=False, doc_id=doc_id)
        return self._docs[doc_id]

    def stream(self):
        return [d.get() for d in self._docs.values() if d.get().exists]


class _FakeIO:
    """In-memory object store standing in for GCS."""

    def __init__(self, objects=None):
        self.objects = dict(objects or {})

    def new_input(self, location):
        objects = self.objects

        class _In:
            def open(self):
                class _H:
                    def read(self_inner):
                        return objects[location]

                    def __enter__(self_inner):
                        return self_inner

                    def __exit__(self_inner, *a):
                        return False

                return _H()

        return _In()

    def new_output(self, location):
        objects = self.objects

        class _Out:
            def create(self):
                class _W:
                    def write(self_inner, data):
                        objects[location] = data

                    def close(self_inner):
                        pass

                return _W()

        return _Out()


_OLD_LOC = "gs://bucket/ws/coll/tbl"
_NEW_LOC = "gs://bucket/ws/newcoll/newtbl"


def _catalog(
    dataset_data=None,
    snapshots=None,
    manifest_rows=None,
    target_exists=False,
):
    """A catalog whose only dataset is coll.tbl, with `snapshots` history."""
    data = {"name": "tbl", "collection": "coll", "location": _OLD_LOC, "locked-by": None}
    data.update(dataset_data or {})

    schemas = _Collection({"sc1": _DocRef(data={"columns": [{"name": "id"}]}, doc_id="sc1")})
    snapshot_docs = {
        str(sid): _DocRef(data=sdata, doc_id=str(sid)) for sid, sdata in (snapshots or {}).items()
    }
    source_ref = _DocRef(
        data=data,
        subcollections={"schemas": schemas, "snapshots": _Collection(snapshot_docs)},
        doc_id="tbl",
    )
    target_ref = _DocRef(exists=target_exists, doc_id="newtbl")
    tombstones = _Collection()

    refs = {("coll", "tbl"): source_ref, ("newcoll", "newtbl"): target_ref}

    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog.gcs_bucket = "bucket"
    catalog._storage_client = None  # force the FileIO copy path
    catalog.io = _FakeIO()
    catalog._dataset_doc_ref = lambda c, n: refs[(c, n)]
    catalog._snapshots_collection = lambda c, n: refs[(c, n)].collection("snapshots")
    catalog._tombstones_collection = lambda: tombstones

    # Manifests are read as parquet bytes; stub the decode so these tests stay
    # about the move, not about rugo's parquet round-trip.
    catalog._captured_manifests = []

    for sid, sdata in (snapshots or {}).items():
        catalog.io.objects[sdata["manifest"]] = f"manifest-{sid}".encode()
    for row in manifest_rows or []:
        catalog.io.objects[row["file_path"]] = b"data"

    return catalog, source_ref, target_ref, tombstones


def _patch_manifest_io(catalog, monkeypatch, rows_by_manifest):
    """Route manifest decode/encode through plain dicts."""
    import opteryx_catalog.catalog.manifest as manifest_module

    def _read_manifest_rows(data: bytes):
        return [dict(r) for r in rows_by_manifest[data.decode()]]

    monkeypatch.setattr(manifest_module, "read_manifest_rows", _read_manifest_rows)

    def _write_parquet_manifest(snapshot_id, entries, dataset_location):
        path = f"{dataset_location}/metadata/manifest-{snapshot_id}.parquet"
        catalog._captured_manifests.append((path, [dict(e) for e in entries]))
        return path

    catalog.write_parquet_manifest = _write_parquet_manifest


def _emitted(capsys):
    out = capsys.readouterr().out
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def _single_snapshot_catalog(monkeypatch, extra_rows=()):
    rows = [{"file_path": f"{_OLD_LOC}/data/a.parquet", "record_count": 1}]
    rows.extend(extra_rows)
    snapshots = {1: {"snapshot-id": 1, "manifest": f"{_OLD_LOC}/metadata/manifest-1.parquet"}}
    catalog, source, target, tombstones = _catalog(snapshots=snapshots, manifest_rows=rows)
    _patch_manifest_io(catalog, monkeypatch, {"manifest-1": rows})
    return catalog, source, target, tombstones


def test_copies_data_files_to_the_new_location(monkeypatch, capsys):
    catalog, _source, _target, _tomb = _single_snapshot_catalog(monkeypatch)

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert f"{_NEW_LOC}/data/a.parquet" in catalog.io.objects
    assert catalog.io.objects[f"{_NEW_LOC}/data/a.parquet"] == b"data"


def test_manifest_is_rewritten_with_remapped_paths(monkeypatch, capsys):
    catalog, _source, _target, _tomb = _single_snapshot_catalog(monkeypatch)

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    path, entries = catalog._captured_manifests[0]
    assert path == f"{_NEW_LOC}/metadata/manifest-1.parquet"
    assert entries[0]["file_path"] == f"{_NEW_LOC}/data/a.parquet"


def test_catalog_entry_moves_with_new_name_collection_and_location(monkeypatch, capsys):
    catalog, source, target, _tomb = _single_snapshot_catalog(monkeypatch)

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert target.written["name"] == "newtbl"
    assert target.written["collection"] == "newcoll"
    assert target.written["location"] == _NEW_LOC
    assert source.deleted is True


def test_snapshot_docs_move_and_point_at_the_new_manifest(monkeypatch, capsys):
    catalog, _source, target, _tomb = _single_snapshot_catalog(monkeypatch)

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    moved = target.collection("snapshots").document("1").get().to_dict()
    assert moved["manifest"] == f"{_NEW_LOC}/metadata/manifest-1.parquet"


def test_all_snapshots_move_not_just_the_current_one(monkeypatch, capsys):
    """History must survive a rename - every historical manifest is rewritten."""
    rows_1 = [{"file_path": f"{_OLD_LOC}/data/a.parquet"}]
    rows_2 = [{"file_path": f"{_OLD_LOC}/data/b.parquet"}]
    snapshots = {
        1: {"snapshot-id": 1, "manifest": f"{_OLD_LOC}/metadata/manifest-1.parquet"},
        2: {"snapshot-id": 2, "manifest": f"{_OLD_LOC}/metadata/manifest-2.parquet"},
    }
    catalog, _source, target, _tomb = _catalog(snapshots=snapshots, manifest_rows=rows_1 + rows_2)
    _patch_manifest_io(catalog, monkeypatch, {"manifest-1": rows_1, "manifest-2": rows_2})

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    written = {path for path, _ in catalog._captured_manifests}
    assert written == {
        f"{_NEW_LOC}/metadata/manifest-1.parquet",
        f"{_NEW_LOC}/metadata/manifest-2.parquet",
    }
    assert f"{_NEW_LOC}/data/a.parquet" in catalog.io.objects
    assert f"{_NEW_LOC}/data/b.parquet" in catalog.io.objects
    snapshots_moved = target.collection("snapshots")
    assert {d.id for d in snapshots_moved.stream()} == {"1", "2"}


def test_file_shared_by_two_snapshots_is_copied_once(monkeypatch, capsys):
    shared = [{"file_path": f"{_OLD_LOC}/data/a.parquet"}]
    snapshots = {
        1: {"snapshot-id": 1, "manifest": f"{_OLD_LOC}/metadata/manifest-1.parquet"},
        2: {"snapshot-id": 2, "manifest": f"{_OLD_LOC}/metadata/manifest-2.parquet"},
    }
    catalog, _source, _target, _tomb = _catalog(snapshots=snapshots, manifest_rows=shared)
    _patch_manifest_io(catalog, monkeypatch, {"manifest-1": shared, "manifest-2": shared})

    copies = []
    original = catalog._copy_object

    def _counting_copy(src, dst):
        copies.append((src, dst))
        return original(src, dst)

    catalog._copy_object = _counting_copy

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert len(copies) == 1


def test_externally_referenced_files_are_left_where_they_are(monkeypatch, capsys):
    """A file outside the dataset's own location was never ours to move."""
    external = {"file_path": "gs://bucket/elsewhere/shared.parquet"}
    catalog, _source, _target, _tomb = _single_snapshot_catalog(monkeypatch, extra_rows=[external])

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    _path, entries = catalog._captured_manifests[0]
    paths = [e["file_path"] for e in entries]
    assert "gs://bucket/elsewhere/shared.parquet" in paths
    assert f"{_NEW_LOC}/elsewhere/shared.parquet" not in catalog.io.objects


def test_old_location_is_tombstoned_not_deleted_inline(monkeypatch, capsys):
    """The vacated prefix goes to the existing 24h reclamation sweep."""
    catalog, _source, _target, tombstones = _single_snapshot_catalog(monkeypatch)

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    tombstone = tombstones.document("coll.tbl").get().to_dict()
    assert tombstone["location"] == _OLD_LOC
    assert tombstone["dropped-by"] == "alice"
    # the original files are still there - the sweep removes them later
    assert f"{_OLD_LOC}/data/a.parquet" in catalog.io.objects


def test_missing_source_raises(monkeypatch):
    catalog, source, _target, _tomb = _single_snapshot_catalog(monkeypatch)
    source._doc.exists = False

    with pytest.raises(DatasetNotFound):
        catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")


def test_existing_target_raises(monkeypatch):
    snapshots = {1: {"snapshot-id": 1, "manifest": f"{_OLD_LOC}/metadata/manifest-1.parquet"}}
    catalog, _source, _target, _tomb = _catalog(snapshots=snapshots, target_exists=True)
    _patch_manifest_io(catalog, monkeypatch, {"manifest-1": []})

    with pytest.raises(DatasetAlreadyExists):
        catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")


def test_locked_dataset_cannot_be_renamed(monkeypatch):
    """The two-person deniability lock outranks a rename, as it does a drop."""
    catalog, source, _target, _tomb = _single_snapshot_catalog(monkeypatch)
    source._doc._data["locked-by"] = "bob"

    with pytest.raises(DatasetLocked):
        catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")


def test_same_name_rejected(monkeypatch):
    catalog, _source, _target, _tomb = _single_snapshot_catalog(monkeypatch)

    with pytest.raises(ValueError, match="same"):
        catalog.rename_dataset("coll.tbl", "coll.tbl", author="alice")


def test_nothing_is_copied_when_the_target_already_exists(monkeypatch):
    """The existence checks run before any byte moves - a rejected rename must
    not leave orphan copies behind."""
    rows = [{"file_path": f"{_OLD_LOC}/data/a.parquet"}]
    snapshots = {1: {"snapshot-id": 1, "manifest": f"{_OLD_LOC}/metadata/manifest-1.parquet"}}
    catalog, _source, _target, _tomb = _catalog(
        snapshots=snapshots, manifest_rows=rows, target_exists=True
    )
    _patch_manifest_io(catalog, monkeypatch, {"manifest-1": rows})

    with pytest.raises(DatasetAlreadyExists):
        catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert f"{_NEW_LOC}/data/a.parquet" not in catalog.io.objects


def test_emits_audit_record(monkeypatch, capsys):
    catalog, _source, _target, _tomb = _single_snapshot_catalog(monkeypatch)

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    record = [r for r in _emitted(capsys) if r["action"] == "rename_dataset"][0]
    assert record["resource_type"] == "dataset"
    assert record["workspace"] == "ws"
    assert record["collection"] == "newcoll"
    assert record["resource"] == "newtbl"
    assert record["author"] == "alice"
    assert record["detail"]["old_identifier"] == "coll.tbl"
    assert record["detail"]["new_identifier"] == "newcoll.newtbl"
    assert record["detail"]["files_copied"] == 1
    assert record["detail"]["snapshots_moved"] == 1


def test_unauthenticated_records_no_author(monkeypatch, capsys):
    catalog, _source, _target, _tomb = _single_snapshot_catalog(monkeypatch)

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl")

    record = [r for r in _emitted(capsys) if r["action"] == "rename_dataset"][0]
    assert record["author"] is None
