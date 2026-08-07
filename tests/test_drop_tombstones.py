from __future__ import annotations

from unittest.mock import patch

from opteryx_catalog.opteryx_catalog import OpteryxCatalog


class _Doc:
    """A stand-in for a Firestore document snapshot."""

    def __init__(self, id_, data=None, exists=True):
        self.id = id_
        self.exists = exists
        self._data = data or {}

    def to_dict(self):
        return self._data


class _DocRef:
    """A stand-in for a Firestore DocumentReference."""

    def __init__(self, id_, data=None, exists=True, log=None):
        self.id = id_
        self._doc = _Doc(id_, data, exists)
        self._subcollections = {}
        self.log = log if log is not None else []
        self.written = None

    def get(self):
        return self._doc

    def set(self, data):
        self.written = data
        self.log.append(("set", self.id))

    def delete(self):
        self.log.append(("delete", self.id))

    def collection(self, name):
        if name not in self._subcollections:
            self._subcollections[name] = _Collection(name, log=self.log)
        return self._subcollections[name]


class _Collection:
    """A stand-in for a Firestore CollectionReference."""

    def __init__(self, name, docs=None, log=None):
        self.name = name
        self.log = log if log is not None else []
        self._docs = {}
        for doc_id in docs or []:
            self._docs[doc_id] = _DocRef(doc_id, log=self.log)

    def document(self, doc_id):
        if doc_id not in self._docs:
            self._docs[doc_id] = _DocRef(doc_id, log=self.log)
        return self._docs[doc_id]

    def stream(self):
        return [ref._doc for ref in self._docs.values()]


def _catalog_with_dataset(exists=True, location="gs://bucket/ws/coll/tbl"):
    """A catalog whose only dataset is coll.tbl, with snapshot and schema history."""
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    log = []

    dataset_ref = _DocRef("tbl", data={"location": location}, exists=exists, log=log)
    dataset_ref.collection("snapshots")._docs = {
        "s1": _DocRef("s1", log=log),
        "s2": _DocRef("s2", log=log),
    }
    dataset_ref.collection("schemas")._docs = {"sc1": _DocRef("sc1", log=log)}

    tombstones = _Collection("datasets", log=log)
    dropped_doc = _DocRef("$dropped", log=log)
    dropped_doc._subcollections["datasets"] = tombstones

    catalog._dataset_doc_ref = lambda c, n: dataset_ref
    catalog._snapshots_collection = lambda c, n: dataset_ref.collection("snapshots")
    catalog._tombstones_collection = lambda: tombstones
    catalog._catalog_ref = _Collection("ws", log=log)

    return catalog, dataset_ref, tombstones, log


def test_drop_dataset_writes_tombstone():
    """Dropping records the location so a later sweep can reclaim its files."""
    catalog, _dataset_ref, tombstones, _log = _catalog_with_dataset()

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_dataset("coll.tbl", author="alice")

    tombstone = tombstones.document("coll.tbl").written
    assert tombstone["location"] == "gs://bucket/ws/coll/tbl"
    assert tombstone["collection"] == "coll"
    assert tombstone["name"] == "tbl"
    assert tombstone["workspace"] == "ws"
    assert tombstone["dropped-by"] == "alice"
    assert isinstance(tombstone["dropped-at-ms"], int)


def test_drop_dataset_tombstone_precedes_deletion():
    """The tombstone is durable before anything is removed, so a crash cannot leak."""
    catalog, _dataset_ref, _tombstones, log = _catalog_with_dataset()

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_dataset("coll.tbl", author="alice")

    first_set = next(i for i, (op, _) in enumerate(log) if op == "set")
    first_delete = next(i for i, (op, _) in enumerate(log) if op == "delete")
    assert first_set < first_delete


def test_drop_dataset_deletes_schemas_subcollection():
    """Firestore does not cascade - the schemas subcollection must be emptied too."""
    catalog, _dataset_ref, _tombstones, log = _catalog_with_dataset()

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_dataset("coll.tbl", author="alice")

    deleted = {name for op, name in log if op == "delete"}
    assert {"s1", "s2", "sc1", "tbl"} <= deleted


def test_drop_dataset_sends_webhook():
    """A drop announces itself, as create already does."""
    catalog, _dataset_ref, _tombstones, _log = _catalog_with_dataset()

    with patch("opteryx_catalog.opteryx_catalog.send_webhook") as hook:
        catalog.drop_dataset("coll.tbl", author="alice")

    assert hook.call_count == 1
    kwargs = hook.call_args.kwargs
    assert kwargs["action"] == "delete"
    assert kwargs["resource_type"] == "dataset"
    assert kwargs["resource_name"] == "tbl"
    assert kwargs["payload"]["dropped_by"] == "alice"
    assert kwargs["payload"]["location"] == "gs://bucket/ws/coll/tbl"


def test_drop_missing_dataset_leaves_no_tombstone():
    """Nothing was dropped, so there is nothing to reclaim and nothing to announce."""
    catalog, _dataset_ref, tombstones, log = _catalog_with_dataset(exists=False)

    with patch("opteryx_catalog.opteryx_catalog.send_webhook") as hook:
        catalog.drop_dataset("coll.tbl", author="alice")

    assert tombstones.document("coll.tbl").written is None
    assert hook.call_count == 0
    assert [op for op, _ in log if op == "delete"] == []


def test_list_and_delete_tombstones():
    """The sweep can enumerate tombstones and clear them once reclaimed."""
    catalog, _dataset_ref, tombstones, log = _catalog_with_dataset()

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_dataset("coll.tbl", author="alice")

    # The stub records writes rather than materialising them, so seed the read side.
    tombstones._docs["coll.tbl"]._doc = _Doc("coll.tbl", tombstones.document("coll.tbl").written)

    listed = catalog.list_dropped_datasets()
    assert len(listed) == 1
    assert listed[0]["id"] == "coll.tbl"
    assert listed[0]["location"] == "gs://bucket/ws/coll/tbl"

    catalog.delete_tombstone("coll.tbl")
    assert ("delete", "coll.tbl") in log


def test_drop_view_sends_webhook_and_needs_no_tombstone():
    """A view owns no storage, so dropping it leaves nothing to reclaim."""
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    log = []
    view_ref = _DocRef("v", data={}, exists=True, log=log)
    view_ref.collection("statement")._docs = {"st1": _DocRef("st1", log=log)}
    catalog._view_doc_ref = lambda c, n: view_ref

    with patch("opteryx_catalog.opteryx_catalog.send_webhook") as hook:
        catalog.drop_view("coll.v", author="bob")

    deleted = {name for op, name in log if op == "delete"}
    assert {"st1", "v"} <= deleted
    assert [op for op, _ in log if op == "set"] == []

    kwargs = hook.call_args.kwargs
    assert kwargs["action"] == "delete"
    assert kwargs["resource_type"] == "view"
    assert kwargs["payload"]["dropped_by"] == "bob"


def test_drop_missing_view_is_silent():
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    log = []
    catalog._view_doc_ref = lambda c, n: _DocRef("v", exists=False, log=log)

    with patch("opteryx_catalog.opteryx_catalog.send_webhook") as hook:
        catalog.drop_view("coll.v", author="bob")

    assert hook.call_count == 0
    assert log == []
