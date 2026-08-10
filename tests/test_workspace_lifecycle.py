"""Tests for CAT-1/CAT-2/CAT-3/CAT-5: workspace soft-delete/lock, the
construction-time gate, the root-level `$dropped-workspaces` tombstone
registry, and the dataset/collection lock fields on `drop_dataset()` /
`drop_collection()`.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from opteryx_catalog.exceptions import CollectionLocked
from opteryx_catalog.exceptions import CollectionNotFound
from opteryx_catalog.exceptions import DatasetLocked
from opteryx_catalog.exceptions import WorkspaceDeleted
from opteryx_catalog.exceptions import WorkspaceNotFound
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
    """A stand-in for a Firestore DocumentReference.

    Unlike a freshly-instantiated real `DocumentReference`, whose `.get()`
    reports `exists=False` until something is written, a doc created here
    explicitly (e.g. in a fixture) can be told to already exist.
    """

    def __init__(self, id_, data=None, exists=False, log=None):
        self.id = id_
        self._doc = _Doc(id_, data, exists)
        self._subcollections = {}
        self.log = log if log is not None else []
        self.written = None
        self.updated = None

    def get(self):
        return self._doc

    def set(self, data):
        self.written = data
        self._doc = _Doc(self.id, dict(data), exists=True)
        self.log.append(("set", self.id))

    def update(self, data):
        self.updated = data
        merged = dict(self._doc._data)
        merged.update(data)
        self._doc = _Doc(self.id, merged, exists=True)
        self.log.append(("update", self.id))

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


class _FirestoreClient:
    """A stand-in for `google.cloud.firestore.Client`.

    Always hands back the same collection regardless of the name asked for -
    good enough for these tests, which only ever need one collection per
    fake client (either the workspace's own top-level collection, for
    `__init__`, or the root `$dropped-workspaces` collection).
    """

    def __init__(self, collection):
        self._collection = collection

    def collection(self, _name):
        return self._collection


# --- __init__ construction-time gate (CAT-1) ---------------------------


def _properties_client(props_data=None, props_exists=True):
    log = []
    catalog_collection = _Collection("ws", log=log)
    catalog_collection._docs["$properties"] = _DocRef(
        "$properties", data=props_data or {}, exists=props_exists, log=log
    )
    return _FirestoreClient(catalog_collection), catalog_collection, log


def test_init_raises_for_deleted_workspace():
    client, _cc, _log = _properties_client(
        props_data={"deleted-at-ms": 12345, "deleted-by": "alice"}
    )
    with (
        patch("opteryx_catalog.opteryx_catalog.firestore.Client", return_value=client),
        pytest.raises(WorkspaceDeleted),
    ):
        OpteryxCatalog(workspace="ws")


def test_init_succeeds_for_deleted_workspace_with_include_deleted():
    client, _cc, _log = _properties_client(
        props_data={"deleted-at-ms": 12345, "deleted-by": "alice"}
    )
    with patch("opteryx_catalog.opteryx_catalog.firestore.Client", return_value=client):
        catalog = OpteryxCatalog(workspace="ws", include_deleted=True)
    assert catalog.workspace == "ws"


def test_init_succeeds_for_non_deleted_existing_workspace():
    client, _cc, _log = _properties_client(props_data={"deleted-at-ms": None})
    with patch("opteryx_catalog.opteryx_catalog.firestore.Client", return_value=client):
        catalog = OpteryxCatalog(workspace="ws")
    assert catalog.workspace == "ws"


def test_init_raises_for_unknown_workspace():
    client, _cc, _log = _properties_client(props_exists=False)
    with (
        patch("opteryx_catalog.opteryx_catalog.firestore.Client", return_value=client),
        pytest.raises(WorkspaceNotFound),
    ):
        OpteryxCatalog(workspace="ws")


def test_init_does_not_write_for_unknown_workspace():
    """A mistyped workspace name must not conjure the workspace into being.

    In Firestore a collection exists only because a document in it does, so
    writing `$properties` here is what created the empty workspace behind a
    failed `banana.banana.banana` query.
    """
    client, catalog_collection, log = _properties_client(props_exists=False)
    with (
        patch("opteryx_catalog.opteryx_catalog.firestore.Client", return_value=client),
        pytest.raises(WorkspaceNotFound),
    ):
        OpteryxCatalog(workspace="ws")

    assert catalog_collection.document("$properties").written is None
    assert log == []


def test_init_creates_properties_doc_with_all_fields_when_missing():
    client, catalog_collection, _log = _properties_client(props_exists=False)
    with patch("opteryx_catalog.opteryx_catalog.firestore.Client", return_value=client):
        OpteryxCatalog(workspace="ws", create_if_missing=True)

    written = catalog_collection.document("$properties").written
    assert written["billing-account-id"] is None
    assert written["owner"] is None
    assert written["deleted-at-ms"] is None
    assert written["deleted-by"] is None
    assert written["locked-by"] is None
    assert written["locked-at-ms"] is None


# --- Workspace lifecycle methods (CAT-2/CAT-3) --------------------------


def _catalog_with_properties(props_data=None, tombstone_data=None):
    log = []
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"

    catalog_collection = _Collection("ws", log=log)
    catalog_collection._docs["$properties"] = _DocRef(
        "$properties", data=props_data or {}, exists=True, log=log
    )
    catalog._catalog_ref = catalog_collection

    dropped_workspaces = _Collection("$dropped-workspaces", log=log)
    if tombstone_data is not None:
        dropped_workspaces._docs["ws"] = _DocRef("ws", data=tombstone_data, exists=True, log=log)
    catalog.firestore_client = _FirestoreClient(dropped_workspaces)

    return catalog, catalog_collection, dropped_workspaces, log


def test_soft_delete_workspace_sets_properties_and_writes_tombstone():
    # deletion_protection is ON unless explicitly cleared, so a workspace that is
    # about to be deleted has had it turned off first.
    catalog, catalog_collection, dropped_workspaces, _log = _catalog_with_properties(
        props_data={"deletion_protection": False}
    )

    with patch("opteryx_catalog.opteryx_catalog.send_webhook") as hook:
        catalog.soft_delete_workspace(author="alice")

    props = catalog_collection.document("$properties").get().to_dict()
    assert props["deleted-by"] == "alice"
    assert isinstance(props["deleted-at-ms"], int)

    tombstone = dropped_workspaces.document("ws").written
    assert tombstone["workspace"] == "ws"
    assert tombstone["dropped-by"] == "alice"
    assert isinstance(tombstone["dropped-at-ms"], int)

    assert hook.call_count == 1
    kwargs = hook.call_args.kwargs
    assert kwargs["action"] == "delete"
    assert kwargs["resource_type"] == "workspace"
    assert kwargs["resource_name"] == "ws"
    assert kwargs["payload"]["dropped_by"] == "alice"


def test_soft_delete_workspace_requires_author():
    catalog, _cc, _dw, _log = _catalog_with_properties()
    with pytest.raises(ValueError):
        catalog.soft_delete_workspace(author=None)


def test_restore_workspace_clears_properties_and_deletes_tombstone():
    catalog, catalog_collection, _dw, log = _catalog_with_properties(
        props_data={"deleted-at-ms": 111, "deleted-by": "alice"},
        tombstone_data={"workspace": "ws", "dropped-at-ms": 111, "dropped-by": "alice"},
    )

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.restore_workspace(author="bob")

    props = catalog_collection.document("$properties").get().to_dict()
    assert props["deleted-at-ms"] is None
    assert props["deleted-by"] is None

    # The tombstone must be cleared too - otherwise a restored workspace is
    # still a candidate for the 24h sweep and would be hard-deleted anyway.
    # (The stub records the delete in `log` rather than materializing it -
    # see `_DocRef.delete` - so that's what's asserted here.)
    assert ("delete", "ws") in log


def test_restore_workspace_requires_author():
    catalog, _cc, _dw, _log = _catalog_with_properties()
    with pytest.raises(ValueError):
        catalog.restore_workspace(author=None)


def test_lock_workspace_sets_fields():
    catalog, catalog_collection, _dw, _log = _catalog_with_properties()

    with patch("opteryx_catalog.opteryx_catalog.send_webhook") as hook:
        catalog.lock_workspace(author="alice")

    props = catalog_collection.document("$properties").get().to_dict()
    assert props["locked-by"] == "alice"
    assert isinstance(props["locked-at-ms"], int)
    assert hook.call_args.kwargs["action"] == "lock"


def test_lock_workspace_requires_author():
    catalog, _cc, _dw, _log = _catalog_with_properties()
    with pytest.raises(ValueError):
        catalog.lock_workspace(author=None)


def test_unlock_workspace_clears_fields():
    catalog, catalog_collection, _dw, _log = _catalog_with_properties(
        props_data={"locked-by": "alice", "locked-at-ms": 123}
    )

    with patch("opteryx_catalog.opteryx_catalog.send_webhook") as hook:
        catalog.unlock_workspace(author="bob")

    props = catalog_collection.document("$properties").get().to_dict()
    assert props["locked-by"] is None
    assert props["locked-at-ms"] is None
    assert hook.call_args.kwargs["action"] == "unlock"


def test_unlock_workspace_requires_author():
    catalog, _cc, _dw, _log = _catalog_with_properties()
    with pytest.raises(ValueError):
        catalog.unlock_workspace(author=None)


def test_list_dropped_workspaces():
    catalog, _cc, _dw, _log = _catalog_with_properties(
        tombstone_data={"workspace": "ws", "dropped-at-ms": 111, "dropped-by": "alice"}
    )

    listed = catalog.list_dropped_workspaces()
    assert len(listed) == 1
    assert listed[0]["id"] == "ws"
    assert listed[0]["dropped-by"] == "alice"


def test_delete_workspace_tombstone():
    catalog, _cc, _dw, log = _catalog_with_properties(tombstone_data={"workspace": "ws"})

    catalog.delete_workspace_tombstone("ws")
    assert ("delete", "ws") in log


# --- Dataset/collection lock fields (CAT-5) ------------------------------


def _catalog_with_dataset(locked=False):
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    log = []
    data = {"location": "gs://bucket/ws/coll/tbl"}
    if locked:
        data["locked-by"] = "alice"
        data["locked-at-ms"] = 123
    dataset_ref = _DocRef("tbl", data=data, exists=True, log=log)
    tombstones = _Collection("datasets", log=log)

    catalog._dataset_doc_ref = lambda c, n: dataset_ref
    catalog._snapshots_collection = lambda c, n: dataset_ref.collection("snapshots")
    catalog._tombstones_collection = lambda: tombstones

    return catalog, dataset_ref, log


def test_drop_dataset_raises_when_locked():
    catalog, _ref, _log = _catalog_with_dataset(locked=True)
    with pytest.raises(DatasetLocked):
        catalog.drop_dataset("coll.tbl", author="bob")


def test_drop_dataset_succeeds_when_not_locked():
    catalog, _ref, log = _catalog_with_dataset(locked=False)
    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_dataset("coll.tbl", author="bob")
    assert ("delete", "tbl") in log


def test_create_dataset_initializes_lock_fields_to_none():
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog.gcs_bucket = "bucket"
    catalog.io = None
    catalog._snapshot_cache = {}
    catalog._schema_cache = {}
    log = []
    catalog._catalog_ref = _Collection("ws", log=log)

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.create_dataset("coll.tbl", schema=None, author="alice")

    doc_ref = catalog._catalog_ref.document("coll").collection("datasets").document("tbl")
    assert doc_ref.written["locked-by"] is None
    assert doc_ref.written["locked-at-ms"] is None


def _catalog_with_collection(locked=False, has_children=False):
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    log = []
    data = {"name": "coll"}
    if locked:
        data["locked-by"] = "alice"
        data["locked-at-ms"] = 123
    coll_ref = _DocRef("coll", data=data, exists=True, log=log)

    datasets_coll = _Collection("datasets", log=log)
    views_coll = _Collection("views", log=log)
    if has_children:
        datasets_coll._docs["tbl"] = _DocRef("tbl", exists=True, log=log)

    catalog._collection_ref = lambda c: coll_ref
    catalog._datasets_collection = lambda c: datasets_coll
    catalog._views_collection = lambda c: views_coll

    return catalog, coll_ref, log


def test_drop_collection_raises_when_locked():
    catalog, _ref, _log = _catalog_with_collection(locked=True)
    with pytest.raises(CollectionLocked):
        catalog.drop_collection("coll", author="bob")


def test_drop_collection_succeeds_when_not_locked_and_empty():
    catalog, _ref, log = _catalog_with_collection(locked=False, has_children=False)
    catalog.drop_collection("coll", author="bob")
    assert ("delete", "coll") in log


def test_drop_collection_still_raises_not_found():
    catalog, _ref, log = _catalog_with_collection(locked=False)
    catalog._collection_ref = lambda c: _DocRef("coll", exists=False, log=log)
    with pytest.raises(CollectionNotFound):
        catalog.drop_collection("coll", author="bob")


def test_create_collection_initializes_lock_fields_to_none():
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    log = []
    coll_ref = _DocRef("coll", exists=False, log=log)
    catalog._collection_ref = lambda c: coll_ref

    catalog.create_collection("coll", author="alice")

    assert coll_ref.written["locked-by"] is None
    assert coll_ref.written["locked-at-ms"] is None
