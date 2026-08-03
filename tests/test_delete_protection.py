from __future__ import annotations

import pytest

from opteryx_catalog.exceptions import WorkspaceDeleteProtected
from opteryx_catalog.opteryx_catalog import OpteryxCatalog


class _Doc:
    def __init__(self, data=None, exists=True):
        self.exists = exists
        self._data = data or {}

    def to_dict(self):
        return self._data


class _DocRef:
    def __init__(self, data=None, exists=True):
        self._doc = _Doc(data, exists)
        self.deleted = False
        self.written = None
        self._subcollections = {}

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
            self._docs[doc_id] = _DocRef(exists=False)
        return self._docs[doc_id]

    def stream(self):
        return []


def _catalog(protected: bool):
    """A catalog whose workspace is (or isn't) delete-protected, with one
    dataset, one collection and one view, all otherwise droppable."""
    props = {"delete_protection": True} if protected else {"delete_protection": False}
    catalog_ref = _Collection({"$properties": _DocRef(data=props)})

    dataset_ref = _DocRef(data={"location": "gs://bucket/ws/coll/tbl", "locked-by": None})
    collection_ref = _DocRef(data={"name": "coll", "locked-by": None})
    view_ref = _DocRef(data={"name": "v"})

    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog._catalog_ref = catalog_ref
    catalog._dataset_doc_ref = lambda c, n: dataset_ref
    catalog._snapshots_collection = lambda c, n: dataset_ref.collection("snapshots")
    catalog._tombstones_collection = lambda: _Collection()
    catalog._collection_ref = lambda c: collection_ref
    catalog._datasets_collection = lambda c: _Collection()
    catalog._views_collection = lambda c: _Collection()
    catalog._view_doc_ref = lambda c, n: view_ref
    catalog._dropped_workspaces_collection = lambda: _Collection()
    return catalog, dataset_ref, collection_ref, view_ref


# --- What delete_protection guards: the workspace, and only the workspace ---


def test_soft_delete_workspace_blocked():
    catalog, _d, _c, _v = _catalog(protected=True)

    with pytest.raises(WorkspaceDeleteProtected, match="delete-protected"):
        catalog.soft_delete_workspace(author="alice")

    assert catalog.get_workspace_properties().get("deleted-at-ms") is None


def test_error_names_the_statement_that_clears_it():
    catalog, _d, _c, _v = _catalog(protected=True)

    with pytest.raises(WorkspaceDeleteProtected, match="SET delete_protection TO OFF"):
        catalog.soft_delete_workspace(author="alice")


@pytest.mark.parametrize("props", [{}, {"delete_protection": False}, {"delete_protection": None}])
def test_unprotected_workspace_deletes_normally(props, monkeypatch):
    """Absent, false and null all mean 'not protected' - only a truthy flag blocks."""
    monkeypatch.setattr("opteryx_catalog.opteryx_catalog.send_webhook", lambda **k: None)
    catalog, _d, _c, _v = _catalog(protected=False)
    catalog._catalog_ref.document("$properties")._doc._data = dict(props)

    catalog.soft_delete_workspace(author="alice")

    assert catalog.get_workspace_properties()["deleted-at-ms"] is not None


def test_protection_is_re_read_not_cached(monkeypatch):
    """A long-lived catalog handle must not still be able to delete the
    workspace after protection is switched on elsewhere."""
    monkeypatch.setattr("opteryx_catalog.opteryx_catalog.send_webhook", lambda **k: None)
    catalog, _d, _c, _v = _catalog(protected=False)

    catalog.get_workspace_properties()  # warm any accidental cache
    catalog._catalog_ref.document("$properties")._doc._data = {"delete_protection": True}

    with pytest.raises(WorkspaceDeleteProtected):
        catalog.soft_delete_workspace(author="alice")


def test_setting_the_flag_then_deleting_is_blocked(capsys):
    """End to end through the public setter: ALTER WORKSPACE ... TO ON, then
    deleting the workspace is refused."""
    catalog, _d, _c, _v = _catalog(protected=False)

    catalog.set_workspace_properties({"delete_protection": True}, author="alice")

    with pytest.raises(WorkspaceDeleteProtected):
        catalog.soft_delete_workspace(author="alice")


def test_clearing_the_flag_re_enables_deletion(monkeypatch, capsys):
    monkeypatch.setattr("opteryx_catalog.opteryx_catalog.send_webhook", lambda **k: None)
    catalog, _d, _c, _v = _catalog(protected=True)

    catalog.set_workspace_properties({"delete_protection": False}, author="alice")
    catalog.soft_delete_workspace(author="alice")

    assert catalog.get_workspace_properties()["deleted-at-ms"] is not None


# --- What it deliberately does NOT guard: everything inside the workspace ---


def test_drop_dataset_is_not_blocked(monkeypatch):
    """delete_protection protects the workspace, not the assets in it.
    Per-asset protection is the `locked-by` two-person lock."""
    monkeypatch.setattr("opteryx_catalog.opteryx_catalog.send_webhook", lambda **k: None)
    catalog, dataset_ref, _c, _v = _catalog(protected=True)

    catalog.drop_dataset("coll.tbl", author="alice")

    assert dataset_ref.deleted is True


def test_drop_collection_is_not_blocked(monkeypatch):
    monkeypatch.setattr("opteryx_catalog.opteryx_catalog.send_webhook", lambda **k: None)
    catalog, _d, collection_ref, _v = _catalog(protected=True)

    catalog.drop_collection("coll", author="alice")

    assert collection_ref.deleted is True


def test_drop_view_is_not_blocked(monkeypatch):
    monkeypatch.setattr("opteryx_catalog.opteryx_catalog.send_webhook", lambda **k: None)
    catalog, _d, _c, view_ref = _catalog(protected=True)

    catalog.drop_view("coll.v", author="alice")

    assert view_ref.deleted is True


def test_rename_is_not_blocked(monkeypatch):
    monkeypatch.setattr("opteryx_catalog.opteryx_catalog.send_webhook", lambda **k: None)
    catalog, _d, _c, _v = _catalog(protected=True)

    target = _DocRef(exists=False)
    source = catalog._dataset_doc_ref("coll", "tbl")
    catalog._dataset_doc_ref = lambda c, n: target if n == "new" else source
    catalog._snapshots_collection = lambda c, n: _Collection()
    catalog.gcs_bucket = "bucket"
    catalog._storage_client = None
    catalog.io = None

    catalog.rename_dataset("coll.tbl", "coll.new", author="alice")

    assert target.written["name"] == "new"
