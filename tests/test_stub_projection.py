"""Stub dataset projection: reconciling a bound workspace's listing.

Covers the Phase 6 primitive from WORKSPACE_CATALOG_RESOLUTION_PLAN.md: what
gets added, what gets removed, what is deliberately left alone (real dataset
documents, in both directions), the freshness stamp written onto the binding
block, and the two refusals - an unknown workspace and an unbound one.
"""

from __future__ import annotations

import pytest

from opteryx_catalog.binding import read_catalog_binding
from opteryx_catalog.exceptions import InvalidCatalogBinding
from opteryx_catalog.exceptions import WorkspaceNotFound
from opteryx_catalog.stub_projection import STUB_MARKER
from opteryx_catalog.stub_projection import sync_stub_datasets


# ---------------------------------------------------------------------------
# A Firestore double with just the shape this module uses: a workspace
# collection of documents, each with a `datasets` subcollection, plus
# field-path updates (the stamp uses escaped `catalog.`x`` paths).
# ---------------------------------------------------------------------------


class _Snapshot:
    def __init__(self, doc_id, data):
        self.id = doc_id
        self.exists = data is not None
        self._data = data

    def to_dict(self):
        return dict(self._data) if self._data is not None else None


def _set_path(data: dict, path: str, value) -> None:
    # "catalog.`listing-count`" -> data["catalog"]["listing-count"]
    parts = [part.strip("`") for part in path.split(".")]
    node = data
    for part in parts[:-1]:
        node = node.setdefault(part, {})
    node[parts[-1]] = value


class _Doc:
    def __init__(self, doc_id):
        self.id = doc_id
        self._data = None
        self._subcollections = {}

    def get(self):
        return _Snapshot(self.id, self._data)

    def set(self, data):
        self._data = dict(data)

    def update(self, fields):
        if self._data is None:
            raise KeyError("update on missing document")
        for key, value in fields.items():
            _set_path(self._data, key, value)

    def delete(self):
        self._data = None

    def collection(self, name):
        return self._subcollections.setdefault(name, _Collection())

    @property
    def _exists_or_has_children(self):
        return self._data is not None or any(
            coll._docs for coll in self._subcollections.values()
        )


class _Collection:
    def __init__(self):
        self._docs = {}

    def document(self, doc_id):
        return self._docs.setdefault(doc_id, _Doc(doc_id))

    def list_documents(self):
        # Firestore returns "missing" parents that still have subcollections,
        # which is exactly what a stub-only collection looks like.
        return [doc for doc in self._docs.values() if doc._exists_or_has_children]

    def stream(self):
        return [_Snapshot(doc.id, doc._data) for doc in self._docs.values() if doc._data is not None]


class _FakeFirestore:
    def __init__(self):
        self._collections = {}

    def collection(self, name):
        return self._collections.setdefault(name, _Collection())


WS = "tarchia"


@pytest.fixture
def db():
    client = _FakeFirestore()
    client.collection(WS).document("$properties").set(
        {
            "timestamp-ms": 1,
            "catalog": {"kind": "iceberg", "config": {}, "version": 7},
        }
    )
    return client


def _stubs(db, workspace=WS):
    found = {}
    for collection_doc in db.collection(workspace).list_documents():
        if collection_doc.id.startswith("$"):
            continue
        for snapshot in collection_doc.collection("datasets").stream():
            found[(collection_doc.id, snapshot.id)] = snapshot.to_dict()
    return found


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------


def test_first_sync_writes_stubs(db):
    result = sync_stub_datasets(db, WS, [("interop", "people"), ("interop", "orders")])
    assert (result.added, result.removed, result.total) == (2, 0, 2)

    stubs = _stubs(db)
    assert set(stubs) == {("interop", "people"), ("interop", "orders")}
    assert stubs[("interop", "people")] == {
        "workspace": WS,
        "collection": "interop",
        "name": "people",
        STUB_MARKER: True,
    }


def test_resync_of_the_same_listing_is_a_no_op(db):
    listing = [("interop", "people"), ("interop", "orders")]
    sync_stub_datasets(db, WS, listing)
    result = sync_stub_datasets(db, WS, listing)
    assert (result.added, result.removed, result.total) == (0, 0, 2)


def test_added_and_removed_are_reported_and_applied(db):
    sync_stub_datasets(db, WS, [("interop", "people"), ("interop", "orders")])
    result = sync_stub_datasets(db, WS, [("interop", "people"), ("sales", "invoices")])
    assert (result.added, result.removed, result.total) == (1, 1, 2)
    assert set(_stubs(db)) == {("interop", "people"), ("sales", "invoices")}


def test_duplicate_entries_count_once(db):
    result = sync_stub_datasets(db, WS, [("interop", "people"), ("interop", "people")])
    assert (result.added, result.total) == (1, 1)


def test_nested_external_namespace_splits_left_anchored(db):
    # `a.b.people` in the external catalog is collection `a`, dataset `b.people`
    result = sync_stub_datasets(db, WS, [("a", "b.people")])
    assert result.added == 1
    assert ("a", "b.people") in _stubs(db)


# ---------------------------------------------------------------------------
# Real dataset documents are not ours to touch, in either direction
# ---------------------------------------------------------------------------


def _seed_real_dataset(db, collection, name):
    db.collection(WS).document(collection).collection("datasets").document(name).set(
        {"workspace": WS, "collection": collection, "name": name, "location": "gs://real"}
    )


def test_a_real_dataset_is_never_overwritten_by_a_stub(db):
    _seed_real_dataset(db, "interop", "people")
    result = sync_stub_datasets(db, WS, [("interop", "people")])
    assert (result.added, result.removed, result.total) == (0, 0, 1)
    doc = db.collection(WS).document("interop").collection("datasets").document("people").get()
    assert doc.to_dict()["location"] == "gs://real"
    assert STUB_MARKER not in doc.to_dict()


def test_a_real_dataset_missing_from_the_listing_is_not_deleted(db):
    _seed_real_dataset(db, "interop", "people")
    result = sync_stub_datasets(db, WS, [("interop", "orders")])
    assert (result.added, result.removed) == (1, 0)
    assert db.collection(WS).document("interop").collection("datasets").document(
        "people"
    ).get().exists


# ---------------------------------------------------------------------------
# The freshness stamp
# ---------------------------------------------------------------------------


def test_stamp_lands_on_the_binding_block_and_reads_back(db):
    result = sync_stub_datasets(db, WS, [("interop", "people")])
    binding = read_catalog_binding(db, WS)
    assert binding.listing_count == 1
    assert binding.listing_synced_at_ms == result.synced_at_ms
    # and the binding itself is untouched
    assert binding.kind == "iceberg"
    assert binding.version == 7


def test_stamp_is_rewritten_on_every_sync(db):
    first = sync_stub_datasets(db, WS, [("interop", "people")])
    second = sync_stub_datasets(db, WS, [])
    assert second.synced_at_ms >= first.synced_at_ms
    binding = read_catalog_binding(db, WS)
    assert binding.listing_count == 0


def test_binding_read_reports_never_synced_before_a_sync(db):
    binding = read_catalog_binding(db, WS)
    assert binding.listing_synced_at_ms is None
    assert binding.listing_count is None


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


def test_unknown_workspace_raises(db):
    with pytest.raises(WorkspaceNotFound):
        sync_stub_datasets(db, "nowhere", [("interop", "people")])


def test_unbound_workspace_raises(db):
    db.collection("native_ws").document("$properties").set({"timestamp-ms": 1})
    with pytest.raises(InvalidCatalogBinding):
        sync_stub_datasets(db, "native_ws", [("interop", "people")])


@pytest.mark.parametrize(
    "entry",
    [("", "people"), ("interop", ""), ("interop", "a/b"), ("a.b", "people"), ("$dropped", "x")],
)
def test_unusable_listing_entries_are_rejected_before_any_write(db, entry):
    with pytest.raises(ValueError):
        sync_stub_datasets(db, WS, [entry])
    assert _stubs(db) == {}


def test_malformed_listing_entry_is_rejected(db):
    with pytest.raises(ValueError):
        sync_stub_datasets(db, WS, ["interop.people"])
