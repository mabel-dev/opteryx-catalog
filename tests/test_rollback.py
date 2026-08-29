"""Rollback: pointing the head at a snapshot that is already there.

A rollback moves one pointer. It writes no data files, deletes nothing, and
leaves every snapshot it moves off live and readable by id - which is what
makes it reversible. The tests here are mostly about the things it must
REFUSE, because each of those refusals is a way the pointer could otherwise
come to name something nobody can read.
"""

from __future__ import annotations

import json
import time

import pytest

from opteryx_catalog.catalog.metadata import SNAPSHOT_EXPIRED_AT_KEY
from opteryx_catalog.exceptions import DatasetLocked
from opteryx_catalog.exceptions import DatasetNotFound
from opteryx_catalog.exceptions import SnapshotMissingError
from opteryx_catalog.opteryx_catalog import TAGS_SUBCOLLECTION
from opteryx_catalog.opteryx_catalog import OpteryxCatalog


# --- an in-memory Firestore ---------------------------------------------


class _Doc:
    def __init__(self, id_, data, exists):
        self.id = id_
        self.exists = exists
        self._data = dict(data)

    def to_dict(self):
        return dict(self._data)


class _DocRef:
    def __init__(self, id_):
        self.id = id_
        self._data = {}
        self._exists = False
        self._subcollections = {}

    def get(self, transaction=None):
        return _Doc(self.id, self._data, self._exists)

    def set(self, data, merge=False):
        self._data = {**self._data, **data} if merge else dict(data)
        self._exists = True

    def update(self, data):
        self._data = {**self._data, **data}

    def collection(self, name):
        if name not in self._subcollections:
            self._subcollections[name] = _Collection()
        return self._subcollections[name]


class _Collection:
    def __init__(self):
        self._docs = {}

    def document(self, doc_id):
        if doc_id not in self._docs:
            self._docs[doc_id] = _DocRef(doc_id)
        return self._docs[doc_id]

    def stream(self, transaction=None):
        return [ref.get() for ref in self._docs.values() if ref._exists]


class _Transaction:
    """Enough of a Firestore transaction for `@firestore.transactional`.

    Reads are served live and writes are buffered until commit, so a callable
    that raises leaves the store untouched - which is what the refusal tests
    below actually assert.
    """

    _read_only = False
    _max_attempts = 1
    _id = b"fake-txn"

    def __init__(self):
        self.writes = []
        self.committed = False

    def _clean_up(self):
        self.writes = []

    def _begin(self, retry_id=None):
        return None

    def _rollback(self):
        self.writes = []

    def _commit(self):
        for ref, data in self.writes:
            ref.update(data)
        self.committed = True
        return []

    def update(self, ref, data):
        self.writes.append((ref, data))


class _FirestoreClient:
    def __init__(self):
        self._collections = {}

    def collection(self, name):
        if name not in self._collections:
            self._collections[name] = _Collection()
        return self._collections[name]

    def transaction(self):
        return _Transaction()


def _catalog():
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog.io = None
    catalog._snapshot_cache = {}
    catalog._schema_cache = {}
    root = {}

    def datasets_collection(coll):
        return root.setdefault(coll, _Collection())

    catalog._datasets_collection = datasets_collection
    catalog._dataset_doc_ref = lambda c, n: datasets_collection(c).document(n)
    catalog._snapshots_collection = lambda c, n: catalog._dataset_doc_ref(c, n).collection(
        "snapshots"
    )
    catalog._tags_collection = lambda c, n: catalog._dataset_doc_ref(c, n).collection(
        TAGS_SUBCOLLECTION
    )
    catalog.firestore_client = _FirestoreClient()
    catalog._catalog_ref = catalog.firestore_client.collection("ws")
    return catalog


IDENTIFIER = "reports.monthly"


def _dataset(catalog, head=None, schema_id=None, locked_by=None):
    coll, name = IDENTIFIER.split(".", 1)
    document = {"name": name, "collection": coll}
    if head is not None:
        document["current-snapshot-id"] = head
    if schema_id is not None:
        document["current-schema-id"] = schema_id
    if locked_by is not None:
        document["locked-by"] = locked_by
    catalog._dataset_doc_ref(coll, name).set(document)


def _snapshot(catalog, snapshot_id, *, manifest="manifest.parquet", schema_id=None, expired=False):
    coll, name = IDENTIFIER.split(".", 1)
    document = {
        "snapshot-id": snapshot_id,
        "timestamp-ms": int(time.time() * 1000),
        "manifest": manifest,
        "schema-id": schema_id,
        "summary": {},
    }
    if expired:
        document[SNAPSHOT_EXPIRED_AT_KEY] = int(time.time() * 1000)
    catalog._snapshots_collection(coll, name).document(str(snapshot_id)).set(document)
    return snapshot_id


def _stored(catalog):
    coll, name = IDENTIFIER.split(".", 1)
    return catalog._dataset_doc_ref(coll, name).get().to_dict()


def _audit(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]


# --- the move ------------------------------------------------------------


def test_rollback_moves_the_head_to_the_named_snapshot():
    catalog = _catalog()
    _snapshot(catalog, 100)
    _snapshot(catalog, 200)
    _dataset(catalog, head=200)

    record = catalog.rollback_dataset(IDENTIFIER, 100, author="someone")

    assert record["moved"] is True
    assert record["snapshot-id"] == 100
    assert record["previous-snapshot-id"] == 200
    assert _stored(catalog)["current-snapshot-id"] == 100


def test_the_snapshot_rolled_off_is_left_alone_so_the_rollback_can_be_undone():
    """Nothing is deleted: rolling forward again is just another rollback."""
    catalog = _catalog()
    _snapshot(catalog, 100)
    _snapshot(catalog, 200)
    _dataset(catalog, head=200)

    catalog.rollback_dataset(IDENTIFIER, 100, author="someone")

    coll, name = IDENTIFIER.split(".", 1)
    assert catalog._snapshots_collection(coll, name).document("200").get().exists

    catalog.rollback_dataset(IDENTIFIER, 200, author="someone")
    assert _stored(catalog)["current-snapshot-id"] == 200


def test_the_schema_pointer_moves_with_the_head():
    """A head at yesterday's data must not advertise today's columns."""
    catalog = _catalog()
    _snapshot(catalog, 100, schema_id="schema-1")
    _snapshot(catalog, 200, schema_id="schema-2")
    _dataset(catalog, head=200, schema_id="schema-2")

    catalog.rollback_dataset(IDENTIFIER, 100, author="someone")

    assert _stored(catalog)["current-schema-id"] == "schema-1"


def test_a_snapshot_with_no_schema_recorded_leaves_the_schema_pointer_alone():
    catalog = _catalog()
    _snapshot(catalog, 100, schema_id=None)
    _snapshot(catalog, 200, schema_id="schema-2")
    _dataset(catalog, head=200, schema_id="schema-2")

    catalog.rollback_dataset(IDENTIFIER, 100, author="someone")

    assert _stored(catalog)["current-schema-id"] == "schema-2"


def test_rolling_back_to_where_the_head_already_is_reports_that_it_did_not_move():
    """Safe to retry, but it must not claim data moved when none did."""
    catalog = _catalog()
    _snapshot(catalog, 100)
    _dataset(catalog, head=100)

    record = catalog.rollback_dataset(IDENTIFIER, 100, author="someone")

    assert record["moved"] is False
    assert _stored(catalog)["current-snapshot-id"] == 100


# --- refusals ------------------------------------------------------------


def test_an_unknown_snapshot_is_refused_and_the_head_does_not_move():
    catalog = _catalog()
    _snapshot(catalog, 200)
    _dataset(catalog, head=200)

    with pytest.raises(SnapshotMissingError):
        catalog.rollback_dataset(IDENTIFIER, 999, author="someone")

    assert _stored(catalog)["current-snapshot-id"] == 200


def test_an_expired_snapshot_is_refused():
    """A tombstone keeps the id addressable; it does not keep the data alive."""
    catalog = _catalog()
    _snapshot(catalog, 100, expired=True)
    _snapshot(catalog, 200)
    _dataset(catalog, head=200)

    with pytest.raises(SnapshotMissingError):
        catalog.rollback_dataset(IDENTIFIER, 100, author="someone")

    assert _stored(catalog)["current-snapshot-id"] == 200


def test_a_snapshot_with_no_manifest_is_refused():
    """Pointing at one would present the dataset as empty, not as rolled back."""
    catalog = _catalog()
    _snapshot(catalog, 100, manifest=None)
    _snapshot(catalog, 200)
    _dataset(catalog, head=200)

    with pytest.raises(SnapshotMissingError):
        catalog.rollback_dataset(IDENTIFIER, 100, author="someone")

    assert _stored(catalog)["current-snapshot-id"] == 200


def test_a_locked_dataset_is_refused():
    catalog = _catalog()
    _snapshot(catalog, 100)
    _snapshot(catalog, 200)
    _dataset(catalog, head=200, locked_by="two-person-rule")

    with pytest.raises(DatasetLocked):
        catalog.rollback_dataset(IDENTIFIER, 100, author="someone")

    assert _stored(catalog)["current-snapshot-id"] == 200


def test_an_unknown_dataset_is_refused():
    catalog = _catalog()

    with pytest.raises(DatasetNotFound):
        catalog.rollback_dataset(IDENTIFIER, 100, author="someone")


def test_an_author_is_required():
    """As it is for every other statement that changes what a read returns."""
    catalog = _catalog()
    _snapshot(catalog, 100)
    _dataset(catalog, head=200)

    with pytest.raises(ValueError):
        catalog.rollback_dataset(IDENTIFIER, 100, author=None)
    with pytest.raises(ValueError):
        catalog.rollback_dataset(IDENTIFIER, 100, author="")


# --- audit ---------------------------------------------------------------


def test_the_rollback_is_audited_with_both_ends_of_the_move(capsys):
    """The id it moved OFF is the only record of how to undo this."""
    catalog = _catalog()
    _snapshot(catalog, 100)
    _snapshot(catalog, 200)
    _dataset(catalog, head=200)
    capsys.readouterr()

    catalog.rollback_dataset(IDENTIFIER, 100, author="someone")

    events = [e for e in _audit(capsys) if e.get("action") == "rollback_dataset"]
    assert len(events) == 1
    assert events[0]["detail"]["snapshot_id"] == 100
    assert events[0]["detail"]["previous_snapshot_id"] == 200
    assert events[0]["author"] == "someone"
