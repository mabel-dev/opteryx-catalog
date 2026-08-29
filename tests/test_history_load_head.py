"""`load_dataset(load_history=True)` must take the head from the POINTER.

The snapshots subcollection streams in Firestore document-id order, which is
lexicographic on the id string and only accidentally chronological. Taking the
tail of that stream as the head was already fragile; with rollback it is wrong
by design, because the head is deliberately not the newest snapshot. Expiration
loads history on every run, so a wrong answer here would roll a rolled-back
dataset forward again without anybody asking.
"""

from __future__ import annotations

from opteryx_catalog.opteryx_catalog import OpteryxCatalog


class _Doc:
    def __init__(self, data=None, exists=True):
        self.exists = exists
        self._data = data or {}

    def to_dict(self):
        return dict(self._data)


class _DocRef:
    def __init__(self, doc_id, data=None, exists=True):
        self.id = doc_id
        self._doc = _Doc(data, exists)
        self._subcollections = {}

    def get(self):
        return self._doc

    def set(self, data):
        self._doc = _Doc(data, exists=True)

    def collection(self, name):
        if name not in self._subcollections:
            self._subcollections[name] = _Collection()
        return self._subcollections[name]


class _Collection:
    def __init__(self):
        self._docs = {}

    def document(self, doc_id):
        if doc_id not in self._docs:
            self._docs[doc_id] = _DocRef(doc_id, exists=False)
        return self._docs[doc_id]

    def stream(self):
        return [r.get() for r in self._docs.values() if r.get().exists]


def _snapshot_document(snapshot_id, sequence_number):
    return {
        "snapshot-id": snapshot_id,
        "timestamp-ms": snapshot_id,
        "sequence-number": sequence_number,
        "manifest": f"manifest-{snapshot_id}.parquet",
        "user-created": True,
        "summary": {},
    }


def _catalog(head, snapshot_ids):
    """A dataset whose snapshots stream in an order that is NOT chronological."""
    dataset_ref = _DocRef("tbl", data={"current-snapshot-id": head} if head else {})
    snapshots = _Collection()
    for position, snapshot_id in enumerate(snapshot_ids, start=1):
        snapshots.document(str(snapshot_id)).set(_snapshot_document(snapshot_id, position))

    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog.gcs_bucket = "bucket"
    catalog.io = None
    catalog._snapshot_cache = {}
    catalog._schema_cache = {}
    catalog._dataset_doc_ref = lambda c, n: dataset_ref
    catalog._snapshots_collection = lambda c, n: snapshots
    catalog._load_tags = lambda c, n: ({}, True)
    return catalog


def test_the_head_comes_from_the_pointer_not_the_tail_of_the_stream():
    # 200 was committed after 100, but the head has been rolled back to 100.
    catalog = _catalog(head=100, snapshot_ids=[100, 200])

    dataset = catalog.load_dataset("coll.tbl", load_history=True)

    assert dataset.metadata.current_snapshot_id == 100
    assert dataset.metadata.current_snapshot().snapshot_id == 100
    # ...and the snapshot it was rolled off is still in the history.
    assert {s.snapshot_id for s in dataset.metadata.snapshots} == {100, 200}


def test_a_dataset_with_no_pointer_falls_back_to_the_newest_by_sequence():
    """Older than the pointer itself; the fallback must still be chronological."""
    catalog = _catalog(head=None, snapshot_ids=[200, 100])

    dataset = catalog.load_dataset("coll.tbl", load_history=True)

    # Sequence numbers were assigned in the order given, so 100 is the newest.
    assert dataset.metadata.current_snapshot_id == 100


def test_a_pointer_naming_an_expired_snapshot_falls_back_rather_than_loading_empty():
    catalog = _catalog(head=999, snapshot_ids=[100, 200])

    dataset = catalog.load_dataset("coll.tbl", load_history=True)

    assert dataset.metadata.current_snapshot_id == 200
