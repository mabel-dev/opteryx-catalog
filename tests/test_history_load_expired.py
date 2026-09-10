"""`load_dataset(load_history=True, include_expired=True)` - the tombstones,
kept apart from the history.

Expiration retires a snapshot by tombstoning its document, and every normal
read drops those on the floor. `SHOW ALL SNAPSHOTS FOR` needs them, so the
loader can be asked for them - but into `metadata.expired_snapshots`, never
into `metadata.snapshots`. What that separation protects is listed on the
loader: expiration's own retention maths, the orphan-detection size threshold,
the ancestry walks, `previous` resolution and the head-pointer fallback all
read `snapshots` and all mean LIVE.
"""

from __future__ import annotations

from opteryx_catalog.catalog.metadata import SNAPSHOT_EXPIRED_AT_KEY
from opteryx_catalog.opteryx_catalog import OpteryxCatalog


# The same Firestore stand-ins test_history_load_head.py uses, restated rather
# than imported: these modules are collected as top-level test files, not as a
# package, so a cross-module import resolves for a single-file run and not for
# a full one.
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


def _snapshot_document(snapshot_id, sequence_number, expired_at=None):
    document = {
        "snapshot-id": snapshot_id,
        "timestamp-ms": snapshot_id,
        "sequence-number": sequence_number,
        "manifest": f"manifest-{snapshot_id}.parquet",
        "user-created": True,
        "summary": {},
    }
    if expired_at is not None:
        document[SNAPSHOT_EXPIRED_AT_KEY] = expired_at
    return document


def _catalog(head, live_ids, expired_ids):
    dataset_ref = _DocRef("tbl", data={"current-snapshot-id": head} if head else {})
    snapshots = _Collection()
    for position, snapshot_id in enumerate(live_ids, start=1):
        snapshots.document(str(snapshot_id)).set(_snapshot_document(snapshot_id, position))
    for position, snapshot_id in enumerate(expired_ids, start=1):
        snapshots.document(str(snapshot_id)).set(
            _snapshot_document(snapshot_id, position, expired_at=900)
        )

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


def test_tombstones_are_dropped_unless_they_are_asked_for():
    catalog = _catalog(head=200, live_ids=[100, 200], expired_ids=[50])

    dataset = catalog.load_dataset("coll.tbl", load_history=True)

    assert {s.snapshot_id for s in dataset.metadata.snapshots} == {100, 200}
    assert dataset.metadata.expired_snapshots == []
    assert list(dataset.expired_snapshots()) == []


def test_include_expired_collects_them_separately():
    catalog = _catalog(head=200, live_ids=[100, 200], expired_ids=[50])

    dataset = catalog.load_dataset("coll.tbl", load_history=True, include_expired=True)

    # The history is unchanged: a tombstone in `snapshots` is what every
    # consumer of that field would get wrong.
    assert {s.snapshot_id for s in dataset.metadata.snapshots} == {100, 200}
    assert [s.snapshot_id for s in dataset.expired_snapshots()] == [50]


def test_the_tombstone_carries_when_it_expired():
    catalog = _catalog(head=200, live_ids=[200], expired_ids=[50])

    dataset = catalog.load_dataset("coll.tbl", load_history=True, include_expired=True)

    assert dataset.expired_snapshots()[0].expired_at_ms == 900
    # ...and a live snapshot carries no stamp at all, on the same load.
    assert dataset.snapshots()[0].expired_at_ms is None


def test_a_tombstone_never_becomes_the_head():
    """The pointer fallback picks the newest LIVE snapshot. A tombstone in that
    set would make an unreadable version the current one for a dataset old
    enough to have no pointer recorded."""
    catalog = _catalog(head=None, live_ids=[100], expired_ids=[500])

    dataset = catalog.load_dataset("coll.tbl", load_history=True, include_expired=True)

    assert dataset.metadata.current_snapshot_id == 100


def test_reading_an_expired_snapshot_by_id_is_still_refused():
    """Listing one is not reading one: `snapshot(id)` refuses a tombstone, which
    is what makes `VERSION AS OF <expired id>` resolve to nothing."""
    catalog = _catalog(head=200, live_ids=[200], expired_ids=[50])

    dataset = catalog.load_dataset("coll.tbl", load_history=True, include_expired=True)

    assert dataset.snapshot(50) is None
