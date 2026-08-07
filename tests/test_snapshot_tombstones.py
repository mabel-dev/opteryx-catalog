"""Expired snapshots are tombstoned, kept for the record window, then purged -
never hard-deleted on the retention decision.

The incident this guards against: expiration hard-deleted snapshot documents
the moment they crossed retention, which destroyed the manifest path - the
thread leading back to the data files while GCS soft-delete could still
produce them. Tombstoning keeps that thread for the record window; the purge
sweep is the only code allowed to delete a snapshot document, and only one
that has carried an `expired-at-ms` stamp for the full window.
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog.catalog.expiration import EXPIRED_SNAPSHOT_RETENTION_MS
from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.metadata import SNAPSHOT_EXPIRED_AT_KEY
from opteryx_catalog.catalog.metadata import snapshot_is_tombstoned

DAY_MS = 24 * 60 * 60 * 1000


# --- the shared predicate ---------------------------------------------------


def test_tombstone_predicate():
    assert not snapshot_is_tombstoned({})
    assert not snapshot_is_tombstoned({"snapshot-id": 1})
    assert snapshot_is_tombstoned({SNAPSHOT_EXPIRED_AT_KEY: 123})
    # A zero timestamp is still a stamp - epoch is not "not expired".
    assert snapshot_is_tombstoned({SNAPSHOT_EXPIRED_AT_KEY: 0})


def test_record_window_matches_the_bucket_soft_delete():
    """7-day tombstones against the bucket's 7-day soft delete, deliberately.

    The tombstone is the restore pointer for as long as the files can be
    produced. Files outlive the record by about a day (the orphan quarantine
    delays their entry into soft-delete) - an accepted edge, because a
    last-day restore can still find them via the storage listing of
    soft-deleted manifests, whose filenames carry the snapshot id. What this
    pin protects against is the window shrinking BELOW the file-recovery
    period, which would orphan restorable files from their record with no
    fallback for most of the window.
    """
    assert EXPIRED_SNAPSHOT_RETENTION_MS == 7 * DAY_MS


# --- purge ------------------------------------------------------------------


class _FakeSnapshotDoc:
    def __init__(self, coll, doc_id):
        self._coll = coll
        self.id = doc_id

    def to_dict(self):
        return self._coll.docs.get(self.id)

    def update(self, payload):
        self._coll.docs[self.id] = {**self._coll.docs.get(self.id, {}), **payload}

    def delete(self):
        self._coll.docs.pop(self.id, None)
        self._coll.deleted.append(self.id)


class _FakeSnapshotColl:
    def __init__(self, docs):
        self.docs = dict(docs)
        self.deleted = []

    def document(self, name):
        return _FakeSnapshotDoc(self, name)

    def stream(self):
        return [_FakeSnapshotDoc(self, doc_id) for doc_id in list(self.docs)]


class _FakeCatalog:
    def __init__(self, snapshot_docs):
        self.io = None
        self.snaps = _FakeSnapshotColl(snapshot_docs)

    def _snapshots_collection(self, collection, dataset_name):
        return self.snaps

    def load_dataset(self, identifier, load_history=False):
        return None  # purge tests never get as far as loading


def _expirer(catalog):
    class _NoQuarantine:  # never reached in these tests
        pass

    return SnapshotExpiration(catalog, author="test", quarantine=_NoQuarantine())


def test_purge_removes_only_tombstones_past_the_window():
    now = int(time.time() * 1000)
    catalog = _FakeCatalog(
        {
            "old": {SNAPSHOT_EXPIRED_AT_KEY: now - EXPIRED_SNAPSHOT_RETENTION_MS - 1},
            "recent": {SNAPSHOT_EXPIRED_AT_KEY: now - DAY_MS},
            "live": {"snapshot-id": 3, "manifest": "gs://b/m.parquet"},
        }
    )

    purged = _expirer(catalog).purge_snapshot_tombstones("github.events")

    assert purged == ["old"]
    assert catalog.snaps.deleted == ["old"]
    assert "recent" in catalog.snaps.docs
    assert "live" in catalog.snaps.docs


def test_purge_never_touches_a_document_without_the_stamp():
    """The stamp is the proof the record-window wait ran. No stamp, no
    deletion - even for a snapshot document that is itself ancient."""
    catalog = _FakeCatalog(
        {
            "ancient-live": {"snapshot-id": 1, "timestamp-ms": 12345},
        }
    )

    assert _expirer(catalog).purge_snapshot_tombstones("github.events") == []
    assert "ancient-live" in catalog.snaps.docs


def test_purge_skips_a_malformed_stamp():
    """An unreadable stamp cannot prove the window elapsed, so the safe
    direction is to keep the record."""
    catalog = _FakeCatalog(
        {
            "bad-stamp": {SNAPSHOT_EXPIRED_AT_KEY: "yesterday-ish"},
        }
    )

    assert _expirer(catalog).purge_snapshot_tombstones("github.events") == []
    assert "bad-stamp" in catalog.snaps.docs


def test_purge_survives_a_catalog_that_cannot_stream():
    """Housekeeping must not break expiration for catalogs (or fakes) whose
    snapshot collections cannot be streamed."""

    class _NoStream:
        def document(self, name):
            raise AssertionError("should not be reached")

    class _Catalog:
        io = None

        def _snapshots_collection(self, collection, dataset_name):
            return _NoStream()

    assert _expirer(_Catalog()).purge_snapshot_tombstones("github.events") == []


def test_dry_run_does_not_purge():
    now = int(time.time() * 1000)
    catalog = _FakeCatalog(
        {"old": {SNAPSHOT_EXPIRED_AT_KEY: now - EXPIRED_SNAPSHOT_RETENTION_MS - 1}}
    )

    # load_dataset returns None so the run itself is a no-op; the point is
    # what the wrapper does before it.
    result = _expirer(catalog).expire_dataset("github.events", dry_run=True)

    assert result is None
    assert catalog.snaps.deleted == []
    assert "old" in catalog.snaps.docs


# --- the loader filter ------------------------------------------------------
#
# This is the piece everything downstream leans on: expiration's retention
# maths, the orphan-detection size threshold, and "which manifests are
# referenced" all read `metadata.snapshots`, so tombstones must never appear
# there. Tested against the REAL `OpteryxCatalog._build_dataset`, not a fake -
# a fake would just restate the assumption.


def _catalog_shell(snapshot_docs):
    """An OpteryxCatalog without __init__ (which would talk to Firestore),
    carrying just enough state for `_build_dataset(load_history=True)`."""
    from opteryx_catalog import OpteryxCatalog

    class _Doc:
        def __init__(self, doc_id, payload):
            self.id = doc_id
            self._payload = payload

        def to_dict(self):
            return self._payload

    class _Coll:
        def __init__(self, docs):
            self._docs = docs

        def stream(self):
            return [_Doc(k, v) for k, v in self._docs.items()]

    class _DocRef:
        def __init__(self, snaps):
            self._snaps = snaps

        def collection(self, name):
            return _Coll(self._snaps if name == "snapshots" else {})

    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "test"
    catalog.gcs_bucket = "bucket"
    catalog.io = None
    catalog._snapshots_collection = lambda c, d: _Coll(snapshot_docs)
    catalog._dataset_doc_ref = lambda c, d: _DocRef(snapshot_docs)
    return catalog


def test_loader_hides_tombstoned_snapshots_from_history():
    now = int(time.time() * 1000)
    catalog = _catalog_shell(
        {
            "100": {"snapshot-id": 100, "timestamp-ms": 100, "manifest": "gs://b/m-100.parquet"},
            "200": {
                "snapshot-id": 200,
                "timestamp-ms": 200,
                "manifest": "gs://b/m-200.parquet",
                SNAPSHOT_EXPIRED_AT_KEY: now,
                "expired-by": "gc",
            },
            "300": {"snapshot-id": 300, "timestamp-ms": 300, "manifest": "gs://b/m-300.parquet"},
        }
    )

    class _DatasetDoc:
        def to_dict(self):
            return {"location": "gs://bucket/test/c/d"}

    dataset = catalog._build_dataset("c.d", "c", "d", _DatasetDoc(), load_history=True)

    ids = [s.snapshot_id for s in dataset.metadata.snapshots]
    assert ids == [100, 300], "tombstoned snapshot leaked into history"
    # The threshold and current-snapshot logic both derive from this list, so
    # the tombstone must not be the "current" snapshot either.
    assert dataset.metadata.current_snapshot_id == 300


def test_execute_purges_even_when_nothing_else_expires():
    now = int(time.time() * 1000)
    catalog = _FakeCatalog(
        {"old": {SNAPSHOT_EXPIRED_AT_KEY: now - EXPIRED_SNAPSHOT_RETENTION_MS - 1}}
    )

    result = _expirer(catalog).expire_dataset("github.events", dry_run=False)

    # No summary (nothing to expire), but the stale record is gone.
    assert result is None
    assert catalog.snaps.deleted == ["old"]
