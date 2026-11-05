"""`VERSION AS OF PREVIOUS` and its retention survive expiration of the links between.

Found on opteryx.test.pypi_large (2026-11-05): an OPTIMIZE loop committed a
compaction every few minutes, expiration tombstoned the compactions and the
current user commit behind the head, and the walk to the previous version -
which refused every tombstone - could no longer reach it. The next run then
stopped protecting the previous version and retired it too.

A tombstoned document still records its parent, so the walk follows tombstones;
only the version it answers with has to be readable. The purge sweep keeps the
records that walk passes through, or the same failure returns a week later.
"""

import time

import pytest

from opteryx_catalog import OpteryxCatalog
from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.expiration import EXPIRED_SNAPSHOT_RETENTION_MS
from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.metadata import SNAPSHOT_EXPIRED_AT_KEY
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot
from opteryx_catalog.exceptions import SnapshotMissingError

_LONG_AGO = int(time.time() * 1000) - EXPIRED_SNAPSHOT_RETENTION_MS - 1


class _Doc:
    def __init__(self, coll, doc_id):
        self._coll = coll
        self.id = doc_id

    @property
    def exists(self):
        return self.id in self._coll.docs

    def get(self):
        return self

    def to_dict(self):
        return dict(self._coll.docs.get(self.id) or {})

    def delete(self):
        self._coll.docs.pop(self.id)
        self._coll.deleted.append(self.id)


class _Coll:
    def __init__(self, docs):
        self.docs = docs
        self.deleted = []

    def document(self, doc_id):
        return _Doc(self, str(doc_id))

    def stream(self):
        return [_Doc(self, doc_id) for doc_id in list(self.docs)]


class _Catalog:
    """The Firestore surface the walk, `snapshot()` and the purge sweep reach for."""

    _snapshot_from_dict = OpteryxCatalog._snapshot_from_dict

    def __init__(self, docs):
        self.io = None
        self.snaps = _Coll(docs)
        self._snapshot_cache = {}
        self.dataset = None

    def _dataset_doc_ref(self, collection, name):
        snaps = self.snaps

        class _Ref:
            def collection(self, sub):
                assert sub == "snapshots"
                return snaps

        return _Ref()

    def _snapshots_collection(self, collection, dataset_name):
        return self.snaps

    def load_dataset(self, identifier, load_history=False):
        return self.dataset


def _doc(sid, user, parent, expired=False):
    doc = {
        "snapshot-id": sid,
        "timestamp-ms": sid,
        "user-created": user,
        "parent-snapshot-id": parent,
        "operation-type": "add-files" if user else "compact",
        "manifest": f"manifest-{sid}.parquet",
    }
    if expired:
        doc[SNAPSHOT_EXPIRED_AT_KEY] = _LONG_AGO
    return doc


def _pypi_large(previous_expired=False, missing=()):
    """user 1 <- compact 2 <- user 3 (current version) <- compact 4 <- compact 5 <- compact 6 (head).

    2..5 are tombstoned, as expiration left pypi_large. 1 and 6 are live, so a
    history load holds them in memory; the rest exist only as documents.
    """
    docs = {
        "1": _doc(1, True, None, expired=previous_expired),
        "2": _doc(2, False, 1, expired=True),
        "3": _doc(3, True, 2, expired=True),
        "4": _doc(4, False, 3, expired=True),
        "5": _doc(5, False, 4, expired=True),
        "6": _doc(6, False, 5),
    }
    for doc_id in missing:
        del docs[doc_id]
    catalog = _Catalog(docs)

    meta = DatasetMetadata(
        dataset_identifier="test.pypi_large", location="mem://", schema=None, properties={}
    )
    meta.snapshots = [
        catalog._snapshot_from_dict(docs[k])
        for k in ("1", "6")
        if k in docs and not (k == "1" and previous_expired)
    ]
    meta.current_snapshot_id = 6
    meta.maintenance_policy = {"retained-snapshot-age-days": None}
    dataset = SimpleDataset(identifier="test.pypi_large", _metadata=meta)
    dataset.catalog = catalog
    catalog.dataset = dataset
    return dataset


def test_previous_is_found_through_tombstoned_links():
    dataset = _pypi_large()

    assert dataset.previous_user_snapshot().snapshot_id == 1
    _previous, lineage = dataset.previous_version_lineage()
    assert [s.snapshot_id for s in lineage] == [6, 5, 4, 3, 2, 1]


def test_an_expired_previous_version_is_reported_as_expired():
    # A long-lived worker cached snapshot 1 while it was live; that copy must
    # not answer for it now.
    dataset = _pypi_large(previous_expired=True)
    dataset.catalog._snapshot_cache[("test", "pypi_large", 1)] = Snapshot(
        snapshot_id=1, timestamp_ms=1, user_created=True, manifest_list="manifest-1.parquet"
    )

    with pytest.raises(SnapshotMissingError, match="snapshot 1"):
        dataset.previous_user_snapshot()


def test_a_purged_link_is_an_error_not_no_previous_version():
    # Before the fix a missing link on the head side answered None: "this is the
    # earliest version of its data", which is false.
    with pytest.raises(SnapshotMissingError, match="no record"):
        _pypi_large(missing=("4",)).previous_user_snapshot()


def test_snapshot_by_id_does_not_serve_a_cached_copy_of_an_expired_snapshot():
    dataset = _pypi_large(previous_expired=True)
    live_copy = Snapshot(
        snapshot_id=1, timestamp_ms=1, user_created=True, manifest_list="manifest-1.parquet"
    )
    dataset.catalog._snapshot_cache[("test", "pypi_large", 1)] = live_copy

    assert dataset.snapshot(1) is None


def test_expiration_keeps_the_previous_version_behind_a_tombstoned_chain():
    dataset = _pypi_large()
    # Off the lineage, so retention has something to retire and reaches
    # `_execute_expiration` rather than the manifest-only tidy-up.
    dataset.metadata.snapshots.append(
        Snapshot(snapshot_id=-100, timestamp_ms=0, user_created=False)
    )
    captured = {}

    def _capture(identifier, ds, snapshots_to_delete, snapshots_to_keep, **kwargs):
        captured["keep"] = {s.snapshot_id for s in snapshots_to_keep}
        captured["delete"] = {s.snapshot_id for s in snapshots_to_delete}
        return {}

    expirer = SnapshotExpiration(dataset.catalog, author="test")
    expirer._execute_expiration = _capture
    expirer.catalog.list_tags = lambda identifier: []
    expirer._expire_dataset("test.pypi_large", dry_run=False)

    assert captured["keep"] == {6, 1}
    assert captured["delete"] == {-100}


def test_purge_keeps_tombstones_the_previous_version_walk_passes_through():
    dataset = _pypi_large()
    dataset.catalog.snaps.docs["99"] = _doc(99, False, None, expired=True)  # off the lineage

    purged = SnapshotExpiration(dataset.catalog, author="test").purge_snapshot_tombstones(
        "test.pypi_large"
    )

    assert purged == ["99"]
    assert dataset.previous_user_snapshot().snapshot_id == 1
