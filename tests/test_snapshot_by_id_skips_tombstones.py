"""`Dataset.snapshot(id)` answers None for an expired snapshot.

An expired snapshot's document is tombstoned rather than deleted, so a
by-id Firestore fetch finds it. Every other reader of a snapshot by id refuses
a tombstone - the tag path, rollback, the history loader - and this one
returned it like any other, so `VERSION AS OF <expired id>` planned a scan
over files that may be gone and a provenance receipt's `source_exists` called
a retired version live.
"""

from types import SimpleNamespace

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.metadata import SNAPSHOT_EXPIRED_AT_KEY
from opteryx_catalog.catalog.metadata import DatasetMetadata


class _Doc:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return dict(self._data or {})


class _Catalog:
    def __init__(self, docs):
        self._docs = docs
        self._snapshot_cache = {}

    def _dataset_doc_ref(self, collection, name):
        docs = self._docs
        return SimpleNamespace(
            collection=lambda sub: SimpleNamespace(
                document=lambda sid: SimpleNamespace(get=lambda: _Doc(docs.get(int(sid))))
            )
        )


def _dataset(docs):
    meta = DatasetMetadata(dataset_identifier="col.t", location="mem://", schema=None, properties={})
    ds = SimpleDataset(identifier="col.t", _metadata=meta)
    ds.catalog = _Catalog(docs)
    return ds


def test_a_live_snapshot_is_fetched_by_id():
    ds = _dataset({7: {"snapshot-id": 7, "timestamp-ms": 7, "manifest": "m"}})
    assert ds.snapshot(7).snapshot_id == 7


def test_an_expired_snapshot_is_absent_by_id():
    ds = _dataset({7: {"snapshot-id": 7, "timestamp-ms": 7, "manifest": "m", SNAPSHOT_EXPIRED_AT_KEY: 1}})
    assert ds.snapshot(7) is None
    assert ds.catalog._snapshot_cache == {}, "a tombstone is not cached as an artifact"


def test_a_missing_snapshot_is_absent_by_id():
    assert _dataset({}).snapshot(7) is None
