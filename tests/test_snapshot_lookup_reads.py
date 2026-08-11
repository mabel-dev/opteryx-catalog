"""
`SimpleDataset.snapshot(snapshot_id)` and where it looks for the answer.

A snapshot id addresses write-once content, so a copy already held is the
answer — this is the same rule that lets `OpteryxCatalog` keep `_snapshot_cache`
at all. The lookup used to go to Firestore first and treat memory as the failure
path, which meant it re-read a document `load_dataset()` had just fetched.

`OpteryxConnector.get_dataset_metadata()` asks for the same id twice (once via
`scan()`, once via `manifest_sketch_vectors()`), so planning a single-table query
cost three document reads of one document: one in `load_dataset`, two here. These
tests pin the order the lookup consults its sources, so that cannot come back.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot


def _snap(sid):
    return Snapshot(
        snapshot_id=sid,
        timestamp_ms=sid,
        sequence_number=1,
        user_created=True,
        manifest_list=f"manifest-{sid}.parquet",
    )


class _CountingDoc:
    """A snapshot document reference that records every read of itself."""

    def __init__(self, catalog, data):
        self._catalog = catalog
        self._data = data

    def get(self):
        self._catalog.document_reads += 1
        return self

    @property
    def exists(self):
        return self._data is not None

    def to_dict(self):
        return self._data


class _CountingCollection:
    def __init__(self, catalog, docs):
        self._catalog = catalog
        self._docs = docs

    def collection(self, _name):
        return self

    def document(self, doc_id):
        return _CountingDoc(self._catalog, self._docs.get(str(doc_id)))


class _CountingCatalog:
    """Serves snapshot documents and counts how many are actually read."""

    def __init__(self, remote_snapshots=()):
        from opteryx_catalog.opteryx_catalog import _snapshot_to_document

        self._docs = {str(s.snapshot_id): _snapshot_to_document(s) for s in remote_snapshots}
        self._snapshot_cache = {}
        self.document_reads = 0

    def _dataset_doc_ref(self, collection, dataset_name):
        return _CountingCollection(self, self._docs)


def _dataset(in_memory, catalog):
    meta = DatasetMetadata(
        dataset_identifier="ops.test", location="mem://", schema=None, properties={}
    )
    meta.snapshots = list(in_memory)
    meta.current_snapshot_id = in_memory[-1].snapshot_id if in_memory else None
    ds = SimpleDataset(identifier="ops.test", _metadata=meta)
    ds.catalog = catalog
    return ds


def test_an_id_already_in_memory_is_not_fetched():
    wanted = _snap(1785532538201)
    catalog = _CountingCatalog(remote_snapshots=[wanted])
    ds = _dataset([wanted], catalog)

    got = ds.snapshot(1785532538201)

    assert got.snapshot_id == 1785532538201
    assert catalog.document_reads == 0, "the snapshot was already in metadata.snapshots"


def test_the_same_id_asked_for_twice_costs_one_read_at_most():
    # This is the get_dataset_metadata() shape: scan() then
    # manifest_sketch_vectors(), both naming the same snapshot id.
    wanted = _snap(1785532538201)
    catalog = _CountingCatalog(remote_snapshots=[wanted])
    ds = _dataset([wanted], catalog)

    first = ds.snapshot(1785532538201)
    second = ds.snapshot(1785532538201)

    assert first.snapshot_id == second.snapshot_id
    assert catalog.document_reads == 0


def test_an_id_in_the_catalog_cache_is_not_fetched():
    wanted = _snap(99)
    catalog = _CountingCatalog(remote_snapshots=[wanted])
    catalog._snapshot_cache[("ops", "test", 99)] = wanted
    # Not in memory: load_history=False keeps only the current snapshot there.
    ds = _dataset([_snap(100)], catalog)

    got = ds.snapshot(99)

    assert got.snapshot_id == 99
    assert catalog.document_reads == 0, "the catalog had already fetched this id"


def test_an_unheld_id_is_fetched_once_and_then_cached():
    historical = _snap(42)
    catalog = _CountingCatalog(remote_snapshots=[historical])
    ds = _dataset([_snap(100)], catalog)

    first = ds.snapshot(42)
    assert first.snapshot_id == 42
    assert catalog.document_reads == 1, "nothing held it, so it had to be read"

    # Seeding the cache is what stops the next Dataset built from this catalog
    # paying for the same document again.
    assert catalog._snapshot_cache[("ops", "test", 42)].snapshot_id == 42

    second = ds.snapshot(42)
    assert second.snapshot_id == 42
    assert catalog.document_reads == 1, "the second ask came from the cache"


def test_an_id_that_exists_nowhere_is_none():
    catalog = _CountingCatalog(remote_snapshots=[])
    ds = _dataset([_snap(100)], catalog)

    assert ds.snapshot(7) is None


def test_no_catalog_still_answers_from_memory():
    wanted = _snap(5)
    ds = _dataset([_snap(4), wanted], catalog=None)

    assert ds.snapshot(5).snapshot_id == 5
    assert ds.snapshot(6) is None


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
