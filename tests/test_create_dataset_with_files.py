"""`create_dataset_with_files` publishes a dataset and its first snapshot as ONE
transaction: the dataset either exists holding its data, or nothing was written.

`create_dataset` then `add_files` is two commits; a failure between them left an
empty, visible dataset whose name a retried CREATE TABLE ... AS then found taken.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import pytest

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.exceptions import DatasetAlreadyExists

from test_commit_pointer_atomic import _AtomicTransaction
from test_commit_pointer_atomic import _atomic_catalog

IDENTIFIER = "reports.created"


@pytest.fixture
def catalog(monkeypatch):
    catalog = _atomic_catalog()
    # Collaborators outside the transaction under test: naming, storage, notices.
    catalog.assert_name_free = lambda *a, **k: None
    catalog._creation_location = lambda identifier, location: "mem://ws/created"
    catalog.write_parquet_manifest = lambda snapshot_id, entries, location: "mem://manifest"
    catalog._announce_dataset_created = lambda *a, **k: None
    monkeypatch.setattr(SimpleDataset, "_after_commit", lambda self, author, snapshot: None)
    return catalog


def _doc(catalog):
    coll, name = IDENTIFIER.split(".")
    return catalog._dataset_doc_ref(coll, name).get()


def _snapshot_docs(catalog):
    coll, name = IDENTIFIER.split(".")
    return catalog._snapshots_collection(coll, name).stream()


def test_dataset_and_first_snapshot_appear_together(catalog):
    dataset = catalog.create_dataset_with_files(
        IDENTIFIER, None, entries=[], author="alice", read_sources=[]
    )

    stored = _doc(catalog)
    assert stored.exists
    head = stored.to_dict()["current-snapshot-id"]
    assert head == dataset.metadata.current_snapshot_id is not None
    assert [s.id for s in _snapshot_docs(catalog)] == [str(head)]


def test_a_refused_commit_publishes_nothing(catalog):
    class _Refused(_AtomicTransaction):
        def _commit(self):
            raise RuntimeError("commit refused")

    catalog.firestore_client.transaction = lambda: _Refused()
    with pytest.raises(RuntimeError, match="commit refused"):
        catalog.create_dataset_with_files(
            IDENTIFIER, None, entries=[], author="alice", read_sources=[]
        )

    assert not _doc(catalog).exists
    assert list(_snapshot_docs(catalog)) == []


def test_an_existing_name_is_refused_without_writing(catalog):
    coll, name = IDENTIFIER.split(".")
    catalog._dataset_doc_ref(coll, name).set({"name": name, "collection": coll})
    before = _doc(catalog).to_dict()

    with pytest.raises(DatasetAlreadyExists):
        catalog.create_dataset_with_files(
            IDENTIFIER, None, entries=[], author="alice", read_sources=[]
        )

    assert _doc(catalog).to_dict() == before
    assert list(_snapshot_docs(catalog)) == []
