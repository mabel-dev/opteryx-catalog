"""The conditional pointer write is ONE transaction: check, compare and write together.

`save_dataset_metadata(expected_current_snapshot_id=...)` used to check the pointer in its
own transaction and then `set()` the document outside any transaction, so two writers built
on the same parent could both pass the check and the later write silently replaced the
earlier commit. The write is now staged on the same transaction as the read, so Firestore's
optimistic concurrency covers both.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import pytest

from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.exceptions import SnapshotRaceError

import test_rollback as fake
from test_rollback import IDENTIFIER, _catalog, _dataset, _stored


class _AtomicTransaction(fake._Transaction):
    """The rollback fake plus `set`. Staged writes land at commit by storing the document
    directly — never through the reference's own `set()`, which the tests forbid."""

    def set(self, ref, data, merge=False):
        self.writes.append((ref, ("set", data)))

    def _commit(self):
        for ref, data in self.writes:
            if type(data) is tuple:
                ref._data = dict(data[1])
                ref._exists = True
            else:
                ref.update(data)
        self.committed = True
        return []


def _atomic_catalog():
    catalog = _catalog()
    catalog.firestore_client.transaction = lambda: _AtomicTransaction()
    return catalog


def _metadata(head):
    meta = DatasetMetadata(dataset_identifier=IDENTIFIER, location="mem://ws/reports/monthly")
    meta.current_snapshot_id = head
    return meta


def _forbid_direct_set(catalog):
    """A conditional commit must not write the dataset document outside its transaction."""
    coll, name = IDENTIFIER.split(".", 1)

    def set_outside_transaction(data, merge=False):
        raise AssertionError("conditional commit wrote outside its transaction")

    catalog._dataset_doc_ref(coll, name).set = set_outside_transaction


def test_unmoved_pointer_commits_through_the_transaction():
    catalog = _atomic_catalog()
    _dataset(catalog, head=100)
    _forbid_direct_set(catalog)
    catalog.save_dataset_metadata(IDENTIFIER, _metadata(200), expected_current_snapshot_id=100)
    assert _stored(catalog)["current-snapshot-id"] == 200


def test_moved_pointer_writes_nothing():
    catalog = _atomic_catalog()
    _dataset(catalog, head=150)                       # another writer already moved it
    before = _stored(catalog)
    _forbid_direct_set(catalog)
    with pytest.raises(SnapshotRaceError, match="moved while this commit was being built"):
        catalog.save_dataset_metadata(IDENTIFIER, _metadata(200), expected_current_snapshot_id=100)
    assert _stored(catalog) == before


def test_unconditional_save_still_writes_directly():
    """No expectation (an annotation, a description) is not a commit: plain set, no read."""
    catalog = _atomic_catalog()
    _dataset(catalog, head=100)
    catalog.save_dataset_metadata(IDENTIFIER, _metadata(100))
    assert _stored(catalog)["current-snapshot-id"] == 100
