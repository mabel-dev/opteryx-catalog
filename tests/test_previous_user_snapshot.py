"""`SimpleDataset.previous_user_snapshot()` - what `VERSION AS OF PREVIOUS` reads.

Somebody asking for the previous version is asking about their DATA, not about
the commit log. Compaction and statistics refresh commit snapshots that change
no rows at all, so the literal parent of the head is routinely the same data an
unqualified read returns - a time-travel read that silently answers with the
present. The walk here steps over those.

It follows `parent_snapshot_id` rather than ranking snapshots by recency,
because after a rollback the newest snapshots are not ancestors of the head:
they are the version that was undone.
"""

from __future__ import annotations

import pytest

from opteryx_catalog.catalog.dataset import MAX_ANCESTOR_WALK
from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot
from opteryx_catalog.exceptions import SnapshotAncestryTooDeep
from opteryx_catalog.exceptions import SnapshotMissingError


def _snap(sid, user_created, parent=None, op=None):
    return Snapshot(
        snapshot_id=sid,
        timestamp_ms=sid,
        sequence_number=sid,
        user_created=user_created,
        operation_type=op,
        parent_snapshot_id=parent,
        manifest_list=f"manifest-{sid}.parquet",
    )


class _FakeDoc:
    def __init__(self, data):
        self._data = data

    def to_dict(self):
        return dict(self._data)


class _FakeCatalog:
    """Serves snapshot documents by id, and counts the reads.

    The walk is meant to cost one read per hop that is not already in memory,
    so the read count is part of what these tests assert.
    """

    def __init__(self, snapshots):
        from opteryx_catalog.opteryx_catalog import _snapshot_to_document

        self._docs = {s.snapshot_id: _snapshot_to_document(s) for s in snapshots}
        self.reads = 0
        self._snapshot_cache = {}

    def _dataset_doc_ref(self, collection, dataset_name):
        catalog = self

        class _DatasetRef:
            def collection(self, name):
                assert name == "snapshots"
                return catalog._snapshots_collection(collection, dataset_name)

        return _DatasetRef()

    def _snapshots_collection(self, collection, dataset_name):
        catalog = self

        class _Coll:
            def document(self, doc_id):
                return _Ref(int(doc_id))

            def stream(self):
                return [_FakeDoc(d) for d in catalog._docs.values()]

        class _Ref:
            def __init__(self, snapshot_id):
                self._id = snapshot_id

            def get(self):
                catalog.reads += 1
                data = catalog._docs.get(self._id)

                class _Snapshot:
                    exists = data is not None

                    def to_dict(self):
                        return dict(data or {})

                return _Snapshot()

        return _Coll()

    def _snapshot_from_dict(self, sd):
        from opteryx_catalog.opteryx_catalog import OpteryxCatalog

        return OpteryxCatalog._snapshot_from_dict(object.__new__(OpteryxCatalog), sd)


def _dataset(in_memory, head_id, catalog=None):
    meta = DatasetMetadata(
        dataset_identifier="ops.test", location="mem://", schema=None, properties={}
    )
    meta.snapshots = list(in_memory)
    meta.current_snapshot_id = head_id
    ds = SimpleDataset(identifier="ops.test", _metadata=meta)
    ds.catalog = catalog
    return ds


# --- the walk ------------------------------------------------------------


def test_two_user_commits_in_a_row_step_back_one():
    history = [_snap(1, True), _snap(2, True, parent=1)]
    ds = _dataset(history, head_id=2, catalog=_FakeCatalog(history))

    assert ds.previous_user_snapshot().snapshot_id == 1


def test_a_compaction_between_two_user_commits_is_skipped():
    """The point of the feature: 2 is the previous VERSION, 3 is not a version."""
    history = [
        _snap(1, True),
        _snap(2, True, parent=1),
        _snap(3, False, parent=2, op="compact"),
        _snap(4, True, parent=3),
    ]
    ds = _dataset([history[-1]], head_id=4, catalog=_FakeCatalog(history))

    got = ds.previous_user_snapshot()

    assert got.snapshot_id == 2
    assert got.user_created is True


def test_a_run_of_maintenance_commits_on_top_of_the_head_is_skipped_too():
    """The head itself need not be a user commit.

    With compaction sitting on top, the version a read currently sees is user
    commit 2, so the PREVIOUS version is 1 - not 2, which is what a naive
    "walk to the first user snapshot behind the head" would answer.
    """
    history = [
        _snap(1, True),
        _snap(2, True, parent=1),
        _snap(3, False, parent=2, op="compact"),
        _snap(4, False, parent=3, op="statistics-refresh"),
    ]
    ds = _dataset([history[-1]], head_id=4, catalog=_FakeCatalog(history))

    assert ds.previous_user_snapshot().snapshot_id == 1


def test_the_first_user_commit_has_no_previous_version():
    history = [_snap(1, True), _snap(2, False, parent=1, op="compact")]
    ds = _dataset([history[-1]], head_id=2, catalog=_FakeCatalog(history))

    assert ds.previous_user_snapshot() is None


def test_a_dataset_whose_only_commits_are_maintenance_has_no_previous_version():
    history = [_snap(1, False, op="compact"), _snap(2, False, parent=1, op="compact")]
    ds = _dataset([history[-1]], head_id=2, catalog=_FakeCatalog(history))

    assert ds.previous_user_snapshot() is None


def test_a_dataset_with_no_commits_has_no_previous_version():
    ds = _dataset([], head_id=None, catalog=_FakeCatalog([]))

    assert ds.previous_user_snapshot() is None


# --- rollback ------------------------------------------------------------


def test_snapshots_ahead_of_a_rolled_back_head_are_not_the_previous_version():
    """They are the version that was undone, and are not ancestors of the head.

    Ranking by recency would answer with 3 - a version NEWER than what the
    dataset currently returns, offered as its history.
    """
    history = [
        _snap(1, True),
        _snap(2, True, parent=1),
        _snap(3, True, parent=2),
    ]
    ds = _dataset([history[1]], head_id=2, catalog=_FakeCatalog(history))

    assert ds.previous_user_snapshot().snapshot_id == 1


def test_last_user_snapshot_ignores_snapshots_ahead_of_a_rolled_back_head():
    """"When did a human last change this?" must not name an undone commit."""
    history = [
        _snap(1, True),
        _snap(2, False, parent=1, op="compact"),
        _snap(3, True, parent=2),
    ]
    # Head rolled back to the compaction, so the visible data is user commit 1.
    ds = _dataset([history[1]], head_id=2, catalog=_FakeCatalog(history))

    assert ds.last_user_snapshot().snapshot_id == 1


# --- cost and failure ----------------------------------------------------


def test_the_walk_reads_one_document_per_hop_it_does_not_already_hold():
    history = [
        _snap(1, True),
        _snap(2, False, parent=1, op="compact"),
        _snap(3, True, parent=2),
    ]
    catalog = _FakeCatalog(history)
    ds = _dataset([history[-1]], head_id=3, catalog=catalog)

    assert ds.previous_user_snapshot().snapshot_id == 1
    # 3 is in memory; 2 and 1 are read. Nothing streams the whole history.
    assert catalog.reads == 2


def test_an_expired_ancestor_is_reported_rather_than_called_absent():
    """There IS a previous version - it just cannot be read any more."""
    history = [_snap(2, True, parent=1)]
    ds = _dataset(history, head_id=2, catalog=_FakeCatalog(history))

    with pytest.raises(SnapshotMissingError):
        ds.previous_user_snapshot()


def test_an_unbounded_chain_of_maintenance_commits_is_refused_not_walked():
    chain = [_snap(1, True)]
    for sid in range(2, MAX_ANCESTOR_WALK + 5):
        chain.append(_snap(sid, False, parent=sid - 1, op="compact"))
    ds = _dataset([chain[-1]], head_id=chain[-1].snapshot_id, catalog=_FakeCatalog(chain))

    with pytest.raises(SnapshotAncestryTooDeep):
        ds.previous_user_snapshot()


# --- visible_history -----------------------------------------------------
#
# The shared rule behind `last_user_snapshot`, expiration's protected-commit
# choice, and the engine's point-in-time reads. All three must agree on which
# snapshots a rolled-back dataset still has, or a snapshot could be invisible
# to one and visible to another.


def test_visible_history_is_the_heads_line_of_descent():
    from opteryx_catalog.catalog.dataset import visible_history

    history = [_snap(1, True), _snap(2, True, parent=1), _snap(3, True, parent=2)]

    seen = visible_history(history[1], history)

    assert {s.snapshot_id for s in seen} == {1, 2}


def test_visible_history_stops_at_a_parent_that_is_not_there():
    """An expired ancestor ends the walk; it does not claim nothing came before."""
    from opteryx_catalog.catalog.dataset import visible_history

    history = [_snap(2, True, parent=1), _snap(3, True, parent=2)]

    seen = visible_history(history[-1], history)

    assert {s.snapshot_id for s in seen} == {2, 3}


def test_visible_history_of_nothing_is_nothing():
    from opteryx_catalog.catalog.dataset import visible_history

    assert visible_history(None, []) == []


def test_a_rolled_off_snapshot_sharing_a_sequence_number_is_still_excluded():
    """After a rollback the next commit's sequence number is allocated from what
    its writer held in memory, so a rolled-off snapshot can share one with a
    live snapshot. Ordering cannot separate those two; ancestry can."""
    from opteryx_catalog.catalog.dataset import visible_history

    rolled_off = _snap(20, True, parent=1)
    rolled_off.sequence_number = 2
    replacement = _snap(30, True, parent=1)
    replacement.sequence_number = 2
    history = [_snap(1, True), rolled_off, replacement]

    seen = visible_history(replacement, history)

    assert {s.snapshot_id for s in seen} == {1, 30}


def test_a_history_with_no_parent_links_falls_back_to_ordering():
    """Snapshots written before `parent_snapshot_id` existed have no chain to
    walk. Ancestry would collapse their history to the head alone - and such a
    dataset cannot have been rolled back, because rollback is newer than parent
    links are."""
    from opteryx_catalog.catalog.dataset import visible_history

    history = [_snap(1, True), _snap(2, True), _snap(3, True)]

    seen = visible_history(history[1], history)

    assert {s.snapshot_id for s in seen} == {1, 2}
