"""
`SimpleDataset.last_user_snapshot()` / `snapshot(user_only=True)`.

The current snapshot is frequently NOT one a user created: compaction,
expiration and statistics refresh all commit their own. Showing that in a UI
as "your last commit" prompts the reasonable question of why there are
commits the user never made — this answers "when did a HUMAN last change
this data?" instead.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

import pytest

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot


def _snap(sid, seq, user_created, op=None):
    return Snapshot(
        snapshot_id=sid,
        timestamp_ms=sid,
        sequence_number=seq,
        user_created=user_created,
        operation_type=op,
        manifest_list=f"manifest-{sid}.parquet",
    )


class _FakeDoc:
    def __init__(self, data):
        self._data = data

    def to_dict(self):
        return self._data


class _FakeSnapsCollection:
    def __init__(self, docs):
        self._docs = docs

    def stream(self):
        return iter(self._docs)


class _FakeCatalog:
    """Serves snapshot history the way Firestore would — deliberately in a
    scrambled order, so any reliance on iteration order shows up."""

    def __init__(self, snapshots):
        from opteryx_catalog.opteryx_catalog import _snapshot_to_document

        self._docs = [_FakeDoc(_snapshot_to_document(s)) for s in snapshots]
        self.streamed = 0

    def _snapshots_collection(self, collection, dataset_name):
        self.streamed += 1
        return _FakeSnapsCollection(self._docs)

    def _snapshot_from_dict(self, sd):
        from opteryx_catalog.opteryx_catalog import OpteryxCatalog

        return OpteryxCatalog._snapshot_from_dict(object.__new__(OpteryxCatalog), sd)


def _dataset(snapshots, current_id, catalog=None):
    meta = DatasetMetadata(
        dataset_identifier="ops.test", location="mem://", schema=None, properties={}
    )
    meta.snapshots = list(snapshots)
    meta.current_snapshot_id = current_id
    ds = SimpleDataset(identifier="ops.test", _metadata=meta)
    ds.catalog = catalog
    return ds


def test_returns_current_when_it_is_user_created_without_reading_history():
    history = [_snap(1, 1, True), _snap(2, 2, True)]
    catalog = _FakeCatalog(history)
    ds = _dataset(history, current_id=2, catalog=catalog)

    got = ds.last_user_snapshot()

    assert got.snapshot_id == 2
    assert catalog.streamed == 0, "should not read history when current is a user commit"


def test_skips_system_snapshots_on_top():
    # A statistics refresh and a compaction landed after the user's write.
    history = [
        _snap(1, 1, True),
        _snap(2, 2, True),
        _snap(3, 3, False, op="compact"),
        _snap(4, 4, False, op="statistics-refresh"),
    ]
    catalog = _FakeCatalog(history)
    # In-memory holds only the current snapshot: load_history=False default.
    ds = _dataset([history[-1]], current_id=4, catalog=catalog)

    got = ds.last_user_snapshot()

    assert got.snapshot_id == 2, "should be the last USER commit, not the refresh"
    assert got.user_created is True


def test_orders_by_sequence_not_iteration_order():
    history = [_snap(30, 3, True), _snap(10, 1, True), _snap(20, 2, True)]
    catalog = _FakeCatalog(history)  # streamed in the scrambled order above
    ds = _dataset([_snap(40, 4, False)], current_id=40, catalog=catalog)

    assert ds.last_user_snapshot().snapshot_id == 30


def test_returns_none_when_no_user_snapshot_exists():
    history = [_snap(1, 1, False, op="compact")]
    ds = _dataset(history, current_id=1, catalog=_FakeCatalog(history))

    assert ds.last_user_snapshot() is None


def test_missing_user_created_is_not_assumed_to_be_a_user_commit():
    # None means "not known to be a user commit" — guessing True would
    # reintroduce the confusion this exists to remove.
    history = [_snap(1, 1, None), _snap(2, 2, False)]
    ds = _dataset(history, current_id=2, catalog=_FakeCatalog(history))

    assert ds.last_user_snapshot() is None


def test_works_without_a_catalog_attached():
    history = [_snap(1, 1, True), _snap(2, 2, False)]
    ds = _dataset(history, current_id=2, catalog=None)

    assert ds.last_user_snapshot().snapshot_id == 1


# ── the param on the existing getter ────────────────────────────────────────


def test_snapshot_user_only_defaults_off():
    history = [_snap(1, 1, True), _snap(2, 2, False, op="statistics-refresh")]
    ds = _dataset(history, current_id=2, catalog=_FakeCatalog(history))

    # Default is unchanged: the CURRENT snapshot, whoever made it.
    assert ds.snapshot().snapshot_id == 2
    assert ds.snapshot(user_only=True).snapshot_id == 1


def test_snapshot_user_only_with_explicit_id_is_refused():
    history = [_snap(1, 1, True)]
    ds = _dataset(history, current_id=1, catalog=None)

    with pytest.raises(ValueError):
        ds.snapshot(snapshot_id=1, user_only=True)


def test_explicit_snapshot_id_lookup_still_works():
    history = [_snap(1, 1, True), _snap(2, 2, False)]
    ds = _dataset(history, current_id=2, catalog=None)

    assert ds.snapshot(snapshot_id=1).snapshot_id == 1


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])


# ── bounded lookback ────────────────────────────────────────────────────────
#
# Expiration bounds its search deliberately: a dataset written once by a human
# and maintained automatically forever would otherwise pin its very first
# snapshot — and every data file that snapshot references — in storage
# permanently. A user commit buried deeper than the window is allowed to
# expire, leaving the current snapshot (always retained) as what the UI shows.

from opteryx_catalog.catalog.dataset import select_last_user_snapshot


def test_select_finds_user_commit_inside_the_window():
    snaps = [_snap(1, 1, True)] + [_snap(i, i, False) for i in range(2, 6)]
    assert select_last_user_snapshot(snaps, lookback=10).snapshot_id == 1


def test_select_ignores_user_commit_outside_the_window():
    # user commit at seq 1, then 12 maintenance commits on top
    snaps = [_snap(1, 1, True)] + [_snap(i, i, False) for i in range(2, 14)]
    assert select_last_user_snapshot(snaps, lookback=10) is None
    # ...but an unbounded search still finds it
    assert select_last_user_snapshot(snaps, lookback=None).snapshot_id == 1


def test_select_window_boundary_is_inclusive():
    # user commit exactly 10 back (the last slot in the window)
    snaps = [_snap(1, 1, True)] + [_snap(i, i, False) for i in range(2, 11)]
    assert len(snaps) == 10
    assert select_last_user_snapshot(snaps, lookback=10).snapshot_id == 1
    # one more maintenance commit pushes it out
    snaps.append(_snap(11, 11, False))
    assert select_last_user_snapshot(snaps, lookback=10) is None


def test_select_picks_the_most_recent_of_several_user_commits():
    snaps = [_snap(1, 1, True), _snap(2, 2, True), _snap(3, 3, False)]
    assert select_last_user_snapshot(snaps, lookback=10).snapshot_id == 2


def test_select_handles_empty_input():
    assert select_last_user_snapshot([], lookback=10) is None


def test_last_user_snapshot_passes_lookback_through():
    history = [_snap(1, 1, True)] + [_snap(i, i, False) for i in range(2, 14)]
    ds = _dataset([history[-1]], current_id=13, catalog=_FakeCatalog(history))

    assert ds.last_user_snapshot(lookback=10) is None
    assert ds.last_user_snapshot().snapshot_id == 1  # unbounded default
