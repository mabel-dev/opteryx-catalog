"""
Snapshot expiration must not delete the last thing a USER did.

Maintenance writes snapshots of its own — compaction, statistics refresh,
expiration — so the newest snapshot is routinely not one anybody made by
hand. Retention previously kept `snapshots[-1]` plus anything inside the age
window and had no notion of who committed what, so a dataset written rarely
but maintained often lost the user's last commit, leaving the UI showing only
commits the user never made.

The protection is bounded to the last USER_SNAPSHOT_LOOKBACK snapshots on
purpose (see the constant): a write-once/maintain-forever dataset must not
pin its first snapshot, and every file it references, in storage permanently.
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(sys.path[0], ".."))

import pytest

from opteryx_catalog.catalog.expiration import USER_SNAPSHOT_LOOKBACK
from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot

_DAY_MS = 24 * 60 * 60 * 1000


def _snap(sid, seq, user_created, age_days):
    ts = int(time.time() * 1000) - int(age_days * _DAY_MS)
    return Snapshot(
        snapshot_id=sid,
        timestamp_ms=ts,
        sequence_number=seq,
        user_created=user_created,
        manifest_list=f"manifest-{sid}.parquet",
    )


class _FakeDataset:
    def __init__(self, snapshots):
        self.metadata = DatasetMetadata(
            dataset_identifier="ops.test", location="mem://", schema=None, properties={}
        )
        self.metadata.snapshots = list(snapshots)
        self.metadata.current_snapshot_id = snapshots[-1].snapshot_id
        self.metadata.maintenance_policy = {"retained-snapshot-age-days": 7}


class _FakeCatalog:
    def __init__(self, dataset):
        self._dataset = dataset

    def load_dataset(self, identifier, load_history=False):
        return self._dataset

    def list_tags(self, identifier):
        # Untagged: expiration reads this on every dataset, and an
        # unreadable tag list must never be answered as "no tags".
        return []


def _run(snapshots):
    """Return (kept_ids, deleted_ids) for a dry-run expiration."""
    captured = {}

    expirer = SnapshotExpiration(_FakeCatalog(_FakeDataset(snapshots)))

    def _capture(identifier, dataset, snapshots_to_delete, snapshots_to_keep, **kwargs):
        captured["keep"] = {s.snapshot_id for s in snapshots_to_keep}
        captured["delete"] = {s.snapshot_id for s in snapshots_to_delete}
        return {}

    expirer._execute_expiration = _capture
    expirer.expire_dataset("ops.test", dry_run=False)
    return captured.get("keep", set()), captured.get("delete", set())


def test_last_user_snapshot_is_retained_when_it_falls_outside_the_window():
    # One real write 30 days ago, then daily maintenance ever since.
    snapshots = [_snap(1, 1, True, age_days=30)] + [
        _snap(i, i, False, age_days=30 - i) for i in range(2, 6)
    ]
    keep, delete = _run(snapshots)

    assert 1 in keep, "the user's only commit was deleted"
    assert 1 not in delete


def test_latest_snapshot_is_still_retained():
    snapshots = [_snap(1, 1, True, age_days=30)] + [
        _snap(i, i, False, age_days=30 - i) for i in range(2, 6)
    ]
    keep, _delete = _run(snapshots)

    assert snapshots[-1].snapshot_id in keep


def test_user_snapshot_beyond_the_lookback_is_not_protected():
    # A user commit buried under more than USER_SNAPSHOT_LOOKBACK maintenance
    # commits is allowed to expire — the deliberate compromise.
    depth = USER_SNAPSHOT_LOOKBACK + 3
    snapshots = [_snap(1, 1, True, age_days=90)] + [
        _snap(i, i, False, age_days=90 - i) for i in range(2, 2 + depth)
    ]
    keep, delete = _run(snapshots)

    assert 1 not in keep
    assert 1 in delete
    # ...and the latest is still kept, so the UI always has something.
    assert snapshots[-1].snapshot_id in keep


def test_recent_user_snapshot_inside_the_age_window_is_untouched():
    # Already kept by the age window; the new logic must not double-add or
    # otherwise disturb it.
    snapshots = [
        _snap(1, 1, True, age_days=30),
        _snap(2, 2, True, age_days=1),
        _snap(3, 3, False, age_days=0),
    ]
    keep, _delete = _run(snapshots)

    assert 2 in keep
    assert len(keep) == len({*keep}), "duplicate entries in keep set"


def test_dataset_with_no_user_snapshots_at_all():
    snapshots = [_snap(i, i, False, age_days=30 - i) for i in range(1, 5)]
    keep, _delete = _run(snapshots)

    # Nothing to protect; the latest is still retained.
    assert snapshots[-1].snapshot_id in keep


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
