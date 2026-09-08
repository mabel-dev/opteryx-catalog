"""Snapshot expiration must keep the previous version of the data, not just
the current one.

`previous_user_snapshot()` already answers "what did `VERSION AS OF PREVIOUS`
mean?" by walking past maintenance commits (compaction, statistics refresh) on
both sides of the chain: once to find the user commit the head rests on
(which IS the current version, byte-for-byte, even when the head itself is a
compaction snapshot), and again to find the user commit before that. Retention
wants the identical answer - keeping `snapshots[-2]` or "the snapshot before
head" instead would routinely keep a maintenance snapshot that is
byte-identical to the current version, protecting nothing a rollback or an
audit would actually need.
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(sys.path[0], ".."))

import pytest

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot
from opteryx_catalog.exceptions import SnapshotMissingError

_DAY_MS = 24 * 60 * 60 * 1000


def _snap(sid, seq, user_created, age_days, parent=None):
    ts = int(time.time() * 1000) - int(age_days * _DAY_MS)
    return Snapshot(
        snapshot_id=sid,
        timestamp_ms=ts,
        sequence_number=seq,
        user_created=user_created,
        manifest_list=f"manifest-{sid}.parquet",
        parent_snapshot_id=parent,
    )


# An old, unrelated snapshot - not an ancestor of any test's head - included
# in every dataset below purely so `snapshots_to_keep` never ends up covering
# every snapshot in the list. When it does, `_expire_dataset` takes the
# manifest-only tidy-up path instead of `_execute_expiration`, which needs a
# real IO/manifest-reading catalog that these tests don't build. Its own fate
# (always expired) is asserted once, in `test_unrelated_noise_snapshot_still_expires`.
_NOISE = _snap(-100, seq=-100, user_created=False, age_days=365, parent=None)


def _dataset(snapshots, head=None, retention_days=7):
    metadata = DatasetMetadata(
        dataset_identifier="ops.test", location="mem://", schema=None, properties={}
    )
    metadata.snapshots = [*snapshots, _NOISE]
    metadata.current_snapshot_id = head if head is not None else snapshots[-1].snapshot_id
    metadata.maintenance_policy = {"retained-snapshot-age-days": retention_days}
    return SimpleDataset(identifier="ops.test", _metadata=metadata)


class _FakeCatalog:
    def __init__(self, dataset):
        self._dataset = dataset

    def load_dataset(self, identifier, load_history=False):
        return self._dataset

    def list_tags(self, identifier):
        return []


def _run(dataset):
    """Return (kept_ids, deleted_ids) for a dry-run expiration."""
    captured = {}

    expirer = SnapshotExpiration(_FakeCatalog(dataset))

    def _capture(identifier, ds, snapshots_to_delete, snapshots_to_keep, **kwargs):
        captured["keep"] = {s.snapshot_id for s in snapshots_to_keep}
        captured["delete"] = {s.snapshot_id for s in snapshots_to_delete}
        return {}

    expirer._execute_expiration = _capture
    expirer.expire_dataset("ops.test", dry_run=False)
    return captured.get("keep", set()), captured.get("delete", set())


def test_previous_user_snapshot_is_retained():
    # user write (1) -> maintenance (2) -> user write (3), all old enough to
    # be outside the retention window on their own.
    snapshots = [
        _snap(1, 1, True, age_days=90, parent=None),
        _snap(2, 2, False, age_days=89, parent=1),
        _snap(3, 3, True, age_days=88, parent=2),
    ]
    keep, delete = _run(_dataset(snapshots))

    assert 3 in keep, "the current (head) snapshot must always be kept"
    assert 1 in keep, "the previous user version must be kept"
    assert 1 not in delete


def test_compacted_current_still_resolves_to_the_user_version_before_it():
    # user write (1) -> user write (2) -> compaction (3, current). The head
    # is a compaction snapshot, byte-identical to (2)'s data, so "previous"
    # must resolve to (1), not to (2) treated naively as "the prior snapshot".
    snapshots = [
        _snap(1, 1, True, age_days=90, parent=None),
        _snap(2, 2, True, age_days=89, parent=1),
        _snap(3, 3, False, age_days=88, parent=2),
    ]
    keep, delete = _run(_dataset(snapshots))

    assert 3 in keep  # current
    assert 1 in keep, "previous must skip past the compaction to the user write before it"
    assert 1 not in delete


def test_no_previous_version_when_there_is_only_one_user_commit():
    snapshots = [
        _snap(1, 1, True, age_days=90, parent=None),
        _snap(2, 2, False, age_days=89, parent=1),
    ]
    keep, _delete = _run(_dataset(snapshots))

    # Nothing before the only user commit; expiration must not error out.
    assert 2 in keep
    assert 1 in keep  # still protected as the last (only) user snapshot


def test_missing_previous_version_does_not_abort_expiration():
    # The chain points at a parent that is not among the loaded snapshots
    # (already expired out of history) - previous_user_snapshot() raises
    # SnapshotMissingError. Expiration must degrade, not blow up.
    snapshots = [
        _snap(2, 2, True, age_days=89, parent=999),
        _snap(3, 3, False, age_days=1, parent=2),
    ]
    dataset = _dataset(snapshots)

    with pytest.raises(SnapshotMissingError):
        # Sanity check: previous_user_snapshot() itself does raise here.
        dataset.previous_user_snapshot()

    keep, _delete = _run(dataset)

    # Expiration still ran and kept what it could establish.
    assert 3 in keep  # current
    assert 2 in keep  # last user snapshot, protected independently


def test_unrelated_noise_snapshot_still_expires():
    # Confirms _NOISE (used to keep every test above out of the
    # manifest-only tidy-up path) is not accidentally protected by the new
    # logic - the new "previous version" retention must stay scoped to the
    # head's own ancestry, not turn into "keep everything".
    snapshots = [
        _snap(1, 1, True, age_days=90, parent=None),
        _snap(2, 2, False, age_days=89, parent=1),
    ]
    _keep, delete = _run(_dataset(snapshots))

    assert -100 in delete


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
