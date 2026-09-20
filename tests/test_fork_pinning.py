"""A fork pins the upstream snapshot it rests on, exactly as a tag does.

`FORKS_DESIGN.md` S5.1. This is the invariant the whole fork design stands on:
a fork's manifest names files it does not own, so the only thing keeping those
files alive is that the upstream is still retaining the snapshot that names
them. Retain the snapshot and the files are never orphans - so nothing
downstream of `pinned_snapshot_ids` has to know forks exist at all.

Tested at the same three levels the tag pin is, because any one of them missing
still leaves a fork pointing at deleted files: the pinned snapshot is not
condemned, its files are therefore not swept, and an unreadable registry aborts
the run rather than reading as "no forks".
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(sys.path[0], ".."))

import pytest

from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.expiration import pinned_snapshot_ids
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot
from opteryx_catalog.exceptions import ManifestProtectionError

DAY_MS = 24 * 60 * 60 * 1000


def _snap(sid, age_days):
    return Snapshot(
        snapshot_id=sid,
        timestamp_ms=int(time.time() * 1000) - int(age_days * DAY_MS),
        sequence_number=sid,
        user_created=False,
        manifest_list=f"manifest-{sid}.parquet",
    )


class _FakeDataset:
    def __init__(self, snapshots, retention_days):
        self.metadata = DatasetMetadata(
            dataset_identifier="samples.tpch_sf1.lineitem",
            location="mem://",
            schema=None,
            properties={},
        )
        self.metadata.snapshots = list(snapshots)
        self.metadata.current_snapshot_id = snapshots[-1].snapshot_id
        self.metadata.maintenance_policy = {"retained-snapshot-age-days": retention_days}
        self.metadata.tags = {}
        self.metadata.tags_loaded = True


class _FakeCatalog:
    """A catalog whose fork registry is whatever the test says it is."""

    def __init__(self, dataset, forks=None, raises=False):
        self._dataset = dataset
        self._forks = forks
        self._raises = raises

    def load_dataset(self, identifier, load_history=False):
        return self._dataset

    def list_forks(self, identifier):
        if self._raises:
            raise RuntimeError("firestore unavailable")
        return list(self._forks or [])


class _CatalogWithoutForks:
    """Predates the fork registry entirely - no `list_forks` method."""

    def __init__(self, dataset):
        self._dataset = dataset

    def load_dataset(self, identifier, load_history=False):
        return self._dataset


def _fork_row(name, snapshot_id):
    return {"fork": name, "pinned-snapshot": snapshot_id, "created-at-ms": 1}


def _run(catalog, dataset):
    """Return (kept_ids, deleted_ids) for one expiration pass."""
    captured = {}
    expirer = SnapshotExpiration(catalog)

    def _capture(identifier, ds, snapshots_to_delete, snapshots_to_keep, **kwargs):
        captured["keep"] = {s.snapshot_id for s in snapshots_to_keep}
        captured["delete"] = {s.snapshot_id for s in snapshots_to_delete}
        return {}

    expirer._execute_expiration = _capture
    expirer.expire_dataset("samples.tpch_sf1.lineitem", dry_run=False)
    return captured.get("keep", set()), captured.get("delete", set())


# --------------------------------------------------------------------------
# 1. The pinned set
# --------------------------------------------------------------------------


def test_a_registered_fork_pins_its_base_snapshot():
    dataset = _FakeDataset([_snap(1, 90)], retention_days=7)
    catalog = _FakeCatalog(dataset, forks=[_fork_row("personal.justin.lineitem", 1)])

    assert pinned_snapshot_ids(catalog, "samples.tpch_sf1.lineitem", dataset.metadata) == {1}


def test_several_forks_on_several_bases_all_pin():
    dataset = _FakeDataset([_snap(i, 90) for i in (1, 2, 3)], retention_days=7)
    catalog = _FakeCatalog(
        dataset,
        forks=[_fork_row("personal.a.lineitem", 1), _fork_row("personal.b.lineitem", 3)],
    )

    assert pinned_snapshot_ids(catalog, "samples.tpch_sf1.lineitem", dataset.metadata) == {1, 3}


def test_tags_and_forks_pin_together():
    # One set, because they are one statement: something outside this
    # dataset's history is standing on these files.
    dataset = _FakeDataset([_snap(i, 90) for i in (1, 2, 3)], retention_days=7)
    dataset.metadata.tags = {"release": 2}
    catalog = _FakeCatalog(dataset, forks=[_fork_row("personal.justin.lineitem", 1)])

    assert pinned_snapshot_ids(catalog, "samples.tpch_sf1.lineitem", dataset.metadata) == {1, 2}


def test_an_upstream_with_no_forks_pins_nothing():
    dataset = _FakeDataset([_snap(1, 90)], retention_days=7)
    catalog = _FakeCatalog(dataset, forks=[])

    assert pinned_snapshot_ids(catalog, "samples.tpch_sf1.lineitem", dataset.metadata) == set()


def test_a_catalog_predating_the_registry_pins_nothing():
    # No `list_forks` at all is the pre-feature catalog, not a broken one.
    dataset = _FakeDataset([_snap(1, 90)], retention_days=7)

    assert (
        pinned_snapshot_ids(_CatalogWithoutForks(dataset), "samples.x.y", dataset.metadata) == set()
    )


def test_a_row_without_a_pinned_snapshot_is_skipped_not_fatal():
    dataset = _FakeDataset([_snap(1, 90)], retention_days=7)
    catalog = _FakeCatalog(
        dataset, forks=[{"fork": "personal.justin.lineitem"}, _fork_row("personal.b.x", 1)]
    )

    assert pinned_snapshot_ids(catalog, "samples.tpch_sf1.lineitem", dataset.metadata) == {1}


# --------------------------------------------------------------------------
# 2. Fail closed
# --------------------------------------------------------------------------


def test_an_unreadable_fork_registry_aborts_the_run():
    # The reading this refuses to make is "no forks", which is the one that
    # deletes exactly the data the pin exists to keep.
    dataset = _FakeDataset([_snap(1, 90)], retention_days=7)
    catalog = _FakeCatalog(dataset, raises=True)

    with pytest.raises(ManifestProtectionError, match="fork registry"):
        pinned_snapshot_ids(catalog, "samples.tpch_sf1.lineitem", dataset.metadata)


def test_an_unreadable_registry_stops_expiration_entirely():
    dataset = _FakeDataset([_snap(i, 90 - i) for i in range(1, 6)], retention_days=1)
    catalog = _FakeCatalog(dataset, raises=True)

    with pytest.raises(ManifestProtectionError):
        _run(catalog, dataset)


# --------------------------------------------------------------------------
# 3. The consequence: a pinned snapshot is not condemned
# --------------------------------------------------------------------------


def test_a_forked_snapshot_older_than_the_window_is_not_condemned():
    # 90 days against a 7-day window: nothing but the fork can save it.
    snapshots = [_snap(1, age_days=90)] + [_snap(i, age_days=90 - i) for i in range(2, 6)]
    dataset = _FakeDataset(snapshots, retention_days=7)
    catalog = _FakeCatalog(dataset, forks=[_fork_row("personal.justin.lineitem", 1)])

    keep, delete = _run(catalog, dataset)

    assert 1 in keep, "an upstream expired a snapshot a fork is standing on"
    assert 1 not in delete


def test_the_pin_survives_the_keep_only_the_latest_branch():
    # retention_days None keeps only the current snapshot and builds the keep
    # set from scratch - a pin applied only to the age-window branch would be
    # silently absent here, which is the harshest retention there is.
    snapshots = [_snap(1, 90), _snap(2, 30), _snap(3, 0)]
    dataset = _FakeDataset(snapshots, retention_days=None)
    catalog = _FakeCatalog(dataset, forks=[_fork_row("personal.justin.lineitem", 1)])

    keep, delete = _run(catalog, dataset)

    assert keep == {1, 3}
    assert delete == {2}


def test_an_unforked_over_age_snapshot_still_expires():
    # The counterweight, without which the pin is not a pin but an accident.
    snapshots = [_snap(i, age_days=90 - i) for i in range(1, 6)]
    dataset = _FakeDataset(snapshots, retention_days=1)
    catalog = _FakeCatalog(dataset, forks=[_fork_row("personal.justin.lineitem", 2)])

    keep, delete = _run(catalog, dataset)

    assert 2 in keep
    assert delete == {1, 3, 4}, "the fork pin protected more than the snapshot it names"
