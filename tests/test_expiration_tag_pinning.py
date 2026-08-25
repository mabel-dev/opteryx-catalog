"""A TAG pins its snapshot from expiry, forever, until the tag is dropped.

`SNAPSHOT_TAGS_DESIGN.md` S4: a tag that does not pin is worse than no tag,
because it is a name promising data that retention will quietly take away. The
pin is one insertion into `snapshots_to_keep`, and these tests exist to hold
each of its three consequences separately, because any one of them missing
still leaves a tag pointing at deleted files:

* the tagged snapshot is not condemned (so it is not tombstoned - and a
  tombstoned snapshot drops out of reads AND out of storage billing);
* its data files land in `kept_files`, so the orphan sweep stops proposing
  them;
* its manifest is a REQUIRED read, so an unreadable one aborts the run rather
  than yielding a short protected set and deleting the tag's data.

And the counterweight, without which the pin is not a pin but an accident:
an UNtagged over-age snapshot still expires exactly as before.
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(sys.path[0], ".."))

import pytest
from test_expiration_quarantine_integration import _build_manifest_bytes
from test_expiration_quarantine_integration import _FakeDatasetDoc
from test_expiration_quarantine_integration import _MemIO

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot
from opteryx_catalog.exceptions import ManifestProtectionError

DAY_MS = 24 * 60 * 60 * 1000
LOCATION = "mem://github/events"


def _snap(sid, age_days):
    return Snapshot(
        snapshot_id=sid,
        timestamp_ms=int(time.time() * 1000) - int(age_days * DAY_MS),
        sequence_number=sid,
        user_created=False,
        manifest_list=f"manifest-{sid}.parquet",
    )


class _FakeDataset:
    def __init__(self, snapshots, tags, retention_days, tags_loaded=True):
        self.metadata = DatasetMetadata(
            dataset_identifier="ops.test", location="mem://", schema=None, properties={}
        )
        self.metadata.snapshots = list(snapshots)
        self.metadata.current_snapshot_id = snapshots[-1].snapshot_id
        self.metadata.maintenance_policy = {"retained-snapshot-age-days": retention_days}
        self.metadata.tags = dict(tags)
        self.metadata.tags_loaded = tags_loaded


class _FakeCatalog:
    def __init__(self, dataset):
        self._dataset = dataset

    def load_dataset(self, identifier, load_history=False):
        return self._dataset


def _run(snapshots, tags, retention_days=7, tags_loaded=True):
    """Return (kept_ids, deleted_ids) for one expiration pass."""
    captured = {}
    dataset = _FakeDataset(snapshots, tags, retention_days, tags_loaded)
    expirer = SnapshotExpiration(_FakeCatalog(dataset))

    def _capture(identifier, ds, snapshots_to_delete, snapshots_to_keep, **kwargs):
        captured["keep"] = {s.snapshot_id for s in snapshots_to_keep}
        captured["delete"] = {s.snapshot_id for s in snapshots_to_delete}
        return {}

    expirer._execute_expiration = _capture
    expirer.expire_dataset("ops.test", dry_run=False)
    return captured.get("keep", set()), captured.get("delete", set())


# --------------------------------------------------------------------------
# 1. The candidate filter
# --------------------------------------------------------------------------


def test_tagged_snapshot_older_than_the_window_is_not_condemned():
    # 90 days old against a 7-day window: nothing but the tag can save it.
    snapshots = [_snap(1, age_days=90)] + [_snap(i, age_days=90 - i) for i in range(2, 6)]
    keep, delete = _run(snapshots, tags={"report_202602": 1})

    assert 1 in keep, "a tagged snapshot was expired; the tag now points at deleted files"
    assert 1 not in delete


def test_tagged_snapshot_survives_the_keep_only_the_latest_branch():
    # retention_days None means "keep only the current snapshot - data is
    # unversioned". That branch builds `snapshots_to_keep` from scratch as a
    # single-element list, so a pin applied only to the age-window branch
    # would be silently absent here - the harshest retention setting there is.
    snapshots = [_snap(1, age_days=90), _snap(2, age_days=30), _snap(3, age_days=0)]
    keep, delete = _run(snapshots, tags={"report": 1}, retention_days=None)

    assert keep == {1, 3}, "the tag did not survive the unversioned-retention branch"
    assert delete == {2}


def test_zero_retention_days_is_the_same_branch_and_pins_too():
    # 0 and None are the same branch; snapshot 2 is the untagged control that
    # keeps this run condemning something.
    snapshots = [_snap(1, age_days=90), _snap(2, age_days=60), _snap(3, age_days=0)]
    keep, delete = _run(snapshots, tags={"report": 1}, retention_days=0)

    assert keep == {1, 3}
    assert delete == {2}


def test_several_tags_on_several_snapshots_all_pin():
    snapshots = [_snap(i, age_days=90 - i) for i in range(1, 6)]
    keep, delete = _run(snapshots, tags={"a": 1, "b": 3}, retention_days=1)

    assert {1, 3}.issubset(keep)
    assert delete == {2, 4}


def test_two_tags_on_the_same_snapshot_pin_it_once():
    # Both names point at snapshot 1; it must appear in the retained set once,
    # or its files are read (and counted) twice for no reason.
    snapshots = [_snap(1, age_days=90), _snap(2, age_days=60), _snap(3, age_days=0)]
    dataset = _FakeDataset(snapshots, {"a": 1, "b": 1}, 7)
    expirer = SnapshotExpiration(_FakeCatalog(dataset))
    captured = {}

    def _capture(identifier, ds, snapshots_to_delete, snapshots_to_keep, **kwargs):
        captured["keep"] = list(snapshots_to_keep)
        return {}

    expirer._execute_expiration = _capture
    expirer.expire_dataset("ops.test", dry_run=False)

    ids = [s.snapshot_id for s in captured["keep"]]
    assert sorted(ids) == [1, 3]


# --------------------------------------------------------------------------
# The counterweight: the pin must not be universal
# --------------------------------------------------------------------------


def test_untagged_over_age_snapshot_still_expires():
    snapshots = [_snap(1, age_days=90)] + [_snap(i, age_days=90 - i) for i in range(2, 6)]
    keep, delete = _run(snapshots, tags={})

    assert 1 in delete, "expiration stopped expiring; the pin is applying to everything"
    assert 1 not in keep


def test_dropping_the_tag_lets_the_snapshot_expire_on_the_next_run():
    # DROP TAG unpins immediately and deliberately - no grace period. Dropping
    # a tag is how you agree to lose the data.
    snapshots = [_snap(1, age_days=90)] + [_snap(i, age_days=90 - i) for i in range(2, 6)]

    keep, delete = _run(snapshots, tags={"report": 1})
    assert 1 in keep and 1 not in delete

    keep, delete = _run(snapshots, tags={})
    assert 1 in delete and 1 not in keep


def test_a_tag_naming_a_snapshot_that_is_not_here_pins_nothing():
    # Pinning means this cannot happen, but if it does it must not silently
    # widen the retained set to something arbitrary.
    snapshots = [_snap(1, age_days=90), _snap(2, age_days=0)]
    keep, delete = _run(snapshots, tags={"ghost": 999})

    assert keep == {2}
    assert delete == {1}


# --------------------------------------------------------------------------
# The tag set itself is a protected input
# --------------------------------------------------------------------------


def test_an_unreadable_tag_set_aborts_rather_than_expiring_anything():
    # "No tags found" and "tags could not be read" are different answers.
    # Treating the second as the first deletes exactly the data a tag exists
    # to keep, so the run refuses.
    snapshots = [_snap(1, age_days=90), _snap(2, age_days=0)]
    dataset = _FakeDataset(snapshots, {}, 7, tags_loaded=False)
    expirer = SnapshotExpiration(_FakeCatalog(dataset))

    def _explode(*args, **kwargs):
        raise AssertionError("expiration ran with an unknown tag set")

    expirer._execute_expiration = _explode

    with pytest.raises(ManifestProtectionError):
        expirer.expire_dataset("ops.test", dry_run=False)


# --------------------------------------------------------------------------
# 2 and 3. kept_files, and the manifest as a required read
# --------------------------------------------------------------------------

TAGGED_FILE = f"{LOCATION}/data/tagged.parquet"
LIVE_FILE = f"{LOCATION}/data/live.parquet"

_FIXTURE_SEQ = [0]


def _storage_dataset(*, tagged_manifest_readable=True):
    """Two snapshots: an over-age tagged one, and the current one.

    Each references its own data file, so what the sweep does with the tagged
    snapshot's file is directly observable.
    """
    # Manifest paths must be unique per fixture, not merely per millisecond:
    # `get_parsed_manifest` caches by path for the life of the process, so two
    # fixtures built in the same millisecond would share a parse - and the
    # unreadable-manifest test would quietly read the readable one.
    _FIXTURE_SEQ[0] += 1
    seq = _FIXTURE_SEQ[0]
    now = int(time.time() * 1000)
    old_id, new_id = now - 90 * DAY_MS, now
    old_manifest = f"{LOCATION}/metadata/manifest-{old_id}-{seq}.parquet"
    new_manifest = f"{LOCATION}/metadata/manifest-{new_id}-{seq}.parquet"

    storage = {
        old_manifest: _build_manifest_bytes(TAGGED_FILE),
        new_manifest: _build_manifest_bytes(LIVE_FILE),
        TAGGED_FILE: b"tagged-data",
        LIVE_FILE: b"live-data",
    }
    if not tagged_manifest_readable:
        # Present in the listing, unreadable on open - the failure the
        # `required=True` read exists to catch.
        storage[old_manifest] = None

    ages = {path: 2 * DAY_MS for path in storage}
    io = _MemIO(storage, ages)

    meta = DatasetMetadata(dataset_identifier="github.events", location=LOCATION)
    meta.snapshots = [
        Snapshot(snapshot_id=old_id, timestamp_ms=old_id, manifest_list=old_manifest),
        Snapshot(snapshot_id=new_id, timestamp_ms=new_id, manifest_list=new_manifest),
    ]
    meta.current_snapshot_id = new_id
    meta.maintenance_policy = {"retained-snapshot-age-days": 7}
    meta.tags = {"report_202602": old_id}
    # As a history load leaves it: the pins are established, so expiration
    # reads them from here rather than going back to the catalog.
    meta.tags_loaded = True

    dataset = SimpleDataset(identifier="github.events", _metadata=meta, io=io)

    class _Catalog:
        def __init__(self):
            self.io = io
            self.store = {}

        def load_dataset(self, identifier, load_history=False):
            return dataset

        def _dataset_doc_ref(self, collection, dataset_name):
            return _FakeDatasetDoc(self.store, f"{collection}/{dataset_name}")

        def _snapshots_collection(self, collection, dataset_name):
            raise AssertionError("a pinned snapshot must never be tombstoned")

    return storage, _Catalog()


def _proposed_orphans(catalog):
    """Every data file this run put forward for deletion.

    A first sighting is quarantined rather than deleted, so "proposed" is the
    union of what was quarantined and what was cleared to delete - the honest
    measure of what the run considered garbage.
    """
    summary = SnapshotExpiration(catalog, author="test").expire_dataset(
        "github.events", dry_run=True
    )
    if summary is None:
        return set()
    return set(summary.get("data_files_to_delete", [])) | set(summary.get("quarantined_files", []))


def test_a_tagged_snapshots_data_files_are_never_orphan_candidates():
    storage, catalog = _storage_dataset()

    assert TAGGED_FILE not in _proposed_orphans(catalog), (
        "a tagged snapshot's data file was proposed for deletion"
    )
    assert TAGGED_FILE in storage

    # The control: the same dataset with the tag removed DOES propose it, so
    # the assertion above is about the pin and not about the fixture.
    _storage2, catalog2 = _storage_dataset()
    catalog2.load_dataset("github.events").metadata.tags = {}

    assert TAGGED_FILE in _proposed_orphans(catalog2), (
        "the untagged control did not propose the file, so the test above proves nothing"
    )


def test_an_unreadable_manifest_on_a_tagged_snapshot_aborts_instead_of_deleting():
    storage, catalog = _storage_dataset(tagged_manifest_readable=False)
    expirer = SnapshotExpiration(catalog, author="test")

    with pytest.raises(ManifestProtectionError):
        expirer.expire_dataset("github.events", dry_run=True)

    # Nothing was reclaimed on the strength of a protected set we could not build.
    assert TAGGED_FILE in storage
    assert LIVE_FILE in storage


# --------------------------------------------------------------------------
# The read path that makes the pin real
# --------------------------------------------------------------------------


def _catalog_with_tags():
    """A catalog holding one dataset, one snapshot and one tag."""
    from test_snapshot_tags import _catalog
    from test_snapshot_tags import _dataset
    from test_snapshot_tags import _snapshot

    catalog = _catalog()
    catalog.gcs_bucket = "bucket"
    identifier = _dataset(catalog)
    _snapshot(catalog, identifier, 11)
    catalog.create_tag(identifier, "Report_202602", 11, author="alice")
    return catalog, identifier


def test_a_history_load_carries_the_pins_on_the_metadata():
    # Expiration reads its pins from here, so a load that does not carry them
    # is a load that cannot protect them.
    catalog, identifier = _catalog_with_tags()
    collection, name = identifier.split(".", 1)

    dataset = catalog._build_dataset(
        identifier, collection, name, catalog._dataset_doc_ref(collection, name).get(), True
    )

    assert dataset.metadata.tags == {"report_202602": 11}, "normalized name -> snapshot id"
    assert dataset.metadata.tags_loaded is True
    assert dataset.metadata.pinned_snapshot_ids() == {11}


def test_a_plain_load_does_not_claim_to_know_the_tags():
    # The cheap path every write uses does not pay for a tag read - and says
    # so, rather than presenting an empty map as "nothing is pinned".
    catalog, identifier = _catalog_with_tags()
    collection, name = identifier.split(".", 1)

    dataset = catalog._build_dataset(
        identifier, collection, name, catalog._dataset_doc_ref(collection, name).get(), False
    )

    assert dataset.metadata.tags == {}
    assert dataset.metadata.tags_loaded is False


def test_metadata_that_never_fetched_tags_says_so():
    # The default matters: a hand-built DatasetMetadata must not read as
    # "checked, and nothing is pinned".
    assert DatasetMetadata(dataset_identifier="a.b").tags_loaded is False
