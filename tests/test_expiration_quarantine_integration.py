"""End-to-end: expiration holds an orphan for a second sighting before deleting.

The unit tests in `test_orphan_quarantine` pin the promotion rule. These pin
that expiration actually routes its deletions through it - that a file which
looks orphaned survives the run that first notices it, and dies on the next one.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.metadata import DatasetMetadata, Snapshot
from opteryx_catalog.catalog.orphan_quarantine import OrphanQuarantine

DAY_MS = 24 * 60 * 60 * 1000
LOCATION = "mem://github/events"
LIVE = f"{LOCATION}/data/live.parquet"
ORPHAN = f"{LOCATION}/data/orphan.parquet"


def _build_manifest_bytes(file_path):
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    columns = {
        "file_path": ([file_path], "VARCHAR"),
        "file_format": (["parquet"], "VARCHAR"),
        "record_count": ([10], "INTEGER"),
        "file_size_in_bytes": ([100], "INTEGER"),
        "uncompressed_size_in_bytes": ([1000], "INTEGER"),
        "column_uncompressed_sizes_in_bytes": ([[100, 400]], "ARRAY"),
        "null_counts": ([[0, 0]], "ARRAY"),
        "min_k_hashes": ([["1,2"]], "ARRAY"),
        "histogram_counts": ([["1,2"]], "ARRAY"),
        "histogram_bins": ([32], "INTEGER"),
        "min_values": ([[10, 20]], "ARRAY"),
        "max_values": ([[100, 400]], "ARRAY"),
        "min_values_display": ([[None, None]], "ARRAY"),
        "max_values_display": ([[None, None]], "ARRAY"),
    }
    m = Morsel()
    for name, (values, dtype) in columns.items():
        m.append_vector(name, vector_from_sequence(values, dtype=dtype))
    return write_parquet(m)


class _MemIO:
    def __init__(self, mapping, ages):
        self._mapping = mapping
        self._ages = ages

    def new_input(self, path):
        mapping = self._mapping

        class In:
            def open(self):
                from io import BytesIO

                if mapping.get(path) is None:
                    raise FileNotFoundError(path)
                return BytesIO(mapping[path])

        return In()

    def list_files(self, prefix):
        return [p for p in list(self._mapping.keys()) if p.startswith(prefix)]

    def list_files_with_age_ms(self, prefix):
        return {p: self._ages.get(p, 0) for p in self.list_files(prefix)}

    def delete(self, path):
        self._mapping.pop(path, None)


class _FakeDoc:
    def __init__(self, store, key):
        self._store = store
        self._key = key

    def get(self):
        payload = self._store.get(self._key)

        class _Snapshot:
            exists = payload is not None

            def to_dict(self_inner):
                return payload

        return _Snapshot()

    def set(self, payload):
        self._store[self._key] = payload

    def delete(self):
        self._store.pop(self._key, None)


class _FakeCollection:
    def __init__(self, store, prefix):
        self._store = store
        self._prefix = prefix

    def document(self, name):
        return _FakeDoc(self._store, f"{self._prefix}/{name}")


class _FakeDatasetDoc:
    def __init__(self, store, prefix):
        self._store = store
        self._prefix = prefix

    def collection(self, name):
        return _FakeCollection(self._store, f"{self._prefix}/{name}")


class _FakeCatalog:
    def __init__(self, io, dataset):
        self.io = io
        self._dataset = dataset
        self.store = {}

    def load_dataset(self, identifier, load_history=False):
        return self._dataset

    def _dataset_doc_ref(self, collection, dataset_name):
        return _FakeDatasetDoc(self.store, f"{collection}/{dataset_name}")

    def _snapshots_collection(self, collection, dataset_name):
        # `update` tombstones the document and removes the snapshot from the
        # in-memory history - mimicking what the real loader's tombstone
        # filter does on the next load - so a later run sees the state the
        # previous run left behind, which is what makes the manifest hand-off
        # below observable. `delete` raises: expiration must never hard-delete
        # a snapshot document (that is the purge sweep's job, a record window later),
        # so a regression back to `.delete()` fails these tests loudly.
        dataset = self._dataset
        store = self.store

        class _SnapshotDoc:
            def __init__(self, snapshot_id):
                self._snapshot_id = snapshot_id

            def update(self, payload):
                key = f"{collection}/{dataset_name}/snapshots/{self._snapshot_id}"
                store[key] = {**store.get(key, {}), **payload}
                dataset.metadata.snapshots = [
                    s
                    for s in dataset.metadata.snapshots
                    if str(s.snapshot_id) != self._snapshot_id
                ]

            def delete(self):
                raise AssertionError(
                    "expiration hard-deleted a snapshot document; "
                    "it must tombstone with update() instead"
                )

        class _SnapshotColl:
            def document(self, name):
                return _SnapshotDoc(name)

        return _SnapshotColl()


class _ClockedQuarantine(OrphanQuarantine):
    """Quarantine driven by a test-controlled clock rather than wall time."""

    def __init__(self, catalog, clock):
        super().__init__(catalog, min_age_ms=DAY_MS)
        self.clock = clock

    def review(self, identifier, candidates, persist=True, now_ms=None):
        return super().review(identifier, candidates, persist=persist, now_ms=self.clock[0])


def _make(referenced=LIVE, extra_files=(ORPHAN,)):
    """A dataset whose manifest references `referenced`, with `extra_files` loose."""
    import time as _time

    snapshot_ts = int(_time.time() * 1000)
    manifest_path = f"{LOCATION}/metadata/manifest-{snapshot_ts}.parquet"

    storage = {
        manifest_path: _build_manifest_bytes(referenced),
        referenced: b"live-data",
    }
    for path in extra_files:
        storage[path] = b"orphan-data"

    # Every data file is well past the physical age gate, so the age gate is
    # not what any assertion here is actually testing.
    ages = {path: 2 * DAY_MS for path in storage}

    io = _MemIO(storage, ages)
    meta = DatasetMetadata(dataset_identifier="github.events", location=LOCATION)
    snap = Snapshot(snapshot_id=snapshot_ts, timestamp_ms=snapshot_ts, manifest_list=manifest_path)
    meta.snapshots.append(snap)
    meta.current_snapshot_id = snapshot_ts

    dataset = SimpleDataset(identifier="github.events", _metadata=meta, io=io)
    catalog = _FakeCatalog(io, dataset)
    return storage, catalog


def _expirer(catalog, clock):
    return SnapshotExpiration(
        catalog, author="test", quarantine=_ClockedQuarantine(catalog, clock)
    )


def test_first_run_quarantines_instead_of_deleting():
    storage, catalog = _make()
    clock = [1_000_000]

    result = _expirer(catalog, clock).expire_dataset("github.events", dry_run=False)

    assert result is not None
    assert result["deleted_files"] == []
    assert result["orphans_newly_quarantined"] == 1
    assert ORPHAN in result["quarantined_files"]
    # The whole point: still there.
    assert ORPHAN in storage
    assert LIVE in storage


def test_second_run_a_day_later_deletes():
    storage, catalog = _make()
    clock = [1_000_000]

    _expirer(catalog, clock).expire_dataset("github.events", dry_run=False)
    assert ORPHAN in storage

    clock[0] += DAY_MS
    result = _expirer(catalog, clock).expire_dataset("github.events", dry_run=False)

    assert result["deleted_files"] == [ORPHAN]
    assert ORPHAN not in storage
    assert LIVE in storage


def test_second_run_too_soon_does_not_delete():
    storage, catalog = _make()
    clock = [1_000_000]

    _expirer(catalog, clock).expire_dataset("github.events", dry_run=False)

    clock[0] += 60_000
    result = _expirer(catalog, clock).expire_dataset("github.events", dry_run=False)

    assert result["deleted_files"] == []
    assert ORPHAN in storage


def test_file_that_stops_looking_orphaned_is_never_deleted():
    """The transient-failure case the quarantine exists for.

    A run flags a file, a later run does not, and a run after that flags it
    again. The file must survive all three - the first sighting has been
    forgotten, so the third run is a first sighting again.
    """
    storage, catalog = _make()
    clock = [1_000_000]
    quarantine = _ClockedQuarantine(catalog, clock)
    expirer = SnapshotExpiration(catalog, author="test", quarantine=quarantine)

    expirer.expire_dataset("github.events", dry_run=False)
    assert quarantine.load("github.events") == {ORPHAN: 1_000_000}

    # The file is referenced again (as it would be if the earlier reading was
    # wrong), so this run does not flag it.
    clock[0] += DAY_MS
    quarantine.review("github.events", set())
    assert quarantine.load("github.events") == {}

    # Flagged once more, much later: back to a first sighting, no deletion.
    clock[0] += 365 * DAY_MS
    result = expirer.expire_dataset("github.events", dry_run=False)

    assert result["deleted_files"] == []
    assert result["orphans_newly_quarantined"] == 1
    assert ORPHAN in storage


def test_dry_run_does_not_advance_a_file_towards_deletion():
    storage, catalog = _make()
    clock = [1_000_000]
    quarantine = _ClockedQuarantine(catalog, clock)
    expirer = SnapshotExpiration(catalog, author="test", quarantine=quarantine)

    expirer.expire_dataset("github.events", dry_run=True)
    assert quarantine.load("github.events") == {}

    # A day of dry runs must not earn the file a second strike.
    clock[0] += DAY_MS
    expirer.expire_dataset("github.events", dry_run=True)
    result = expirer.expire_dataset("github.events", dry_run=False)

    assert result["deleted_files"] == []
    assert ORPHAN in storage


def _make_two_snapshots():
    """An old snapshot due to expire, and the current one."""
    import time as _time

    now_ms = int(_time.time() * 1000)
    old_ts = now_ms - (2 * DAY_MS)
    new_ts = now_ms - (60 * 60 * 1000)

    old_manifest = f"{LOCATION}/metadata/manifest-{old_ts}.parquet"
    new_manifest = f"{LOCATION}/metadata/manifest-{new_ts}.parquet"
    old_data = f"{LOCATION}/data/old.parquet"

    storage = {
        old_manifest: _build_manifest_bytes(old_data),
        new_manifest: _build_manifest_bytes(LIVE),
        old_data: b"old-data",
        LIVE: b"live-data",
    }
    ages = {path: 2 * DAY_MS for path in storage}

    io = _MemIO(storage, ages)
    meta = DatasetMetadata(dataset_identifier="github.events", location=LOCATION)
    meta.snapshots.append(
        Snapshot(snapshot_id=old_ts, timestamp_ms=old_ts, manifest_list=old_manifest)
    )
    meta.snapshots.append(
        Snapshot(snapshot_id=new_ts, timestamp_ms=new_ts, manifest_list=new_manifest)
    )
    meta.current_snapshot_id = new_ts

    dataset = SimpleDataset(identifier="github.events", _metadata=meta, io=io)
    return storage, _FakeCatalog(io, dataset), old_manifest, old_data, new_manifest


def test_condemned_snapshots_manifest_is_not_deleted_with_its_snapshot():
    """The manifest outlives the snapshot document by design.

    Deleting it inline would be a first-sight deletion, and a manifest removed
    while anything still points at it is the truncation failure the surrounding
    code exists to prevent. It is left to be reclaimed as an orphan instead,
    which costs a cycle and requires two sightings.
    """
    storage, catalog, old_manifest, old_data, new_manifest = _make_two_snapshots()
    clock = [1_000_000]
    quarantine = _ClockedQuarantine(catalog, clock)
    expirer = SnapshotExpiration(catalog, author="test", quarantine=quarantine)

    # Run 1: the old snapshot is expired, but neither its manifest nor its data
    # file is deleted - both are seen for the first time.
    result = expirer.expire_dataset("github.events", dry_run=False)
    assert result["snapshots_to_delete"] == 1
    assert result["deleted_manifests"] == []
    assert result["deleted_files"] == []
    assert old_manifest in storage
    assert old_data in storage

    # The snapshot document was tombstoned, not deleted: the record carries
    # the expiry stamp, and the fake's delete() would have raised.
    tombstones = [
        v for k, v in catalog.store.items() if "/snapshots/" in k and "expired-at-ms" in v
    ]
    assert len(tombstones) == 1
    assert tombstones[0]["expired-by"] == "test"

    # Run 2: the data file has its second sighting and goes. The manifest is
    # only now unreferenced, so this is its first sighting.
    clock[0] += DAY_MS
    result = expirer.expire_dataset("github.events", dry_run=False)
    assert result["deleted_files"] == [old_data]
    assert old_data not in storage
    assert result.get("deleted_manifests", []) == []
    assert old_manifest in storage

    # Run 3: the manifest's second sighting.
    clock[0] += DAY_MS
    result = expirer.expire_dataset("github.events", dry_run=False)
    assert old_manifest in result.get("deleted_manifests", [])
    assert old_manifest not in storage

    # The live snapshot's manifest and data were never candidates.
    assert new_manifest in storage
    assert LIVE in storage


def test_deletion_stalls_when_the_record_is_unavailable():
    """No record means no proof of a second sighting, so nothing is deleted."""
    storage, catalog = _make()

    class _Broken(OrphanQuarantine):
        def review(self, *args, **kwargs):
            from opteryx_catalog.catalog.orphan_quarantine import QuarantineUnavailable

            raise QuarantineUnavailable("firestore down")

    expirer = SnapshotExpiration(catalog, author="test", quarantine=_Broken(catalog))
    result = expirer.expire_dataset("github.events", dry_run=False)

    assert result["quarantine_available"] is False
    assert result["deleted_files"] == []
    assert ORPHAN in storage
