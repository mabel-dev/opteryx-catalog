"""Deep clean holds an unreferenced file for a second sighting before deleting.

Also pins the interaction with expiration: the two passes share one quarantine
record per dataset, so a file flagged by one and then by the other has been
condemned by two different implementations, and either pass finding it
referenced clears it.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.deep_clean import DatasetDeepClean
from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot
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
        return _FakeCollection(self.store, f"{collection}/{dataset_name}/snapshots")


class _ClockedQuarantine(OrphanQuarantine):
    def __init__(self, catalog, clock):
        super().__init__(catalog, min_age_ms=DAY_MS)
        self.clock = clock

    def review(self, identifier, candidates, persist=True, now_ms=None):
        return super().review(identifier, candidates, persist=persist, now_ms=self.clock[0])


def _make():
    import time as _time

    snapshot_ts = int(_time.time() * 1000)
    manifest_path = f"{LOCATION}/metadata/manifest-{snapshot_ts}.parquet"

    storage = {
        manifest_path: _build_manifest_bytes(LIVE),
        LIVE: b"live-data",
        ORPHAN: b"orphan-data",
    }
    ages = {path: 2 * DAY_MS for path in storage}

    io = _MemIO(storage, ages)
    meta = DatasetMetadata(dataset_identifier="github.events", location=LOCATION)
    snap = Snapshot(snapshot_id=snapshot_ts, timestamp_ms=snapshot_ts, manifest_list=manifest_path)
    meta.snapshots.append(snap)
    meta.current_snapshot_id = snapshot_ts

    dataset = SimpleDataset(identifier="github.events", _metadata=meta, io=io)
    return storage, _FakeCatalog(io, dataset)


def test_deep_clean_first_run_quarantines_instead_of_deleting():
    storage, catalog = _make()
    clock = [1_000_000]
    cleaner = DatasetDeepClean(catalog, quarantine=_ClockedQuarantine(catalog, clock))

    result = cleaner.clean_dataset("github.events", dry_run=False)

    assert result is not None
    assert result["deleted_files"] == []
    assert result["orphans_newly_quarantined"] == 1
    assert ORPHAN in storage
    assert LIVE in storage


def test_deep_clean_second_run_a_day_later_deletes():
    storage, catalog = _make()
    clock = [1_000_000]
    cleaner = DatasetDeepClean(catalog, quarantine=_ClockedQuarantine(catalog, clock))

    cleaner.clean_dataset("github.events", dry_run=False)
    assert ORPHAN in storage

    clock[0] += DAY_MS
    result = cleaner.clean_dataset("github.events", dry_run=False)

    assert result["deleted_files"] == [ORPHAN]
    assert ORPHAN not in storage
    assert LIVE in storage


def test_deep_clean_second_run_too_soon_does_not_delete():
    storage, catalog = _make()
    clock = [1_000_000]
    cleaner = DatasetDeepClean(catalog, quarantine=_ClockedQuarantine(catalog, clock))

    cleaner.clean_dataset("github.events", dry_run=False)
    clock[0] += 60_000
    result = cleaner.clean_dataset("github.events", dry_run=False)

    assert result["deleted_files"] == []
    assert ORPHAN in storage


def test_deep_clean_dry_run_does_not_advance_towards_deletion():
    storage, catalog = _make()
    clock = [1_000_000]
    quarantine = _ClockedQuarantine(catalog, clock)
    cleaner = DatasetDeepClean(catalog, quarantine=quarantine)

    cleaner.clean_dataset("github.events", dry_run=True)
    assert quarantine.load("github.events") == {}

    clock[0] += DAY_MS
    result = cleaner.clean_dataset("github.events", dry_run=False)

    assert result["deleted_files"] == []
    assert ORPHAN in storage


def test_deep_clean_deletion_stalls_when_record_unavailable():
    storage, catalog = _make()

    class _Broken(OrphanQuarantine):
        def review(self, *args, **kwargs):
            from opteryx_catalog.catalog.orphan_quarantine import QuarantineUnavailable

            raise QuarantineUnavailable("firestore down")

    result = DatasetDeepClean(catalog, quarantine=_Broken(catalog)).clean_dataset(
        "github.events", dry_run=False
    )

    assert result["quarantine_available"] is False
    assert result["deleted_files"] == []
    assert ORPHAN in storage


def test_a_clean_deep_clean_run_clears_stale_sightings():
    """An empty candidate set from a complete observation is a real statement.

    Without this, a path that is deleted and later recreated would inherit the
    old file's strike and be deleted on its first sighting.
    """
    storage, catalog = _make()
    clock = [1_000_000]
    quarantine = _ClockedQuarantine(catalog, clock)
    cleaner = DatasetDeepClean(catalog, quarantine=quarantine)

    cleaner.clean_dataset("github.events", dry_run=False)
    assert quarantine.load("github.events") == {ORPHAN: 1_000_000}

    # The file is gone, so nothing is unreferenced any more.
    storage.pop(ORPHAN)
    clock[0] += DAY_MS
    assert cleaner.clean_dataset("github.events", dry_run=False) is None
    assert quarantine.load("github.events") == {}


def test_expiration_and_deep_clean_corroborate_through_one_record():
    """A sighting by either pass counts towards the other's second strike."""
    storage, catalog = _make()
    clock = [1_000_000]
    quarantine = _ClockedQuarantine(catalog, clock)

    expirer = SnapshotExpiration(catalog, author="test", quarantine=quarantine)
    cleaner = DatasetDeepClean(catalog, quarantine=quarantine)

    # Expiration takes the first sighting.
    expirer.expire_dataset("github.events", dry_run=False)
    assert ORPHAN in storage
    assert quarantine.load("github.events") == {ORPHAN: 1_000_000}

    # Deep clean, a day later, is the independent second one.
    clock[0] += DAY_MS
    result = cleaner.clean_dataset("github.events", dry_run=False)

    assert result["deleted_files"] == [ORPHAN]
    assert ORPHAN not in storage


def test_either_pass_finding_the_file_referenced_clears_it():
    storage, catalog = _make()
    clock = [1_000_000]
    quarantine = _ClockedQuarantine(catalog, clock)

    expirer = SnapshotExpiration(catalog, author="test", quarantine=quarantine)

    expirer.expire_dataset("github.events", dry_run=False)
    assert quarantine.load("github.events") == {ORPHAN: 1_000_000}

    # Deep clean sees the file as referenced - the exoneration path.
    clock[0] += DAY_MS
    quarantine.review("github.events", set())
    assert quarantine.load("github.events") == {}

    # Expiration flagging it again is a first sighting, not a second.
    clock[0] += DAY_MS
    result = expirer.expire_dataset("github.events", dry_run=False)

    assert result["deleted_files"] == []
    assert result["orphans_newly_quarantined"] == 1
    assert ORPHAN in storage
