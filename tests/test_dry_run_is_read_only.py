"""A dry run must not mutate anything, anywhere.

This is the property the production rollout plan rests on: that
`dry_run=True` can be pointed at a live workspace to see what a real run would
do, without the seeing changing anything. "It returns a plan" is not the same
claim - the plan could be produced by code that also wrote a quarantine record,
deleted a snapshot document, or removed a file on the way.

So rather than assert on the returned summary, every fake here RAISES on any
mutating call. A dry run that writes fails these tests loudly instead of
quietly passing on an assertion nobody thought to make.

The case that matters most is `test_..._when_files_are_due_for_deletion`: a run
where the quarantine record already holds a second strike, so the plan says
"this file would be deleted right now". That is the moment a dry run is most
likely to reach for the delete.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.deep_clean import DatasetDeepClean
from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.metadata import DatasetMetadata, Snapshot
from opteryx_catalog.catalog.orphan_quarantine import (
    MAINTENANCE_SUBCOLLECTION,
    QUARANTINE_DOC,
    OrphanQuarantine,
)

DAY_MS = 24 * 60 * 60 * 1000
LOCATION = "mem://github/events"
LIVE = f"{LOCATION}/data/live.parquet"
ORPHAN = f"{LOCATION}/data/orphan.parquet"


class DryRunViolation(AssertionError):
    """Raised the moment a dry run attempts to change anything."""


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


class _StrictIO:
    """FileIO that records every mutation attempt, then refuses it.

    Recording matters more than raising: the code under test catches broadly in
    several places, so an exception alone can be swallowed and the test would
    pass while the delete had in fact been attempted. `violations` cannot be
    swallowed.
    """

    def __init__(self, mapping, ages, violations):
        self._mapping = mapping
        self._ages = ages
        self.violations = violations

    def new_input(self, path):
        mapping = self._mapping

        class In:
            def open(self):
                from io import BytesIO

                if mapping.get(path) is None:
                    raise FileNotFoundError(path)
                return BytesIO(mapping[path])

        return In()

    def new_output(self, path):
        self.violations.append(f"storage write: {path}")
        raise DryRunViolation(f"wrote to storage: {path}")

    def list_files(self, prefix):
        return [p for p in list(self._mapping.keys()) if p.startswith(prefix)]

    def list_files_with_age_ms(self, prefix):
        return {p: self._ages.get(p, 0) for p in self.list_files(prefix)}

    def delete(self, path):
        self.violations.append(f"storage delete: {path}")
        raise DryRunViolation(f"deleted from storage: {path}")


class _StrictDoc:
    """Firestore document that records and refuses writes and deletes."""

    def __init__(self, store, key, violations):
        self._store = store
        self._key = key
        self._violations = violations

    def get(self):
        payload = self._store.get(self._key)

        class _Snapshot:
            exists = payload is not None

            def to_dict(self_inner):
                return payload

        return _Snapshot()

    def set(self, payload):
        self._violations.append(f"firestore set: {self._key}")
        raise DryRunViolation(f"wrote Firestore document: {self._key}")

    def update(self, payload):
        self._violations.append(f"firestore update: {self._key}")
        raise DryRunViolation(f"updated Firestore document: {self._key}")

    def delete(self):
        self._violations.append(f"firestore delete: {self._key}")
        raise DryRunViolation(f"deleted Firestore document: {self._key}")


class _StrictCollection:
    def __init__(self, store, prefix, violations):
        self._store = store
        self._prefix = prefix
        self._violations = violations

    def document(self, name):
        return _StrictDoc(self._store, f"{self._prefix}/{name}", self._violations)

    def stream(self):
        return iter(())


class _StrictDatasetDoc:
    def __init__(self, store, prefix, violations):
        self._store = store
        self._prefix = prefix
        self._violations = violations

    def collection(self, name):
        return _StrictCollection(self._store, f"{self._prefix}/{name}", self._violations)


class _StrictCatalog:
    def __init__(self, io, dataset, violations):
        self.io = io
        self._dataset = dataset
        self.store = {}
        self.violations = violations

    def load_dataset(self, identifier, load_history=False):
        return self._dataset

    def _dataset_doc_ref(self, collection, dataset_name):
        return _StrictDatasetDoc(self.store, f"{collection}/{dataset_name}", self.violations)

    def _snapshots_collection(self, collection, dataset_name):
        return _StrictCollection(
            self.store, f"{collection}/{dataset_name}/snapshots", self.violations
        )


def _make(snapshot_count=1):
    """A dataset with a loose orphan file, and optionally a condemned snapshot."""
    import time as _time

    now_ms = int(_time.time() * 1000)
    new_ts = now_ms - (60 * 60 * 1000)
    new_manifest = f"{LOCATION}/metadata/manifest-{new_ts}.parquet"

    storage = {
        new_manifest: _build_manifest_bytes(LIVE),
        LIVE: b"live-data",
        ORPHAN: b"orphan-data",
    }

    meta = DatasetMetadata(dataset_identifier="github.events", location=LOCATION)

    if snapshot_count > 1:
        # An older snapshot, outside retention, whose manifest is also an
        # orphaned-manifest candidate once expired.
        old_ts = now_ms - (2 * DAY_MS)
        old_manifest = f"{LOCATION}/metadata/manifest-{old_ts}.parquet"
        old_data = f"{LOCATION}/data/old.parquet"
        storage[old_manifest] = _build_manifest_bytes(old_data)
        storage[old_data] = b"old-data"
        meta.snapshots.append(
            Snapshot(snapshot_id=old_ts, timestamp_ms=old_ts, manifest_list=old_manifest)
        )

    meta.snapshots.append(
        Snapshot(snapshot_id=new_ts, timestamp_ms=new_ts, manifest_list=new_manifest)
    )
    meta.current_snapshot_id = new_ts

    ages = {path: 2 * DAY_MS for path in storage}
    violations = []
    io = _StrictIO(storage, ages, violations)
    dataset = SimpleDataset(identifier="github.events", _metadata=meta, io=io)
    return storage, _StrictCatalog(io, dataset, violations)


def _arm_the_quarantine(catalog, paths, first_seen_ms):
    """Pre-load the record so the named paths are due for deletion."""
    catalog.store[f"github/events/{MAINTENANCE_SUBCOLLECTION}/{QUARANTINE_DOC}"] = {
        "entries": [{"path": p, "first-seen-ms": first_seen_ms} for p in paths],
        "updated-at-ms": first_seen_ms,
    }


class _ClockedQuarantine(OrphanQuarantine):
    def __init__(self, catalog, clock):
        super().__init__(catalog, min_age_ms=DAY_MS)
        self.clock = clock

    def review(self, identifier, candidates, persist=True, now_ms=None):
        return super().review(identifier, candidates, persist=persist, now_ms=self.clock[0])


# --- expiration -------------------------------------------------------------


def test_expiration_dry_run_writes_nothing_on_a_fresh_record():
    storage, catalog = _make()
    before = dict(storage)
    clock = [1_000_000]

    plan = SnapshotExpiration(
        catalog, author="test", quarantine=_ClockedQuarantine(catalog, clock)
    ).expire_dataset("github.events", dry_run=True)

    assert plan is not None
    assert catalog.violations == []
    assert storage == before
    assert catalog.store == {}


def test_expiration_dry_run_writes_nothing_when_files_are_due_for_deletion():
    """The dangerous case: the plan says these would go right now."""
    storage, catalog = _make()
    before = dict(storage)
    clock = [1_000_000]
    _arm_the_quarantine(catalog, [ORPHAN], first_seen_ms=clock[0] - DAY_MS)
    record_before = dict(catalog.store)

    plan = SnapshotExpiration(
        catalog, author="test", quarantine=_ClockedQuarantine(catalog, clock)
    ).expire_dataset("github.events", dry_run=True)

    # The plan must actually be non-trivial, or this test proves nothing.
    assert ORPHAN in plan.get("data_files_to_delete", [])
    assert catalog.violations == []
    assert storage == before
    assert catalog.store == record_before


def test_expiration_dry_run_does_not_delete_condemned_snapshot_documents():
    storage, catalog = _make(snapshot_count=2)
    before = dict(storage)
    clock = [1_000_000]

    plan = SnapshotExpiration(
        catalog, author="test", quarantine=_ClockedQuarantine(catalog, clock)
    ).expire_dataset("github.events", dry_run=True)

    assert plan["snapshots_to_delete"] == 1
    assert plan["deleted_snapshots"] != []  # planned, not performed
    assert catalog.violations == []
    assert storage == before
    assert catalog.store == {}


def test_expiration_dry_run_writes_nothing_when_a_manifest_is_due():
    storage, catalog = _make(snapshot_count=2)
    before = dict(storage)
    clock = [1_000_000]
    manifests = [p for p in storage if "/metadata/manifest-" in p]
    _arm_the_quarantine(catalog, manifests, first_seen_ms=clock[0] - DAY_MS)
    record_before = dict(catalog.store)

    SnapshotExpiration(
        catalog, author="test", quarantine=_ClockedQuarantine(catalog, clock)
    ).expire_dataset("github.events", dry_run=True)

    assert catalog.violations == []
    assert storage == before
    assert catalog.store == record_before


# --- deep clean -------------------------------------------------------------


def test_deep_clean_dry_run_writes_nothing_on_a_fresh_record():
    storage, catalog = _make()
    before = dict(storage)
    clock = [1_000_000]

    summary = DatasetDeepClean(
        catalog, quarantine=_ClockedQuarantine(catalog, clock)
    ).clean_dataset("github.events", dry_run=True)

    assert summary is not None
    assert catalog.violations == []
    assert storage == before
    assert catalog.store == {}


def test_deep_clean_dry_run_writes_nothing_when_files_are_due_for_deletion():
    storage, catalog = _make()
    before = dict(storage)
    clock = [1_000_000]
    _arm_the_quarantine(catalog, [ORPHAN], first_seen_ms=clock[0] - DAY_MS)
    record_before = dict(catalog.store)

    summary = DatasetDeepClean(
        catalog, quarantine=_ClockedQuarantine(catalog, clock)
    ).clean_dataset("github.events", dry_run=True)

    assert ORPHAN in summary.get("orphaned_files", [])
    assert catalog.violations == []
    assert storage == before
    assert catalog.store == record_before


# --- the fakes themselves ---------------------------------------------------


def test_the_strict_fakes_actually_catch_a_write():
    """Guards the guard: these tests are only worth anything if a real
    mutation would be caught."""
    storage, catalog = _make()

    with pytest.raises(DryRunViolation):
        catalog.io.delete(ORPHAN)

    with pytest.raises(DryRunViolation):
        catalog._dataset_doc_ref("github", "events").collection("x").document("y").set({})

    with pytest.raises(DryRunViolation):
        catalog._snapshots_collection("github", "events").document("1").delete()


class _MemoryQuarantine(OrphanQuarantine):
    """Quarantine backed by a dict instead of the strict Firestore fake.

    Needed for the execute-run counterpart below: with the strict fake, the
    record write fails first and the run correctly aborts before reaching
    storage, so the delete never happens and the test proves nothing.
    """

    def __init__(self, clock, entries=None):
        self.clock = clock
        self.min_age_ms = DAY_MS
        self._entries = dict(entries or {})

    def load(self, identifier):
        return dict(self._entries)

    def save(self, identifier, entries):
        self._entries = dict(entries)

    def review(self, identifier, candidates, persist=True, now_ms=None):
        return super().review(identifier, candidates, persist=persist, now_ms=self.clock[0])


def test_execute_run_would_trip_the_strict_fakes():
    """The counterpart: an execute run on the same fixture DOES reach for the
    delete, so the dry-run results above are a real difference in behaviour and
    not an artefact of a fixture where nothing was ever deletable."""
    storage, catalog = _make()
    clock = [1_000_000]
    quarantine = _MemoryQuarantine(clock, {ORPHAN: clock[0] - DAY_MS})

    SnapshotExpiration(catalog, author="test", quarantine=quarantine).expire_dataset(
        "github.events", dry_run=False
    )

    # The delete was reached and refused. Asserted on the recorded attempt
    # rather than on a raised exception, because the surrounding code catches
    # broadly and swallows it.
    assert f"storage delete: {ORPHAN}" in catalog.violations


def test_a_failed_quarantine_write_aborts_before_any_deletion():
    """Fail-closed, proven against a store that rejects the write.

    The record is written before anything is deleted precisely so this
    ordering holds: if the record cannot be updated, the run has no way to
    show a file was seen twice, and must not delete.
    """
    storage, catalog = _make()
    before = dict(storage)
    clock = [1_000_000]
    _arm_the_quarantine(catalog, [ORPHAN], first_seen_ms=clock[0] - DAY_MS)

    result = SnapshotExpiration(
        catalog, author="test", quarantine=_ClockedQuarantine(catalog, clock)
    ).expire_dataset("github.events", dry_run=False)

    assert result["quarantine_available"] is False
    assert result["deleted_files"] == []
    assert storage == before
    # It stopped at the record write; no storage deletion was ever attempted.
    assert not [v for v in catalog.violations if v.startswith("storage")]
