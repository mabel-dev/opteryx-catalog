import time


# Add local paths like other tests
import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-catalog"))

from opteryx_catalog.catalog.metadata import DatasetMetadata, Snapshot
from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.orphan_quarantine import OrphanQuarantine


class _MemIOWithList:
    def __init__(self, mapping: dict):
        self._mapping = mapping

    def new_input(self, path: str):
        class In:
            def __init__(self, data):
                self._data = data

            def open(self):
                from io import BytesIO

                if self._data is None:
                    raise FileNotFoundError(path)
                return BytesIO(self._data)

        return In(self._mapping.get(path))

    def new_output(self, path: str):
        class Out:
            def __init__(self, mapping, path):
                from io import BytesIO

                self._buf = BytesIO()
                self._mapping = mapping
                self._path = path

            def write(self, data: bytes):
                self._buf.write(data)

            def close(self):
                self._mapping[self._path] = self._buf.getvalue()

            def create(self):
                return self

        return Out(self._mapping, path)

    def list_files(self, prefix: str):
        return [p for p in list(self._mapping.keys()) if p.startswith(prefix)]

    def delete(self, path: str):
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
        return True


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
        # Backs the orphan quarantine, which expiration needs in order to
        # delete anything at all - without a readable record it cannot show a
        # file has been seen twice, so it deletes nothing.
        self.store = {}

    def load_dataset(self, identifier: str, load_history: bool = False):
        return self._dataset

    def _dataset_doc_ref(self, collection, dataset_name):
        return _FakeDatasetDoc(self.store, f"{collection}/{dataset_name}")

    def _snapshots_collection(self, collection, dataset_name):
        return _FakeCollection(self.store, f"{collection}/{dataset_name}/snapshots")


def _build_manifest_bytes():
    """A real, parseable manifest for the retained snapshot.

    Expiration now refuses to delete anything for a dataset whose retained
    snapshots' manifests it cannot read, because an unreadable manifest means
    the set of protected files is incomplete and everything looks orphaned.
    This fixture used to be the byte string b"x", which only worked because the
    read failure was swallowed - exactly the behaviour that let a real dataset
    lose its history.
    """
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    columns = {
        "file_path": (["f1.parquet"], "VARCHAR"),
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


DAY_MS = 24 * 60 * 60 * 1000


class _ClockedQuarantine(OrphanQuarantine):
    """Quarantine driven by a test-controlled clock rather than wall time."""

    def __init__(self, catalog, clock):
        super().__init__(catalog, min_age_ms=DAY_MS)
        self.clock = clock

    def review(self, identifier, candidates, persist=True, now_ms=None):
        return super().review(identifier, candidates, persist=persist, now_ms=self.clock[0])


def _dataset_with_orphan_manifest():
    """One referenced manifest, one unreferenced manifest two days old."""
    now_ms = int(time.time() * 1000)
    dataset_location = "mem://github/events"

    recent_ts = now_ms - (60 * 60 * 1000)  # 1 hour ago
    old_ts = now_ms - (2 * DAY_MS)  # 2 days ago

    manifest_recent = f"{dataset_location}/metadata/manifest-{recent_ts}.parquet"
    manifest_old = f"{dataset_location}/metadata/manifest-{old_ts}.parquet"

    # The retained manifest must be readable; the orphan's contents are never
    # parsed (it is selected by filename age alone), so a stub is fine there.
    storage = {manifest_recent: _build_manifest_bytes(), manifest_old: b"y"}
    io = _MemIOWithList(storage)

    # Dataset metadata references only the recent manifest
    meta = DatasetMetadata(dataset_identifier="github.events", location=dataset_location)
    snap = Snapshot(snapshot_id=recent_ts, timestamp_ms=recent_ts, manifest_list=manifest_recent)
    meta.snapshots.append(snap)
    meta.current_snapshot_id = recent_ts

    ds = SimpleDataset(identifier="github.events", _metadata=meta, io=io)
    return storage, _FakeCatalog(io, ds), manifest_old, manifest_recent


def test_orphan_manifest_is_quarantined_on_first_sight():
    """Manifests get the same two-strike treatment as data files.

    Deleting a manifest that something still references is the truncation
    failure this module exists to avoid, so age alone is not enough to justify
    it - the file has to be seen unreferenced twice.
    """
    storage, catalog, manifest_old, manifest_recent = _dataset_with_orphan_manifest()
    clock = [1_000_000]
    expiration = SnapshotExpiration(
        catalog, author="test", quarantine=_ClockedQuarantine(catalog, clock)
    )

    result = expiration.expire_dataset("github.events", dry_run=False)

    assert result is not None
    assert result.get("deleted_manifests", []) == []
    assert result["orphans_newly_quarantined"] == 1
    assert manifest_old in storage
    assert manifest_recent in storage


def test_orphan_manifest_is_deleted_on_second_sight():
    storage, catalog, manifest_old, manifest_recent = _dataset_with_orphan_manifest()
    clock = [1_000_000]
    expiration = SnapshotExpiration(
        catalog, author="test", quarantine=_ClockedQuarantine(catalog, clock)
    )

    expiration.expire_dataset("github.events", dry_run=False)
    assert manifest_old in storage

    clock[0] += DAY_MS
    result = expiration.expire_dataset("github.events", dry_run=False)

    assert manifest_old in result.get("deleted_manifests", [])
    assert manifest_old not in storage
    # The referenced manifest is never a candidate.
    assert manifest_recent in storage


def test_dry_run_plans_what_would_actually_be_deleted():
    """A plan reports the deletions an execute run would perform right now.

    With no prior sighting on record that is nothing, which is the honest
    answer - and the dry run must not earn the file its first strike either.
    """
    storage, catalog, manifest_old, _ = _dataset_with_orphan_manifest()
    clock = [1_000_000]
    quarantine = _ClockedQuarantine(catalog, clock)
    expiration = SnapshotExpiration(catalog, author="test", quarantine=quarantine)

    plan = expiration.expire_dataset("github.events", dry_run=True)

    assert plan is not None
    assert plan.get("orphaned_manifests_count", 0) == 0
    assert plan.get("manifests_to_delete", []) == []
    assert manifest_old in storage
    assert quarantine.load("github.events") == {}

    # Once the file has a strike on record, the plan reports it.
    expiration.expire_dataset("github.events", dry_run=False)
    clock[0] += DAY_MS
    plan = expiration.expire_dataset("github.events", dry_run=True)

    assert manifest_old in plan.get("manifests_to_delete", [])
    assert manifest_old in storage
