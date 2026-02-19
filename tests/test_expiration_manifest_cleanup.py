import time

import pytest

# Add local paths like other tests
import os, sys
sys.path.insert(0, os.path.join(sys.path[0], ".."))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))
sys.path.insert(1, os.path.join(sys.path[0], "../pyiceberg-firestore-gcs"))

from opteryx_catalog.catalog.metadata import DatasetMetadata, Snapshot
from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.expiration import SnapshotExpiration


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


class _FakeCatalog:
    def __init__(self, io, dataset):
        self.io = io
        self._dataset = dataset

    def load_dataset(self, identifier: str, load_history: bool = False):
        return self._dataset

    def _snapshots_collection(self, collection, dataset_name):
        class _Coll:
            def document(self, _id):
                class _Doc:
                    def delete(self):
                        return True

                return _Doc()

        return _Coll()


def test_manifest_orphan_cleanup_dry_run_and_execute():
    now_ms = int(time.time() * 1000)
    dataset_location = "mem://github/events"

    # Create two manifest files in storage: one recent (referenced), one old (orphan)
    recent_ts = now_ms - (60 * 60 * 1000)  # 1 hour ago
    old_ts = now_ms - (2 * 24 * 60 * 60 * 1000)  # 2 days ago

    manifest_recent = f"{dataset_location}/metadata/manifest-{recent_ts}.parquet"
    manifest_old = f"{dataset_location}/metadata/manifest-{old_ts}.parquet"

    storage = {manifest_recent: b"x", manifest_old: b"y"}
    io = _MemIOWithList(storage)

    # Dataset metadata references only the recent manifest
    meta = DatasetMetadata(dataset_identifier="github.events", location=dataset_location)
    snap = Snapshot(snapshot_id=recent_ts, timestamp_ms=recent_ts, manifest_list=manifest_recent)
    meta.snapshots.append(snap)
    meta.current_snapshot_id = recent_ts

    ds = SimpleDataset(identifier="github.events", _metadata=meta, io=io)

    catalog = _FakeCatalog(io, ds)
    expiration = SnapshotExpiration(catalog, author="test")

    # Dry-run should report the old manifest as candidate (older than 1 day)
    plan = expiration.expire_dataset("github.events", dry_run=True)
    assert plan is not None
    assert plan.get("orphaned_manifests_count", 0) == 1
    assert manifest_old in plan.get("manifests_to_delete", [])

    # Dry-run must not delete anything
    assert manifest_old in storage

    # Execute should remove the old manifest from storage
    result = expiration.expire_dataset("github.events", dry_run=False)
    assert result is not None
    assert manifest_old in result.get("deleted_manifests", [])
    # Ensure storage no longer contains the orphan manifest
    assert manifest_old not in storage
