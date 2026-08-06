"""Regression tests for the silent manifest-truncation data loss.

A live snapshot's manifest was deleted from storage. The next commit read it,
got a 404, swallowed the error, and wrote a manifest containing only the newly
added file - orphaning every file committed before it. The snapshot summary
carried the old totals forward independently, so the catalog kept reporting the
pre-loss row count over a table that had lost its history, and garbage
collection then reclaimed the now-unreferenced data files.

Each test here pins one link of that chain.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.metadata import DatasetMetadata, Snapshot
from opteryx_catalog.exceptions import ManifestProtectionError, ManifestReadError


def _build_manifest_bytes(file_path="f1.parquet", record_count=10, file_size=100):
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    columns = {
        "file_path": ([file_path], "VARCHAR"),
        "file_format": (["parquet"], "VARCHAR"),
        "record_count": ([record_count], "INTEGER"),
        "file_size_in_bytes": ([file_size], "INTEGER"),
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
    """In-memory FileIO. `ages` drives the orphan age gate."""

    def __init__(self, mapping: dict, ages: dict | None = None):
        self._mapping = mapping
        self._ages = ages or {}

    def new_input(self, path: str):
        mapping = self._mapping

        class In:
            def open(self):
                from io import BytesIO

                if mapping.get(path) is None:
                    raise FileNotFoundError(path)
                return BytesIO(mapping[path])

        return In()

    def new_output(self, path: str):
        mapping = self._mapping

        class Out:
            def __init__(self):
                from io import BytesIO

                self._buf = BytesIO()

            def create(self):
                return self

            def write(self, data: bytes):
                self._buf.write(data)

            def close(self):
                mapping[path] = self._buf.getvalue()

        return Out()

    def list_files(self, prefix: str):
        return [p for p in list(self._mapping.keys()) if p.startswith(prefix)]

    def list_files_with_age_ms(self, prefix: str):
        return {p: self._ages.get(p, 0) for p in self.list_files(prefix)}

    def delete(self, path: str):
        self._mapping.pop(path, None)


class _RecordingCatalog:
    """Captures the entry list each commit writes to its manifest."""

    def __init__(self, io):
        self.io = io
        self.written = []

    def write_parquet_manifest(self, snapshot_id, entries, location):
        self.written.append(list(entries))
        return f"{location}/metadata/manifest-{snapshot_id}.parquet"

    def save_snapshot(self, identifier, snap):
        pass

    def save_dataset_metadata(self, identifier, metadata):
        pass


def _dataset_with_parent(storage, parent_manifest_path, parent_summary=None, ages=None):
    location = "mem://ichnos/landing/scan_metadata"
    io = _MemIO(storage, ages=ages)
    meta = DatasetMetadata(dataset_identifier="landing.scan_metadata", location=location)
    parent = Snapshot(
        snapshot_id=1000,
        timestamp_ms=1000,
        manifest_list=parent_manifest_path,
        summary=parent_summary,
    )
    meta.snapshots.append(parent)
    meta.current_snapshot_id = 1000
    ds = SimpleDataset(identifier="landing.scan_metadata", _metadata=meta, io=io)
    return ds, io, location


def test_add_files_refuses_to_commit_when_parent_manifest_is_missing():
    """The exact incident: parent manifest 404s, so the commit must not land.

    Previously this wrote a manifest holding only the new entries, silently
    dropping every previously committed file.
    """
    location = "mem://ichnos/landing/scan_metadata"
    parent_manifest = f"{location}/metadata/manifest-1000.parquet"
    storage = {}  # parent manifest deliberately absent

    ds, io, _ = _dataset_with_parent(storage, parent_manifest)
    catalog = _RecordingCatalog(io)
    ds.catalog = catalog

    with pytest.raises(ManifestReadError):
        ds.add_files([], author="test")

    # Nothing was written: no truncated manifest, no snapshot.
    assert catalog.written == []
    assert storage == {}


def test_totals_are_derived_from_the_manifest_not_the_parent_summary():
    """Summary counters must describe the manifest actually written.

    The parent below claims 99 files / 12345 records while its manifest holds a
    single 10-record file. Carrying those numbers forward is what let the
    catalog report 746 rows over a table holding 2.
    """
    location = "mem://ichnos/landing/scan_metadata"
    parent_manifest = f"{location}/metadata/manifest-1000.parquet"
    storage = {parent_manifest: _build_manifest_bytes(record_count=10, file_size=100)}

    ds, io, _ = _dataset_with_parent(
        storage,
        parent_manifest,
        parent_summary={
            "total-data-files": 99,
            "total-records": 12345,
            "total-files-size": 999999,
            "total-data-size": 999999,
        },
    )
    catalog = _RecordingCatalog(io)
    ds.catalog = catalog

    ds.add_files([], author="test")

    committed = ds.metadata.snapshots[-1]
    assert committed.summary["total-data-files"] == 1
    assert committed.summary["total-records"] == 10
    assert committed.summary["total-files-size"] == 100
    # and it matches what actually went into the manifest
    assert len(catalog.written[-1]) == committed.summary["total-data-files"]


def test_protected_file_set_refuses_to_come_back_short():
    """An unreadable retained manifest must not be read as "protects nothing".

    This set is subtracted from the physical files and the remainder deleted,
    so returning a short set turns a reclaim pass into a delete-everything
    pass. Asserted directly on the query rather than through `expire_dataset`,
    because the outer flow has several ways to bail out early and a test that
    merely observes "nothing was deleted" passes whether or not the guard
    exists.
    """
    location = "mem://ichnos/landing/scan_metadata"
    retained_manifest = f"{location}/metadata/manifest-2000.parquet"
    io = _MemIO({retained_manifest: b"not-a-parquet"})

    class _Catalog:
        def __init__(self, io):
            self.io = io

    expiration = SnapshotExpiration(_Catalog(io), author="test")
    snapshots = [Snapshot(snapshot_id=2000, timestamp_ms=2000, manifest_list=retained_manifest)]

    # Protecting files: an unreadable manifest is fatal.
    with pytest.raises(ManifestProtectionError):
        expiration._get_files_in_snapshots(snapshots, required=True)

    # Selecting files to reclaim: the same failure is survivable, because a
    # short set here reclaims less rather than deleting more.
    assert expiration._get_files_in_snapshots(snapshots, required=False) == set()


def test_expire_collection_skips_the_unprotectable_dataset_and_continues():
    """One dataset we cannot reason about must not abort the whole sweep."""
    location = "mem://ws/coll/broken"
    retained_manifest = f"{location}/metadata/manifest-3000.parquet"
    storage = {retained_manifest: b"not-a-parquet"}
    io = _MemIO(storage)

    meta = DatasetMetadata(dataset_identifier="coll.broken", location=location)
    meta.snapshots.append(
        Snapshot(snapshot_id=3000, timestamp_ms=3000, manifest_list=retained_manifest)
    )
    meta.current_snapshot_id = 3000
    ds = SimpleDataset(identifier="coll.broken", _metadata=meta, io=io)

    class _Catalog:
        workspace = "ws"

        def __init__(self, io, dataset):
            self.io = io
            self._dataset = dataset

        def list_datasets(self, collection):
            return ["broken"]

        def load_dataset(self, identifier, load_history=False):
            return self._dataset

        def _snapshots_collection(self, collection, dataset_name):
            raise AssertionError("must not reach snapshot deletion")

    expiration = SnapshotExpiration(_Catalog(io, ds), author="test")
    results = expiration.expire_collection("coll", dry_run=False)

    assert results["datasets_processed"] == 1
    # Recorded as skipped rather than quietly counted as cleaned, so a sweep
    # that could not reason about a dataset says so.
    assert results["datasets_skipped_unprotectable"] == ["coll.broken"]
    assert retained_manifest in storage


def test_deep_clean_holds_back_files_too_new_to_reclaim():
    """Deep clean had no age gate, so it deleted files seconds after they landed.

    `clean_dataset` reads the snapshots, then lists storage. Anything committed
    between those two observations is unreferenced-but-live.
    """
    from opteryx_catalog.catalog.deep_clean import DatasetDeepClean

    location = "mem://ichnos/landing/scan_metadata"
    manifest = f"{location}/metadata/manifest-4000.parquet"
    just_written = f"{location}/data/part-0000-inflight.parquet"

    storage = {manifest: _build_manifest_bytes(), just_written: b"fresh"}
    ages = {just_written: 40 * 60 * 1000}  # 40 minutes, as in the incident

    io = _MemIO(storage, ages=ages)
    meta = DatasetMetadata(dataset_identifier="landing.scan_metadata", location=location)
    meta.snapshots.append(Snapshot(snapshot_id=4000, timestamp_ms=4000, manifest_list=manifest))
    meta.current_snapshot_id = 4000
    ds = SimpleDataset(identifier="landing.scan_metadata", _metadata=meta, io=io)

    class _Catalog:
        def __init__(self, io, dataset):
            self.io = io
            self._dataset = dataset

        def load_dataset(self, identifier, load_history=False):
            return self._dataset

    cleaner = DatasetDeepClean(_Catalog(io, ds))
    cleaner.clean_dataset("landing.scan_metadata", dry_run=False)

    assert just_written in storage


def test_deep_clean_aborts_rather_than_deleting_on_an_unreadable_manifest():
    from opteryx_catalog.catalog.deep_clean import DatasetDeepClean

    location = "mem://ichnos/landing/scan_metadata"
    manifest = f"{location}/metadata/manifest-5000.parquet"
    data_file = f"{location}/data/part-0000-old.parquet"

    storage = {manifest: b"not-a-parquet", data_file: b"real-data"}
    ages = {data_file: 30 * 24 * 60 * 60 * 1000}

    io = _MemIO(storage, ages=ages)
    meta = DatasetMetadata(dataset_identifier="landing.scan_metadata", location=location)
    meta.snapshots.append(Snapshot(snapshot_id=5000, timestamp_ms=5000, manifest_list=manifest))
    meta.current_snapshot_id = 5000
    ds = SimpleDataset(identifier="landing.scan_metadata", _metadata=meta, io=io)

    class _Catalog:
        def __init__(self, io, dataset):
            self.io = io
            self._dataset = dataset

        def load_dataset(self, identifier, load_history=False):
            return self._dataset

    cleaner = DatasetDeepClean(_Catalog(io, ds))
    with pytest.raises(ManifestProtectionError):
        cleaner.clean_dataset("landing.scan_metadata", dry_run=False)

    assert data_file in storage
