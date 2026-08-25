"""Two guards against a lost commit: unique filenames, and a conditional pointer.

Every commit writes NEW immutable files and then moves ONE pointer. Nothing is
overwritten, so the whole race lives in that pointer — plus, before this, in the
filenames, because a snapshot id is not unique across writers.
"""

import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import pytest

from opteryx_catalog.catalog.deletes import delete_vector_path
from opteryx_catalog.catalog.deletes import is_delete_vector_path

from test_mor_deletes import _seed_dataset


# ---------------------------------------------------------------------------
# Filenames: the snapshot id is NOT unique across writers
# ---------------------------------------------------------------------------


def test_two_writers_on_the_same_snapshot_id_get_different_sidecar_paths():
    """`_allocate_snapshot_id` maxes over the writer's OWN in-memory history, so
    two writers holding the same parent compute the SAME id. Without a nonce
    they would write the same path and one would silently replace the other's
    delete state — before either reached a commit that could detect the race."""
    a = delete_vector_path("mem://ws/ds", 1755000000000)
    b = delete_vector_path("mem://ws/ds", 1755000000000)
    assert a != b
    assert is_delete_vector_path(a) and is_delete_vector_path(b)


def test_sidecar_name_still_carries_its_snapshot_id():
    """Age-gated sweeps parse the id back out of the name; the nonce must not
    hide it."""
    path = delete_vector_path("mem://ws/ds", 1755000000000)
    assert re.search(r"deletes-(\d+)(?:-[0-9a-f]+)?\.parquet$", path).group(1) == (
        "1755000000000"
    )


def test_manifest_names_are_unique_per_write():
    ds, storage = _seed_dataset({"f1.parquet": [1, 2, 3]})
    ds.merge_commit([], {"f1.parquet": [0]}, author="a")
    first = ds.snapshot(None).manifest_list
    ds.merge_commit([], {"f1.parquet": [1]}, author="a")
    second = ds.snapshot(None).manifest_list
    assert first != second
    assert re.search(r"manifest-\d+(?:-[0-9a-f]+)?\.parquet$", second)


# ---------------------------------------------------------------------------
# The pointer: a commit is conditional on the parent it was built against
# ---------------------------------------------------------------------------


class _RecordingCatalog:
    """Captures what the commit paths pass to save_dataset_metadata."""

    def __init__(self, inner):
        self._inner = inner
        self.io = inner.io
        self.expectations = []

    write_parquet_manifest = staticmethod(lambda *a, **k: None)

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def save_dataset_metadata(self, identifier, metadata, **kwargs):
        self.expectations.append(kwargs.get("expected_current_snapshot_id"))


def test_a_commit_expects_the_parent_it_was_built_against():
    """Not the snapshot it is creating, and not whatever the pointer says by the
    time the save runs — the value the manifest was built from."""
    ds, _ = _seed_dataset({"f1.parquet": [1, 2, 3]})
    parent = ds.metadata.current_snapshot_id

    recorder = _RecordingCatalog(ds.catalog)
    recorder.write_parquet_manifest = ds.catalog.write_parquet_manifest
    ds.catalog = recorder

    ds.merge_commit([], {"f1.parquet": [0]}, author="a")

    assert recorder.expectations == [parent]
    # And the in-memory pointer moved off it, which is why the expectation had
    # to be captured at the advance rather than re-read at save time.
    assert ds.metadata.current_snapshot_id != parent


def test_successive_commits_each_expect_their_own_parent():
    ds, _ = _seed_dataset({"f1.parquet": [1, 2, 3]})
    seeded = ds.metadata.current_snapshot_id

    ds.merge_commit([], {"f1.parquet": [0]}, author="a")
    first = ds.metadata.current_snapshot_id

    recorder = _RecordingCatalog(ds.catalog)
    recorder.write_parquet_manifest = ds.catalog.write_parquet_manifest
    ds.catalog = recorder
    ds.merge_commit([], {"f1.parquet": [1]}, author="a")

    assert recorder.expectations == [first]
    assert first != seeded


def test_the_expectation_does_not_leak_into_a_later_save():
    """Cleared after use: a save that does not advance the pointer must not
    inherit the last commit's expectation and refuse for no reason."""
    ds, _ = _seed_dataset({"f1.parquet": [1, 2, 3]})
    ds.merge_commit([], {"f1.parquet": [0]}, author="a")

    recorder = _RecordingCatalog(ds.catalog)
    recorder.write_parquet_manifest = ds.catalog.write_parquet_manifest
    ds.catalog = recorder
    ds._persist_metadata()

    from opteryx_catalog.catalog.dataset import _NO_SNAPSHOT_EXPECTATION

    assert recorder.expectations == [_NO_SNAPSHOT_EXPECTATION]
