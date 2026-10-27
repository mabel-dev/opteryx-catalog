"""RESYNC and DETACH, driven through the catalog's own `resync_fork` / `detach_fork`.

`FORKS_DESIGN.md` S8.2, S8.3. `test_fork_clone_mechanics.py` drives the clone
mechanism underneath `clone_dataset`; nothing drove the two maintenance
operations at all. Here the REAL methods run - the drift and no-op refusals,
the re-borrow, the fork block's rewrite, the copy-and-rewrite of a detach - on
the same in-memory world (two datasets, one storage map).

Only the Firestore half is replaced, by the smallest stand-ins that answer it:
dataset lookup by name, the stored schema document, the fork registry and the
metadata save, each recording what it was asked so the tests can hold the
catalog to the order and content of its writes. File copies go through the
catalog's own FileIO fallback (`_copy_object` with no GCS bucket), so a detach
moves real bytes in the storage map.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import pytest
from test_fork_clone_mechanics import FORK
from test_fork_clone_mechanics import FORK_LOCATION
from test_fork_clone_mechanics import UPSTREAM
from test_fork_clone_mechanics import UPSTREAM_LOCATION
from test_fork_clone_mechanics import _clone
from test_fork_clone_mechanics import _data_files
from test_fork_clone_mechanics import _entries
from test_fork_clone_mechanics import _stage
from test_fork_clone_mechanics import _world

import opteryx_catalog.opteryx_catalog as catalog_module
from opteryx_catalog.exceptions import DatasetNotFound
from opteryx_catalog.exceptions import ForkError
from opteryx_catalog.opteryx_catalog import OpteryxCatalog

# What `_stored_schema_of` hands back for the upstream: a resync adopts it.
UPSTREAM_SCHEMA = {"columns": [{"name": "value", "type": "INTEGER"}]}


class _Recorder:
    """The Firestore writes the two operations make, in the order they made them."""

    def __init__(self):
        self.calls = []

    def __call__(self, name):
        def record(*args, **kwargs):
            self.calls.append((name, args, kwargs))

        return record

    def named(self, name):
        return [(args, kwargs) for call, args, kwargs in self.calls if call == name]


def _catalog(monkeypatch, storage, upstream, fork, *, upstream_exists=True):
    """A `personal` workspace catalog holding `fork`, able to see `upstream` in `samples`."""
    workspace, fork_collection, fork_name = FORK.split(".")
    up_workspace, up_collection, up_name = UPSTREAM.split(".")

    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = workspace
    catalog.io = fork.io
    catalog._storage_client = None
    catalog.gcs_bucket = None

    local = {f"{fork_collection}.{fork_name}": fork}
    foreign = {f"{up_collection}.{up_name}": upstream} if upstream_exists else {}

    class _Foreign:
        @staticmethod
        def load_dataset(identifier, load_history=False):
            return foreign.get(identifier)

    catalog.load_dataset = lambda identifier, load_history=False: local.get(identifier)
    catalog._catalog_for = lambda ws: catalog if ws == workspace else _Foreign
    catalog._stored_schema_of = lambda identifier: UPSTREAM_SCHEMA

    recorder = _Recorder()
    catalog.register_fork = recorder("register_fork")
    catalog.deregister_fork = recorder("deregister_fork")
    catalog.save_dataset_metadata = recorder("save_dataset_metadata")
    monkeypatch.setattr(catalog_module, "emit_audit", recorder("emit_audit"))
    return catalog, recorder


def _forked_world(monkeypatch, upstream_files=1):
    """An upstream with `upstream_files` committed files, cloned once."""
    storage, upstream, fork = _world()
    for i in range(upstream_files):
        upstream.add_files(
            [_stage(storage, UPSTREAM_LOCATION, f"part-{i:05d}.parquet", [i])],
            author="gen",
            read_sources=[],
        )
    _clone(upstream, fork)
    catalog, recorder = _catalog(monkeypatch, storage, upstream, fork)
    return storage, upstream, fork, catalog, recorder


def _paths(dataset):
    return sorted(entry["file_path"] for entry in _entries(dataset))


# --------------------------------------------------------------------------
# 1. RESYNC
# --------------------------------------------------------------------------


def test_resync_makes_the_fork_equal_its_upstream_again(monkeypatch):
    storage, upstream, fork, catalog, _ = _forked_world(monkeypatch)
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00001.parquet", [2])],
        author="gen",
        read_sources=[],
    )
    assert _paths(fork) != _paths(upstream)

    catalog.resync_fork(FORK, author="justin")

    assert _paths(fork) == _paths(upstream)


def test_resync_copies_no_data_files(monkeypatch):
    storage, upstream, fork, catalog, _ = _forked_world(monkeypatch)
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00001.parquet", [2])],
        author="gen",
        read_sources=[],
    )
    before = _data_files(storage)

    catalog.resync_fork(FORK, author="justin")

    assert _data_files(storage) == before, "resync copied data files; it must not"


def test_after_a_resync_the_fork_is_in_sync_both_ways(monkeypatch):
    storage, upstream, fork, catalog, _ = _forked_world(monkeypatch)
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00001.parquet", [2])],
        author="gen",
        read_sources=[],
    )

    catalog.resync_fork(FORK, author="justin")

    state = fork.fork_state(upstream=upstream)
    assert state["revisions_behind"] == 0
    assert state["revisions_ahead"] == 0


def test_resync_rests_the_fork_on_the_upstreams_current_snapshot(monkeypatch):
    storage, upstream, fork, catalog, recorder = _forked_world(monkeypatch)
    previous_base = fork.metadata.fork.source.snapshot_id
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00001.parquet", [2])],
        author="gen",
        read_sources=[],
    )
    current = upstream.snapshot(None).snapshot_id

    result = catalog.resync_fork(FORK, author="justin")

    assert fork.metadata.fork.source.snapshot_id == current
    assert result["base_snapshot"] == current
    assert result["previous_base_snapshot"] == previous_base
    assert result["forced"] is False
    # The new pin is registered on the upstream before anything else is written.
    [(args, kwargs)] = recorder.named("register_fork")
    assert args == (UPSTREAM, FORK, current)
    assert recorder.calls[0][0] == "register_fork"


def test_resync_adopts_the_upstreams_schema(monkeypatch):
    storage, upstream, fork, catalog, recorder = _forked_world(monkeypatch)
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00001.parquet", [2])],
        author="gen",
        read_sources=[],
    )

    catalog.resync_fork(FORK, author="justin")

    assert fork.metadata.schema == UPSTREAM_SCHEMA
    [(args, _kwargs)] = recorder.named("save_dataset_metadata")
    assert args[1].schema == UPSTREAM_SCHEMA


def test_resync_refuses_a_fork_with_edits_of_its_own_without_force(monkeypatch):
    storage, upstream, fork, catalog, recorder = _forked_world(monkeypatch)
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00001.parquet", [2])],
        author="gen",
        read_sources=[],
    )
    fork.add_files(
        [_stage(storage, FORK_LOCATION, "own-00000.parquet", [9])],
        author="justin",
        read_sources=[],
    )
    before = _paths(fork)

    with pytest.raises(ForkError, match="FORCE"):
        catalog.resync_fork(FORK, author="justin")

    assert _paths(fork) == before
    assert recorder.calls == []


def test_resync_force_supersedes_the_forks_edits_but_keeps_them_readable(monkeypatch):
    storage, upstream, fork, catalog, _ = _forked_world(monkeypatch)
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00001.parquet", [2])],
        author="gen",
        read_sources=[],
    )
    own = _stage(storage, FORK_LOCATION, "own-00000.parquet", [9])
    fork.add_files([own], author="justin", read_sources=[])
    edited = fork.snapshot(None).snapshot_id

    result = catalog.resync_fork(FORK, author="justin", force=True)

    assert result["forced"] is True
    assert _paths(fork) == _paths(upstream)
    assert own not in _paths(fork)
    superseded = fork._parent_manifest_entries(fork.snapshot(edited))
    assert own in {entry["file_path"] for entry in superseded}
    assert own in storage, "a forced resync deleted the fork's own file"


def test_resync_refuses_a_fork_that_is_already_up_to_date(monkeypatch):
    _storage, _upstream, _fork, catalog, recorder = _forked_world(monkeypatch)

    with pytest.raises(ForkError, match="already up to date"):
        catalog.resync_fork(FORK, author="justin")

    assert recorder.calls == []


def test_resync_refuses_a_dataset_that_is_not_a_fork(monkeypatch):
    storage, upstream, fork = _world()
    fork.add_files(
        [_stage(storage, FORK_LOCATION, "own-00000.parquet", [9])],
        author="justin",
        read_sources=[],
    )
    catalog, _ = _catalog(monkeypatch, storage, upstream, fork)

    with pytest.raises(ForkError, match="not a fork"):
        catalog.resync_fork(FORK, author="justin")


def test_resync_refuses_when_the_upstream_is_gone(monkeypatch):
    storage, upstream, fork = _world()
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00000.parquet", [1])],
        author="gen",
        read_sources=[],
    )
    _clone(upstream, fork)
    catalog, _ = _catalog(monkeypatch, storage, upstream, fork, upstream_exists=False)

    with pytest.raises(DatasetNotFound, match="no longer exists"):
        catalog.resync_fork(FORK, author="justin")


# --------------------------------------------------------------------------
# 2. DETACH
# --------------------------------------------------------------------------


def test_detach_copies_every_borrowed_file_into_the_forks_own_location(monkeypatch):
    storage, upstream, fork, catalog, _ = _forked_world(monkeypatch, upstream_files=2)
    borrowed = _paths(fork)

    result = catalog.detach_fork(FORK, author="justin")

    assert result["files_copied"] == len(borrowed)
    owned = _paths(fork)
    assert all(path.startswith(f"{FORK_LOCATION}/") for path in owned), owned
    assert sorted(storage[path] for path in owned) == sorted(storage[path] for path in borrowed)


def test_detach_leaves_the_upstreams_files_where_they_were(monkeypatch):
    storage, upstream, fork, catalog, _ = _forked_world(monkeypatch, upstream_files=2)
    upstream_files = {path: storage[path] for path in _paths(upstream)}

    catalog.detach_fork(FORK, author="justin")

    assert {path: storage[path] for path in upstream_files} == upstream_files
    assert _paths(upstream) == sorted(upstream_files)


def test_detach_copies_only_what_was_borrowed(monkeypatch):
    storage, upstream, fork, catalog, _ = _forked_world(monkeypatch)
    own = _stage(storage, FORK_LOCATION, "own-00000.parquet", [9])
    fork.add_files([own], author="justin", read_sources=[])

    result = catalog.detach_fork(FORK, author="justin")

    assert result["files_copied"] == 1
    assert own in _paths(fork)


def test_detach_ends_the_fork_and_releases_its_pin(monkeypatch):
    _storage, _upstream, fork, catalog, recorder = _forked_world(monkeypatch)

    catalog.detach_fork(FORK, author="justin")

    assert fork.metadata.fork is None
    [(args, _kwargs)] = recorder.named("save_dataset_metadata")
    assert args[1].fork is None
    assert recorder.named("deregister_fork") == [((UPSTREAM, FORK), {})]
    # The fork block is cleared before the pin is released (S8.3).
    order = [name for name, _a, _k in recorder.calls]
    assert order.index("save_dataset_metadata") < order.index("deregister_fork")


def test_a_detached_dataset_cannot_be_resynced(monkeypatch):
    _storage, _upstream, _fork, catalog, _ = _forked_world(monkeypatch)
    catalog.detach_fork(FORK, author="justin")

    with pytest.raises(ForkError, match="not a fork"):
        catalog.resync_fork(FORK, author="justin")


def test_detach_refuses_a_dataset_that_is_not_a_fork(monkeypatch):
    storage, upstream, fork = _world()
    fork.add_files(
        [_stage(storage, FORK_LOCATION, "own-00000.parquet", [9])],
        author="justin",
        read_sources=[],
    )
    catalog, _ = _catalog(monkeypatch, storage, upstream, fork)

    with pytest.raises(ForkError, match="not a fork"):
        catalog.detach_fork(FORK, author="justin")
