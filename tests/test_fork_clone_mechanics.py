"""Cloning moves no bytes, and the fork reads the upstream's files.

`FORKS_DESIGN.md` S4, S6. The catalog-level `clone_dataset` needs Firestore;
what is driven here is the mechanism underneath it against real manifests and
real storage - the upstream's entries committed verbatim onto a second dataset
in a different location.

The assertion that matters is the negative one: after a clone, storage holds
exactly the data files it held before. If that ever fails, something has
started copying, and the whole point of the design has gone.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import pytest
from rugo.parquet import write_parquet
from test_provenance_commits import _DocumentStore
from test_provenance_commits import _MemIO
from test_provenance_commits import _morsel

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Fork
from opteryx_catalog.catalog.metadata import ForkSource
from opteryx_catalog.catalog.metadata import ForkTarget

UPSTREAM = "samples.tpch_sf1.lineitem"
UPSTREAM_LOCATION = "mem://samples/tpch_sf1/lineitem"
FORK = "personal.justin.lineitem"
FORK_LOCATION = "mem://personal/justin/lineitem"


def _world():
    """One storage map, two datasets in different locations."""
    clear_parsed_manifest_cache()
    storage: dict[str, bytes] = {}
    mem_io = _MemIO(storage)

    def _make(identifier, location):
        meta = DatasetMetadata(
            dataset_identifier=identifier, location=location, schema=None, properties={}
        )
        ds = SimpleDataset(identifier=identifier, _metadata=meta)
        ds.io = mem_io
        ds.catalog = _DocumentStore(mem_io)
        return ds

    return storage, _make(UPSTREAM, UPSTREAM_LOCATION), _make(FORK, FORK_LOCATION)


def _stage(storage, location, name, values):
    path = f"{location}/data/{name}"
    storage[path] = write_parquet(_morsel(list(values)), compression="zstd")
    return path


def _data_files(storage):
    return {p for p in storage if p.endswith(".parquet") and "/data/" in p}


def _entries(dataset):
    return dataset._parent_manifest_entries(dataset.snapshot(None))


def _clone(upstream, fork, author="justin"):
    """What `clone_dataset` does to the fork, minus the Firestore half."""
    snapshot = upstream.snapshot(None)
    fork.truncate_and_add_files(
        entries=upstream._parent_manifest_entries(snapshot),
        author=author,
        commit_message=f"CLONE {UPSTREAM} AT VERSION {snapshot.snapshot_id}",
        read_sources=[(UPSTREAM, snapshot.snapshot_id, "version")],
    )
    fork.metadata.fork = Fork(
        source=ForkSource(
            dataset=UPSTREAM,
            snapshot_id=snapshot.snapshot_id,
            sequence_number=int(snapshot.sequence_number or 0),
        ),
        target=ForkTarget(sequence_number=fork.current_sequence_number(), last_sync_ms=1),
        forked_at_ms=1,
        forked_by=author,
    )
    return fork


# --------------------------------------------------------------------------
# 1. No bytes move
# --------------------------------------------------------------------------


def test_a_clone_copies_no_data_files():
    storage, upstream, fork = _world()
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00000.parquet", [1, 2, 3])],
        author="gen",
        read_sources=[],
    )
    before = _data_files(storage)

    _clone(upstream, fork)

    assert _data_files(storage) == before, "cloning copied data files; it must not"


def test_the_fork_manifest_names_the_upstreams_paths():
    storage, upstream, fork = _world()
    path = _stage(storage, UPSTREAM_LOCATION, "part-00000.parquet", [1, 2, 3])
    upstream.add_files([path], author="gen", read_sources=[])

    _clone(upstream, fork)

    assert [e["file_path"] for e in _entries(fork)] == [path]
    assert path.startswith(UPSTREAM_LOCATION), "the borrowed path is the upstream's"


def test_the_fork_carries_the_upstreams_statistics_verbatim():
    # The whole cost the design removes: these numbers were computed once, by
    # the writer that had the bytes. A clone that recomputed them would be the
    # copy path wearing a different name.
    storage, upstream, fork = _world()
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00000.parquet", [1, 2, 3, 4])],
        author="gen",
        read_sources=[],
    )

    _clone(upstream, fork)

    assert _entries(fork) == _entries(upstream)


def test_the_fork_reports_the_upstreams_row_count():
    storage, upstream, fork = _world()
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00000.parquet", [1, 2, 3, 4, 5])],
        author="gen",
        read_sources=[],
    )

    _clone(upstream, fork)

    assert fork.snapshot(None).summary["total-records"] == 5


def test_the_clone_receipt_names_the_upstream_snapshot():
    storage, upstream, fork = _world()
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00000.parquet", [1])],
        author="gen",
        read_sources=[],
    )
    base = upstream.snapshot(None).snapshot_id

    _clone(upstream, fork)

    receipt = fork.snapshot(None).read_sources
    assert receipt == [{"dataset": UPSTREAM, "snapshot-id": base, "resolved-by": "version"}]
    # And the standing source list is exactly the upstream, not an accumulation:
    # "the content IS that content" is a rewrite.
    assert fork.metadata.sources == [UPSTREAM]


# --------------------------------------------------------------------------
# 2. Sync state
# --------------------------------------------------------------------------


def test_a_fresh_fork_is_in_sync_both_ways():
    storage, upstream, fork = _world()
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00000.parquet", [1])],
        author="gen",
        read_sources=[],
    )
    _clone(upstream, fork)

    state = fork.fork_state(upstream=upstream)

    assert state["revisions_behind"] == 0
    assert state["revisions_ahead"] == 0
    assert state["upstream"] == UPSTREAM


def test_a_commit_on_the_upstream_puts_the_fork_behind():
    storage, upstream, fork = _world()
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00000.parquet", [1])],
        author="gen",
        read_sources=[],
    )
    _clone(upstream, fork)

    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00001.parquet", [2])],
        author="gen",
        read_sources=[],
    )

    state = fork.fork_state(upstream=upstream)
    assert state["revisions_behind"] == 1
    assert state["revisions_ahead"] == 0


def test_a_commit_on_the_fork_is_drift():
    storage, upstream, fork = _world()
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00000.parquet", [1])],
        author="gen",
        read_sources=[],
    )
    _clone(upstream, fork)

    fork.add_files(
        [_stage(storage, FORK_LOCATION, "own-00000.parquet", [9])],
        author="justin",
        read_sources=[],
    )

    state = fork.fork_state(upstream=upstream)
    assert state["revisions_ahead"] == 1
    assert state["revisions_behind"] == 0


def test_both_sides_can_move_at_once():
    storage, upstream, fork = _world()
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00000.parquet", [1])],
        author="gen",
        read_sources=[],
    )
    _clone(upstream, fork)
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

    state = fork.fork_state(upstream=upstream)
    assert state["revisions_behind"] == 1
    assert state["revisions_ahead"] == 1


def test_a_plain_dataset_has_no_fork_state():
    storage, upstream, _fork = _world()
    upstream.add_files(
        [_stage(storage, UPSTREAM_LOCATION, "part-00000.parquet", [1])],
        author="gen",
        read_sources=[],
    )

    assert upstream.fork_state() is None


# --------------------------------------------------------------------------
# 3. A fork's own GC never touches what it borrowed
# --------------------------------------------------------------------------


def test_the_forks_expiration_will_not_delete_the_upstreams_files():
    # The fork drifts, so its clone snapshot is superseded and the borrowed
    # files stop being referenced by its head. Its own orphan sweep will
    # propose them - and must refuse to delete them (S5.2).
    storage, upstream, fork = _world()
    borrowed = _stage(storage, UPSTREAM_LOCATION, "part-00000.parquet", [1])
    upstream.add_files([borrowed], author="gen", read_sources=[])
    _clone(upstream, fork)

    deleted = []

    class _IO:
        def delete(self, path):
            deleted.append(path)

    expirer = SnapshotExpiration(catalog=None)
    own = _stage(storage, FORK_LOCATION, "own-00000.parquet", [9])

    assert expirer._delete_file(_IO(), borrowed, FORK_LOCATION) is False
    assert expirer._delete_file(_IO(), own, FORK_LOCATION) is True
    assert borrowed not in deleted


# --------------------------------------------------------------------------
# 4. A fork carries its upstream's schema
# --------------------------------------------------------------------------
#
# Found in production, on a real user's fork: the dataset had six million rows,
# a correct manifest and a correct fork block, and NO SCHEMA - so every page
# that describes it showed nothing at all.
#
# The cause is a trap this catalog sets in several places: `load_dataset`
# resolves `current_schema_id` and leaves `metadata.schema` as None. Reading
# `upstream.metadata.schema` after a load therefore gets None, and passing that
# to `create_dataset` makes a dataset with no schema document rather than
# failing. Both halves are held here - that the schema is carried, and that a
# missing one is refused rather than silently produced.


class _StoredSchemaCatalog:
    """Just enough catalog to drive `_stored_schema_of`'s contract."""

    def __init__(self, schema_id, columns):
        self.workspace = "samples"
        self._schema_id = schema_id
        self._columns = columns

    def _qualify(self, name):
        return name if name.count(".") >= 2 else f"{self.workspace}.{name}"

    def _split_qualified(self, name):
        return tuple(name.split(".", 2))

    def _catalog_for(self, workspace):
        return self

    def _dataset_doc_ref(self, collection, dataset_name):
        outer = self

        class _Schemas:
            def document(self, _id):
                class _D:
                    def get(self_inner):
                        return type(
                            "Doc", (), {"to_dict": lambda _s: {"columns": outer._columns}}
                        )()

                return _D()

        class _Ref:
            def get(self_inner):
                return type(
                    "Doc",
                    (),
                    {"to_dict": lambda _s: {"current-schema-id": outer._schema_id}},
                )()

            def collection(self_inner, _name):
                return _Schemas()

        return _Ref()


COLUMNS = [
    {"name": "n_nationkey", "type": "INT32"},
    {"name": "n_name", "type": "VARCHAR"},
]


def test_the_upstreams_stored_columns_are_carried_across_verbatim():
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    catalog = _StoredSchemaCatalog("sid-1", COLUMNS)

    stored = OpteryxCatalog._stored_schema_of(catalog, "samples.tpch_sf1.nation")

    # The stored spelling, which `create_dataset` accepts directly - NOT a
    # RelationSchema, whose round trip is lossy in exactly this direction.
    assert stored == {"columns": COLUMNS}


def test_an_upstream_with_no_current_schema_is_refused():
    from opteryx_catalog.exceptions import ForkError
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    catalog = _StoredSchemaCatalog(None, COLUMNS)

    with pytest.raises(ForkError, match="no current schema"):
        OpteryxCatalog._stored_schema_of(catalog, "samples.tpch_sf1.nation")


def test_an_empty_schema_document_is_refused():
    # The failure that actually happened, seen from the other side: rather than
    # creating a fork with nothing able to describe it, refuse.
    from opteryx_catalog.exceptions import ForkError
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    catalog = _StoredSchemaCatalog("sid-1", [])

    with pytest.raises(ForkError, match="missing or empty"):
        OpteryxCatalog._stored_schema_of(catalog, "samples.tpch_sf1.nation")


# --- what may be forked at all ------------------------------------------


class _Upstream:
    """The two things `_upstream_entries` asks an upstream about."""

    def __init__(self, identifier, external_catalog, snapshot=None):
        self.identifier = identifier
        self.metadata = DatasetMetadata(
            dataset_identifier=identifier, location="mem://x", schema=None, properties={}
        )
        self.metadata.external_catalog = external_catalog
        self._snapshot = snapshot

    def snapshot(self, snapshot_id=None):
        return self._snapshot

    def _parent_manifest_entries(self, snapshot):
        return []


def test_a_dataset_projected_from_an_external_catalog_cannot_be_cloned():
    """An Iceberg or Postgres relation reaches this catalog as a stub: a
    dataset document with no snapshots, because nothing commits to one. There
    is no manifest of ours to borrow and no fork registry that can stop its
    files moving, so it is refused - and refused for THAT reason, which is the
    point of checking before the snapshot is asked for."""
    from opteryx_catalog.exceptions import ForkError
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    upstream = _Upstream("google.public_data.nyc_taxicab_2021", external_catalog=True)

    with pytest.raises(ForkError, match="projected from an external catalog"):
        OpteryxCatalog._upstream_entries(None, upstream, None)


def test_a_native_dataset_with_nothing_committed_still_says_so():
    """The reason the check above exists is that this message was given for
    both cases. A native dataset really can have no commits yet, and that
    caller is being told something true and actionable - come back after
    writing to it - so the two answers must stay distinct."""
    from opteryx_catalog.exceptions import ForkError
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    upstream = _Upstream("personal.justin.fresh", external_catalog=False)

    with pytest.raises(ForkError, match="no commits yet"):
        OpteryxCatalog._upstream_entries(None, upstream, None)
