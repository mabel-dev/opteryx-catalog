"""Vector indexes in the catalog: definitions, sidecar references and their lifecycle.

Definitions live in the dataset's `indexes` subcollection; sidecars are referenced per data
file from the manifest (three parallel columns) and live and die with that file, like
delete vectors. What each group protects:

  * definitions — validated on the way in (refuse, never coerce); `async` by default
    (ruled 2026-10-02); one name per dataset, enforced in the create transaction; ALTER
    changes only the build mode;
  * sidecar commits — attach/replace per index, never disturb other indexes or columns,
    refuse files retired since the build was planned, and a drop detaches only its own;
  * lifecycle — every other commit carries references forward (append, refresh), deep
    clean and expiry treat them as live, and time travel sees each snapshot's own set;
  * accounting (C1b, design §5.5) — every index file carries the sizes the builder
    wrote (refused without them); the summary's index counters follow every commit and
    never touch the data totals; expiry counts the recorded sizes;
  * the maintenance lease (§5.7) — one holder at a time, refused loudly, claimable once
    expired, and a late renew/release never acts on someone else's claim.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import pytest

from opteryx_catalog.catalog.vector_indexes import IndexFiles
from opteryx_catalog.catalog.vector_indexes import index_files
from opteryx_catalog.catalog.vector_indexes import index_refs
from opteryx_catalog.catalog.vector_indexes import is_vector_index_path
from opteryx_catalog.catalog.vector_indexes import new_index_definition
from opteryx_catalog.catalog.vector_indexes import vector_index_path
from opteryx_catalog.exceptions import DatasetNotFound
from opteryx_catalog.exceptions import MaintenanceLeaseHeld
from opteryx_catalog.exceptions import MaintenanceLeaseLost
from opteryx_catalog.exceptions import VectorIndexAlreadyExists
from opteryx_catalog.exceptions import VectorIndexNotFound

import test_rollback as fake
from test_commit_pointer_atomic import _AtomicTransaction
from test_mor_deletes import LOCATION
from test_mor_deletes import _current_entries
from test_mor_deletes import _make_morsel
from test_mor_deletes import _seed_dataset

INDEX_A = "a" * 32
INDEX_B = "b" * 32
IDENTITY = "minilm-l6-v2:256:sha256:" + "0" * 64


def _define(**overrides):
    args = dict(
        name="Docs_Idx", column="body", method="ivf", metric="cosine", build=None,
        clusters=0, embedding_identity=IDENTITY, dimensions=384,
        author="tester", created_at_ms=1,
    )
    args.update(overrides)
    return new_index_definition(**args)


# --- definitions -----------------------------------------------------------


def test_definition_defaults_and_normalises():
    record = _define()
    assert record["name"] == "docs_idx"
    assert record["build"] == "async"
    assert len(record["index-id"]) == 32
    assert record["embedding-identity"] == IDENTITY


@pytest.mark.parametrize(
    "overrides",
    [
        {"method": "hnsw"},
        {"metric": "euclidean"},
        {"build": "eventually"},
        {"clusters": -1},
        {"dimensions": 0},
        {"embedding_identity": ""},
        {"name": "1bad"},
        {"name": "has-hyphen"},
        {"column": ""},
        {"author": ""},
    ],
)
def test_definition_refuses_bad_fields(overrides):
    with pytest.raises(ValueError):
        _define(**overrides)


def _catalog_with_dataset():
    catalog = fake._catalog()
    catalog.firestore_client.transaction = lambda: _AtomicTransaction()
    fake._dataset(catalog, head=100)
    return catalog


def test_create_list_get_alter():
    catalog = _catalog_with_dataset()
    created = catalog.create_vector_index(
        fake.IDENTIFIER, "Second", "body", embedding_identity=IDENTITY, dimensions=384, author="t"
    )
    catalog.create_vector_index(
        fake.IDENTIFIER, "first", "title", embedding_identity=IDENTITY, dimensions=384,
        author="t", build="sync",
    )
    assert [i["name"] for i in catalog.list_vector_indexes(fake.IDENTIFIER)] == ["first", "second"]
    assert catalog.get_vector_index(fake.IDENTIFIER, "SECOND")["index-id"] == created["index-id"]

    altered = catalog.alter_vector_index(fake.IDENTIFIER, "second", build="sync", author="t")
    assert altered["build"] == "sync"
    assert altered["index-id"] == created["index-id"]
    assert catalog.get_vector_index(fake.IDENTIFIER, "second")["build"] == "sync"


def test_create_refuses_a_duplicate_name_and_a_missing_dataset():
    catalog = _catalog_with_dataset()
    catalog.create_vector_index(
        fake.IDENTIFIER, "idx", "body", embedding_identity=IDENTITY, dimensions=384, author="t"
    )
    with pytest.raises(VectorIndexAlreadyExists):
        catalog.create_vector_index(
            fake.IDENTIFIER, "IDX", "other", embedding_identity=IDENTITY, dimensions=384, author="t"
        )
    with pytest.raises(DatasetNotFound):
        catalog.create_vector_index(
            "reports.absent", "idx", "body", embedding_identity=IDENTITY, dimensions=384, author="t"
        )


def test_alter_and_get_refuse_unknown_index_or_mode():
    catalog = _catalog_with_dataset()
    with pytest.raises(VectorIndexNotFound):
        catalog.get_vector_index(fake.IDENTIFIER, "nope")
    with pytest.raises(VectorIndexNotFound):
        catalog.alter_vector_index(fake.IDENTIFIER, "nope", build="sync", author="t")
    catalog.create_vector_index(
        fake.IDENTIFIER, "idx", "body", embedding_identity=IDENTITY, dimensions=384, author="t"
    )
    with pytest.raises(ValueError):
        catalog.alter_vector_index(fake.IDENTIFIER, "idx", build="later", author="t")


# --- sidecar paths ---------------------------------------------------------


def test_sidecar_paths_are_unique_and_recognised():
    a = vector_index_path(LOCATION, INDEX_A, "mem://ws/mor/data/f1.parquet")
    b = vector_index_path(LOCATION, INDEX_A, "mem://ws/mor/data/f1.parquet")
    assert a != b
    for path in (a, b):
        assert path.endswith(".vidx") and is_vector_index_path(path), path
    assert f"/index/{INDEX_A}/f1-" in a
    with pytest.raises(ValueError):
        vector_index_path(LOCATION, "not-an-id", "f1.parquet")


# --- sidecar commits -------------------------------------------------------


FILE_BYTES, FOOTER_BYTES, LOGICAL_BYTES = 1000, 100, 1000


def _files(index_id, data_path):
    return index_files(
        vector_index_path(LOCATION, index_id, data_path),
        file_bytes=FILE_BYTES, footer_bytes=FOOTER_BYTES, logical_bytes=LOGICAL_BYTES,
    )


def _attach(ds, index_id, *paths):
    files = {p: _files(index_id, p) for p in paths}
    ds.commit_vector_index_files(index_id, files, author="builder", agent="test")
    return files


def test_an_index_build_commit_message_names_the_index_not_its_id():
    """It is what readers see as the latest commit's description."""
    ds, _ = _seed_dataset({"mem://f1.parquet": [1, 2], "mem://f2.parquet": [3]})
    files = {p: _files(INDEX_A, p) for p in ("mem://f1.parquet", "mem://f2.parquet")}
    ds.commit_vector_index_files(
        INDEX_A, files, author="builder", agent="test", index_name="body_idx"
    )
    assert ds.snapshot(None).commit_message == "Indexed 2 files for vector index body_idx"

    _attach(ds, INDEX_B, "mem://f1.parquet")
    # No name given and no catalog definition to find it in: the id stands in.
    assert ds.snapshot(None).commit_message == f"Indexed 1 file for vector index {INDEX_B}"


def _refs(ds):
    return {e["file_path"]: index_refs(e) for e in _current_entries(ds)}


def test_commit_attaches_and_replaces_only_its_own_index():
    ds, _ = _seed_dataset({"mem://f1.parquet": [1, 2], "mem://f2.parquet": [3]})
    first_a = _attach(ds, INDEX_A, "mem://f1.parquet", "mem://f2.parquet")
    b = _attach(ds, INDEX_B, "mem://f1.parquet")
    second_a = _attach(ds, INDEX_A, "mem://f1.parquet")     # rebuild f1 for A

    refs = _refs(ds)
    assert refs["mem://f1.parquet"] == {
        INDEX_A: second_a["mem://f1.parquet"], INDEX_B: b["mem://f1.parquet"],
    }
    assert refs["mem://f2.parquet"] == {INDEX_A: first_a["mem://f2.parquet"]}
    snap = ds.snapshot(None)
    assert snap.operation_type == "index-build"
    assert snap.summary["added-data-files"] == 0
    assert snap.summary["total-data-files"] == 2


def test_commit_refuses_a_file_not_in_the_current_snapshot():
    ds, _ = _seed_dataset({"mem://f1.parquet": [1, 2]})
    before = ds.metadata.current_snapshot_id
    with pytest.raises(ValueError, match="not in the current snapshot"):
        _attach(ds, INDEX_A, "mem://gone.parquet")
    assert ds.metadata.current_snapshot_id == before


def test_remove_detaches_only_that_index_and_is_idempotent():
    ds, _ = _seed_dataset({"mem://f1.parquet": [1, 2]})
    _attach(ds, INDEX_A, "mem://f1.parquet")
    b = _attach(ds, INDEX_B, "mem://f1.parquet")
    assert ds.remove_vector_index_files(INDEX_A, author="t") is not None
    assert ds.snapshot(None).operation_type == "index-drop"
    assert _refs(ds)["mem://f1.parquet"] == {INDEX_B: b["mem://f1.parquet"]}
    assert ds.remove_vector_index_files(INDEX_A, author="t") is None   # nothing left to drop


def test_build_plan_lists_the_uncovered_files_with_size_deletes_and_new_paths():
    ds, _ = _seed_dataset({"mem://f1.parquet": [1, 2, 3], "mem://f2.parquet": [4], "mem://f3.parquet": [5]})
    _attach(ds, INDEX_A, "mem://f2.parquet")
    _attach(ds, INDEX_B, "mem://f1.parquet")                # another index covers nothing of A's
    ds.delete_rows({"mem://f1.parquet": [0, 2]}, author="tester")
    sizes = {e["file_path"]: e["file_size_in_bytes"] for e in _current_entries(ds)}

    plan = ds.vector_index_build_plan(INDEX_A)
    assert [t.data_file for t in plan] == ["mem://f1.parquet", "mem://f3.parquet"]
    f1, f3 = plan
    assert (f1.data_bytes, f1.deleted) == (sizes["mem://f1.parquet"], (0, 2))
    assert (f3.data_bytes, f3.deleted) == (sizes["mem://f3.parquet"], ())
    for task in plan:
        assert task.path.startswith(f"{LOCATION}/index/{INDEX_A}/")
        assert task.path.endswith(".vidx") and is_vector_index_path(task.path)
    assert plan[0].path != ds.vector_index_build_plan(INDEX_A)[0].path   # minted fresh

    _attach(ds, INDEX_A, "mem://f1.parquet", "mem://f3.parquet")
    assert ds.vector_index_build_plan(INDEX_A) == []        # up to date


# --- lifecycle -------------------------------------------------------------


def test_append_carries_references_forward():
    ds, _ = _seed_dataset({"mem://f1.parquet": [1, 2]})
    files = _attach(ds, INDEX_A, "mem://f1.parquet")
    ds.append(_make_morsel([7, 8]), author="tester")
    refs = _refs(ds)
    assert refs["mem://f1.parquet"] == {INDEX_A: files["mem://f1.parquet"]}
    assert [r for p, r in refs.items() if p != "mem://f1.parquet"] == [{}]   # new file: unindexed


def test_statistics_refresh_carries_references():
    ds, _ = _seed_dataset({"mem://f1.parquet": [1, 2]})
    files = _attach(ds, INDEX_A, "mem://f1.parquet")
    ds.refresh_manifest(agent="test", author="t")
    assert _refs(ds)["mem://f1.parquet"] == {INDEX_A: files["mem://f1.parquet"]}


def test_time_travel_sees_each_snapshots_own_references():
    from opteryx_catalog.catalog.manifest import read_manifest_rows

    ds, _ = _seed_dataset({"mem://f1.parquet": [1, 2]})
    seeded = ds.snapshot(None)
    _attach(ds, INDEX_A, "mem://f1.parquet")
    with ds.io.new_input(seeded.manifest_list).open() as f:
        old = read_manifest_rows(f.read())
    assert index_refs(old[0]) == {}


def test_deep_clean_and_expiry_protect_sidecars():
    from opteryx_catalog.catalog.deep_clean import DatasetDeepClean
    from opteryx_catalog.catalog.expiration import SnapshotExpiration

    ds, _ = _seed_dataset({"mem://f1.parquet": [1, 2]})
    path = _attach(ds, INDEX_A, "mem://f1.parquet")["mem://f1.parquet"].path

    class _Cat:
        io = ds.io

    protected = DatasetDeepClean(_Cat()).get_all_manifest_files(ds.metadata.snapshots)
    assert path in set(protected)

    mgr = SnapshotExpiration.__new__(SnapshotExpiration)
    mgr.catalog = _Cat()
    kept = mgr._get_file_sizes_in_snapshots(ds.metadata.snapshots, required=True)
    assert path in set(kept)


def test_inconsistent_columns_are_refused():
    with pytest.raises(ValueError, match="inconsistent"):
        index_refs({"file_path": "x", "vidx_ids": [INDEX_A], "vidx_paths": []})
    # Paths without their sizes are as inconsistent as a missing path.
    with pytest.raises(ValueError, match="inconsistent"):
        index_refs({"file_path": "x", "vidx_ids": [INDEX_A], "vidx_paths": ["v"], "vidx_bytes": [1]})


def test_the_retired_two_file_columns_read_as_not_indexed():
    """A manifest written before 2026-10-04 carries the skene pair's six columns. This
    reader does not know them: the file is not indexed (its old files are orphans the
    sweeps reclaim - they still classify as index files), never an error."""
    assert index_refs({
        "file_path": "x", "vector_index_ids": [INDEX_A], "vector_index_vectors": ["v.vectors.skene"],
        "vector_index_centroids": ["c.centroids.skene"], "vector_index_vectors_bytes": [1],
        "vector_index_centroids_bytes": [1], "vector_index_logical_bytes": [2],
    }) == {}
    assert is_vector_index_path(f"{LOCATION}/index/{INDEX_A}/f-0a1b.vectors.skene")
    assert is_vector_index_path(f"{LOCATION}/index/{INDEX_A}/f-0a1b.centroids.skene")


# --- accounting (C1b) ------------------------------------------------------


@pytest.mark.parametrize(
    "sizes",
    [
        {"file_bytes": 0, "footer_bytes": 1, "logical_bytes": 1},
        {"file_bytes": 10, "footer_bytes": -1, "logical_bytes": 1},
        {"file_bytes": 10, "footer_bytes": 1, "logical_bytes": None},
        {"file_bytes": True, "footer_bytes": 1, "logical_bytes": 1},
        {"file_bytes": 1.5, "footer_bytes": 1, "logical_bytes": 1},
    ],
)
def test_an_index_file_needs_its_sizes(sizes):
    with pytest.raises(ValueError, match="positive integer"):
        index_files("v", **sizes)


def test_a_footer_cannot_be_the_whole_file():
    with pytest.raises(ValueError, match="smaller than file_bytes"):
        index_files("v", file_bytes=10, footer_bytes=10, logical_bytes=10)


def test_a_commit_without_sizes_is_refused():
    ds, _ = _seed_dataset({"mem://f1.parquet": [1, 2]})
    before = ds.metadata.current_snapshot_id
    path = vector_index_path(LOCATION, INDEX_A, "mem://f1.parquet")
    with pytest.raises(ValueError, match="must be IndexFiles"):
        ds.commit_vector_index_files(INDEX_A, {"mem://f1.parquet": (path, 1, 1)}, author="b", agent="t")
    with pytest.raises(ValueError, match="positive integer"):
        ds.commit_vector_index_files(
            INDEX_A, {"mem://f1.parquet": IndexFiles(path, 0, 1, 1)}, author="b", agent="t",
        )
    assert ds.metadata.current_snapshot_id == before


def test_sizes_round_trip_through_the_manifest():
    ds, _ = _seed_dataset({"mem://f1.parquet": [1, 2]})
    files = _attach(ds, INDEX_A, "mem://f1.parquet")
    (stored,) = _refs(ds)["mem://f1.parquet"].values()
    assert stored == files["mem://f1.parquet"]
    assert (stored.file_bytes, stored.footer_bytes, stored.logical_bytes) == (
        FILE_BYTES, FOOTER_BYTES, LOGICAL_BYTES,
    )


def _counters(ds):
    summary = ds.snapshot(None).summary
    return (
        summary["total-index-files"], summary["total-index-size"], summary["total-index-data-size"]
    )


def _data_totals(ds):
    summary = ds.snapshot(None).summary
    return summary["total-data-files"], summary["total-files-size"], summary["total-data-size"]


def test_summary_counters_follow_build_drop_append_and_refresh():
    ds, _ = _seed_dataset({"mem://f1.parquet": [1, 2], "mem://f2.parquet": [3]})
    ds.refresh_manifest(agent="test", author="t")         # the seed's summary is hand-made
    data = _data_totals(ds)
    assert _counters(ds) == (0, 0, 0)

    _attach(ds, INDEX_A, "mem://f1.parquet", "mem://f2.parquet")
    _attach(ds, INDEX_B, "mem://f1.parquet")
    assert _counters(ds) == (3, 3 * FILE_BYTES, 3 * LOGICAL_BYTES)
    assert _data_totals(ds) == data                       # an index never moves the data figures

    _attach(ds, INDEX_A, "mem://f1.parquet")              # a rebuild replaces, never adds
    assert _counters(ds) == (3, 3 * FILE_BYTES, 3 * LOGICAL_BYTES)

    ds.remove_vector_index_files(INDEX_A, author="t")
    assert _counters(ds) == (1, FILE_BYTES, LOGICAL_BYTES)
    assert _data_totals(ds) == data

    ds.append(_make_morsel([7, 8]), author="tester")      # carried; the new file is unindexed
    assert _counters(ds) == (1, FILE_BYTES, LOGICAL_BYTES)
    ds.refresh_manifest(agent="test", author="t")
    assert _counters(ds) == (1, FILE_BYTES, LOGICAL_BYTES)


def _compaction_output(storage, values, name="out"):
    from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry_from_bytes
    from rugo.parquet import write_parquet

    out = f"mem://ws/mor/data/{name}.parquet"
    data = write_parquet(_make_morsel(values), compression="zstd")
    storage[out] = data
    return out, build_parquet_manifest_entry_from_bytes(data, out, len(data)).to_dict()


def test_compaction_carries_index_files_in_the_same_commit():
    """§5.6: indexed inputs give an output whose CARRIED index files land in the
    compaction commit itself; the retired inputs' index files leave the head with them."""
    ds, storage = _seed_dataset({"mem://f1.parquet": [1, 2], "mem://f2.parquet": [3]})
    _attach(ds, INDEX_A, "mem://f1.parquet", "mem://f2.parquet")
    out, entry = _compaction_output(storage, [1, 2, 3])
    carried = _files(INDEX_A, out)
    ds.compaction_commit(
        entries=[entry], retired_files=["mem://f1.parquet", "mem://f2.parquet"], author="tester",
        index_files={out: {INDEX_A: carried}},
    )
    assert _refs(ds) == {out: {INDEX_A: carried}}
    assert _counters(ds) == (1, FILE_BYTES, LOGICAL_BYTES)


@pytest.mark.parametrize(
    "carry, message",
    [
        (lambda out: None, "must keep exactly its inputs' index coverage"),        # dropped coverage
        (lambda out: {out: {INDEX_A: _files(INDEX_A, out), INDEX_B: _files(INDEX_B, out)}}, "exactly"),
        (lambda out: {"mem://other.parquet": {INDEX_A: _files(INDEX_A, out)}}, "does not write"),
        (lambda out: {out: {INDEX_A: (_files(INDEX_A, out)[0], "c")}}, "must be IndexFiles"),
    ],
)
def test_compaction_refuses_to_change_index_coverage(carry, message):
    from opteryx_catalog.exceptions import CompactionInvariantError

    ds, storage = _seed_dataset({"mem://f1.parquet": [1, 2]})
    _attach(ds, INDEX_A, "mem://f1.parquet")
    head = ds.metadata.current_snapshot_id
    out, entry = _compaction_output(storage, [1, 2])
    with pytest.raises((CompactionInvariantError, ValueError), match=message):
        ds.compaction_commit(
            entries=[entry], retired_files=["mem://f1.parquet"], author="tester",
            index_files=carry(out),
        )
    assert ds.metadata.current_snapshot_id == head


def test_compaction_refuses_inputs_with_different_coverage():
    from opteryx_catalog.exceptions import CompactionInvariantError

    ds, storage = _seed_dataset({"mem://f1.parquet": [1, 2], "mem://f2.parquet": [3]})
    _attach(ds, INDEX_A, "mem://f1.parquet")
    out, entry = _compaction_output(storage, [1, 2, 3])
    with pytest.raises(CompactionInvariantError, match="same vector index coverage"):
        ds.compaction_commit(
            entries=[entry], retired_files=["mem://f1.parquet", "mem://f2.parquet"], author="tester",
        )


def test_compaction_of_unindexed_files_stays_unindexed():
    ds, storage = _seed_dataset({"mem://f1.parquet": [1, 2], "mem://f2.parquet": [3]})
    _attach(ds, INDEX_A, "mem://f2.parquet")
    out, entry = _compaction_output(storage, [1, 2])
    ds.compaction_commit(entries=[entry], retired_files=["mem://f1.parquet"], author="tester")
    assert _refs(ds)[out] == {}


def test_expiry_counts_the_recorded_sizes():
    from opteryx_catalog.catalog.expiration import SnapshotExpiration

    ds, _ = _seed_dataset({"mem://f1.parquet": [1, 2]})
    files = _attach(ds, INDEX_A, "mem://f1.parquet")["mem://f1.parquet"]

    class _Cat:
        io = ds.io

    mgr = SnapshotExpiration.__new__(SnapshotExpiration)
    mgr.catalog = _Cat()
    sizes = mgr._get_file_sizes_in_snapshots(ds.metadata.snapshots, required=True)
    assert sizes[files.path] == FILE_BYTES


# --- maintenance lease (§5.7) ------------------------------------------------


class _LeaseTransaction(_AtomicTransaction):
    """The atomic fake plus `delete`, staged like `set`."""

    def delete(self, ref):
        self.writes.append((ref, ("delete",)))

    def _commit(self):
        for ref, data in self.writes:
            if data == ("delete",):
                ref._data, ref._exists = {}, False
            elif type(data) is tuple:
                ref._data, ref._exists = dict(data[1]), True
            else:
                ref.update(data)
        self.committed = True
        return []


@pytest.fixture
def lease_catalog(monkeypatch):
    import opteryx_catalog.opteryx_catalog as module

    clock = {"ms": 1_000_000}
    monkeypatch.setattr(module.time, "time", lambda: clock["ms"] / 1000)
    catalog = fake._catalog()
    catalog.firestore_client.transaction = lambda: _LeaseTransaction()
    fake._dataset(catalog, head=100)
    return catalog, clock


def _stored_lease(catalog):
    coll, name = fake.IDENTIFIER.split(".", 1)
    doc = catalog._lease_ref(coll, name).get()
    return doc.to_dict() if doc.exists else None


def test_lease_is_exclusive_and_refused_loudly(lease_catalog):
    catalog, _ = lease_catalog
    lease = catalog.claim_maintenance_lease(
        fake.IDENTIFIER, holder="compactor-1", operation="compaction", ttl_seconds=60
    )
    assert _stored_lease(catalog)["claim-id"] == lease.claim_id
    with pytest.raises(MaintenanceLeaseHeld, match="compaction by compactor-1"):
        catalog.claim_maintenance_lease(
            fake.IDENTIFIER, holder="refresh-index", operation="index-build", ttl_seconds=60
        )
    assert _stored_lease(catalog)["claim-id"] == lease.claim_id      # the refusal wrote nothing


def test_an_expired_lease_is_claimable_and_the_late_holder_loses_it(lease_catalog):
    catalog, clock = lease_catalog
    stale = catalog.claim_maintenance_lease(
        fake.IDENTIFIER, holder="crashed", operation="index-build", ttl_seconds=60
    )
    clock["ms"] += 60_000                                            # expires exactly now
    fresh = catalog.claim_maintenance_lease(
        fake.IDENTIFIER, holder="compactor", operation="compaction", ttl_seconds=60
    )
    with pytest.raises(MaintenanceLeaseLost, match="now held for compaction by compactor"):
        catalog.renew_maintenance_lease(stale, ttl_seconds=60)
    assert catalog.release_maintenance_lease(stale) is False        # never removes fresh's claim
    assert _stored_lease(catalog)["claim-id"] == fresh.claim_id


def test_renew_extends_and_release_frees(lease_catalog):
    catalog, clock = lease_catalog
    lease = catalog.claim_maintenance_lease(
        fake.IDENTIFIER, holder="h", operation="index-build", ttl_seconds=60
    )
    clock["ms"] += 50_000
    renewed = catalog.renew_maintenance_lease(lease, ttl_seconds=60)
    assert renewed.expires_at_ms == clock["ms"] + 60_000
    assert renewed.claim_id == lease.claim_id
    clock["ms"] += 30_000                                            # past the ORIGINAL expiry
    with pytest.raises(MaintenanceLeaseHeld):
        catalog.claim_maintenance_lease(fake.IDENTIFIER, holder="x", operation="compaction", ttl_seconds=5)
    assert catalog.release_maintenance_lease(renewed) is True
    assert _stored_lease(catalog) is None
    with pytest.raises(MaintenanceLeaseLost):                       # released = no longer held
        catalog.renew_maintenance_lease(renewed, ttl_seconds=60)
    catalog.claim_maintenance_lease(fake.IDENTIFIER, holder="x", operation="compaction", ttl_seconds=5)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"holder": "", "operation": "compaction", "ttl_seconds": 60},
        {"holder": "h", "operation": "vacuum", "ttl_seconds": 60},
        {"holder": "h", "operation": "compaction", "ttl_seconds": 0},
        {"holder": "h", "operation": "compaction", "ttl_seconds": 3601},
    ],
)
def test_lease_requests_are_validated(lease_catalog, kwargs):
    catalog, _ = lease_catalog
    with pytest.raises(ValueError):
        catalog.claim_maintenance_lease(fake.IDENTIFIER, **kwargs)
    assert _stored_lease(catalog) is None


def test_lease_on_a_missing_dataset_is_refused(lease_catalog):
    catalog, _ = lease_catalog
    with pytest.raises(DatasetNotFound):
        catalog.claim_maintenance_lease("reports.absent", holder="h", operation="compaction", ttl_seconds=5)


# --- the dataset's own lifecycle -----------------------------------------------


def test_drop_dataset_removes_its_index_definitions(monkeypatch):
    """A same-named table created after a DROP must not inherit the old definitions:
    they lived on the dropped dataset's document."""
    monkeypatch.setattr(fake._DocRef, "delete", lambda self: setattr(self, "_exists", False), raising=False)
    catalog = _catalog_with_dataset()
    catalog.create_vector_index(
        fake.IDENTIFIER, "idx", "body", embedding_identity=IDENTITY, dimensions=384, author="t"
    )
    catalog.drop_dataset(fake.IDENTIFIER, author="t")
    with pytest.raises(DatasetNotFound):
        catalog.list_vector_indexes(fake.IDENTIFIER)
    fake._dataset(catalog, head=100)                          # the same name, created again
    assert catalog.list_vector_indexes(fake.IDENTIFIER) == []


@pytest.mark.parametrize("conditional", [False, True])
def test_a_commit_from_stale_metadata_keeps_a_definition_created_meanwhile(conditional):
    """Definitions live on the dataset document, which a commit writes whole. The commit
    carries them from its own transactional read, so an index created after the commit
    loaded its metadata survives it - on the conditional (pointer) path and the plain one."""
    from opteryx_catalog.catalog.metadata import DatasetMetadata

    catalog = _catalog_with_dataset()
    stale = DatasetMetadata(dataset_identifier=fake.IDENTIFIER, location="mem://ws/reports/monthly")
    stale.current_snapshot_id = 100
    catalog.create_vector_index(
        fake.IDENTIFIER, "idx", "body", embedding_identity=IDENTITY, dimensions=384,
        author="t", build="sync",
    )
    if conditional:
        catalog.save_dataset_metadata(fake.IDENTIFIER, stale, expected_current_snapshot_id=100)
    else:
        catalog.save_dataset_metadata(fake.IDENTIFIER, stale)
    assert [i["name"] for i in catalog.list_vector_indexes(fake.IDENTIFIER)] == ["idx"]


def test_the_loaded_dataset_carries_its_definitions(monkeypatch):
    """A query plans an index search from the dataset it loaded - no read of its own."""
    monkeypatch.setattr(fake._DocRef, "path", property(lambda self: f"docs/{id(self)}"), raising=False)
    catalog = _catalog_with_dataset()
    catalog.gcs_bucket = "bucket"
    for name in ("second", "first"):
        catalog.create_vector_index(
            fake.IDENTIFIER, name, "body", embedding_identity=IDENTITY, dimensions=384,
            author="t", build="sync",
        )
    loaded = catalog.load_dataset(fake.IDENTIFIER)
    assert [i["name"] for i in loaded.metadata.vector_indexes] == ["first", "second"]


def test_dropping_an_index_leaves_the_others():
    catalog = _catalog_with_dataset()
    for name in ("keep", "gone"):
        catalog.create_vector_index(
            fake.IDENTIFIER, name, "body", embedding_identity=IDENTITY, dimensions=384,
            author="t", build="sync",
        )
    catalog.load_dataset = lambda identifier: type("D", (), {"remove_vector_index_files": lambda *a, **k: None})()
    catalog.drop_vector_index(fake.IDENTIFIER, "gone", author="t")
    assert [i["name"] for i in catalog.list_vector_indexes(fake.IDENTIFIER)] == ["keep"]


# --- sync indexes: a write references its new files' index files in the same commit ----


def test_add_files_references_sync_index_files_in_the_same_commit():
    ds, storage = _seed_dataset({"mem://f1.parquet": [1, 2]})
    out, entry = _compaction_output(storage, [3, 4], name="added")
    files = _files(INDEX_A, out)
    ds.add_files(entries=[entry], author="tester", index_files={out: {INDEX_A: files}})
    assert _refs(ds)[out] == {INDEX_A: files}
    assert _refs(ds)["mem://f1.parquet"] == {}
    assert _counters(ds) == (1, FILE_BYTES, LOGICAL_BYTES)


def test_merge_commit_references_sync_index_files_in_the_same_commit():
    ds, storage = _seed_dataset({"mem://f1.parquet": [1, 2]})
    out, entry = _compaction_output(storage, [9], name="merged")
    files = _files(INDEX_A, out)
    ds.merge_commit(
        entries=[entry], positions={"mem://f1.parquet": [0]}, author="tester",
        index_files={out: {INDEX_A: files}},
    )
    assert _refs(ds)[out] == {INDEX_A: files}


@pytest.mark.parametrize(
    "given, message",
    [
        (lambda out: {"mem://elsewhere.parquet": {INDEX_A: _files(INDEX_A, out)}}, "does not add"),
        (lambda out: {out: {INDEX_A: _files(INDEX_A, out)[:2]}}, "must be IndexFiles"),
    ],
)
def test_a_write_refuses_index_files_it_cannot_attach(given, message):
    ds, storage = _seed_dataset({"mem://f1.parquet": [1, 2]})
    head = ds.metadata.current_snapshot_id
    out, entry = _compaction_output(storage, [3], name="added")
    with pytest.raises(ValueError, match=message):
        ds.add_files(entries=[entry], author="tester", index_files=given(out))
    assert ds.metadata.current_snapshot_id == head


def test_an_async_create_fires_its_refresh_and_a_sync_one_does_not(monkeypatch):
    """An async index covers none of the dataset's files when created: CREATE fires its
    REFRESH INDEX (D-16), for that index only. A sync index was built by the CREATE."""
    from opteryx_catalog import trigger_firing

    fired = []
    monkeypatch.setattr(
        trigger_firing, "fire_index_refreshes",
        lambda catalog, dataset, author, snapshot_id=None, only=None: fired.append((dataset, author, only)),
    )
    catalog = _catalog_with_dataset()
    catalog.create_vector_index(fake.IDENTIFIER, "Lazy", "body", embedding_identity=IDENTITY, dimensions=384, author="t")
    catalog.create_vector_index(
        fake.IDENTIFIER, "eager", "body", embedding_identity=IDENTITY, dimensions=384, author="t", build="sync",
    )
    assert fired == [(fake.IDENTIFIER, "t", "lazy")]


def test_vector_index_files_lists_what_one_index_covers_at_a_snapshot():
    ds, _ = _seed_dataset({"mem://f1.parquet": [1, 2], "mem://f2.parquet": [3]})
    seeded = ds.metadata.current_snapshot_id
    a = _attach(ds, INDEX_A, "mem://f1.parquet")
    _attach(ds, INDEX_B, "mem://f2.parquet")
    assert ds.vector_index_files(INDEX_A) == a
    assert ds.vector_index_files(INDEX_A, seeded) == {}          # time travel: not yet built


def test_an_index_build_commit_message_looks_the_name_up_from_the_definition():
    ds, _ = _seed_dataset({"mem://f1.parquet": [1]})

    class _Catalog:
        def list_vector_indexes(self, identifier):
            assert identifier == ds.identifier
            return [{"index-id": INDEX_B, "name": "other_idx"}, {"index-id": INDEX_A, "name": "body_idx"}]

    ds.catalog = _Catalog()
    assert ds._index_build_message(INDEX_A, None, 3) == "Indexed 3 files for vector index body_idx"
