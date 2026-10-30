"""RENAME is metadata only: one Firestore transaction, no storage I/O.

See RENAME_METADATA_ONLY_DESIGN.md. Everything here runs against an in-memory
Firestore keyed by document path, so "nothing left under the old path" and
"the whole rename or none of it" are real assertions rather than mock calls.
"""

from __future__ import annotations

import json
import logging

import pytest
from google.api_core.exceptions import InvalidArgument

from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.exceptions import DatasetAlreadyExists
from opteryx_catalog.exceptions import DatasetLocationReusable
from opteryx_catalog.exceptions import DatasetLocked
from opteryx_catalog.exceptions import DatasetNotFound
from opteryx_catalog.exceptions import ForkError
from opteryx_catalog.exceptions import MaterializedViewError
from opteryx_catalog.exceptions import NameInUse
from opteryx_catalog.exceptions import RenameTooLarge
from opteryx_catalog.opteryx_catalog import OpteryxCatalog

# A minted location (`OpteryxCatalog._mint_location`): not derivable from any name.
LOCATION = "gs://bucket/ws/3f2a9c1e5b7d4a608c1d2e3f4a5b6c7d"
LEGACY_LOCATION = "gs://bucket/ws/coll/tbl"

OLD = ("ws", "coll", "datasets", "tbl")
NEW = ("ws", "newcoll", "datasets", "newtbl")


class _Snap:
    def __init__(self, ref, data):
        self.reference = ref
        self.id = ref.path[-1]
        self.exists = data is not None
        self._data = data

    def to_dict(self):
        return None if self._data is None else dict(self._data)


class _Ref:
    def __init__(self, client, path):
        self._client = client
        self.path = path

    def get(self, transaction=None):
        if transaction is not None:
            transaction.check_read()
        return _Snap(self, self._client.docs.get(self.path))

    def set(self, data, merge=False):
        self._client.docs[self.path] = (
            {**self._client.docs.get(self.path, {}), **data} if merge else dict(data)
        )

    def update(self, data):
        self._client.docs[self.path] = {**self._client.docs[self.path], **data}

    def delete(self):
        self._client.docs.pop(self.path, None)

    def collection(self, name):
        return _Coll(self._client, self.path + (name,))


class _Coll:
    def __init__(self, client, path):
        self._client = client
        self.path = path

    def document(self, doc_id):
        return _Ref(self._client, self.path + (doc_id,))

    def stream(self, transaction=None):
        if transaction is not None:
            transaction.check_read()
        if self.path in self._client.broken_streams:
            raise RuntimeError("firestore unavailable")
        n = len(self.path)
        return [
            _Snap(_Ref(self._client, p), d)
            for p, d in sorted(self._client.docs.items())
            if len(p) == n + 1 and p[:n] == self.path
        ]


class _Txn:
    """Enough of a Firestore transaction for `@firestore.transactional`.

    Writes buffer until commit. A read after a write raises, as Firestore does,
    so the ordering rule is tested rather than assumed.
    """

    _read_only = False
    _max_attempts = 1
    _id = b"fake-txn"

    def __init__(self, client):
        self._client = client
        self.writes = []

    def check_read(self):
        assert not self.writes, "read after write in a Firestore transaction"

    def _clean_up(self):
        self.writes = []

    def _begin(self, retry_id=None):
        return None

    def _rollback(self):
        self.writes = []

    def _commit(self):
        if isinstance(self._client.fail_commit, Exception):
            raise self._client.fail_commit
        if self._client.fail_commit:
            raise RuntimeError("commit failed")
        for kind, ref, data in self.writes:
            if kind == "set":
                ref.set(data)
            else:
                ref.delete()
        return []

    def set(self, ref, data):
        self.writes.append(("set", ref, dict(data)))

    def delete(self, ref):
        self.writes.append(("delete", ref, None))


class _Client:
    def __init__(self):
        self.docs = {}
        self.fail_commit = False
        self.broken_streams = set()

    def collection(self, name):
        return _Coll(self, (name,))

    def transaction(self):
        return _Txn(self)


class _NoStorage:
    """Any touch of storage fails the test: a rename moves no bytes."""

    def __getattr__(self, name):
        raise AssertionError(f"rename touched storage: io.{name}")


def _catalog(*, dataset=None, extra=None):
    """A catalog whose only dataset is coll.tbl at an id-bearing location."""
    client = _Client()
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog.gcs_bucket = "bucket"
    catalog.firestore_client = client
    catalog._catalog_ref = client.collection("ws")
    catalog.io = _NoStorage()
    catalog._storage_client = None
    catalog._copy_object = _forbid("_copy_object")
    catalog._reclaim_paths = _forbid("_reclaim_paths")
    # The inbound sweep is a collection group query (covered where relationships are
    # tested); here it is a recorded no-op unless a test replaces it.
    catalog._repoint_inbound_relationships = lambda *args, **kwargs: []

    client.docs[OLD] = {
        "name": "tbl",
        "collection": "coll",
        "workspace": "ws",
        "location": LOCATION,
        "locked-by": None,
        "properties": {"k": "v"},
        "vector-indexes": {"idx": {"name": "idx", "index-id": "a" * 32}},
        "current-snapshot-id": 2,
        **(dataset or {}),
    }
    client.docs[OLD + ("schemas", "sc1")] = {"columns": [{"name": "id", "id": 1}]}
    client.docs[OLD + ("snapshots", "1")] = {
        "snapshot-id": 1,
        "manifest": f"{LOCATION}/metadata/manifest-1.parquet",
    }
    client.docs[OLD + ("snapshots", "2")] = {
        "snapshot-id": 2,
        "manifest": f"{LOCATION}/metadata/manifest-2.parquet",
    }
    client.docs[OLD + ("tags", "release")] = {"snapshot-id": 1}
    client.docs[OLD + ("relationships", "r1")] = {
        "name": "r1",
        "collection": "coll",
        "dataset": "tbl",
        "references-dataset": "other",
    }
    client.docs[OLD + ("relationship-suppressions", "s1")] = {
        "collection": "coll",
        "dataset": "tbl",
    }
    client.docs[OLD + ("maintenance", "orphan-quarantine")] = {
        "entries": {f"{LOCATION}/data/stray.parquet": 123}
    }
    client.docs[OLD + ("maintenance", "lease")] = {"claim-id": "c1", "holder": "compactor"}
    client.docs.update(extra or {})
    return catalog, client


def _forbid(name):
    def _refuse(*args, **kwargs):
        raise AssertionError(f"rename called {name}")

    return _refuse


def _under(client, path):
    return {p for p in client.docs if p[: len(path)] == path}


def _emitted(capsys):
    out = capsys.readouterr().out
    return [json.loads(line) for line in out.splitlines() if line.strip()]


# --- what a rename does ------------------------------------------------------


def test_the_dataset_document_moves_under_the_new_name_keeping_its_location():
    catalog, client = _catalog()

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    moved = client.docs[NEW]
    assert moved["name"] == "newtbl"
    assert moved["collection"] == "newcoll"
    assert moved["location"] == LOCATION
    # Everything else on the document rides along unchanged.
    assert moved["properties"] == {"k": "v"}
    assert moved["vector-indexes"] == {"idx": {"name": "idx", "index-id": "a" * 32}}
    assert moved["current-snapshot-id"] == 2
    assert OLD not in client.docs


def test_nothing_is_left_under_the_old_path():
    """Firestore never cascades, so a missed subcollection is a ghost path."""
    catalog, client = _catalog()

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert _under(client, OLD) == set()


def test_snapshots_keep_their_manifests_unchanged():
    """No manifest is rewritten: the files never moved, so the paths stay true."""
    catalog, client = _catalog()

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert client.docs[NEW + ("snapshots", "1")]["manifest"] == (
        f"{LOCATION}/metadata/manifest-1.parquet"
    )
    assert client.docs[NEW + ("snapshots", "2")]["manifest"] == (
        f"{LOCATION}/metadata/manifest-2.parquet"
    )


def test_schemas_and_tags_travel_verbatim():
    """A tag left behind would unpin its snapshot, and the next expiration run
    would reclaim exactly the data the tag exists to hold."""
    catalog, client = _catalog()

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert client.docs[NEW + ("schemas", "sc1")] == {"columns": [{"name": "id", "id": 1}]}
    assert client.docs[NEW + ("tags", "release")] == {"snapshot-id": 1}


def test_relationships_and_suppressions_travel_with_their_address_rewritten():
    catalog, client = _catalog()

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    relationship = client.docs[NEW + ("relationships", "r1")]
    assert relationship["collection"] == "newcoll"
    assert relationship["dataset"] == "newtbl"
    assert relationship["references-dataset"] == "other"
    suppression = client.docs[NEW + ("relationship-suppressions", "s1")]
    assert (suppression["collection"], suppression["dataset"]) == ("newcoll", "newtbl")


def test_the_orphan_quarantine_is_carried_because_its_paths_are_still_true():
    catalog, client = _catalog()

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert client.docs[NEW + ("maintenance", "orphan-quarantine")] == {
        "entries": {f"{LOCATION}/data/stray.parquet": 123}
    }


def test_the_maintenance_lease_is_not_carried_and_is_not_left_behind():
    catalog, client = _catalog()

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert NEW + ("maintenance", "lease") not in client.docs
    assert OLD + ("maintenance", "lease") not in client.docs


def test_workspace_deletion_protection_does_not_block_a_rename():
    """It protects the workspace from being deleted, not the assets inside it."""
    catalog, client = _catalog()
    catalog._assert_not_deletion_protected = _forbid("_assert_not_deletion_protected")

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert NEW in client.docs


def test_rename_within_a_collection():
    catalog, client = _catalog()

    catalog.rename_dataset("coll.tbl", "coll.renamed", author="alice")

    assert client.docs[("ws", "coll", "datasets", "renamed")]["location"] == LOCATION
    assert client.docs[("ws", "coll", "datasets", "renamed")]["collection"] == "coll"
    assert OLD not in client.docs


def test_no_storage_is_touched():
    """`_NoStorage` and the forbidden copy/reclaim helpers fail the test on any
    storage call; reaching here is the assertion."""
    catalog, client = _catalog()

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert NEW in client.docs


# --- the transaction ---------------------------------------------------------


def test_a_failed_commit_leaves_the_old_dataset_exactly_as_it_was():
    """All or nothing: there is no state with both names, or with neither."""
    catalog, client = _catalog()
    before = dict(client.docs)
    client.fail_commit = True

    with pytest.raises(RuntimeError, match="commit failed"):
        catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert client.docs == before


def test_a_transaction_firestore_refuses_as_too_big_says_so_and_changes_nothing():
    """The whole rename is one commit, bounded by Firestore's request size: a
    dataset with thousands of snapshots crosses it. The caller is told what to
    do rather than handed a bare InvalidArgument."""
    catalog, client = _catalog()
    before = dict(client.docs)
    client.fail_commit = InvalidArgument("Request payload size exceeds the limit")

    with pytest.raises(RenameTooLarge, match=r"9 documents to move.*Expire old snapshots"):
        catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert client.docs == before


# --- refusals ----------------------------------------------------------------


def _refused_and_untouched(catalog, client, exc, **match):
    before = dict(client.docs)
    with pytest.raises(exc, **match):
        catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")
    assert client.docs == before


def test_missing_source_raises():
    catalog, client = _catalog()
    del client.docs[OLD]

    with pytest.raises(DatasetNotFound):
        catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")


def test_existing_target_raises_and_nothing_is_written():
    catalog, client = _catalog(extra={NEW: {"name": "newtbl", "location": "gs://bucket/ws/x"}})

    _refused_and_untouched(catalog, client, DatasetAlreadyExists)


@pytest.mark.parametrize(
    "holder", [("ws", "newcoll", "views", "newtbl"), ("ws", "newcoll", "tasks", "newtbl")]
)
def test_a_name_held_by_a_view_or_task_is_refused(holder):
    """Tables, views and tasks share one namespace; `create_dataset` enforces it
    and a rename into the same name must too."""
    catalog, client = _catalog(extra={holder: {"name": "newtbl"}})

    _refused_and_untouched(catalog, client, NameInUse)


def test_locked_dataset_cannot_be_renamed():
    """The two-person deniability lock outranks a rename, as it does a drop."""
    catalog, client = _catalog(dataset={"locked-by": "bob"})

    _refused_and_untouched(catalog, client, DatasetLocked)


def test_a_materialized_view_cannot_be_renamed():
    catalog, client = _catalog(dataset={"dataset-type": "materialized_view"})

    _refused_and_untouched(catalog, client, MaterializedViewError, match="materialized view")


def test_a_dataset_with_triggers_cannot_be_renamed():
    catalog, client = _catalog(extra={OLD + ("triggers", "t1"): {"kind": "commit"}})

    _refused_and_untouched(catalog, client, MaterializedViewError, match="triggers")


def test_an_upstream_with_forks_cannot_be_renamed():
    catalog, client = _catalog(
        extra={OLD + ("forks", "ws:other:copy"): {"fork": "ws.other.copy"}}
    )

    _refused_and_untouched(catalog, client, ForkError, match="ws.other.copy")


def test_a_fork_cannot_be_renamed():
    fork = {
        "source": {"dataset": "ws.other.up", "snapshot-id": 5, "sequence-number": 1},
        "target": {"sequence-number": 1, "last-sync-ms": 1},
        "forked-at-ms": 1,
        "forked-by": "alice",
    }
    catalog, client = _catalog(dataset={"fork": fork})

    _refused_and_untouched(catalog, client, ForkError, match="is a fork")


def test_an_unreadable_fork_registry_refuses_rather_than_assuming_none():
    catalog, client = _catalog()
    client.broken_streams.add(OLD + ("forks",))

    _refused_and_untouched(catalog, client, ForkError, match="Refusing")


def test_same_name_rejected():
    catalog, _client = _catalog()

    with pytest.raises(ValueError, match="same"):
        catalog.rename_dataset("coll.tbl", "coll.tbl", author="alice")


# --- the stored location -----------------------------------------------------


def test_a_location_derived_from_the_old_name_is_carried_as_stored():
    """Datasets created before locations were minted keep the prefix they have. No create
    derives it again, so the vacated name cannot bind it (tests/test_create_location.py)."""
    catalog, client = _catalog(dataset={"location": LEGACY_LOCATION})

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert client.docs[NEW]["location"] == LEGACY_LOCATION


def test_a_document_with_no_location_is_refused_not_guessed():
    catalog, client = _catalog()
    del client.docs[OLD]["location"]

    _refused_and_untouched(catalog, client, DatasetLocationReusable, match="no location")


# --- re-running after the commit --------------------------------------------


def test_a_rerun_after_commit_raises_not_found_and_changes_nothing():
    catalog, client = _catalog()
    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")
    after_first = dict(client.docs)

    with pytest.raises(DatasetNotFound):
        catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert client.docs == after_first


def test_a_rerun_after_the_old_name_is_recreated_does_not_rename_the_newcomer():
    """The target still exists, so a retried rename cannot take an unrelated
    dataset that now answers to the old name."""
    catalog, client = _catalog()
    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")
    client.docs[OLD] = {"name": "tbl", "collection": "coll", "location": "gs://bucket/ws/other"}
    after_first = dict(client.docs)

    with pytest.raises(DatasetAlreadyExists):
        catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert client.docs == after_first


@pytest.mark.parametrize(
    "expectation",
    [{}, {"expected_current_snapshot_id": None}],
    ids=["unconditional-save", "first-commit"],
)
def test_a_save_built_before_the_rename_does_not_bring_the_old_name_back(expectation):
    """A description or annotation saved from metadata loaded before the rename
    would recreate `coll.tbl` on the renamed dataset's location - and that
    ghost's expiration would reclaim the renamed dataset's new files."""
    catalog, client = _catalog()
    stale = DatasetMetadata(dataset_identifier="coll.tbl", location=LOCATION)
    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")
    after_rename = dict(client.docs)

    with pytest.raises(DatasetNotFound):
        catalog.save_dataset_metadata("coll.tbl", stale, **expectation)

    assert client.docs == after_rename


# --- what happens after the commit ------------------------------------------


def test_inbound_relationships_are_repointed_after_the_commit():
    catalog, client = _catalog()
    seen = {}

    def _repoint(old_c, old_d, new_c, new_d, *, author):
        seen["args"] = (old_c, old_d, new_c, new_d, author)
        seen["committed"] = NEW in client.docs and OLD not in client.docs
        return []

    catalog._repoint_inbound_relationships = _repoint

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert seen["args"] == ("coll", "tbl", "newcoll", "newtbl", "alice")
    assert seen["committed"] is True


def test_a_failed_repoint_does_not_undo_or_fail_the_rename(caplog):
    catalog, client = _catalog()

    def _boom(*args, **kwargs):
        raise RuntimeError("collection group index missing")

    catalog._repoint_inbound_relationships = _boom

    with caplog.at_level(logging.WARNING):
        catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    assert NEW in client.docs and OLD not in client.docs
    assert "could not repoint inbound relationships" in caplog.text


def test_emits_audit_record(capsys):
    catalog, _client = _catalog()

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl", author="alice")

    record = next(r for r in _emitted(capsys) if r["action"] == "rename_dataset")
    assert record["resource_type"] == "dataset"
    assert record["workspace"] == "ws"
    assert record["collection"] == "newcoll"
    assert record["resource"] == "newtbl"
    assert record["author"] == "alice"
    assert record["detail"]["old_identifier"] == "coll.tbl"
    assert record["detail"]["new_identifier"] == "newcoll.newtbl"
    assert record["detail"]["location"] == LOCATION
    # dataset doc + schema + 2 snapshots + tag + relationship + suppression + quarantine
    assert record["detail"]["documents_moved"] == 8


def test_unauthenticated_records_no_author(capsys):
    catalog, _client = _catalog()

    catalog.rename_dataset("coll.tbl", "newcoll.newtbl")

    record = next(r for r in _emitted(capsys) if r["action"] == "rename_dataset")
    assert record["author"] is None
