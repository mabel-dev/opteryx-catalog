"""Tests for CAT-1/CAT-2/CAT-3/CAT-5: workspace drop/lock, the
construction-time gate, and the dataset/collection lock fields on
`drop_dataset()` / `drop_collection()`.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from opteryx_catalog.exceptions import CollectionLocked
from opteryx_catalog.exceptions import CollectionNotFound
from opteryx_catalog.exceptions import DatasetLocked
from opteryx_catalog.exceptions import WorkspaceDeletionProtected
from opteryx_catalog.exceptions import WorkspaceNotFound
from opteryx_catalog.exceptions import WorkspaceStorageReclaimFailed
from opteryx_catalog.opteryx_catalog import OpteryxCatalog


class _Doc:
    """A stand-in for a Firestore document snapshot."""

    def __init__(self, id_, data=None, exists=True):
        self.id = id_
        self.exists = exists
        self._data = data or {}

    def to_dict(self):
        return self._data


class _DocRef:
    """A stand-in for a Firestore DocumentReference.

    Unlike a freshly-instantiated real `DocumentReference`, whose `.get()`
    reports `exists=False` until something is written, a doc created here
    explicitly (e.g. in a fixture) can be told to already exist.
    """

    def __init__(self, id_, data=None, exists=False, log=None):
        self.id = id_
        self._doc = _Doc(id_, data, exists)
        self._subcollections = {}
        self.log = log if log is not None else []
        self.written = None
        self.updated = None

    def get(self):
        return self._doc

    def set(self, data):
        self.written = data
        self._doc = _Doc(self.id, dict(data), exists=True)
        self.log.append(("set", self.id))

    def update(self, data):
        self.updated = data
        merged = dict(self._doc._data)
        merged.update(data)
        self._doc = _Doc(self.id, merged, exists=True)
        self.log.append(("update", self.id))

    def delete(self):
        self.log.append(("delete", self.id))

    def collection(self, name):
        if name not in self._subcollections:
            self._subcollections[name] = _Collection(name, log=self.log)
        return self._subcollections[name]


class _Collection:
    """A stand-in for a Firestore CollectionReference."""

    def __init__(self, name, docs=None, log=None):
        self.name = name
        self.log = log if log is not None else []
        self._docs = {}
        for doc_id in docs or []:
            self._docs[doc_id] = _DocRef(doc_id, log=self.log)

    def document(self, doc_id):
        if doc_id not in self._docs:
            self._docs[doc_id] = _DocRef(doc_id, log=self.log)
        return self._docs[doc_id]

    def stream(self):
        return [ref._doc for ref in self._docs.values()]


class _FirestoreClient:
    """A stand-in for `google.cloud.firestore.Client`.

    Always hands back the same collection regardless of the name asked for -
    good enough for these tests, which only ever need one collection per
    fake client (either the workspace's own top-level collection, for
    `__init__`, or the root `$dropped-workspaces` collection).
    """

    def __init__(self, collection):
        self._collection = collection

    def collection(self, _name):
        return self._collection


# --- __init__ construction-time gate (CAT-1) ---------------------------


def _properties_client(props_data=None, props_exists=True):
    log = []
    catalog_collection = _Collection("ws", log=log)
    catalog_collection._docs["$properties"] = _DocRef(
        "$properties", data=props_data or {}, exists=props_exists, log=log
    )
    return _FirestoreClient(catalog_collection), catalog_collection, log


def test_init_succeeds_for_workspace_with_legacy_deleted_at_ms():
    """`deleted-at-ms` is a legacy field from the soft-delete model DROP
    WORKSPACE replaced - nothing sets it anymore, and construction must not
    refuse a workspace just because it still carries one from before this
    change (that was the exact bug: it blocked drop_workspace() itself from
    ever reaching such a workspace, since _get_catalog constructs with the
    default include_deleted=False)."""
    client, _cc, _log = _properties_client(
        props_data={"deleted-at-ms": 12345, "deleted-by": "alice"}
    )
    with patch("opteryx_catalog.opteryx_catalog.firestore.Client", return_value=client):
        catalog = OpteryxCatalog(workspace="ws")
    assert catalog.workspace == "ws"


def test_init_succeeds_for_deleted_workspace_with_include_deleted():
    """include_deleted is now inert - True behaves identically to the
    default False. Kept as its own test so a future reader can see both
    spellings are equivalent, not just the default one."""
    client, _cc, _log = _properties_client(
        props_data={"deleted-at-ms": 12345, "deleted-by": "alice"}
    )
    with patch("opteryx_catalog.opteryx_catalog.firestore.Client", return_value=client):
        catalog = OpteryxCatalog(workspace="ws", include_deleted=True)
    assert catalog.workspace == "ws"


def test_init_succeeds_for_non_deleted_existing_workspace():
    client, _cc, _log = _properties_client(props_data={"deleted-at-ms": None})
    with patch("opteryx_catalog.opteryx_catalog.firestore.Client", return_value=client):
        catalog = OpteryxCatalog(workspace="ws")
    assert catalog.workspace == "ws"


def test_init_raises_for_unknown_workspace():
    client, _cc, _log = _properties_client(props_exists=False)
    with (
        patch("opteryx_catalog.opteryx_catalog.firestore.Client", return_value=client),
        pytest.raises(WorkspaceNotFound),
    ):
        OpteryxCatalog(workspace="ws")


def test_init_does_not_write_for_unknown_workspace():
    """A mistyped workspace name must not conjure the workspace into being.

    In Firestore a collection exists only because a document in it does, so
    writing `$properties` here is what created the empty workspace behind a
    failed `banana.banana.banana` query.
    """
    client, catalog_collection, log = _properties_client(props_exists=False)
    with (
        patch("opteryx_catalog.opteryx_catalog.firestore.Client", return_value=client),
        pytest.raises(WorkspaceNotFound),
    ):
        OpteryxCatalog(workspace="ws")

    assert catalog_collection.document("$properties").written is None
    assert log == []


def test_init_creates_properties_doc_with_all_fields_when_missing():
    client, catalog_collection, _log = _properties_client(props_exists=False)
    with patch("opteryx_catalog.opteryx_catalog.firestore.Client", return_value=client):
        OpteryxCatalog(workspace="ws", create_if_missing=True)

    written = catalog_collection.document("$properties").written
    assert written["billing-account-id"] is None
    assert written["owner"] is None
    assert written["deleted-at-ms"] is None
    assert written["deleted-by"] is None
    assert written["locked-by"] is None
    assert written["locked-at-ms"] is None


# --- Workspace lifecycle methods (CAT-2/CAT-3) --------------------------


def _catalog_with_properties(props_data=None):
    log = []
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"

    catalog_collection = _Collection("ws", log=log)
    catalog_collection._docs["$properties"] = _DocRef(
        "$properties", data=props_data or {}, exists=True, log=log
    )
    catalog._catalog_ref = catalog_collection
    catalog.firestore_client = _FirestoreClient(catalog_collection)

    return catalog, catalog_collection, log


def _catalog_with_contents(props_data=None, collections=None, missing_collection_docs=()):
    """A catalog whose list_collections/list_datasets/list_views/
    list_materialized_views/drop_dataset/drop_view/drop_materialized_view/
    drop_collection are stubbed directly rather than built up from fake
    Firestore docs - drop_workspace only calls the public
    enumeration/drop methods, so faking Firestore itself under them would
    test nothing extra.

    `collections` is
    `{collection_name: {"datasets": [...], "views": [...], "materialized_views": [...]}}`.
    `materialized_views` is a SEPARATE list from `datasets` here (the real
    catalog stores them in the same subcollection, but list_materialized_views
    is what filters that distinction out for callers, and this fixture
    mirrors what that call returns, not the raw storage).

    `missing_collection_docs` names collections whose own document is
    already gone by the time drop_collection would run - simulating one
    created only implicitly (never given its own field data), which stops
    existing on its own once its last child is dropped, same as the real
    CollectionNotFound-tolerated case in drop_workspace.
    """
    catalog, catalog_collection, log = _catalog_with_properties(props_data)
    collections = collections or {}

    catalog.list_collections = lambda: list(collections)
    catalog.list_datasets = lambda c: list(collections.get(c, {}).get("datasets", []))
    catalog.list_views = lambda c: list(collections.get(c, {}).get("views", []))
    catalog.list_materialized_views = lambda c: list(
        collections.get(c, {}).get("materialized_views", [])
    )

    dropped = []
    catalog.drop_dataset = lambda identifier, author=None: dropped.append(("dataset", identifier, author))
    catalog.drop_view = lambda identifier, author=None: dropped.append(("view", identifier, author))
    catalog.drop_materialized_view = lambda identifier, author=None: dropped.append(
        ("mv", identifier, author)
    )

    def _drop_collection(name, author=None):
        if name in missing_collection_docs:
            raise CollectionNotFound(f"Collection not found: {name}")
        dropped.append(("collection", name, author))

    catalog.drop_collection = _drop_collection

    return catalog, catalog_collection, dropped, log


def test_drop_workspace_drops_everything_then_the_properties_doc():
    # deletion_protection is ON unless explicitly cleared, so a workspace that is
    # about to be dropped has had it turned off first.
    catalog, _cc, dropped, log = _catalog_with_contents(
        props_data={"deletion_protection": False},
        collections={
            "coll1": {"datasets": ["tbl1", "tbl2"], "views": ["v1"]},
            "coll2": {"datasets": [], "views": []},
        },
    )

    with patch("opteryx_catalog.opteryx_catalog.send_webhook") as hook:
        catalog.drop_workspace(author="alice")

    assert ("dataset", "coll1.tbl1", "alice") in dropped
    assert ("dataset", "coll1.tbl2", "alice") in dropped
    assert ("view", "coll1.v1", "alice") in dropped
    # Every collection is dropped too, empty ones included - not just the
    # datasets/views inside them.
    assert ("collection", "coll1", "alice") in dropped
    assert ("collection", "coll2", "alice") in dropped
    assert len(dropped) == 5

    # No tombstone, no grace period - the $properties doc itself is gone.
    assert ("delete", "$properties") in log

    assert hook.call_count == 1
    kwargs = hook.call_args.kwargs
    assert kwargs["action"] == "delete"
    assert kwargs["resource_type"] == "workspace"
    assert kwargs["resource_name"] == "ws"
    assert kwargs["payload"]["dropped_by"] == "alice"


def test_drop_workspace_requires_author():
    catalog, _cc, _dropped, _log = _catalog_with_contents()
    with pytest.raises(ValueError):
        catalog.drop_workspace(author=None)


def test_drop_workspace_drops_materialized_views_before_any_plain_dataset():
    """drop_dataset refuses a dataset a materialized view still reads from
    (see its own docstring). If a plain dataset in ANY collection were
    dropped before every materialized view in the workspace, this would
    intermittently raise MaterializedViewError partway through a real drop -
    the exact "remaining elements" failure mode this ordering exists to
    rule out. Two collections, so a same-collection-only fix wouldn't be
    caught by this test."""
    catalog, _cc, dropped, _log = _catalog_with_contents(
        props_data={"deletion_protection": False},
        collections={
            "coll1": {"datasets": ["source1"], "materialized_views": ["mv1"]},
            "coll2": {"datasets": ["source2"], "materialized_views": ["mv2"]},
        },
    )

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_workspace(author="alice")

    kinds_in_order = [entry[0] for entry in dropped]
    last_mv_index = max(i for i, k in enumerate(kinds_in_order) if k == "mv")
    first_dataset_index = min(i for i, k in enumerate(kinds_in_order) if k == "dataset")
    assert last_mv_index < first_dataset_index, (
        f"a plain dataset was dropped before every materialized view: {dropped}"
    )
    assert ("mv", "coll1.mv1", "alice") in dropped
    assert ("mv", "coll2.mv2", "alice") in dropped
    assert ("dataset", "coll1.source1", "alice") in dropped
    assert ("dataset", "coll2.source2", "alice") in dropped


def test_drop_workspace_drops_the_collection_document_itself():
    """Not just the datasets/views inside a collection - the collection
    document too, or it's left behind as an empty orphan Firestore cannot
    cascade away on its own."""
    catalog, _cc, dropped, _log = _catalog_with_contents(
        props_data={"deletion_protection": False},
        collections={"coll1": {"datasets": ["tbl1"]}},
    )

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_workspace(author="alice")

    assert ("collection", "coll1", "alice") in dropped
    # And after everything inside it, not before - drop_collection raises
    # CollectionNotEmpty against a real catalog if anything is still there.
    collection_index = dropped.index(("collection", "coll1", "alice"))
    dataset_index = dropped.index(("dataset", "coll1.tbl1", "alice"))
    assert dataset_index < collection_index


def test_drop_workspace_clears_policy_opteryx_access_grants():
    """$policies/access is policy.opteryx's data, not opteryx_catalog's, but
    it lives in the same shared Firestore database and nothing else cleans
    it up (policy.opteryx has no webhook consumer for workspace deletion).
    Left behind, a later workspace created under the same name would
    silently inherit these grants."""
    catalog, catalog_collection, _dropped, log = _catalog_with_contents(
        props_data={"deletion_protection": False},
        collections={},
    )
    access_coll = catalog_collection.document("$policies").collection("access")
    access_coll._docs["grant1"] = _DocRef(
        "grant1", data={"role": "owner", "pattern": "ws.*"}, exists=True, log=log
    )
    access_coll._docs["grant2"] = _DocRef(
        "grant2", data={"role": "reader", "pattern": "ws.coll.*"}, exists=True, log=log
    )

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_workspace(author="alice")

    assert ("delete", "grant1") in log
    assert ("delete", "grant2") in log


def test_drop_workspace_tolerates_a_collection_with_no_document_of_its_own():
    """A collection created only implicitly - by a dataset inside it, never
    through create_collection() - stops existing on its own once its last
    child is dropped. drop_collection raising CollectionNotFound for that
    is expected, not a bug, and must not abort the rest of the workspace
    drop."""
    catalog, _cc, dropped, _log = _catalog_with_contents(
        props_data={"deletion_protection": False},
        collections={
            "implicit_coll": {"datasets": ["tbl1"]},
            "coll2": {"datasets": ["tbl2"]},
        },
        missing_collection_docs=["implicit_coll"],
    )

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_workspace(author="alice")  # must not raise

    assert ("dataset", "implicit_coll.tbl1", "alice") in dropped
    assert ("collection", "coll2", "alice") in dropped
    assert not any(entry[0] == "collection" and entry[1] == "implicit_coll" for entry in dropped)


def test_drop_workspace_reclaims_storage_synchronously_not_on_a_grace_period():
    """The datasets/views are tombstoned (via the mocked drop_dataset/
    drop_view above), but nothing in this codebase runs the sweep that
    reclaims a tombstone's storage on a schedule - so drop_workspace has to
    do it inline, immediately (min_age_ms=0), or the files are stranded
    forever. This is the difference between "the workspace is gone" and
    "the workspace is gone AND the storage is reclaimed" - see the
    workspace-delete-billing-coupling incident for why that distinction
    matters."""
    catalog, _cc, _dropped, _log = _catalog_with_contents(
        props_data={"deletion_protection": False},
        collections={"coll1": {"datasets": ["tbl1"], "views": []}},
    )

    calls = []

    class _FakeSweep:
        def __init__(self, cat, author=None, min_age_ms=None):
            calls.append({"catalog": cat, "author": author, "min_age_ms": min_age_ms})

        def sweep(self, dry_run=True):
            calls.append({"dry_run": dry_run})
            return {"tombstones": 0, "reclaimed": 0, "skipped": 0, "errors": 0, "details": []}

    with (
        patch("opteryx_catalog.catalog.dropped_sweep.DroppedDatasetSweep", _FakeSweep),
        patch("opteryx_catalog.opteryx_catalog.send_webhook"),
    ):
        catalog.drop_workspace(author="alice")

    assert calls[0] == {"catalog": catalog, "author": "alice", "min_age_ms": 0}
    assert calls[1] == {"dry_run": False}


def test_drop_workspace_raises_and_keeps_properties_doc_when_reclaim_fails():
    """If the sweep can't confirm every location was reclaimed, drop_workspace
    must not delete $properties anyway - doing so would orphan the failed
    file(s) and their tombstone(s) permanently, since nothing could ever
    construct a normal handle on this workspace again to retry them."""
    catalog, catalog_collection, _dropped, _log = _catalog_with_contents(
        props_data={"deletion_protection": False},
        collections={"coll1": {"datasets": ["tbl1"]}},
    )

    class _FailingSweep:
        def __init__(self, cat, author=None, min_age_ms=None):
            pass

        def sweep(self, dry_run=True):
            return {
                "tombstones": 1,
                "reclaimed": 0,
                "skipped": 0,
                "errors": 1,
                "details": [{"id": "tbl1", "action": "error", "reason": "list-failed"}],
            }

    with (
        patch("opteryx_catalog.catalog.dropped_sweep.DroppedDatasetSweep", _FailingSweep),
        patch("opteryx_catalog.opteryx_catalog.send_webhook") as hook,
        pytest.raises(WorkspaceStorageReclaimFailed),
    ):
        catalog.drop_workspace(author="alice")

    # $properties must still be there - not deleted, not even attempted.
    assert catalog_collection.document("$properties").get().exists is True
    # And the "workspace dropped" webhook, which fires after that delete,
    # must not have gone out either - nothing downstream should be told
    # this workspace is gone when it demonstrably isn't yet.
    hook.assert_not_called()


def test_drop_workspace_blocked_when_deletion_protected():
    catalog, _cc, dropped, _log = _catalog_with_contents(
        props_data={"deletion_protection": True},
        collections={"coll1": {"datasets": ["tbl1"], "views": []}},
    )
    with pytest.raises(WorkspaceDeletionProtected):
        catalog.drop_workspace(author="alice")
    # Refused before touching anything inside the workspace.
    assert dropped == []


def test_lock_workspace_sets_fields():
    catalog, catalog_collection, _log = _catalog_with_properties()

    with patch("opteryx_catalog.opteryx_catalog.send_webhook") as hook:
        catalog.lock_workspace(author="alice")

    props = catalog_collection.document("$properties").get().to_dict()
    assert props["locked-by"] == "alice"
    assert isinstance(props["locked-at-ms"], int)
    assert hook.call_args.kwargs["action"] == "lock"


def test_lock_workspace_requires_author():
    catalog, _cc, _log = _catalog_with_properties()
    with pytest.raises(ValueError):
        catalog.lock_workspace(author=None)


def test_unlock_workspace_clears_fields():
    catalog, catalog_collection, _log = _catalog_with_properties(
        props_data={"locked-by": "alice", "locked-at-ms": 123}
    )

    with patch("opteryx_catalog.opteryx_catalog.send_webhook") as hook:
        catalog.unlock_workspace(author="bob")

    props = catalog_collection.document("$properties").get().to_dict()
    assert props["locked-by"] is None
    assert props["locked-at-ms"] is None
    assert hook.call_args.kwargs["action"] == "unlock"


def test_unlock_workspace_requires_author():
    catalog, _cc, _log = _catalog_with_properties()
    with pytest.raises(ValueError):
        catalog.unlock_workspace(author=None)


# --- Dataset/collection lock fields (CAT-5) ------------------------------


def _catalog_with_dataset(locked=False):
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    log = []
    data = {"location": "gs://bucket/ws/coll/tbl"}
    if locked:
        data["locked-by"] = "alice"
        data["locked-at-ms"] = 123
    dataset_ref = _DocRef("tbl", data=data, exists=True, log=log)
    tombstones = _Collection("datasets", log=log)

    catalog._dataset_doc_ref = lambda c, n: dataset_ref
    catalog._snapshots_collection = lambda c, n: dataset_ref.collection("snapshots")
    catalog._tombstones_collection = lambda: tombstones

    return catalog, dataset_ref, log


def test_drop_dataset_raises_when_locked():
    catalog, _ref, _log = _catalog_with_dataset(locked=True)
    with pytest.raises(DatasetLocked):
        catalog.drop_dataset("coll.tbl", author="bob")


def test_drop_dataset_succeeds_when_not_locked():
    catalog, _ref, log = _catalog_with_dataset(locked=False)
    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_dataset("coll.tbl", author="bob")
    assert ("delete", "tbl") in log


def test_create_dataset_initializes_lock_fields_to_none():
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog.gcs_bucket = "bucket"
    catalog.io = None
    catalog._snapshot_cache = {}
    catalog._schema_cache = {}
    log = []
    catalog._catalog_ref = _Collection("ws", log=log)

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.create_dataset("coll.tbl", schema=None, author="alice")

    doc_ref = catalog._catalog_ref.document("coll").collection("datasets").document("tbl")
    assert doc_ref.written["locked-by"] is None
    assert doc_ref.written["locked-at-ms"] is None


def _catalog_with_collection(locked=False, has_children=False):
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    log = []
    data = {"name": "coll"}
    if locked:
        data["locked-by"] = "alice"
        data["locked-at-ms"] = 123
    coll_ref = _DocRef("coll", data=data, exists=True, log=log)

    datasets_coll = _Collection("datasets", log=log)
    views_coll = _Collection("views", log=log)
    if has_children:
        datasets_coll._docs["tbl"] = _DocRef("tbl", exists=True, log=log)

    catalog._collection_ref = lambda c: coll_ref
    catalog._datasets_collection = lambda c: datasets_coll
    catalog._views_collection = lambda c: views_coll

    return catalog, coll_ref, log


def test_drop_collection_raises_when_locked():
    catalog, _ref, _log = _catalog_with_collection(locked=True)
    with pytest.raises(CollectionLocked):
        catalog.drop_collection("coll", author="bob")


def test_drop_collection_succeeds_when_not_locked_and_empty():
    catalog, _ref, log = _catalog_with_collection(locked=False, has_children=False)
    catalog.drop_collection("coll", author="bob")
    assert ("delete", "coll") in log


def test_drop_collection_still_raises_not_found():
    catalog, _ref, log = _catalog_with_collection(locked=False)
    catalog._collection_ref = lambda c: _DocRef("coll", exists=False, log=log)
    with pytest.raises(CollectionNotFound):
        catalog.drop_collection("coll", author="bob")


def test_create_collection_initializes_lock_fields_to_none():
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    log = []
    coll_ref = _DocRef("coll", exists=False, log=log)
    catalog._collection_ref = lambda c: coll_ref

    catalog.create_collection("coll", author="alice")

    assert coll_ref.written["locked-by"] is None
    assert coll_ref.written["locked-at-ms"] is None
