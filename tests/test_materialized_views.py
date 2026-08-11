"""Materialized views and their refresh triggers - catalog-side (Phase 1).

An MV is a normal dataset document wearing `dataset-type: materialized_view`,
its defining SQL versioned in a `statement` subcollection, and one refresh
trigger document under EACH source dataset's `triggers` subcollection. These
tests cover registration, trigger reconciliation, cycle rejection, the cascade
rules in drop/rename, and the workspace egress lock.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from opteryx_catalog import trigger_firing
from opteryx_catalog.exceptions import DatasetNotFound
from opteryx_catalog.exceptions import EgressRestricted
from opteryx_catalog.exceptions import MaterializedViewError
from opteryx_catalog.exceptions import TriggerNotFound
from opteryx_catalog.opteryx_catalog import MATERIALIZED_VIEW_TYPE
from opteryx_catalog.opteryx_catalog import TRIGGERS_SUBCOLLECTION
from opteryx_catalog.opteryx_catalog import OpteryxCatalog
from opteryx_catalog.trigger_firing import fire_triggers


class _Doc:
    """A stand-in for a Firestore document snapshot."""

    def __init__(self, id_, data, exists):
        self.id = id_
        self.exists = exists
        self._data = dict(data)

    def to_dict(self):
        return dict(self._data)


class _DocRef:
    """A stand-in for a Firestore DocumentReference."""

    def __init__(self, id_):
        self.id = id_
        self._data = {}
        self._exists = False
        self._subcollections = {}

    def get(self):
        return _Doc(self.id, self._data, self._exists)

    def set(self, data, merge=False):
        self._data = {**self._data, **data} if merge else dict(data)
        self._exists = True

    def update(self, data):
        if not self._exists:
            raise KeyError(f"no document to update: {self.id}")
        self._data.update(data)

    def delete(self):
        self._data = {}
        self._exists = False

    def collection(self, name):
        if name not in self._subcollections:
            self._subcollections[name] = _Collection()
        return self._subcollections[name]


class _Collection:
    """A stand-in for a Firestore CollectionReference."""

    def __init__(self):
        self._docs = {}

    def document(self, doc_id):
        if doc_id not in self._docs:
            self._docs[doc_id] = _DocRef(doc_id)
        return self._docs[doc_id]

    def stream(self):
        return [ref.get() for ref in self._docs.values() if ref._exists]


class _FirestoreClient:
    """A stand-in for the Firestore client, whose root collections are the
    workspaces - which is what lets a handle bound to one workspace read
    another's `$properties` (the egress lock lives there)."""

    def __init__(self):
        self._collections = {}

    def collection(self, name):
        if name not in self._collections:
            self._collections[name] = _Collection()
        return self._collections[name]


def _catalog():
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog.io = None
    catalog._snapshot_cache = {}
    catalog._schema_cache = {}
    root = {}

    def datasets_collection(coll):
        return root.setdefault(coll, _Collection())

    catalog._datasets_collection = datasets_collection
    catalog._dataset_doc_ref = lambda c, n: datasets_collection(c).document(n)
    catalog._snapshots_collection = lambda c, n: catalog._dataset_doc_ref(c, n).collection(
        "snapshots"
    )
    tombstones = _Collection()
    catalog._tombstones_collection = lambda: tombstones
    catalog.firestore_client = _FirestoreClient()
    catalog._catalog_ref = catalog.firestore_client.collection("ws")
    return catalog


def _set_egress_restriction(catalog, workspace, restricted):
    """Set `workspace`'s egress lock explicitly, in either direction.

    Written straight to `$properties`: `set_workspace_properties` only ever
    writes the workspace its handle is bound to, so this is what a handle bound
    to `workspace` - an operator's `ALTER WORKSPACE <workspace> SET
    egress_protection TO ON/OFF` - would have left behind.

    Needed mostly for the OFF direction. The lock is on by default, so a test
    that wants it on generally has to do nothing at all.
    """
    catalog.firestore_client.collection(workspace).document("$properties").set(
        {"egress_protection": restricted}, merge=True
    )


def _add_dataset(catalog, identifier, **fields):
    coll, name = identifier.split(".", 1)
    ref = catalog._dataset_doc_ref(coll, name)
    ref.set(
        {
            "name": name,
            "collection": coll,
            "workspace": "ws",
            "location": f"gs://bucket/ws/{coll}/{name}",
            **fields,
        }
    )
    return ref


def _register_mv(catalog, identifier="mart.daily", sources=("src.a",), **kwargs):
    coll, name = identifier.split(".", 1)
    if not catalog._dataset_doc_ref(coll, name).get().exists:
        _add_dataset(catalog, identifier)
    catalog.create_materialized_view(
        identifier,
        "SELECT * FROM src.a",
        list(sources),
        author="alice",
        **kwargs,
    )


# --- triggers -----------------------------------------------------------


def test_create_trigger_requires_author():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    with pytest.raises(ValueError):
        catalog.create_trigger("src.a", "t1", target_view="mart.daily")


def test_create_trigger_requires_dataset():
    catalog = _catalog()
    with pytest.raises(DatasetNotFound):
        catalog.create_trigger("src.missing", "t1", target_view="mart.daily", author="alice")


def test_create_trigger_writes_document():
    catalog = _catalog()
    ref = _add_dataset(catalog, "src.a")
    catalog.create_trigger(
        "src.a", "t1", target_view="mart.daily", statement_id="123", author="alice"
    )

    written = ref.collection(TRIGGERS_SUBCOLLECTION).document("t1").get().to_dict()
    assert written["kind"] == "materialized_view_refresh"
    assert written["target-view"] == "ws.mart.daily"
    assert written["statement-id"] == "123"
    assert written["created-by"] == "alice"
    assert written["last-fired-at-ms"] is None


def test_drop_trigger_missing_raises_unless_missing_ok():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    with pytest.raises(TriggerNotFound):
        catalog.drop_trigger("src.a", "nope", author="alice")
    catalog.drop_trigger("src.a", "nope", author="alice", missing_ok=True)


def test_list_triggers_and_mark_fired():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    catalog.create_trigger("src.a", "t1", target_view="mart.daily", author="alice")

    [trigger] = catalog.list_triggers("src.a")
    assert trigger["name"] == "t1"

    catalog.mark_trigger_fired("src.a", "t1", status="enqueued")
    [trigger] = catalog.list_triggers("src.a")
    assert trigger["last-fired-status"] == "enqueued"
    assert isinstance(trigger["last-fired-at-ms"], int)


def test_workspace_qualified_names_are_reduced():
    """The engine hands over 'ws.collection.dataset'; the trigger must land on
    'collection.dataset' in this workspace, not on a collection named 'ws'."""
    catalog = _catalog()
    ref = _add_dataset(catalog, "src.a")
    catalog.create_trigger("ws.src.a", "t1", target_view="mart.daily", author="alice")
    assert ref.collection(TRIGGERS_SUBCOLLECTION).document("t1").get().exists


# --- materialized view registration -------------------------------------


def test_create_mv_registers_and_creates_triggers():
    catalog = _catalog()
    src_a = _add_dataset(catalog, "src.a")
    src_b = _add_dataset(catalog, "src.b")
    mv_ref = _add_dataset(catalog, "mart.daily")

    catalog.create_materialized_view(
        "mart.daily",
        "SELECT * FROM src.a JOIN src.b USING (id)",
        ["ws.src.a", "src.b"],
        author="alice",
    )

    doc = mv_ref.get().to_dict()
    assert doc["dataset-type"] == MATERIALIZED_VIEW_TYPE
    assert doc["source-tables"] == ["ws.src.a", "ws.src.b"]
    statement = mv_ref.collection("statement").document(doc["statement-id"]).get().to_dict()
    assert statement["sql"].startswith("SELECT")
    assert statement["sequence-number"] == 1

    for src in (src_a, src_b):
        trigger = (
            src.collection(TRIGGERS_SUBCOLLECTION).document(OpteryxCatalog._mv_trigger_name("ws.mart.daily")).get().to_dict()
        )
        assert trigger["target-view"] == "ws.mart.daily"
        assert trigger["statement-id"] == doc["statement-id"]


def test_create_mv_requires_backing_table():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    with pytest.raises(DatasetNotFound):
        catalog.create_materialized_view("mart.missing", "SELECT 1", ["src.a"], author="alice")


def test_create_mv_requires_sources_exist():
    catalog = _catalog()
    _add_dataset(catalog, "mart.daily")
    with pytest.raises(DatasetNotFound):
        catalog.create_materialized_view("mart.daily", "SELECT 1", ["src.missing"], author="alice")


def test_create_mv_rejects_empty_and_self_sources():
    catalog = _catalog()
    _add_dataset(catalog, "mart.daily")
    with pytest.raises(MaterializedViewError):
        catalog.create_materialized_view("mart.daily", "SELECT 1", [], author="alice")
    with pytest.raises(MaterializedViewError):
        catalog.create_materialized_view("mart.daily", "SELECT 1", ["mart.daily"], author="alice")


def test_create_mv_twice_needs_update_if_exists():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog)
    with pytest.raises(MaterializedViewError):
        _register_mv(catalog)


def test_update_mv_reconciles_triggers_and_bumps_sequence():
    catalog = _catalog()
    src_a = _add_dataset(catalog, "src.a")
    src_b = _add_dataset(catalog, "src.b")
    mv_ref = _add_dataset(catalog, "mart.daily")

    catalog.create_materialized_view("mart.daily", "SELECT * FROM src.a", ["src.a"], author="alice")
    catalog.create_materialized_view(
        "mart.daily",
        "SELECT * FROM src.b",
        ["src.b"],
        author="alice",
        update_if_exists=True,
    )

    trigger_name = OpteryxCatalog._mv_trigger_name("ws.mart.daily")
    assert not src_a.collection(TRIGGERS_SUBCOLLECTION).document(trigger_name).get().exists
    assert src_b.collection(TRIGGERS_SUBCOLLECTION).document(trigger_name).get().exists

    doc = mv_ref.get().to_dict()
    statement = mv_ref.collection("statement").document(doc["statement-id"]).get().to_dict()
    assert statement["sequence-number"] == 2


def test_mv_cannot_read_another_mv():
    """Policy: a view's sources are plain datasets. No stacking, upward."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.mv1", sources=("src.a",))
    _add_dataset(catalog, "mart.mv2")

    with pytest.raises(MaterializedViewError, match="cannot read another materialized view"):
        catalog.create_materialized_view(
            "mart.mv2", "SELECT * FROM mart.mv1", ["mart.mv1"], author="alice"
        )
    # Rejected before anything was written: no trigger landed on mv1.
    assert catalog.list_triggers("mart.mv1") == []
    assert catalog._dataset_doc_ref("mart", "mv2").get().to_dict().get("dataset-type") is None


def test_mv_cannot_read_another_mv_via_workspace_qualified_name():
    """The same rejection, with the engine's fully-qualified source name."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.mv1", sources=("src.a",))
    _add_dataset(catalog, "mart.mv2")

    with pytest.raises(MaterializedViewError, match="cannot read another materialized view"):
        catalog.create_materialized_view(
            "mart.mv2", "SELECT * FROM mart.mv1", ["ws.mart.mv1"], author="alice"
        )


def test_cannot_register_an_mv_over_a_dataset_a_view_reads():
    """The same policy from the other end: src.a already feeds mv1, so turning
    src.a into a view would stack mv1 on top of it after the fact."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(catalog, "src.b")
    _register_mv(catalog, "mart.mv1", sources=("src.a",))

    with pytest.raises(MaterializedViewError, match="it is a source of materialized view ws.mart.mv1"):
        catalog.create_materialized_view("src.a", "SELECT * FROM src.b", ["src.b"], author="alice")
    # src.b picked up no trigger from the rejected registration.
    assert catalog.list_triggers("src.b") == []


def test_re_registering_an_mv_is_unaffected_by_its_own_triggers():
    """An MV's refresh triggers live on its sources, never on itself, so the
    no-stacking check must not trip on a plain CoRTAS re-registration."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(catalog, "src.b")
    _register_mv(catalog, "mart.mv1", sources=("src.a",))

    catalog.create_materialized_view(
        "mart.mv1",
        "SELECT * FROM src.b",
        ["src.b"],
        author="alice",
        update_if_exists=True,
    )
    assert catalog.get_materialized_view("mart.mv1")["source-tables"] == ["ws.src.b"]


def test_mv_cycle_rejected():
    """The backstop behind the no-stacking policy.

    A stacked graph cannot be built through `create_materialized_view` any more,
    so the transitive walk is driven directly against documents seeded the way a
    pre-policy catalog (or an out-of-band edit) would leave them: mv2 reads mv3,
    mv3 reads mv1, so pointing mv1 at mv2 closes the loop.
    """
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(
        catalog,
        "mart.mv2",
        **{"dataset-type": MATERIALIZED_VIEW_TYPE, "source-tables": ["ws.mart.mv3"]},
    )
    _add_dataset(
        catalog,
        "mart.mv3",
        **{"dataset-type": MATERIALIZED_VIEW_TYPE, "source-tables": ["ws.mart.mv1"]},
    )

    with pytest.raises(MaterializedViewError, match="cycle"):
        catalog._assert_no_materialized_view_cycle("ws.mart.mv1", ["ws.mart.mv2"])

    # A chain that never reaches back is fine.
    catalog._dataset_doc_ref("mart", "mv3").update({"source-tables": ["ws.src.a"]})
    catalog._assert_no_materialized_view_cycle("ws.mart.mv1", ["ws.mart.mv2"])


# --- runs-as (the pinned refresh identity) ------------------------------


def test_runs_as_is_pinned_to_the_creator():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.daily", sources=("src.a",))

    assert catalog.get_materialized_view("mart.daily")["runs-as"] == "alice"


def test_editing_a_view_does_not_transfer_who_it_runs_as():
    """The whole point of pinning. Someone else redefining the view records
    them as the statement's author, but the view keeps refreshing under the
    identity it was created with - transferring that takes an explicit act."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(catalog, "src.b")
    _register_mv(catalog, "mart.daily", sources=("src.a",))

    catalog.create_materialized_view(
        "mart.daily",
        "SELECT * FROM src.b",
        ["src.b"],
        author="bob",
        update_if_exists=True,
    )

    record = catalog.get_materialized_view("mart.daily")
    assert record["runs-as"] == "alice"  # unchanged by bob's edit
    assert record["last-updated-by"] == "bob"  # but bob is on the record
    assert isinstance(record["last-updated-at-ms"], int)


def test_set_materialized_view_owner_moves_it():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.daily", sources=("src.a",))

    catalog.set_materialized_view_owner("mart.daily", "svc-etl", author="admin")

    record = catalog.get_materialized_view("mart.daily")
    assert record["runs-as"] == "svc-etl"
    # A transfer is not an edit: no new statement version, sources untouched.
    assert record["last-updated-by"] == "alice"
    assert record["source-tables"] == ["ws.src.a"]


def test_set_materialized_view_owner_requires_author_and_a_view():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(catalog, "mart.plain")
    _register_mv(catalog, "mart.daily", sources=("src.a",))

    with pytest.raises(ValueError):
        catalog.set_materialized_view_owner("mart.daily", "svc-etl", author=None)
    with pytest.raises(ValueError):
        catalog.set_materialized_view_owner("mart.daily", "", author="admin")
    with pytest.raises(MaterializedViewError):
        catalog.set_materialized_view_owner("mart.plain", "svc-etl", author="admin")


# --- trigger naming -----------------------------------------------------


def test_trigger_names_do_not_collide_across_views():
    """`refresh__{collection}__{dataset}` alone collides: 'mart' + 'a__b' and
    'mart__a' + 'b' produce the same string. The digest separates them."""
    assert OpteryxCatalog._mv_trigger_name("ws.mart.a__b") != OpteryxCatalog._mv_trigger_name(
        "ws.mart__a.b"
    )
    # Still readable, and stable for the same target.
    name = OpteryxCatalog._mv_trigger_name("ws.mart.daily")
    assert name.startswith("refresh__mart__daily__")
    assert name == OpteryxCatalog._mv_trigger_name("ws.mart.daily")


def test_create_trigger_refuses_to_steal_another_views_trigger():
    """The safety net behind the digest. A blind overwrite would leave the
    first view with no trigger and nothing to report it - it would simply stop
    refreshing, forever."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    catalog.create_trigger("src.a", "shared", target_view="mart.one", author="alice")

    with pytest.raises(MaterializedViewError, match="already refreshes"):
        catalog.create_trigger("src.a", "shared", target_view="mart.two", author="bob")

    # Re-registering the SAME view through the same trigger is fine.
    catalog.create_trigger("src.a", "shared", target_view="mart.one", author="alice")
    [trigger] = catalog.list_triggers("src.a")
    assert trigger["target-view"] == "ws.mart.one"


def test_get_and_list_materialized_views():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(catalog, "mart.plain")
    _register_mv(catalog)

    record = catalog.get_materialized_view("mart.daily")
    assert record["sql"] == "SELECT * FROM src.a"
    assert record["source-tables"] == ["ws.src.a"]
    assert record["last-refreshed-at-ms"] is None

    assert catalog.list_materialized_views("mart") == ["daily"]
    with pytest.raises(MaterializedViewError):
        catalog.get_materialized_view("mart.plain")


def test_registration_survives_a_commit():
    """`save_dataset_metadata` replaces the whole dataset document, so the MV
    fields must round-trip through DatasetMetadata - otherwise a materialized
    view de-registers itself on its own first refresh commit."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog)
    catalog.mark_materialized_view_refreshed(
        "mart.daily", status="success", execution_id="job-1", author="alice"
    )

    # What a commit does: read the dataset, then write its metadata back.
    doc = catalog._dataset_doc_ref("mart", "daily").get()
    dataset = catalog._build_dataset("mart.daily", "mart", "daily", doc, False)
    catalog.save_dataset_metadata("mart.daily", dataset.metadata)

    record = catalog.get_materialized_view("mart.daily")
    assert record["source-tables"] == ["ws.src.a"]
    assert record["sql"] == "SELECT * FROM src.a"
    assert record["last-refresh-status"] == "success"
    assert record["last-refresh-execution-id"] == "job-1"
    # The pinned owner must survive too. It is written once at registration and
    # never rewritten, so a commit that dropped it would silently return the
    # view to running as whoever's write fired it - failing open, and only
    # visible as a permission denial on the next refresh.
    assert record["runs-as"] == "alice"


def test_mark_materialized_view_refreshed():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog)

    catalog.mark_materialized_view_refreshed(
        "mart.daily", status="success", execution_id="job-1", author="alice"
    )
    record = catalog.get_materialized_view("mart.daily")
    assert record["last-refresh-status"] == "success"
    assert record["last-refresh-execution-id"] == "job-1"
    assert isinstance(record["last-refreshed-at-ms"], int)


# --- drop and rename cascades -------------------------------------------


def test_drop_mv_removes_source_triggers_and_dataset():
    catalog = _catalog()
    src_a = _add_dataset(catalog, "src.a")
    _register_mv(catalog)

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_materialized_view("mart.daily", author="alice")

    trigger_ref = src_a.collection(TRIGGERS_SUBCOLLECTION).document(OpteryxCatalog._mv_trigger_name("ws.mart.daily"))
    assert not trigger_ref.get().exists
    assert not catalog._dataset_doc_ref("mart", "daily").get().exists


def test_drop_mv_rejects_plain_dataset():
    catalog = _catalog()
    _add_dataset(catalog, "mart.plain")
    with pytest.raises(MaterializedViewError):
        catalog.drop_materialized_view("mart.plain", author="alice")


def test_drop_dataset_directly_on_mv_still_cleans_source_triggers():
    """DROP TABLE should be rejected engine-side, but the catalog must not
    leak source triggers if a raw drop_dataset lands on an MV anyway."""
    catalog = _catalog()
    src_a = _add_dataset(catalog, "src.a")
    _register_mv(catalog)

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_dataset("mart.daily", author="alice")

    trigger_ref = src_a.collection(TRIGGERS_SUBCOLLECTION).document(OpteryxCatalog._mv_trigger_name("ws.mart.daily"))
    assert not trigger_ref.get().exists


def test_drop_dataset_deletes_trigger_and_statement_subcollections():
    """Subcollections go with the document.

    Uses a non-refresh trigger: a dataset carrying a *refresh* trigger is a
    materialized view's source and is refused outright (below), so it could
    never reach the deletion this asserts.
    """
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    src_ref = catalog._dataset_doc_ref("src", "a")
    catalog.create_trigger("src.a", "t1", target_view="mart.daily", author="alice", kind="other")
    src_ref.collection("statement").document("s1").set({"sql": "SELECT 1"})

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_dataset("src.a", author="alice")

    assert not src_ref.collection(TRIGGERS_SUBCOLLECTION).document("t1").get().exists
    assert not src_ref.collection("statement").document("s1").get().exists


def test_drop_dataset_refuses_a_source_a_view_reads():
    """Dropping a source would take its refresh triggers with it, leaving every
    dependent view refreshing on whatever sources remain - silently partial, or
    silently never again. Refuse instead, as rename already does."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.daily", sources=("src.a",))

    with pytest.raises(MaterializedViewError, match="source of materialized view"):
        catalog.drop_dataset("src.a", author="alice")
    assert catalog._dataset_doc_ref("src", "a").get().exists

    # Dropping the view first releases the source.
    catalog.drop_materialized_view("mart.daily", author="alice")
    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_dataset("src.a", author="alice")
    assert not catalog._dataset_doc_ref("src", "a").get().exists


def test_rename_rejects_mv_and_triggered_datasets():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog)

    with pytest.raises(MaterializedViewError):
        catalog.rename_dataset("mart.daily", "mart.renamed", author="alice")
    with pytest.raises(MaterializedViewError):
        catalog.rename_dataset("src.a", "src.renamed", author="alice")


# --- egress lock --------------------------------------------------------
#
# `egress_protection` on a workspace refuses automated copies of its datasets
# INTO ANOTHER workspace, and is ON unless explicitly turned off. Cross-workspace
# MV sources are not representable yet, so these tests pin the gate itself, the
# name disambiguation it rests on, and the creation- and fire-time behaviour a
# cross-workspace source will meet when they are.


def test_egress_protection_is_an_ordinary_settable_property():
    """Not a reserved lifecycle field - it goes through the same
    ALTER WORKSPACE ... SET path as deletion_protection, in both directions."""
    catalog = _catalog()
    assert catalog.is_egress_restricted() is True  # on from birth

    catalog.set_workspace_properties({"egress_protection": False}, author="alice")
    assert catalog.is_egress_restricted() is False

    catalog.set_workspace_properties({"egress_protection": True}, author="alice")
    assert catalog.is_egress_restricted() is True


@pytest.mark.parametrize(
    "props", [None, {}, {"egress_protection": None}], ids=["no-document", "absent", "null"]
)
def test_egress_is_restricted_from_birth(props):
    """No properties document, no property, or an explicit null: every state
    that means "nobody has decided" resolves to restricted. Sharing a
    workspace's data out is opted into, never defaulted into."""
    catalog = _catalog()
    if props is not None:
        catalog.firestore_client.collection("ichnos").document("$properties").set(dict(props))

    assert catalog.is_egress_restricted("ichnos") is True


def test_egress_lock_does_not_restrict_copies_within_the_workspace():
    """The flag protects data *leaving*. A view materializing its own
    workspace's data back into that workspace is not egress, so the lock -
    on by default, and never explicitly cleared here - says nothing about it.
    This is what keeps a default-on flag liveable."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")

    _register_mv(catalog, "mart.daily", sources=("src.a",))

    assert catalog.get_materialized_view("mart.daily")["source-tables"] == ["ws.src.a"]


def test_egress_lock_blocks_creation_reading_another_workspace():
    """Nothing is set on `ichnos` at all - the default is what refuses."""
    catalog = _catalog()
    _add_dataset(catalog, "mart.copy")

    with pytest.raises(
        EgressRestricted, match="ALTER WORKSPACE ichnos SET egress_protection TO OFF"
    ):
        catalog.create_materialized_view(
            "mart.copy",
            "SELECT * FROM ichnos.landing.orders",
            ["ichnos.landing.orders"],
            author="alice",
        )

    # Refused before anything was written: still a plain dataset.
    assert catalog._dataset_doc_ref("mart", "copy").get().to_dict().get("dataset-type") is None


def test_with_the_lock_cleared_a_cross_workspace_source_is_still_unreachable():
    """`ichnos` has opted out, so the egress gate allows and ordinary source
    validation decides. It refuses: a handle bound to `ws` cannot read or write
    another workspace's datasets, which is why cross-workspace sources are not
    representable yet. The guard is waiting on that, not inert."""
    catalog = _catalog()
    _add_dataset(catalog, "mart.copy")
    _set_egress_restriction(catalog, "ichnos", False)

    with pytest.raises(MaterializedViewError, match="belongs to workspace ichnos"):
        catalog.create_materialized_view(
            "mart.copy",
            "SELECT * FROM ichnos.landing.orders",
            ["ichnos.landing.orders"],
            author="alice",
        )


def test_a_local_collection_may_share_a_workspace_name():
    """A collection here called `ichnos` is no longer confusable with the
    workspace `ichnos`.

    This used to be genuinely ambiguous - `ichnos.landing.orders` could be
    either - and was resolved by probing Firestore and letting the local
    reading win. Qualified names remove the question: the local dataset is
    `ws.ichnos.landing.orders`, and nothing about a same-named workspace's
    egress lock touches it."""
    catalog = _catalog()
    _add_dataset(catalog, "ichnos.landing.orders")
    _add_dataset(catalog, "mart.copy")
    # The other workspace stays locked; it is simply not involved.
    _set_egress_restriction(catalog, "ichnos", True)

    catalog.create_materialized_view(
        "mart.copy",
        "SELECT * FROM ws.ichnos.landing.orders",
        ["ws.ichnos.landing.orders"],
        author="alice",
    )

    assert catalog.get_materialized_view("mart.copy")["source-tables"] == [
        "ws.ichnos.landing.orders"
    ]


def test_egress_lock_turned_on_after_registration_blocks_refresh():
    """The lock arrives after the view does - the case creation-time checking
    cannot cover, and the whole reason the gate runs at fire time too.

    `ichnos` starts opted out, so the view refreshes; then it opts back in and
    the next refresh is refused. The source list is edited onto the document
    rather than registered because a cross-workspace source is not
    representable yet (see the section note) - the fire-time contract is what
    is being pinned: blocked before the job document exists, surfaced as an
    alert and a trigger status, never raised into the commit.
    """
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.daily", sources=("src.a",))
    catalog._dataset_doc_ref("mart", "daily").update({"source-tables": ["ichnos.landing.orders"]})
    _set_egress_restriction(catalog, "ichnos", False)

    with (
        patch.object(trigger_firing, "_jobs_client"),
        patch.object(trigger_firing, "_enqueue_refresh_task", return_value="enqueued") as enqueue,
        patch.object(trigger_firing, "_policies_for", return_value=None),
    ):
        fire_triggers(catalog, "src.a", author="alice")

    assert enqueue.call_count == 1
    assert catalog.list_triggers("src.a")[0]["last-fired-status"] == "enqueued"

    # ichnos changes its mind.
    _set_egress_restriction(catalog, "ichnos", True)

    with (
        patch.object(trigger_firing, "_alert") as alert,
        patch.object(trigger_firing, "write_audit_record") as audit,
        patch.object(trigger_firing, "_jobs_client") as jobs_client,
        patch.object(trigger_firing, "_enqueue_refresh_task") as enqueue,
    ):
        fire_triggers(catalog, "src.a", author="alice")  # must not raise

    jobs_client.assert_not_called()
    enqueue.assert_not_called()
    assert isinstance(alert.call_args.args[0], EgressRestricted)
    assert audit.call_args.args[0]["event"] == "trigger.fire_failed"

    (trigger,) = catalog.list_triggers("src.a")
    assert trigger["last-fired-status"] == "egress-blocked"
