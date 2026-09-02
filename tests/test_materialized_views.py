"""Materialized views and their refresh triggers - catalog-side (Phase 1).

An MV is a normal dataset document wearing `dataset-type: materialized_view`,
its defining SQL versioned in a `statement` subcollection, and one refresh
trigger document under EACH source dataset's `triggers` subcollection. These
tests cover registration, trigger reconciliation, cycle rejection, the cascade
rules in drop/rename, and the workspace egress lock.
"""

from __future__ import annotations

from unittest.mock import MagicMock
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

    # `transaction` is accepted and ignored: these fakes are single-threaded, so
    # a transactional read sees what an ordinary one would. What the transaction
    # is here to prove is the ORDER (every read before any write) and the
    # ALL-OR-NOTHING, which `_Transaction` below mimics.
    def get(self, transaction=None):
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

    def stream(self, transaction=None):
        return [ref.get() for ref in self._docs.values() if ref._exists]


class _Transaction:
    """Enough of a Firestore transaction for `@firestore.transactional`.

    The decorator drives four private methods on whatever it is handed
    (`_clean_up`, `_begin`, `_commit`, plus `_read_only`/`_max_attempts`/`_id`),
    so a double has to answer them. Implemented rather than mocked away because
    the point is to keep the guarantee honest: writes land together at commit,
    and a refusal raised inside the body leaves NOTHING behind - which is what
    `create_trigger`'s one-trigger refusal depends on.
    """

    _read_only = False
    _max_attempts = 1
    _id = b"fake-txn"

    def __init__(self):
        self.writes = []
        self.committed = False

    def _clean_up(self):
        self.writes = []

    def _begin(self, retry_id=None):
        return None

    def _rollback(self):
        self.writes = []

    def _commit(self):
        for op, ref, data in self.writes:
            if op == "set":
                ref.set(data)
            elif op == "update":
                ref.update(data)
            else:
                ref.delete()
        self.committed = True
        return []

    def set(self, ref, data, merge=False):
        self.writes.append(("set", ref, data))

    def update(self, ref, data):
        self.writes.append(("update", ref, data))

    def delete(self, ref):
        self.writes.append(("delete", ref, None))


class _Batch:
    """Enough of a Firestore write batch: nothing lands until `commit`, and
    then everything does - which is what `set_materialized_view_owner` leans
    on to keep a view's refresh triggers agreeing at every moment."""

    def __init__(self):
        self.writes = []
        self.committed = False

    def update(self, ref, data):
        self.writes.append((ref, data))

    def commit(self):
        for ref, data in self.writes:
            ref.update(data)
        self.committed = True
        return []


class _FirestoreClient:
    """A stand-in for the Firestore client, whose root collections are the
    workspaces - which is what lets a handle bound to one workspace read
    another's `$properties` (the egress lock lives there)."""

    def __init__(self):
        self._collections = {}
        self.batches = []

    def collection(self, name):
        if name not in self._collections:
            self._collections[name] = _Collection()
        return self._collections[name]

    def transaction(self):
        return _Transaction()

    def batch(self):
        batch = _Batch()
        self.batches.append(batch)
        return batch


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


def test_two_registrations_in_one_millisecond_keep_both_statements():
    """The statement id is the millisecond it was written, so a redefinition
    inside the same millisecond must not overwrite the version the sequence
    number still claims is there."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(catalog, "src.b")
    mv_ref = _add_dataset(catalog, "mart.daily")

    with patch("opteryx_catalog.opteryx_catalog.time.time", return_value=1_700_000_000.0):
        catalog.create_materialized_view(
            "mart.daily", "SELECT * FROM src.a", ["src.a"], author="alice"
        )
        first = mv_ref.get().to_dict()["statement-id"]
        catalog.create_materialized_view(
            "mart.daily",
            "SELECT * FROM src.b",
            ["src.b"],
            author="alice",
            update_if_exists=True,
        )

    second = mv_ref.get().to_dict()["statement-id"]
    assert second != first

    statements = {doc.id: doc.to_dict() for doc in mv_ref.collection("statement").stream()}
    assert set(statements) == {first, second}
    assert statements[first]["sql"] == "SELECT * FROM src.a"
    assert statements[second]["sql"] == "SELECT * FROM src.b"
    assert statements[first]["sequence-number"] == 1
    assert statements[second]["sequence-number"] == 2


def test_two_view_registrations_in_one_millisecond_keep_both_statements():
    """`create_view` shares the millisecond-as-document-id convention."""
    catalog = _catalog()
    view_ref = catalog._view_doc_ref("mart", "daily")

    with patch("opteryx_catalog.opteryx_catalog.time.time", return_value=1_700_000_000.0):
        catalog.create_view("mart.daily", "SELECT 1", author="alice")
        first = view_ref.get().to_dict()["statement-id"]
        catalog.create_view("mart.daily", "SELECT 2", author="alice", update_if_exists=True)

    second = view_ref.get().to_dict()["statement-id"]
    assert second != first

    statements = {doc.id: doc.to_dict() for doc in view_ref.collection("statement").stream()}
    assert set(statements) == {first, second}
    assert statements[first]["sql"] == "SELECT 1"
    assert statements[second]["sql"] == "SELECT 2"
    assert statements[first]["sequence-number"] == 1
    assert statements[second]["sequence-number"] == 2


def test_mv_can_read_another_mv():
    """Chains are allowed: mv2 reads mv1, and picks up a trigger on it."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.mv1", sources=("src.a",))
    _add_dataset(catalog, "mart.mv2")

    catalog.create_materialized_view(
        "mart.mv2", "SELECT * FROM mart.mv1", ["mart.mv1"], author="alice"
    )

    assert catalog.get_materialized_view("mart.mv2")["source-tables"] == ["ws.mart.mv1"]
    # The refresh of mv1 is what fires mv2, so mv2's trigger lives on mv1.
    triggers = catalog.list_triggers("mart.mv1")
    assert [t["target-view"] for t in triggers] == ["ws.mart.mv2"]


def test_mv_can_be_registered_over_a_dataset_a_view_already_reads():
    """The same chain built from the other end: src.a already feeds mv1, and
    turning src.a into a view now simply puts mv1 on top of it."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(catalog, "src.b")
    _register_mv(catalog, "mart.mv1", sources=("src.a",))

    catalog.create_materialized_view("src.a", "SELECT * FROM src.b", ["src.b"], author="alice")

    assert catalog.get_materialized_view("src.a")["source-tables"] == ["ws.src.b"]
    # src.a keeps mv1's trigger and gains one of its own on src.b.
    assert [t["target-view"] for t in catalog.list_triggers("src.a")] == ["ws.mart.mv1"]
    assert [t["target-view"] for t in catalog.list_triggers("src.b")] == ["ws.src.a"]


def test_mv_cannot_read_itself_through_a_chain():
    """The loop is what registration refuses, at any depth."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.mv1", sources=("src.a",))
    _add_dataset(catalog, "mart.mv2")
    _register_mv(catalog, "mart.mv2", sources=("mart.mv1",))

    # Repointing mv1 at mv2 would close mv1 -> mv2 -> mv1.
    with pytest.raises(MaterializedViewError, match="cycle"):
        catalog.create_materialized_view(
            "mart.mv1",
            "SELECT * FROM mart.mv2",
            ["mart.mv2"],
            author="alice",
            update_if_exists=True,
        )
    # Rejected before anything was written: mv1 still reads src.a.
    assert catalog.get_materialized_view("mart.mv1")["source-tables"] == ["ws.src.a"]
    assert catalog.list_triggers("mart.mv2") == []


def test_mv_cycle_rejected_via_workspace_qualified_name():
    """The same rejection, with the engine's fully-qualified source name."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.mv1", sources=("src.a",))
    _add_dataset(catalog, "mart.mv2")
    _register_mv(catalog, "mart.mv2", sources=("mart.mv1",))

    with pytest.raises(MaterializedViewError, match="cycle"):
        catalog.create_materialized_view(
            "mart.mv1",
            "SELECT * FROM mart.mv2",
            ["ws.mart.mv2"],
            author="alice",
            update_if_exists=True,
        )


def test_create_trigger_refuses_to_close_a_loop():
    """The enforcing check, on the graph that fires. src.a -> mv1 -> mv2
    already exists; a trigger on mv2 aimed back at mv1 would close it."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.mv1", sources=("src.a",))
    _add_dataset(catalog, "mart.mv2")
    _register_mv(catalog, "mart.mv2", sources=("mart.mv1",))

    with pytest.raises(MaterializedViewError, match="trigger cycle"):
        catalog.create_trigger("mart.mv2", "hand_made", target_view="mart.mv1", author="alice")
    # Nothing was written: mv2 still carries no triggers.
    assert catalog.list_triggers("mart.mv2") == []


def test_create_trigger_refuses_a_self_loop():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")

    with pytest.raises(MaterializedViewError, match="trigger cycle"):
        catalog.create_trigger("src.a", "self", target_view="src.a", author="alice")


def test_create_trigger_allows_an_edge_that_extends_a_chain():
    """The DAG check refuses cycles, not depth."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.mv1", sources=("src.a",))
    _add_dataset(catalog, "mart.mv2")
    _register_mv(catalog, "mart.mv2", sources=("mart.mv1",))
    _add_dataset(catalog, "mart.mv3")

    # A second, independent edge INTO mv3 from the top of the chain is fine -
    # a DAG may have two paths to the same node, it just may not have a loop.
    catalog.create_trigger("src.a", "extra", target_view="mart.mv3", author="alice")
    catalog.create_trigger("mart.mv2", "extra", target_view="mart.mv3", author="alice")

    assert [t["target-view"] for t in catalog.list_triggers("mart.mv2")] == ["ws.mart.mv3"]


def test_create_trigger_cycle_check_ignores_task_triggers():
    """A task is not a node: it declares SQL, never what the SQL writes, so a
    task trigger on the path must neither be followed nor block an edge."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(catalog, "mart.mv1")
    catalog.create_task("ops.roll", "INSERT INTO src.a SELECT 1", author="alice")
    catalog.create_trigger(
        "mart.mv1",
        "run_task",
        target_view=None,
        target_task="ops.roll",
        kind="task",
        author="alice",
    )

    # Walking from mv1 finds only the task trigger, which is not an edge.
    catalog.create_trigger("src.a", "refresh", target_view="mart.mv1", author="alice")

    assert len(catalog.list_triggers("mart.mv1")) == 1


def test_re_registering_an_mv_is_unaffected_by_its_own_triggers():
    """An MV's refresh triggers live on its sources, never on itself, so a
    plain CoRTAS re-registration must not read as a loop."""
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
    """The transitive walk, driven directly, at a depth registration reaches
    only through a chain: mv2 reads mv3, mv3 reads mv1, so pointing mv1 at mv2
    closes the loop.
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


# --- runs-as (the refresh identity, on the triggers) --------------------
#
# A trigger is an event plus a `runs-as`. The view is stored SQL with no
# identity of its own - a person running REFRESH runs it as themselves - so
# the identity an unattended refresh carries lives on each refresh trigger,
# exactly where a task trigger keeps the identity of the run it starts.


def _refresh_triggers(catalog, view="ws.mart.daily", sources=("src.a",)):
    """`{source: runs-as}` for the view's refresh trigger on each source."""
    name = OpteryxCatalog._mv_trigger_name(view)
    found = {}
    for source in sources:
        [trigger] = [t for t in catalog.list_triggers(source) if t["name"] == name]
        found[source] = trigger.get("runs-as")
    return found


def test_runs_as_is_pinned_on_each_refresh_trigger_not_the_view():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(catalog, "src.b")
    _register_mv(catalog, "mart.daily", sources=("src.a", "src.b"))

    assert _refresh_triggers(catalog, sources=("src.a", "src.b")) == {
        "src.a": "alice",
        "src.b": "alice",
    }
    # Nothing on the view: not on its record, not in its document.
    assert "runs-as" not in catalog.get_materialized_view("mart.daily")
    assert "runs-as" not in catalog._dataset_doc_ref("mart", "daily").get().to_dict()


def test_a_refresh_trigger_is_gated_like_a_task_trigger():
    """Pinned to the author under the same platform-identity rule
    `create_trigger` applies to a task's trigger, and refused up front - before
    the statement version is written - rather than partway through the
    triggers."""
    from opteryx_catalog.exceptions import PlatformIdentityOwnerRefused

    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(catalog, "mart.daily")

    with pytest.raises(PlatformIdentityOwnerRefused):
        catalog.create_materialized_view(
            "mart.daily", "SELECT * FROM src.a", ["src.a"], author="federator"
        )

    assert catalog.list_triggers("src.a") == []
    assert catalog._dataset_doc_ref("mart", "daily").get().to_dict().get("dataset-type") is None


def test_editing_a_view_does_not_transfer_who_it_runs_as():
    """The whole point of pinning. Someone else redefining the view records
    them as the statement's author, but the trigger on a source the view
    already read keeps the identity it was created with; only the trigger on a
    NEWLY read source is pinned to the editor. Moving the rest takes an
    explicit act."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(catalog, "src.b")
    _register_mv(catalog, "mart.daily", sources=("src.a",))

    catalog.create_materialized_view(
        "mart.daily",
        "SELECT * FROM src.a JOIN src.b",
        ["src.a", "src.b"],
        author="bob",
        update_if_exists=True,
    )

    assert _refresh_triggers(catalog, sources=("src.a", "src.b")) == {
        "src.a": "alice",  # unchanged by bob's edit
        "src.b": "bob",  # new source, new trigger, bob's
    }
    record = catalog.get_materialized_view("mart.daily")
    assert record["last-updated-by"] == "bob"  # bob is on the record
    assert isinstance(record["last-updated-at-ms"], int)


def test_a_legacy_view_level_owner_is_inherited_by_a_trigger_it_would_otherwise_pin_to_the_editor():
    """A view registered before the identity moved onto its triggers carries
    `runs-as` on its own document, and its triggers carry none. Until the
    backfill script has moved it, a re-registration must not pin such a
    trigger to whoever is editing - that is the silent transfer the pin
    exists to prevent - so a trigger the edit creates or first pins inherits
    the view's value instead."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(catalog, "src.b")
    _register_mv(catalog, "mart.daily", sources=("src.a",))
    # Rewind to the old model: identity on the view, none on the trigger.
    catalog._dataset_doc_ref("mart", "daily").update({"runs-as": "alice"})
    name = OpteryxCatalog._mv_trigger_name("ws.mart.daily")
    trigger_ref = catalog._triggers_collection("src", "a").document(name)
    trigger_ref.set({k: v for k, v in trigger_ref.get().to_dict().items() if k != "runs-as"})
    assert _refresh_triggers(catalog) == {"src.a": None}

    catalog.create_materialized_view(
        "mart.daily",
        "SELECT * FROM src.a JOIN src.b",
        ["src.a", "src.b"],
        author="bob",
        update_if_exists=True,
    )

    assert _refresh_triggers(catalog, sources=("src.a", "src.b")) == {
        "src.a": "alice",
        "src.b": "alice",
    }
    # The legacy field is left for the backfill to retire, not destroyed here.
    assert catalog._dataset_doc_ref("mart", "daily").get().to_dict()["runs-as"] == "alice"


def test_set_materialized_view_owner_moves_every_refresh_trigger_at_once():
    """The convenience over `set_trigger_owner`: N sources are N triggers, and
    they are repointed in ONE batch so the view never refreshes as two
    identities depending on which source was written last."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(catalog, "src.b")
    _register_mv(catalog, "mart.daily", sources=("src.a", "src.b"))

    catalog.set_materialized_view_owner("mart.daily", "svc-etl", author="admin")

    assert _refresh_triggers(catalog, sources=("src.a", "src.b")) == {
        "src.a": "svc-etl",
        "src.b": "svc-etl",
    }
    [batch] = catalog.firestore_client.batches
    assert batch.committed and len(batch.writes) == 2
    record = catalog.get_materialized_view("mart.daily")
    # A transfer is not an edit: no new statement version, sources untouched,
    # and still nothing on the view itself.
    assert record["last-updated-by"] == "alice"
    assert record["source-tables"] == ["ws.src.a", "ws.src.b"]
    assert "runs-as" not in catalog._dataset_doc_ref("mart", "daily").get().to_dict()


def test_set_materialized_view_owner_finds_triggers_by_target_not_by_name():
    """The generated name has changed shape before; a trigger written under an
    older scheme is still this view's, and a repoint that missed it would
    leave the view refreshing as two identities."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.daily", sources=("src.a",))
    catalog.create_trigger("src.a", "refresh__mart__daily", target_view="mart.daily", author="alice")
    catalog.create_trigger("src.a", "other", target_view="mart.other", author="alice")

    catalog.set_materialized_view_owner("mart.daily", "svc-etl", author="admin")

    by_name = {t["name"]: t["runs-as"] for t in catalog.list_triggers("src.a")}
    assert by_name["refresh__mart__daily"] == "svc-etl"
    assert by_name[OpteryxCatalog._mv_trigger_name("ws.mart.daily")] == "svc-etl"
    assert by_name["other"] == "alice"  # another view's trigger is not touched


def test_set_materialized_view_owner_keeps_a_legacy_view_field_in_step():
    """A view still carrying the old field is kept agreeing with its triggers
    rather than having the field deleted here: retiring it is the backfill's
    job, and a reader still on the old model must see the same answer."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.daily", sources=("src.a",))
    catalog._dataset_doc_ref("mart", "daily").update({"runs-as": "alice"})

    catalog.set_materialized_view_owner("mart.daily", "svc-etl", author="admin")

    assert _refresh_triggers(catalog) == {"src.a": "svc-etl"}
    assert catalog._dataset_doc_ref("mart", "daily").get().to_dict()["runs-as"] == "svc-etl"


def test_set_materialized_view_owner_refuses_a_platform_identity_and_a_view_with_no_triggers():
    from opteryx_catalog.exceptions import PlatformIdentityOwnerRefused

    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.daily", sources=("src.a",))

    with pytest.raises(PlatformIdentityOwnerRefused):
        catalog.set_materialized_view_owner("mart.daily", "federator", author="admin")
    assert _refresh_triggers(catalog) == {"src.a": "alice"}

    # Nothing to repoint is a refusal, not a silent no-op.
    catalog.drop_trigger("src.a", OpteryxCatalog._mv_trigger_name("ws.mart.daily"), author="a")
    with pytest.raises(MaterializedViewError, match="no refresh trigger"):
        catalog.set_materialized_view_owner("mart.daily", "svc-etl", author="admin")


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
    # The refresh identity is on the triggers, which a commit to the view does
    # not touch - and the view's document gains no `runs-as` on the way back.
    assert _refresh_triggers(catalog) == {"src.a": "alice"}
    assert "runs-as" not in catalog._dataset_doc_ref("mart", "daily").get().to_dict()


def test_a_legacy_view_level_owner_survives_a_commit_until_the_backfill_retires_it():
    """Nothing writes the view-level field any more, but a commit must not
    DESTROY it before the backfill has copied it onto the triggers - the trap
    `DatasetMetadata` exists to close, one last time."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog)
    catalog._dataset_doc_ref("mart", "daily").update({"runs-as": "alice"})

    doc = catalog._dataset_doc_ref("mart", "daily").get()
    dataset = catalog._build_dataset("mart.daily", "mart", "daily", doc, False)
    catalog.save_dataset_metadata("mart.daily", dataset.metadata)

    assert catalog._dataset_doc_ref("mart", "daily").get().to_dict()["runs-as"] == "alice"


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
        patch.object(trigger_firing, "_submit_refresh_job", return_value=("exec-1", "enqueued")) as enqueue,
    ):
        fire_triggers(catalog, "src.a", author="alice")

    assert enqueue.call_count == 1
    assert catalog.list_triggers("src.a")[0]["last-fired-status"] == "enqueued"

    # ichnos changes its mind. The next commit lands after the trigger's
    # firing floor - inside it the fire would be throttled before the gate
    # is reached, which is a different arm.
    _set_egress_restriction(catalog, "ichnos", True)

    import time as _time

    with (
        patch.object(_time, "time", return_value=_time.time() + 200),
        patch.object(trigger_firing, "_alert") as alert,
        patch.object(trigger_firing, "write_audit_record") as audit,
        patch.object(trigger_firing, "_submit_refresh_job", return_value=("exec-1", "enqueued")) as enqueue,
    ):
        fire_triggers(catalog, "src.a", author="alice")  # must not raise

    enqueue.assert_not_called()
    assert isinstance(alert.call_args.args[0], EgressRestricted)
    assert audit.call_args.args[0]["event"] == "trigger.fire_failed"

    (trigger,) = catalog.list_triggers("src.a")
    assert trigger["last-fired-status"] == "egress-blocked"


# --- suspension ---------------------------------------------------------


def test_a_suspended_view_does_not_refresh():
    """SUSPEND stops the refresh without dismantling the machinery that does
    it. The trigger still fires and still records that it did - a suspended
    view says it is deliberately off, where a dropped trigger looked identical
    to one that was never created or that something broke."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.daily", sources=("src.a",))
    catalog.set_materialized_view_suspended("mart.daily", True, author="admin")

    with (
        patch.object(trigger_firing, "_submit_refresh_job", return_value=("exec-1", "enqueued")) as enq,
        patch.object(trigger_firing, "_alert") as alert,
    ):
        fire_triggers(catalog, "src.a", author="alice", snapshot_id=1)

    enq.assert_not_called()
    alert.assert_not_called()  # suspension is the setting working, not a failure
    assert catalog.list_triggers("src.a")[0]["last-fired-status"] == "suspended"

    record = catalog.get_materialized_view("mart.daily")
    assert record["suspended-by"] == "admin"
    assert isinstance(record["suspended-at-ms"], int)


def test_resume_lets_it_refresh_again():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.daily", sources=("src.a",))
    catalog.set_materialized_view_suspended("mart.daily", True, author="admin")
    catalog.set_materialized_view_suspended("mart.daily", False, author="admin")

    record = catalog.get_materialized_view("mart.daily")
    assert record["suspended-at-ms"] is None
    assert record["suspended-by"] is None

    with (
        patch.object(trigger_firing, "_submit_refresh_job", return_value=("exec-1", "enqueued")) as enq,
    ):
        fire_triggers(catalog, "src.a", author="alice", snapshot_id=1)
    enq.assert_called_once()


def test_suspension_survives_a_commit():
    """Same trap `runs-as` fell into: `save_dataset_metadata` replaces the whole
    document, so a suspended view that took any commit would silently resume."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.daily", sources=("src.a",))
    catalog.set_materialized_view_suspended("mart.daily", True, author="admin")

    doc = catalog._dataset_doc_ref("mart", "daily").get()
    dataset = catalog._build_dataset("mart.daily", "mart", "daily", doc, False)
    catalog.save_dataset_metadata("mart.daily", dataset.metadata)

    record = catalog.get_materialized_view("mart.daily")
    assert record["suspended-by"] == "admin"
    assert isinstance(record["suspended-at-ms"], int)


def test_a_trigger_never_survives_a_concurrent_dataset_drop():
    """The ghost-dataset race: `create_trigger` checks the dataset exists, then
    writes the trigger - and a `drop_dataset` landing between the two sweeps
    the triggers subcollection BEFORE this document arrives, then deletes the
    dataset. The write path must notice and undo, or the trigger survives as
    the only thing under a ghost path, invisible to every read but
    `list_documents()`. This happened in production."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(catalog, "mart.daily")

    dataset_ref = catalog._dataset_doc_ref("src", "a")
    trigger_ref = catalog._triggers_collection("src", "a").document("t")
    original_set = trigger_ref.set

    def set_and_lose_the_race(data, merge=False):
        original_set(data, merge=merge)
        dataset_ref.delete()

    trigger_ref.set = set_and_lose_the_race

    with pytest.raises(DatasetNotFound):
        catalog.create_trigger("src.a", "t", target_view="mart.daily", author="alice")

    assert not any(True for _ in catalog._triggers_collection("src", "a").stream())


# --- the firing floor ----------------------------------------------------


def _trigger_doc(catalog, dataset="src.a", name="t1"):
    collection, dataset_name = dataset.split(".")
    return (
        catalog._dataset_doc_ref(collection, dataset_name)
        .collection(TRIGGERS_SUBCOLLECTION)
        .document(name)
    )


def test_create_trigger_records_the_default_floor():
    """A NEW trigger takes the default. Written onto the record rather than
    read as a fallback at fire time, which is what keeps it off every trigger
    that already exists."""
    from opteryx_catalog.opteryx_catalog import DEFAULT_MINIMUM_INTERVAL_SECONDS

    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    catalog.create_trigger("src.a", "t1", target_view="mart.daily", author="alice")

    written = _trigger_doc(catalog).get().to_dict()
    assert written["minimum-interval-seconds"] == DEFAULT_MINIMUM_INTERVAL_SECONDS == 120
    assert written["last-claimed-at-ms"] is None


def test_create_trigger_takes_an_explicit_floor_and_zero_means_none():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    catalog.create_trigger(
        "src.a", "t1", target_view="mart.daily", author="alice", minimum_interval_seconds=30
    )
    assert _trigger_doc(catalog).get().to_dict()["minimum-interval-seconds"] == 30

    catalog.create_trigger(
        "src.a", "t2", target_view="mart.daily", author="alice", minimum_interval_seconds=0
    )
    assert _trigger_doc(catalog, name="t2").get().to_dict()["minimum-interval-seconds"] == 0


@pytest.mark.parametrize("bad", [-1, True, "120", 1.5])
def test_create_trigger_refuses_a_floor_that_is_not_a_whole_number_of_seconds(bad):
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    with pytest.raises(ValueError):
        catalog.create_trigger(
            "src.a", "t1", target_view="mart.daily", author="alice", minimum_interval_seconds=bad
        )
    assert not _trigger_doc(catalog).get().exists


def test_re_registration_keeps_the_floor_the_record_holds_unless_one_is_named():
    """CREATE OR REPLACE MATERIALIZED VIEW re-registers every source trigger.
    That must not reset a floor an operator set, and must not reopen a window
    the standing claim was closing."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    catalog.create_trigger(
        "src.a", "t1", target_view="mart.daily", author="alice", minimum_interval_seconds=30
    )
    _trigger_doc(catalog).update({"last-claimed-at-ms": 999})

    catalog.create_trigger("src.a", "t1", target_view="mart.daily", author="alice")
    written = _trigger_doc(catalog).get().to_dict()
    assert written["minimum-interval-seconds"] == 30
    assert written["last-claimed-at-ms"] == 999

    catalog.create_trigger(
        "src.a", "t1", target_view="mart.daily", author="alice", minimum_interval_seconds=45
    )
    assert _trigger_doc(catalog).get().to_dict()["minimum-interval-seconds"] == 45


def test_a_record_that_predates_the_floor_is_granted_without_a_write():
    """Existing triggers are not affected: no field, no floor, no claim
    written - the fire costs what it always did."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _trigger_doc(catalog).set({"name": "t1", "kind": "materialized_view_refresh"})

    claim = catalog.claim_trigger_fire("src.a", "t1")

    assert claim.granted
    assert claim.interval_seconds == 0
    assert "last-claimed-at-ms" not in _trigger_doc(catalog).get().to_dict()


def test_a_claim_is_refused_inside_the_floor_and_granted_after_it():
    import time as _time

    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    catalog.create_trigger(
        "src.a", "t1", target_view="mart.daily", author="alice", minimum_interval_seconds=120
    )

    with patch.object(_time, "time", return_value=1_000.0):
        first = catalog.claim_trigger_fire("src.a", "t1")
    assert first.granted
    assert first.at_ms == 1_000_000
    assert first.previous_ms is None
    assert _trigger_doc(catalog).get().to_dict()["last-claimed-at-ms"] == 1_000_000

    # 119.999s later: refused, and the refusal names the claim that holds.
    with patch.object(_time, "time", return_value=1_119.999):
        second = catalog.claim_trigger_fire("src.a", "t1")
    assert not second.granted
    assert second.at_ms == 1_000_000
    assert second.interval_seconds == 120
    assert _trigger_doc(catalog).get().to_dict()["last-claimed-at-ms"] == 1_000_000

    # Exactly the interval later: the floor is "less than", so this fires.
    with patch.object(_time, "time", return_value=1_120.0):
        third = catalog.claim_trigger_fire("src.a", "t1")
    assert third.granted
    assert third.previous_ms == 1_000_000
    assert _trigger_doc(catalog).get().to_dict()["last-claimed-at-ms"] == 1_120_000


def test_the_claim_is_read_and_written_in_one_transaction():
    """The point of the whole thing. The fake transaction records what was
    staged; the claim's write must go through it, never straight to the
    document - a direct update is exactly the read-then-stamp gap."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    catalog.create_trigger(
        "src.a", "t1", target_view="mart.daily", author="alice", minimum_interval_seconds=120
    )
    transaction = _Transaction()
    with patch.object(catalog.firestore_client, "transaction", return_value=transaction):
        claim = catalog.claim_trigger_fire("src.a", "t1")

    assert claim.granted
    assert transaction.committed
    assert [(op, data) for op, _, data in transaction.writes] == [
        ("update", {"last-claimed-at-ms": claim.at_ms})
    ]


def test_a_missing_trigger_cannot_be_claimed():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    with pytest.raises(TriggerNotFound):
        catalog.claim_trigger_fire("src.a", "nope")


def test_release_puts_the_previous_claim_back():
    """A fire that raised after claiming must not silence the next interval."""
    import time as _time

    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    catalog.create_trigger(
        "src.a", "t1", target_view="mart.daily", author="alice", minimum_interval_seconds=120
    )
    _trigger_doc(catalog).update({"last-claimed-at-ms": 500_000})

    with patch.object(_time, "time", return_value=1_000.0):
        claim = catalog.claim_trigger_fire("src.a", "t1")
    assert claim.granted and claim.previous_ms == 500_000

    catalog.release_trigger_fire("src.a", "t1", claim)
    assert _trigger_doc(catalog).get().to_dict()["last-claimed-at-ms"] == 500_000

    # ...and the very next claim is granted again.
    with patch.object(_time, "time", return_value=1_001.0):
        assert catalog.claim_trigger_fire("src.a", "t1").granted


def test_release_does_not_clobber_a_newer_claim():
    """A late release - the interval elapsed and another commit claimed - must
    leave the live claim alone, or it reopens the window the floor closed."""
    import time as _time

    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    catalog.create_trigger(
        "src.a", "t1", target_view="mart.daily", author="alice", minimum_interval_seconds=120
    )
    with patch.object(_time, "time", return_value=1_000.0):
        stale = catalog.claim_trigger_fire("src.a", "t1")
    with patch.object(_time, "time", return_value=2_000.0):
        live = catalog.claim_trigger_fire("src.a", "t1")
    assert stale.granted and live.granted

    catalog.release_trigger_fire("src.a", "t1", stale)
    assert _trigger_doc(catalog).get().to_dict()["last-claimed-at-ms"] == live.at_ms


def test_release_of_an_ungranted_or_floorless_claim_writes_nothing():
    from opteryx_catalog.opteryx_catalog import TriggerFireClaim

    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _trigger_doc(catalog).set({"name": "t1", "kind": "materialized_view_refresh"})

    refused = TriggerFireClaim(granted=False, at_ms=1, previous_ms=None, interval_seconds=120)
    floorless = TriggerFireClaim(granted=True, at_ms=1, previous_ms=None, interval_seconds=0)
    catalog.release_trigger_fire("src.a", "t1", refused)
    catalog.release_trigger_fire("src.a", "t1", floorless)
    assert "last-claimed-at-ms" not in _trigger_doc(catalog).get().to_dict()


def test_set_trigger_minimum_interval_is_how_an_existing_trigger_acquires_a_floor():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _trigger_doc(catalog).set({"name": "t1", "kind": "materialized_view_refresh"})

    with patch("opteryx_catalog.opteryx_catalog.emit_audit") as audit:
        catalog.set_trigger_minimum_interval("src.a", "t1", 60, author="olive")
    assert _trigger_doc(catalog).get().to_dict()["minimum-interval-seconds"] == 60
    assert audit.call_args[0][0] == "alter_trigger_minimum_interval"
    assert audit.call_args[1]["minimum_interval_seconds"] == 60

    catalog.set_trigger_minimum_interval("src.a", "t1", 0, author="olive")
    assert catalog.claim_trigger_fire("src.a", "t1").interval_seconds == 0

    with pytest.raises(ValueError):
        catalog.set_trigger_minimum_interval("src.a", "t1", -5, author="olive")
    with pytest.raises(TriggerNotFound):
        catalog.set_trigger_minimum_interval("src.a", "nope", 60, author="olive")
