"""Materialized views and their refresh triggers - catalog-side (Phase 1).

An MV is a normal dataset document wearing `dataset-type: materialized_view`,
its defining SQL versioned in a `statement` subcollection, and one refresh
trigger document under EACH source dataset's `triggers` subcollection. These
tests cover registration, trigger reconciliation, cycle rejection, and the
cascade rules in drop/rename.
"""

from __future__ import annotations

import pytest
from unittest.mock import patch

from opteryx_catalog.exceptions import DatasetNotFound
from opteryx_catalog.exceptions import MaterializedViewError
from opteryx_catalog.exceptions import TriggerNotFound
from opteryx_catalog.opteryx_catalog import MATERIALIZED_VIEW_TYPE
from opteryx_catalog.opteryx_catalog import OpteryxCatalog
from opteryx_catalog.opteryx_catalog import TRIGGERS_SUBCOLLECTION


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

    def set(self, data):
        self._data = dict(data)
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
    return catalog


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
    assert written["target-view"] == "mart.daily"
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
    assert doc["source-tables"] == ["src.a", "src.b"]
    statement = (
        mv_ref.collection("statement").document(doc["statement-id"]).get().to_dict()
    )
    assert statement["sql"].startswith("SELECT")
    assert statement["sequence-number"] == 1

    for src in (src_a, src_b):
        trigger = (
            src.collection(TRIGGERS_SUBCOLLECTION)
            .document("refresh__mart__daily")
            .get()
            .to_dict()
        )
        assert trigger["target-view"] == "mart.daily"
        assert trigger["statement-id"] == doc["statement-id"]


def test_create_mv_requires_backing_table():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    with pytest.raises(DatasetNotFound):
        catalog.create_materialized_view(
            "mart.missing", "SELECT 1", ["src.a"], author="alice"
        )


def test_create_mv_requires_sources_exist():
    catalog = _catalog()
    _add_dataset(catalog, "mart.daily")
    with pytest.raises(DatasetNotFound):
        catalog.create_materialized_view(
            "mart.daily", "SELECT 1", ["src.missing"], author="alice"
        )


def test_create_mv_rejects_empty_and_self_sources():
    catalog = _catalog()
    _add_dataset(catalog, "mart.daily")
    with pytest.raises(MaterializedViewError):
        catalog.create_materialized_view("mart.daily", "SELECT 1", [], author="alice")
    with pytest.raises(MaterializedViewError):
        catalog.create_materialized_view(
            "mart.daily", "SELECT 1", ["mart.daily"], author="alice"
        )


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

    catalog.create_materialized_view(
        "mart.daily", "SELECT * FROM src.a", ["src.a"], author="alice"
    )
    catalog.create_materialized_view(
        "mart.daily",
        "SELECT * FROM src.b",
        ["src.b"],
        author="alice",
        update_if_exists=True,
    )

    trigger_name = "refresh__mart__daily"
    assert not src_a.collection(TRIGGERS_SUBCOLLECTION).document(trigger_name).get().exists
    assert src_b.collection(TRIGGERS_SUBCOLLECTION).document(trigger_name).get().exists

    doc = mv_ref.get().to_dict()
    statement = (
        mv_ref.collection("statement").document(doc["statement-id"]).get().to_dict()
    )
    assert statement["sequence-number"] == 2


def test_mv_cycle_rejected():
    """mv2 reads mv1; re-pointing mv1 at mv2 would refresh forever."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.mv1", sources=("src.a",))
    _register_mv(catalog, "mart.mv2", sources=("mart.mv1",))

    with pytest.raises(MaterializedViewError):
        catalog.create_materialized_view(
            "mart.mv1",
            "SELECT * FROM mart.mv2",
            ["mart.mv2"],
            author="alice",
            update_if_exists=True,
        )


def test_get_and_list_materialized_views():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _add_dataset(catalog, "mart.plain")
    _register_mv(catalog)

    record = catalog.get_materialized_view("mart.daily")
    assert record["sql"] == "SELECT * FROM src.a"
    assert record["source-tables"] == ["src.a"]
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
    assert record["source-tables"] == ["src.a"]
    assert record["sql"] == "SELECT * FROM src.a"
    assert record["last-refresh-status"] == "success"
    assert record["last-refresh-execution-id"] == "job-1"


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

    trigger_ref = src_a.collection(TRIGGERS_SUBCOLLECTION).document("refresh__mart__daily")
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

    trigger_ref = src_a.collection(TRIGGERS_SUBCOLLECTION).document("refresh__mart__daily")
    assert not trigger_ref.get().exists


def test_drop_dataset_deletes_trigger_and_statement_subcollections():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    src_ref = catalog._dataset_doc_ref("src", "a")
    catalog.create_trigger("src.a", "t1", target_view="mart.daily", author="alice")
    src_ref.collection("statement").document("s1").set({"sql": "SELECT 1"})

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_dataset("src.a", author="alice")

    assert not src_ref.collection(TRIGGERS_SUBCOLLECTION).document("t1").get().exists
    assert not src_ref.collection("statement").document("s1").get().exists


def test_rename_rejects_mv_and_triggered_datasets():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog)

    with pytest.raises(MaterializedViewError):
        catalog.rename_dataset("mart.daily", "mart.renamed", author="alice")
    with pytest.raises(MaterializedViewError):
        catalog.rename_dataset("src.a", "src.renamed", author="alice")
