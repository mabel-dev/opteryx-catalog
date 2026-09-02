"""`scripts/backfill_refresh_trigger_identity.py` - moving a view's `runs-as`
onto its refresh triggers, and retiring the view-level field afterwards.

The fakes are the ones `test_materialized_views` uses, extended with the two
things the script reads that the catalog itself never does: a collection-group
stream and a snapshot's `reference`.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from test_materialized_views import _Collection
from test_materialized_views import _Doc
from test_materialized_views import _DocRef
from test_materialized_views import _FirestoreClient

_SCRIPT = Path(__file__).parent.parent / "scripts" / "backfill_refresh_trigger_identity.py"
_spec = importlib.util.spec_from_file_location("backfill_refresh_trigger_identity", _SCRIPT)
backfill = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = backfill
_spec.loader.exec_module(backfill)


class _Snapshot(_Doc):
    def __init__(self, ref):
        super().__init__(ref.id, ref._data, ref._exists)
        self.reference = ref


class _Ref(_DocRef):
    """A document reference that knows its own path, as Firestore's does."""

    def __init__(self, id_, path):
        super().__init__(id_)
        self.path = path

    def get(self, transaction=None):
        return _Snapshot(self)

    def collection(self, name):
        if name not in self._subcollections:
            self._subcollections[name] = _PathedCollection(f"{self.path}/{name}")
        return self._subcollections[name]


class _PathedCollection(_Collection):
    def __init__(self, path):
        super().__init__()
        self.path = path

    def document(self, doc_id):
        if doc_id not in self._docs:
            self._docs[doc_id] = _Ref(doc_id, f"{self.path}/{doc_id}")
        return self._docs[doc_id]


class _Client(_FirestoreClient):
    def collection(self, name):
        if name not in self._collections:
            self._collections[name] = _PathedCollection(name)
        return self._collections[name]

    def collection_group(self, name):
        """Every document under any collection called `name`, at any depth."""
        found = []

        def walk(collection):
            for ref in collection._docs.values():
                if ref._exists and collection.path.rsplit("/", 1)[-1] == name:
                    found.append(_Snapshot(ref))
                for sub in ref._subcollections.values():
                    walk(sub)

        for root in self._collections.values():
            walk(root)

        class _Group:
            def stream(self_inner):
                return found

        return _Group()


def _dataset(client, qualified, **fields):
    ws, coll, name = qualified.split(".")
    ref = client.collection(ws).document(coll).collection("datasets").document(name)
    ref.set({"name": name, **fields})
    return ref


def _view(client, qualified, sources, runs_as="alice"):
    fields = {"dataset-type": "materialized_view", "source-tables": list(sources)}
    if runs_as:
        fields["runs-as"] = runs_as
    return _dataset(client, qualified, **fields)


def _trigger(client, source, view, runs_as=None, name="refresh__x"):
    ws, coll, dataset = source.split(".")
    ref = (
        client.collection(ws)
        .document(coll)
        .collection("datasets")
        .document(dataset)
        .collection("triggers")
        .document(name)
    )
    record = {"name": name, "kind": "materialized_view_refresh", "target-view": view}
    if runs_as:
        record["runs-as"] = runs_as
    ref.set(record)
    return ref


def test_plan_pins_a_trigger_with_no_owner_and_leaves_one_that_agrees():
    client = _Client()
    _dataset(client, "ws.src.a")
    _dataset(client, "ws.src.b")
    _view(client, "ws.mart.daily", ["ws.src.a", "ws.src.b"])
    _trigger(client, "ws.src.a", "ws.mart.daily")
    _trigger(client, "ws.src.b", "ws.mart.daily", runs_as="alice")

    [action] = backfill.plan(client, backfill.collect(client))

    assert action["view"] == "ws.mart.daily"
    assert [t["source"] for t in action["pin"]] == ["ws.src.a"]
    assert [t["source"] for t in action["current"]] == ["ws.src.b"]
    assert action["problems"] == []
    assert action["retirable"] is True


def test_plan_reports_a_trigger_that_disagrees_and_will_not_retire_the_view():
    """Somebody moved this trigger on its own. Which answer is right is a
    question about intent; the script reports it and touches nothing."""
    client = _Client()
    _dataset(client, "ws.src.a")
    _view(client, "ws.mart.daily", ["ws.src.a"])
    _trigger(client, "ws.src.a", "ws.mart.daily", runs_as="ginny")

    [action] = backfill.plan(client, backfill.collect(client))

    assert action["pin"] == []
    assert action["problems"] == ["refresh__x ON ws.src.a runs as ginny, the view says alice"]
    assert action["retirable"] is False


def test_plan_reports_a_source_with_no_trigger_and_a_view_with_nothing_to_copy():
    client = _Client()
    _dataset(client, "ws.src.a")
    _view(client, "ws.mart.daily", ["ws.src.a"])  # no trigger on the source
    _dataset(client, "ws.src.b")
    _view(client, "ws.mart.other", ["ws.src.b"], runs_as=None)
    _trigger(client, "ws.src.b", "ws.mart.other")  # nothing on either side
    _dataset(client, "ws.plain.table")  # not a view: not listed at all

    actions = {a["view"]: a for a in backfill.plan(client, backfill.collect(client))}

    assert set(actions) == {"ws.mart.daily", "ws.mart.other"}
    assert actions["ws.mart.daily"]["problems"] == [
        "source ws.src.a has no refresh trigger targeting the view"
    ]
    assert actions["ws.mart.other"]["problems"] == [
        "refresh__x ON ws.src.b has no runs-as and the view holds none to copy"
    ]
    assert not actions["ws.mart.other"]["retirable"]


def test_collect_can_be_limited_to_workspaces():
    client = _Client()
    _view(client, "ws.mart.daily", [])
    _view(client, "other.mart.daily", [])

    assert [v["view"] for v in backfill.collect(client, {"other"})] == ["other.mart.daily"]


def test_pin_writes_only_where_nothing_arrived_since():
    client = _Client()
    _dataset(client, "ws.src.a")
    _view(client, "ws.mart.daily", ["ws.src.a"])
    trigger = _trigger(client, "ws.src.a", "ws.mart.daily")

    assert backfill.pin(client, trigger, "alice") is None
    assert trigger.get().to_dict()["runs-as"] == "alice"
    # Idempotent: a trigger that already agrees is left alone, silently.
    assert backfill.pin(client, trigger, "alice") is None
    # A trigger pinned to someone ELSE since the plan was made is refused,
    # because overwriting it is exactly the silent transfer this avoids.
    trigger.update({"runs-as": "ginny"})
    assert "ginny" in backfill.pin(client, trigger, "alice")
    assert trigger.get().to_dict()["runs-as"] == "ginny"


def test_retire_removes_the_field_only_while_it_still_says_what_was_planned(monkeypatch):
    from google.cloud import firestore

    client = _Client()
    view = _view(client, "ws.mart.daily", [])

    # The fake `update` stores the sentinel; real Firestore deletes the field.
    assert backfill.retire(client, view, "alice") is None
    assert view.get().to_dict()["runs-as"] is firestore.DELETE_FIELD

    view.update({"runs-as": "ginny"})
    assert "ginny" in backfill.retire(client, view, "alice")
    assert view.get().to_dict()["runs-as"] == "ginny"
