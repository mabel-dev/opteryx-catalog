"""Task notification subscriptions - catalog side.

A subscription lives in a `listeners` subcollection under the TASK document,
keyed by the subscriber's identity. Under the task because a subscription is a
property of the task: `drop_task` sweeps them, and sweeping something held under
the task's own document cannot leave an orphan the way reaching across to
another dataset's documents can - which is exactly why `drop_task` does NOT
sweep the triggers that fire it.

The user's identity being the DOCUMENT ID is what makes one subscription per
user a property of the storage rather than a rule something has to remember.
"""

from __future__ import annotations

import pytest

from opteryx_catalog.exceptions import ListenerAlreadyExists
from opteryx_catalog.exceptions import ListenerNotFound
from opteryx_catalog.exceptions import TaskNotFound
from opteryx_catalog.opteryx_catalog import LISTENERS_SUBCOLLECTION
from opteryx_catalog.opteryx_catalog import OpteryxCatalog

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


class _Query:
    """Enough of a Firestore query for the collection-group read: filters are
    ANDed, and `stream` applies them. Like every fake in this suite it is
    restated per file rather than shared: each test file states exactly the
    Firestore surface its subject touches, so a change to that surface fails
    legibly here instead of somewhere else."""

    def __init__(self, docs, filters=()):
        self._docs = list(docs)
        self._filters = list(filters)

    def where(self, filter=None):
        return _Query(self._docs, self._filters + [filter])

    def stream(self):
        return [
            doc
            for doc in self._docs
            if all(doc.to_dict().get(f.field_path) == f.value for f in self._filters)
        ]


class _ClientWithGroups:
    """A stand-in Firestore client with `collection_group(name)` over the tree.

    Walks every document's subcollections looking for the name, which is what
    the real one does across the whole database - and is why the WORKSPACE
    filter, not the tree position, is what confines the query to one tenant.
    """

    def __init__(self):
        self._collections = {}

    def collection(self, name):
        if name not in self._collections:
            self._collections[name] = _Collection()
        return self._collections[name]

    def collection_group(self, name):
        docs = []

        def walk(collection):
            for ref in collection._docs.values():
                for sub_name, sub in ref._subcollections.items():
                    if sub_name == name:
                        docs.extend(sub.stream())
                    walk(sub)

        for collection in self._collections.values():
            walk(collection)
        return _Query(docs)


def _catalog():
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog.io = None
    catalog._snapshot_cache = {}
    catalog._schema_cache = {}
    catalog.firestore_client = _ClientWithGroups()
    catalog._catalog_ref = catalog.firestore_client.collection("ws")
    # Collections hang off the workspace root as documents, so the tasks - and
    # the listeners under them - are reachable from the client, which is what
    # the collection-group walk needs.
    catalog._collection_ref = lambda coll: catalog._catalog_ref.document(coll)
    return catalog


def _add_task(catalog, identifier="ops.rollup", writes=("marts.daily",)):
    collection, name = identifier.split(".", 1)
    ref = catalog._task_doc_ref(collection, name)
    ref.set({"writes": list(writes), "created-by": "olive", "statement-id": "1"})
    return ref


def _add_mv(catalog, identifier="marts.daily"):
    """A materialized view is a dataset document wearing the MV type."""
    collection, name = identifier.split(".", 1)
    ref = catalog._dataset_doc_ref(collection, name)
    ref.set({"dataset-type": "materialized_view", "name": name, "collection": collection})
    return ref


def _listener_ids(catalog, identifier="ops.rollup"):
    collection, name = identifier.split(".", 1)
    return sorted(
        doc.id for doc in catalog._listeners_collection(collection, name).stream()
    )


# --- adding


def test_the_user_is_the_document_id(capsys):
    catalog = _catalog()
    _add_task(catalog)

    catalog.add_listener("ops.rollup", user="alice", outcome="ERROR")

    assert _listener_ids(catalog) == ["alice"]


def test_the_subscription_records_what_was_asked_for(capsys):
    catalog = _catalog()
    _add_task(catalog)

    catalog.add_listener("ops.rollup", user="alice", outcome="ERROR")

    row = catalog.list_listeners("ops.rollup")[0]
    assert row["user"] == "alice"
    assert row["outcome"] == "ERROR"
    assert row["workspace"] == "ws"
    assert row["collection"] == "ops"
    assert row["object"] == "rollup"
    assert row["kind"] == "task"
    assert row["created-at-ms"] > 0


def test_the_default_is_every_outcome(capsys):
    catalog = _catalog()
    _add_task(catalog)

    catalog.add_listener("ops.rollup", user="alice")

    assert catalog.list_listeners("ops.rollup")[0]["outcome"] == "EVERYTHING"


def test_an_unknown_outcome_is_refused(capsys):
    catalog = _catalog()
    _add_task(catalog)

    with pytest.raises(ValueError, match="unknown listener outcome"):
        catalog.add_listener("ops.rollup", user="alice", outcome="SOMETIMES")


def test_subscribing_to_a_task_that_does_not_exist_is_refused(capsys):
    """A subscription to a name nothing answers to can never fire, and a typo is
    the likely cause."""
    catalog = _catalog()

    with pytest.raises(TaskNotFound):
        catalog.add_listener("ops.nothing", user="alice")


def test_a_second_subscription_by_the_same_user_is_refused(capsys):
    """One per user. Refusing is what lets the caller be TOLD what they already
    hold; an upsert would silently change a filter they set deliberately."""
    catalog = _catalog()
    _add_task(catalog)
    catalog.add_listener("ops.rollup", user="alice", outcome="ERROR")

    with pytest.raises(ListenerAlreadyExists):
        catalog.add_listener("ops.rollup", user="alice", outcome="SUCCESS")

    assert catalog.list_listeners("ops.rollup")[0]["outcome"] == "ERROR"


def test_two_users_hold_separate_subscriptions(capsys):
    catalog = _catalog()
    _add_task(catalog)

    catalog.add_listener("ops.rollup", user="alice", outcome="ERROR")
    catalog.add_listener("ops.rollup", user="rhea", outcome="SUCCESS")

    assert _listener_ids(catalog) == ["alice", "rhea"]


# --- removing


def test_dropping_removes_only_that_users_subscription(capsys):
    catalog = _catalog()
    _add_task(catalog)
    catalog.add_listener("ops.rollup", user="alice")
    catalog.add_listener("ops.rollup", user="rhea")

    catalog.drop_listener("ops.rollup", user="alice")

    assert _listener_ids(catalog) == ["rhea"]


def test_dropping_a_subscription_that_is_not_held_is_refused(capsys):
    """Not a silent no-op: that would tell someone they had unsubscribed from
    notifications they were never receiving, leaving the real subscription -
    under the name they meant - running."""
    catalog = _catalog()
    _add_task(catalog)

    with pytest.raises(ListenerNotFound):
        catalog.drop_listener("ops.rollup", user="alice")


# --- lifecycle


def test_drop_task_sweeps_the_subscriptions(capsys):
    """Subscriptions are a property of the task. Safe to sweep here in a way
    triggers are not: these live under the task's OWN document."""
    catalog = _catalog()
    task_ref = _add_task(catalog)
    catalog.add_listener("ops.rollup", user="alice")
    assert _listener_ids(catalog) == ["alice"]

    catalog.drop_task("ops.rollup", author="olive")

    assert task_ref.collection(LISTENERS_SUBCOLLECTION).stream() == []


# --- materialized views, on the same terms
#
# A trigger either EXECUTEs a task or REFRESHes a view, and the two paths differ
# only in the statement they build - so the subscribable object is whatever a
# trigger targets, and both kinds live in the same subcollection under it.


def test_a_materialized_view_can_be_subscribed_to(capsys):
    catalog = _catalog()
    _add_mv(catalog)

    catalog.add_listener("marts.daily", user="alice", outcome="ERROR")

    row = catalog.list_listeners("marts.daily")[0]
    assert row["object"] == "daily"
    assert row["kind"] == "materialized_view"


def test_the_kind_is_recorded_not_re_derived(capsys):
    """A table, a view and a task share one namespace, so the caller never says
    which - and the answer is stored rather than looked up per row."""
    catalog = _catalog()
    _add_task(catalog, "ops.rollup")
    _add_mv(catalog, "marts.daily")
    catalog.add_listener("ops.rollup", user="alice")
    catalog.add_listener("marts.daily", user="alice")

    kinds = {row["object"]: row["kind"] for row in catalog.list_listeners_for_user("alice")}

    assert kinds == {"rollup": "task", "daily": "materialized_view"}


def test_a_plain_table_cannot_be_subscribed_to(capsys):
    """Nothing fires a table, so the subscription could never be delivered."""
    catalog = _catalog()
    catalog._dataset_doc_ref("raw", "events").set({"name": "events", "collection": "raw"})

    with pytest.raises(TaskNotFound):
        catalog.add_listener("raw.events", user="alice")


def test_dropping_the_dataset_sweeps_a_views_subscriptions(capsys):
    catalog = _catalog()
    mv_ref = _add_mv(catalog)
    catalog.add_listener("marts.daily", user="alice")

    catalog._delete_subcollection(mv_ref.collection(LISTENERS_SUBCOLLECTION))

    assert mv_ref.collection(LISTENERS_SUBCOLLECTION).stream() == []


# --- listing for a user


def test_a_users_listing_covers_every_task_they_listen_to(capsys):
    catalog = _catalog()
    _add_task(catalog, "ops.rollup")
    _add_task(catalog, "ops.compact")
    catalog.add_listener("ops.rollup", user="alice", outcome="ERROR")
    catalog.add_listener("ops.compact", user="alice", outcome="SUCCESS")
    catalog.add_listener("ops.rollup", user="rhea")

    rows = catalog.list_listeners_for_user("alice")

    assert sorted(row["object"] for row in rows) == ["compact", "rollup"]
    assert {row["user"] for row in rows} == {"alice"}


def test_a_user_with_no_subscriptions_lists_nothing(capsys):
    catalog = _catalog()
    _add_task(catalog)
    catalog.add_listener("ops.rollup", user="alice")

    assert catalog.list_listeners_for_user("mallory") == []


def test_listing_requires_a_user(capsys):
    catalog = _catalog()

    with pytest.raises(ValueError, match="user must be provided"):
        catalog.list_listeners_for_user("")

