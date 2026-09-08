"""The integrity sweep: ghosts and orphans the write paths promise not to make.

Firestore does not cascade deletes, so a bug or race in any delete path
leaves a GHOST - a document path with no document, kept addressable by
leftover subcollection documents and invisible to every `stream()`. The
production incident these tests encode: a refresh trigger under
`datasets/create` survived its dataset by three weeks because nothing in
normal operation can see a ghost. `audit_workspace` is the read that can.

The fakes here model the one Firestore behavior the whole module rests on:
`list_documents()` returns a reference for a path that holds subcollection
documents even when the document itself does not exist, while `stream()`
never does.
"""

from __future__ import annotations

from opteryx_catalog.integrity import audit_workspace


class _Doc:
    def __init__(self, id_, data, exists):
        self.id = id_
        self.exists = exists
        self._data = dict(data)

    def to_dict(self):
        return dict(self._data)


class _DocRef:
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

    def delete(self):
        self._data = {}
        self._exists = False

    def collection(self, name):
        if name not in self._subcollections:
            self._subcollections[name] = _Collection(name)
        return self._subcollections[name]

    def collections(self):
        return list(self._subcollections.values())


class _Collection:
    def __init__(self, id_):
        self.id = id_
        self._docs = {}

    def document(self, doc_id):
        if doc_id not in self._docs:
            self._docs[doc_id] = _DocRef(doc_id)
        return self._docs[doc_id]

    def stream(self):
        return [ref.get() for ref in self._docs.values() if ref._exists]

    def list_documents(self):
        # Firestore semantics: a missing document with subcollection documents
        # under it IS listed - that is the only read that surfaces a ghost.
        return [
            ref
            for ref in self._docs.values()
            if ref._exists
            or any(any(True for _ in sub.list_documents()) for sub in ref.collections())
        ]


class _Client:
    def __init__(self):
        self._collections = {}

    def collection(self, name):
        if name not in self._collections:
            self._collections[name] = _Collection(name)
        return self._collections[name]


def _dataset_ref(client, workspace, collection, dataset):
    coll_doc = client.collection(workspace).document(collection)
    coll_doc.set({"name": collection})
    return coll_doc.collection("datasets").document(dataset)


def _add_dataset(client, workspace, collection, dataset, **fields):
    ref = _dataset_ref(client, workspace, collection, dataset)
    ref.set({"name": dataset, **fields})
    return ref


def test_a_clean_workspace_has_no_findings():
    client = _Client()
    _add_dataset(client, "ws", "src", "a")
    assert audit_workspace(client, "ws") == []


def test_a_ghost_dataset_is_reported_with_what_keeps_it_addressable():
    # The production shape: the dataset document is gone, its trigger is not.
    client = _Client()
    ghost = _dataset_ref(client, "ws", "bastian", "create")
    ghost.collection("triggers").document("refresh__bastian__create_mv").set(
        {"name": "refresh__bastian__create_mv", "target-view": "bastian.create_mv"}
    )

    findings = audit_workspace(client, "ws")

    assert len(findings) == 1
    assert findings[0]["kind"] == "ghost-dataset"
    assert findings[0]["path"] == "ws/bastian/datasets/create"
    assert "triggers" in findings[0]["detail"]


def test_a_trigger_whose_target_view_is_gone_is_dangling():
    client = _Client()
    src = _add_dataset(client, "ws", "src", "a")
    src.collection("triggers").document("t").set(
        {"name": "t", "kind": "materialized_view_refresh", "target-view": "ws.mart.gone"}
    )

    findings = audit_workspace(client, "ws")

    assert [f["kind"] for f in findings] == ["dangling-trigger"]
    assert "ws.mart.gone" in findings[0]["detail"]


def test_a_legacy_two_part_target_resolves_relative_to_the_workspace():
    # Trigger records from before `_qualify` was the rule are two-part; they
    # must resolve, not be flagged as dangling.
    client = _Client()
    src = _add_dataset(client, "ws", "src", "a")
    _add_dataset(client, "ws", "mart", "daily", **{"dataset-type": "materialized_view"})
    src.collection("triggers").document("t").set(
        {"name": "t", "kind": "materialized_view_refresh", "target-view": "mart.daily"}
    )

    kinds = [f["kind"] for f in audit_workspace(client, "ws")]

    assert "dangling-trigger" not in kinds


def test_a_trigger_whose_target_task_is_gone_is_dangling():
    client = _Client()
    src = _add_dataset(client, "ws", "src", "a")
    src.collection("triggers").document("t").set(
        {"name": "t", "kind": "task", "target-task": "ws.ops.gone"}
    )

    findings = audit_workspace(client, "ws")

    assert [f["kind"] for f in findings] == ["dangling-trigger"]


def test_a_trigger_whose_target_task_exists_is_fine():
    client = _Client()
    src = _add_dataset(client, "ws", "src", "a")
    client.collection("ws").document("ops").collection("tasks").document("ingest").set(
        {"name": "ingest"}
    )
    src.collection("triggers").document("t").set(
        {"name": "t", "kind": "task", "target-task": "ws.ops.ingest"}
    )

    assert audit_workspace(client, "ws") == []


def test_an_mv_whose_source_lost_its_trigger_is_reported():
    # The other direction of the same inconsistency: the MV exists, the
    # source exists, and nothing connects them - the MV never refreshes.
    client = _Client()
    _add_dataset(client, "ws", "src", "a")
    _add_dataset(
        client,
        "ws",
        "mart",
        "daily",
        **{"dataset-type": "materialized_view", "source-tables": ["ws.src.a"]},
    )

    findings = audit_workspace(client, "ws")

    assert [f["kind"] for f in findings] == ["missing-source-trigger"]
    assert "ws.src.a" in findings[0]["detail"]


def test_an_mv_source_trigger_is_matched_by_target_not_by_name():
    # `_mv_trigger_name` has changed shape before (the digest suffix is newer
    # than the oldest live triggers); a trigger written under the old naming
    # still counts as long as it points back at the view.
    client = _Client()
    src = _add_dataset(client, "ws", "src", "a")
    _add_dataset(
        client,
        "ws",
        "mart",
        "daily",
        **{"dataset-type": "materialized_view", "source-tables": ["ws.src.a"]},
    )
    src.collection("triggers").document("refresh__mart__daily").set(
        {
            "name": "refresh__mart__daily",
            "kind": "materialized_view_refresh",
            "target-view": "ws.mart.daily",
        }
    )

    assert audit_workspace(client, "ws") == []


def _mv_with_trigger(client, workspace, collection, dataset, sources):
    """An MV reading `sources`, with each source's refresh trigger in place -
    so the only thing these fixtures can be reported for is the cycle."""
    _add_dataset(
        client,
        workspace,
        collection,
        dataset,
        **{"dataset-type": "materialized_view", "source-tables": list(sources)},
    )
    target = f"{workspace}.{collection}.{dataset}"
    for source in sources:
        src_ws, src_coll, src_name = source.split(".", 2)
        ref = _dataset_ref(client, src_ws, src_coll, src_name)
        ref.collection("triggers").document(f"refresh__{collection}__{dataset}").set(
            {"kind": "materialized_view_refresh", "target-view": target}
        )


def test_a_trigger_cycle_is_reported():
    # `create_trigger` refuses to close a loop, but two concurrent writes can
    # each read a graph that is still acyclic. This is the backstop that sees
    # the result: the trigger graph is what fires, and it must be a DAG.
    client = _Client()
    _mv_with_trigger(client, "ws", "mart", "a", ["ws.mart.b"])
    _mv_with_trigger(client, "ws", "mart", "b", ["ws.mart.a"])

    findings = [f for f in audit_workspace(client, "ws") if f["kind"] == "trigger-cycle"]

    assert len(findings) == 1
    assert findings[0]["path"] == "ws/mart/datasets/a"
    assert "ws.mart.a" in findings[0]["detail"] and "ws.mart.b" in findings[0]["detail"]


def test_a_trigger_chain_is_not_a_cycle():
    # Stacking is allowed; only a loop is a finding.
    client = _Client()
    _add_dataset(client, "ws", "src", "raw")
    _mv_with_trigger(client, "ws", "mart", "a", ["ws.src.raw"])
    _mv_with_trigger(client, "ws", "mart", "b", ["ws.mart.a"])
    _mv_with_trigger(client, "ws", "mart", "c", ["ws.mart.b"])

    assert audit_workspace(client, "ws") == []


def test_a_trigger_cycle_no_source_list_mentions_is_still_reported():
    # The whole reason the walk is over triggers and not `source-tables`: an
    # extra refresh trigger - written directly, or left behind by a
    # reconciliation that failed partway - is a firing edge that appears in no
    # source list. The source graph here is a clean chain.
    client = _Client()
    _add_dataset(client, "ws", "src", "raw")
    _mv_with_trigger(client, "ws", "mart", "a", ["ws.src.raw"])
    _mv_with_trigger(client, "ws", "mart", "b", ["ws.mart.a"])
    # b refreshes a, though a does not list b as a source.
    _dataset_ref(client, "ws", "mart", "b").collection("triggers").document("stale").set(
        {"kind": "materialized_view_refresh", "target-view": "ws.mart.a"}
    )

    findings = [f for f in audit_workspace(client, "ws") if f["kind"] == "trigger-cycle"]

    assert len(findings) == 1
    assert "ws.mart.a -> ws.mart.b -> ws.mart.a" in findings[0]["detail"]


def test_a_task_trigger_is_not_an_edge_in_the_trigger_graph():
    # A task records its SQL, never what the SQL writes, so it cannot be a node
    # and the walk must not try to make it one.
    client = _Client()
    src = _add_dataset(client, "ws", "src", "raw")
    client.collection("ws").document("ops").collection("tasks").document("roll").set({})
    src.collection("triggers").document("t").set({"kind": "task", "target-task": "ws.ops.roll"})

    assert audit_workspace(client, "ws") == []


def test_a_trigger_owned_by_a_platform_identity_is_reported():
    """The grandfathered rows. `create_trigger` and `set_trigger_owner` refuse a
    platform identity now, but only going forward - documents written before
    that gate keep firing as whatever they hold, because the fire path reads
    `runs-as` without re-judging it. The ones already there are the ones nobody
    will notice, precisely because they work."""
    client = _Client()
    src = _add_dataset(client, "ws", "src", "a")
    client.collection("ws").document("ops").collection("tasks").document("ingest").set(
        {"name": "ingest"}
    )
    src.collection("triggers").document("t").set(
        {"name": "t", "kind": "task", "target-task": "ws.ops.ingest", "runs-as": "federator"}
    )

    findings = audit_workspace(client, "ws")

    assert [f["kind"] for f in findings] == ["platform-identity-owner"]
    assert "federator" in findings[0]["detail"]


def test_a_refresh_trigger_owned_by_a_platform_identity_is_reported():
    """A refresh runs as its trigger's `runs-as`, like any other unattended
    work, so a platform identity there is the same unbilled standing compute it
    is on a task trigger. It used to be exempt here because the identity lived
    on the view and the trigger's field decided nothing."""
    client = _Client()
    src = _add_dataset(client, "ws", "src", "a")
    _add_dataset(client, "ws", "mart", "daily")
    src.collection("triggers").document("t").set(
        {
            "name": "t",
            "kind": "materialized_view_refresh",
            "target-view": "ws.mart.daily",
            "runs-as": "federator",
        }
    )

    findings = audit_workspace(client, "ws")

    assert [f["kind"] for f in findings] == ["platform-identity-owner"]
    assert "federator" in findings[0]["detail"]


def test_an_account_owned_trigger_is_fine():
    client = _Client()
    src = _add_dataset(client, "ws", "src", "a")
    client.collection("ws").document("ops").collection("tasks").document("ingest").set(
        {"name": "ingest"}
    )
    src.collection("triggers").document("t").set(
        {"name": "t", "kind": "task", "target-task": "ws.ops.ingest", "runs-as": "olive"}
    )

    assert audit_workspace(client, "ws") == []


# --- provenance (PROVENANCE_DESIGN.md S3)

_AFTER = 1_800_000_000_000  # after receipts became required
_BEFORE = 1_700_000_000_000


def _current_snapshot(ref, snapshot_id, **fields):
    ref.set({**ref.get().to_dict(), "current-snapshot-id": snapshot_id})
    ref.collection("snapshots").document(str(snapshot_id)).set(
        {"snapshot-id": snapshot_id, "timestamp-ms": _AFTER, "operation-type": "add-files", **fields}
    )


def _receipt(*names):
    return [{"dataset": n, "snapshot-id": 5, "resolved-by": "current"} for n in names]


def test_a_current_snapshot_with_no_receipt_is_reported():
    client = _Client()
    ref = _add_dataset(client, "ws", "mart", "y")
    _current_snapshot(ref, 42)

    findings = audit_workspace(client, "ws")

    assert [f["kind"] for f in findings] == ["missing-receipt"]
    assert findings[0]["path"] == "ws/mart/datasets/y/snapshots/42"


def test_pre_feature_history_is_not_a_missing_receipt():
    client = _Client()
    ref = _add_dataset(client, "ws", "mart", "y")
    _current_snapshot(ref, 42, **{"timestamp-ms": _BEFORE})

    assert audit_workspace(client, "ws") == []


def test_an_empty_receipt_is_a_receipt():
    client = _Client()
    ref = _add_dataset(client, "ws", "mart", "y")
    _current_snapshot(ref, 42, **{"read-sources": []})

    assert audit_workspace(client, "ws") == []


def _task(client, workspace, collection, name, **fields):
    coll_doc = client.collection(workspace).document(collection)
    coll_doc.set({"name": collection})
    ref = coll_doc.collection("tasks").document(name)
    ref.set({"name": name, **fields})
    return ref


def test_a_receipt_outside_the_producing_tasks_declaration_is_reported():
    client = _Client()
    _task(client, "ws", "ops", "t", reads=["ws.ops.a"])
    ref = _add_dataset(client, "ws", "mart", "y")
    _current_snapshot(
        ref, 42, **{"read-sources": _receipt("ws.ops.a", "ws.ops.b", "ws.mart.y"), "produced-by": "task:ws.ops.t"}
    )

    findings = audit_workspace(client, "ws")

    assert [f["kind"] for f in findings] == ["receipt-outside-declaration"]
    assert "ws.ops.b" in findings[0]["detail"]
    # The dataset itself (a MERGE reads its target) is never outside anything.
    assert "ws.mart.y" not in findings[0]["detail"]


def test_a_task_with_no_reads_recorded_yet_is_not_checked():
    client = _Client()
    _task(client, "ws", "ops", "t", writes=["ws.mart.y"])
    ref = _add_dataset(client, "ws", "mart", "y")
    _current_snapshot(ref, 42, **{"read-sources": _receipt("ws.ops.b"), "produced-by": "task:ws.ops.t"})

    assert audit_workspace(client, "ws") == []


def test_a_receipt_outside_a_views_source_tables_is_reported():
    client = _Client()
    _add_dataset(client, "ws", "ops", "a")
    _mv_with_trigger(client, "ws", "mart", "v", ["ws.ops.a"])
    ref = _dataset_ref(client, "ws", "mart", "v")
    _current_snapshot(ref, 42, **{"read-sources": _receipt("ws.ops.a", "ws.ops.b"), "produced-by": "view:ws.mart.v"})

    findings = audit_workspace(client, "ws")

    assert [f["kind"] for f in findings] == ["receipt-outside-declaration"]
    assert "ws.ops.b" in findings[0]["detail"]


def test_a_hand_run_statement_is_checked_against_nothing():
    client = _Client()
    ref = _add_dataset(client, "ws", "mart", "y")
    _current_snapshot(ref, 42, **{"read-sources": _receipt("ws.ops.b")})

    assert audit_workspace(client, "ws") == []


def test_a_windowed_task_that_read_past_its_window_is_reported():
    """The fire bound `current_version` 100; the run read the window source at
    120. A commit landed in between, and the next window binds those rows
    again - the only place the platform can see that."""
    client = _Client()
    _task(
        client, "ws", "ops", "t",
        reads=["ws.ops.a"],
        **{"last-window-to": 100, "trigger": {"source": "ws.ops.a", "name": "fire_t"}},
    )
    _add_dataset(client, "ws", "ops", "a")
    ref = _add_dataset(client, "ws", "mart", "y")
    _current_snapshot(
        ref, 42,
        **{"read-sources": [{"dataset": "ws.ops.a", "snapshot-id": 120, "resolved-by": "current"}],
           "produced-by": "task:ws.ops.t"},
    )

    findings = audit_workspace(client, "ws")

    assert [f["kind"] for f in findings] == ["window-overrun"]
    assert "120" in findings[0]["detail"] and "100" in findings[0]["detail"]


def test_a_windowed_task_that_read_its_window_exactly_is_fine():
    client = _Client()
    _task(
        client, "ws", "ops", "t",
        reads=["ws.ops.a"],
        **{"last-window-to": 100, "trigger": {"source": "ws.ops.a", "name": "fire_t"}},
    )
    _add_dataset(client, "ws", "ops", "a")
    ref = _add_dataset(client, "ws", "mart", "y")
    _current_snapshot(
        ref, 42,
        **{"read-sources": [{"dataset": "ws.ops.a", "snapshot-id": 100, "resolved-by": "current"}],
           "produced-by": "task:ws.ops.t"},
    )

    assert audit_workspace(client, "ws") == []


def test_a_task_held_trigger_windows_over_its_declared_dataset():
    """A schedule trigger lives on the TASK and names its window with OVER;
    the holder is not the window source."""
    client = _Client()
    task = _task(
        client, "ws", "ops", "t",
        reads=["ws.ops.a"],
        **{"last-window-to": 100, "trigger": {"source": "ws.ops.t", "name": "nightly"}},
    )
    task.collection("triggers").document("nightly").set(
        {"name": "nightly", "kind": "schedule", "target-task": "ws.ops.t", "window-source": "ws.ops.a"}
    )
    _add_dataset(client, "ws", "ops", "a")
    ref = _add_dataset(client, "ws", "mart", "y")
    _current_snapshot(
        ref, 42,
        **{"read-sources": [{"dataset": "ws.ops.a", "snapshot-id": 150, "resolved-by": "current"}],
           "produced-by": "task:ws.ops.t"},
    )

    assert [f["kind"] for f in audit_workspace(client, "ws")] == ["window-overrun"]
