"""Reading the trigger graph backwards: what puts work INTO a dataset.

The gap this closes is a cross-workspace one, so every test here puts the source
in a different workspace from the target - that is the case the forward read
(one workspace's `information_schema.triggers`) structurally cannot answer, and
the only reason this exists.

The fake Firestore is restated in this file rather than shared, as in
test_listeners.py: what matters here is the DOCUMENT PATH, since neither record
carries its workspace, so this fake gives every document a `reference.path` and
the collection-group walk builds those paths the way the real database does.
"""

from __future__ import annotations

import pytest

from opteryx_catalog.inbound_edges import find_inbound_edges


class _Ref:
    def __init__(self, path):
        self.path = path


class _Doc:
    def __init__(self, doc_id, data, path):
        self.id = doc_id
        self._data = dict(data)
        self.reference = _Ref(path)

    def to_dict(self):
        return dict(self._data)


class _Query:
    """Enough of a Firestore query for a collection-group read: `==` and
    `array_contains`, ANDed, applied at `stream`."""

    def __init__(self, docs, filters=()):
        self._docs = list(docs)
        self._filters = list(filters)

    def where(self, filter=None):
        return _Query(self._docs, self._filters + [filter])

    def _matches(self, doc, f):
        value = doc.to_dict().get(f.field_path)
        if f.op_string == "array_contains":
            return isinstance(value, (list, tuple)) and f.value in value
        if f.op_string == "==":
            return value == f.value
        raise AssertionError(f"unexpected operator in this query: {f.op_string}")

    def stream(self):
        return [doc for doc in self._docs if all(self._matches(doc, f) for f in self._filters)]


class _Client:
    """Documents addressed by their full path, and grouped by the name of the
    collection they sit in - which is what `collection_group` selects on."""

    def __init__(self):
        self._docs = []

    def add(self, path, data):
        self._docs.append(_Doc(path.split("/")[-1], data, path))
        return self

    def collection_group(self, name):
        return _Query(
            [doc for doc in self._docs if doc.reference.path.split("/")[-2] == name]
        )


def _trigger(**overrides):
    record = {
        "name": "mvrefresh_events_7c29",
        "kind": "materialized_view_refresh",
        "holder": "ops.ingest.stdout_log",
        "target-view": "platform.billing.events",
        "target-task": None,
        "runs-as": "federator",
        "last-fired-at-ms": 1788516990288,
        "last-fired-status": "enqueued",
        "suspended-at-ms": None,
    }
    record.update(overrides)
    return record


TARGET = "platform.billing.events"


def test_a_trigger_in_another_workspace_is_an_inbound_edge():
    client = _Client().add(
        "ops/ingest/datasets/stdout_log/triggers/mvrefresh_events_7c29", _trigger()
    )

    rows = find_inbound_edges(client, TARGET)

    assert rows == [
        {
            "target": TARGET,
            "source": "ops.ingest.stdout_log",
            "workspace": "ops",
            "source_kind": "dataset",
            "kind": "materialized_view_refresh",
            "trigger": "mvrefresh_events_7c29",
            "runs_as": "federator",
            "last_fired_at_ms": 1788516990288,
            "last_fired_status": "enqueued",
            "suspended_at_ms": None,
        }
    ]


def test_a_trigger_pointing_at_a_task_is_found_by_the_other_spelling():
    """`target-task`, not `target-view`. One target, two fields; a lookup that
    asked only the first would miss every task in the catalog."""
    client = _Client().add(
        "ops/ingest/datasets/stdout_log/triggers/exec_ingest",
        _trigger(
            name="exec_ingest",
            kind="task",
            **{"target-view": None, "target-task": "platform.billing.ingest_task"},
        ),
    )

    rows = find_inbound_edges(client, "platform.billing.ingest_task")

    assert [row["trigger"] for row in rows] == ["exec_ingest"]
    assert rows[0]["kind"] == "task"


def test_a_task_that_writes_the_target_is_an_inbound_edge():
    """The case this was built for: a plain table is written by a TASK, and no
    trigger anywhere names the table. Asking only about triggers answers
    "nothing upstream" for exactly the dataset someone is looking at."""
    client = _Client().add(
        "ops/ingest/tasks/billing_events_ingest",
        {"name": "billing_events_ingest", "writes": [TARGET, "ops.ingest.audit"]},
    )

    rows = find_inbound_edges(client, TARGET)

    assert len(rows) == 1
    assert rows[0]["source"] == "ops.ingest.billing_events_ingest"
    assert rows[0]["kind"] == "writes"
    assert rows[0]["source_kind"] == "task"
    # A `writes` edge is a declaration, not a firing: there is no trigger name
    # and no fire to report on it.
    assert rows[0]["trigger"] is None
    assert rows[0]["last_fired_status"] is None


def test_a_task_that_writes_the_target_unqualified_is_found_in_its_own_workspace():
    """`writes` carries whatever the authoring statement named, and a bare
    `collection.name` means the task's own workspace."""
    client = _Client().add(
        "platform/reports/tasks/events_ingest",
        {"name": "events_ingest", "writes": ["billing.events"]},
    )

    rows = find_inbound_edges(client, TARGET)

    assert [row["source"] for row in rows] == ["platform.reports.events_ingest"]


def test_an_unqualified_write_in_another_workspace_is_not_this_dataset():
    """`billing.events` in the ops workspace is ops's own billing.events. Taking
    it would report an edge from one tenant into another's dataset."""
    client = _Client().add(
        "ops/ingest/tasks/events_ingest",
        {"name": "events_ingest", "writes": ["billing.events"]},
    )

    assert find_inbound_edges(client, TARGET) == []


def test_a_task_naming_the_target_both_ways_is_one_edge():
    client = _Client().add(
        "platform/reports/tasks/events_ingest",
        {"name": "events_ingest", "writes": [TARGET, "billing.events"]},
    )

    assert len(find_inbound_edges(client, TARGET)) == 1


def test_a_trigger_on_a_task_that_also_writes_the_target_is_two_edges():
    """Two different facts about the same task: what fires it, and what it
    writes. Collapsing them would lose the schedule."""
    client = (
        _Client()
        .add(
            "platform/reports/tasks/events_ingest",
            {"name": "events_ingest", "writes": [TARGET]},
        )
        .add(
            "platform/reports/tasks/events_ingest/triggers/schedule",
            _trigger(name="schedule", holder="platform.reports.events_ingest"),
        )
    )

    assert sorted(row["kind"] for row in find_inbound_edges(client, TARGET)) == [
        "materialized_view_refresh",
        "writes",
    ]


def test_a_trigger_held_by_a_task_reports_the_task_as_its_source():
    client = _Client().add(
        "ops/ingest/tasks/nightly/triggers/schedule",
        _trigger(name="schedule", holder="ops.ingest.nightly"),
    )

    rows = find_inbound_edges(client, TARGET)

    assert rows[0]["source"] == "ops.ingest.nightly"
    assert rows[0]["source_kind"] == "task"


def test_edges_into_other_datasets_are_not_returned():
    client = (
        _Client()
        .add(
            "ops/ingest/datasets/stdout_log/triggers/elsewhere",
            _trigger(**{"target-view": "platform.billing.other"}),
        )
        .add(
            "ops/ingest/tasks/other_ingest",
            {"name": "other_ingest", "writes": ["platform.billing.other"]},
        )
    )

    assert find_inbound_edges(client, TARGET) == []


def test_a_trigger_whose_record_and_path_disagree_is_dropped():
    """Neither name is safe to report. The row exists to say what is upstream,
    and this row cannot say which of two answers that is."""
    client = _Client().add(
        "ops/ingest/datasets/stdout_log/triggers/mvrefresh_events_7c29",
        _trigger(holder="somewhere.else.entirely"),
    )

    assert find_inbound_edges(client, TARGET) == []


def test_rows_come_back_in_a_stable_order():
    client = (
        _Client()
        .add(
            "ops/ingest/datasets/zulu/triggers/second",
            _trigger(name="second", holder="ops.ingest.zulu"),
        )
        .add(
            "ops/ingest/datasets/alpha/triggers/first",
            _trigger(name="first", holder="ops.ingest.alpha"),
        )
        .add(
            "ops/ingest/tasks/mid_ingest",
            {"name": "mid_ingest", "writes": [TARGET]},
        )
    )

    assert [row["source"] for row in find_inbound_edges(client, TARGET)] == [
        "ops.ingest.alpha",
        "ops.ingest.mid_ingest",
        "ops.ingest.zulu",
    ]


@pytest.mark.parametrize("target", ["", None, "billing.events", "events"])
def test_a_target_that_is_not_fully_qualified_is_refused(target):
    """A bare `collection.dataset` means a different thing in every workspace,
    and what is stored on both records is qualified - so this cannot be matched
    and must not be guessed at."""
    with pytest.raises(ValueError):
        find_inbound_edges(_Client(), target)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(pytest.main([__file__, "-q"]))
