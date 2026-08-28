"""Commit-time task firing.

A user-created commit reads the dataset's triggers and, for each task trigger,
submits `EXECUTE <task> USING <parent> AS parent_version, <current> AS
current_version` to jobs. The window is bound HERE, at fire time, so a run
means the same thing however late a worker picks it up.

GCP edges are mocked; these cover the statement built, the identity contract,
the suspension and owner-missing arms, and the never-break-the-commit rule.
"""

from __future__ import annotations

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from opteryx_catalog import trigger_firing
from opteryx_catalog.exceptions import TaskError
from opteryx_catalog.exceptions import TaskOwnerMissing
from opteryx_catalog.trigger_firing import fire_triggers


def _catalog_stub(triggers=None, task=None):
    catalog = MagicMock()
    catalog.workspace = "ws"
    catalog.list_triggers.return_value = triggers or [_task_trigger()]
    catalog.get_task.return_value = task or {
        "identifier": "ws.ops.compaction_log_ingest",
        "name": "compaction_log_ingest",
        "collection": "ops",
        "sql": "INSERT INTO ops.compaction_log SELECT 1",
        "runs-as": "federator",
        "suspended-at-ms": None,
    }
    return catalog


def _task_trigger(name="task__ops__compaction_log_ingest", target="ws.ops.compaction_log_ingest"):
    return {"name": name, "kind": "task", "target-task": target}


def _fire(catalog, snapshot_id=200, parent_snapshot_id=100):
    with (
        patch.object(trigger_firing, "_post_job", return_value={"execution_id": "x1"}) as post,
        patch.object(trigger_firing, "write_audit_record") as audit,
    ):
        fire_triggers(
            catalog,
            "ops.catalog_changes",
            author="alice",
            snapshot_id=snapshot_id,
            parent_snapshot_id=parent_snapshot_id,
        )
    return post, audit


# --- the statement


def test_window_is_bound_at_fire_time():
    catalog = _catalog_stub()
    post, _ = _fire(catalog)

    sql = post.call_args[0][0]["sql_text"]
    assert sql == (
        "EXECUTE ws.ops.compaction_log_ingest "
        "USING 100 AS parent_version, 200 AS current_version"
    )
    # Nothing relative reaches the worker - that is the whole point.
    assert "PREVIOUS" not in sql.upper()


def test_first_commit_has_no_parent_and_takes_everything():
    """A floor, not a skip: skipping would drop the first commit's rows, and
    provisioning a task before any data lands is the normal order."""
    catalog = _catalog_stub()
    post, _ = _fire(catalog, snapshot_id=200, parent_snapshot_id=None)

    assert "USING 1 AS parent_version" in post.call_args[0][0]["sql_text"]


def test_the_no_parent_floor_is_never_the_reserved_zero():
    """`VERSION AS OF 0` is the engine's sentinel for PREVIOUS, resolved against
    the chain WHEN THE QUERY RUNS. Binding 0 into a task that time-travels on
    :parent_version would silently restore the race this design removes - and
    the rewriter's refusal of a literal 0 does not catch it, because that runs
    on SQL text before parsing while binding happens after."""
    catalog = _catalog_stub()
    post, _ = _fire(catalog, snapshot_id=200, parent_snapshot_id=None)

    assert trigger_firing.NO_PARENT_VERSION_FLOOR != 0
    assert "USING 0 AS parent_version" not in post.call_args[0][0]["sql_text"]


def test_submission_names_the_task_never_the_principal():
    """jobs resolves runs-as from the task; a payload that could name the actor
    could name it wrongly."""
    catalog = _catalog_stub()
    post, _ = _fire(catalog)

    payload = post.call_args[0][0]
    assert payload["client_info"]["trigger"]["target_task"] == "ws.ops.compaction_log_ingest"
    assert "runs_as" not in payload
    assert "federator" not in str(payload["client_info"])


def test_fired_status_and_audit_are_recorded():
    catalog = _catalog_stub()
    _, audit = _fire(catalog)

    catalog.mark_trigger_fired.assert_called_once_with(
        "ops.catalog_changes", "task__ops__compaction_log_ingest", status="enqueued"
    )
    record = audit.call_args[0][0]
    assert record["event"] == "task.fired"
    assert record["parent_version"] == 100
    assert record["current_version"] == 200


# --- refusals and suppression


def test_a_suspended_task_records_and_enqueues_nothing():
    catalog = _catalog_stub(
        task={
            "identifier": "ws.ops.t",
            "sql": "SELECT 1",
            "runs-as": "federator",
            "suspended-at-ms": 1700000000000,
        }
    )
    post, _ = _fire(catalog)

    post.assert_not_called()
    catalog.mark_trigger_fired.assert_called_once_with(
        "ops.catalog_changes", "task__ops__compaction_log_ingest", status="suspended"
    )


def test_a_task_with_no_owner_refuses_to_fire():
    """Defaulting to the committer would silently reinstate invoker semantics."""
    catalog = _catalog_stub(
        task={"identifier": "ws.ops.t", "sql": "SELECT 1", "runs-as": None}
    )
    with (
        patch.object(trigger_firing, "_post_job") as post,
        patch.object(trigger_firing, "_alert") as alert,
        patch.object(trigger_firing, "write_audit_record"),
    ):
        fire_triggers(
            catalog, "ops.catalog_changes", author="alice", snapshot_id=200, parent_snapshot_id=100
        )

    post.assert_not_called()
    catalog.mark_trigger_fired.assert_called_once_with(
        "ops.catalog_changes", "task__ops__compaction_log_ingest", status="owner-missing"
    )
    assert isinstance(alert.call_args[0][0], TaskOwnerMissing)


def test_a_task_with_no_statement_alerts_and_enqueues_nothing():
    catalog = _catalog_stub(task={"identifier": "ws.ops.t", "sql": None, "runs-as": "federator"})
    with (
        patch.object(trigger_firing, "_post_job") as post,
        patch.object(trigger_firing, "_alert") as alert,
        patch.object(trigger_firing, "write_audit_record"),
    ):
        fire_triggers(
            catalog, "ops.catalog_changes", author="alice", snapshot_id=200, parent_snapshot_id=100
        )

    post.assert_not_called()
    assert isinstance(alert.call_args[0][0], TaskError)


def test_a_failing_task_never_breaks_the_commit():
    catalog = _catalog_stub()
    with (
        patch.object(trigger_firing, "_post_job", side_effect=RuntimeError("jobs down")),
        patch.object(trigger_firing, "_alert") as alert,
        patch.object(trigger_firing, "write_audit_record"),
    ):
        fire_triggers(
            catalog, "ops.catalog_changes", author="alice", snapshot_id=200, parent_snapshot_id=100
        )

    assert alert.called


def test_unknown_trigger_kinds_are_ignored():
    catalog = _catalog_stub(triggers=[{"name": "n", "kind": "something_else", "target-task": "t"}])
    post, _ = _fire(catalog)
    post.assert_not_called()


def test_mv_and_task_triggers_coexist_on_one_dataset():
    """Both kinds fire from the same commit; neither swallows the other."""
    catalog = _catalog_stub(
        triggers=[
            {"name": "refresh__m__d", "kind": "materialized_view_refresh", "target-view": "ws.m.d"},
            _task_trigger(),
        ]
    )
    catalog.get_materialized_view.return_value = {
        "identifier": "ws.m.d",
        "sql": "SELECT 1",
        "runs-as": "olive",
        "source-tables": [],
    }
    with (
        patch.object(trigger_firing, "_post_job", return_value={"execution_id": "x1"}) as post,
        patch.object(trigger_firing, "write_audit_record"),
    ):
        fire_triggers(
            catalog, "ops.catalog_changes", author="alice", snapshot_id=200, parent_snapshot_id=100
        )

    submitted = [c[0][0]["sql_text"] for c in post.call_args_list]
    assert any(s.startswith("REFRESH MATERIALIZED VIEW") for s in submitted)
    assert any(s.startswith("EXECUTE ws.ops.compaction_log_ingest") for s in submitted)
