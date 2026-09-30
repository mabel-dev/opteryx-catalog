"""Commit-time trigger firing (Phase 2).

A user-created commit reads the dataset's triggers and, per distinct target
MV, writes a jobs/{execution_id} document and enqueues a named Cloud Task to
worker.opteryx. These tests exercise the flow with the GCP edges mocked:
job-document shape, invoker identity, dedup naming, housekeeping exclusion,
and the never-break-the-commit failure contract.
"""

from __future__ import annotations

from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from opteryx_catalog import trigger_firing
from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.metadata import Snapshot
from opteryx_catalog.exceptions import MaterializedViewError
from opteryx_catalog.trigger_firing import fire_triggers


def _snapshot(user_created=True):
    return Snapshot(
        snapshot_id=123,
        timestamp_ms=123,
        author="alice",
        sequence_number=1,
        user_created=user_created,
        operation_type="append",
    )


def _catalog_stub(triggers=None, mv=None):
    catalog = MagicMock()
    catalog.workspace = "ws"
    catalog.list_triggers.return_value = triggers or []
    catalog.get_materialized_view.return_value = mv or {
        "identifier": "ws.mart.daily",
        "name": "daily",
        "collection": "mart",
        "sql": "SELECT * FROM ws.src.a",
        "statement-id": "1",
        "source-tables": ["ws.src.a"],
    }
    return catalog


def _refresh_trigger(name="refresh__mart__daily", target="ws.mart.daily", runs_as="olive"):
    # `runs-as` on the TRIGGER: the identity an unattended refresh carries,
    # exactly as a task trigger carries the identity of the run it starts.
    trigger = {"name": name, "kind": "materialized_view_refresh", "target-view": target}
    if runs_as is not None:
        trigger["runs-as"] = runs_as
    return trigger


# --- pure helpers --------------------------------------------------------


# --- fire_triggers flow --------------------------------------------------


def test_a_trigger_with_no_owner_refuses_to_fire():
    """A refresh trigger with no `runs-as` is a damaged record, not a caller
    error, and the tempting default - the committing user - is the one answer
    guaranteed to be wrong: it silently reinstates invoker semantics, so the
    loss resurfaces hours later as a baffling permission denial.

    Nothing is enqueued, the trigger records why, and it alerts - naming the
    trigger and the statement that fixes it.
    """
    catalog = _catalog_stub(triggers=[_refresh_trigger(runs_as=None)])

    with (
        patch.object(trigger_firing, "_alert") as alert,
        patch.object(trigger_firing, "_submit_refresh_job", return_value=("exec-1", "enqueued")) as enq,
    ):
        # Never raises into the commit path, whatever it finds.
        fire_triggers(catalog, "src.a", author="alice", snapshot_id=123)

    enq.assert_not_called()
    alert.assert_called_once()
    message = str(alert.call_args.args[0])
    assert "refresh__mart__daily" in message
    assert "ALTER TRIGGER refresh__mart__daily ON src.a OWNER TO" in message
    assert "ALTER MATERIALIZED VIEW ws.mart.daily OWNER TO" in message
    catalog.mark_trigger_fired.assert_called_once_with(
        "src.a", "refresh__mart__daily", status="owner-missing"
    )


def test_the_views_own_record_is_never_a_fallback():
    """A view registered under the old model still carries `runs-as` on its
    own document. Reading it here would keep the old model alive one record at
    a time - the backfill script is what moves it - so a trigger with no
    identity is refused even when the view has one to offer."""
    mv = dict(_catalog_stub().get_materialized_view.return_value)
    mv["runs-as"] = "olive"
    catalog = _catalog_stub(triggers=[_refresh_trigger(runs_as=None)], mv=mv)

    with (
        patch.object(trigger_firing, "_alert") as alert,
        patch.object(trigger_firing, "_submit_refresh_job") as enq,
    ):
        fire_triggers(catalog, "src.a", author="alice", snapshot_id=123)

    enq.assert_not_called()
    alert.assert_called_once()
    catalog.mark_trigger_fired.assert_called_once_with(
        "src.a", "refresh__mart__daily", status="owner-missing"
    )


def test_the_missing_owner_error_is_alertable():
    """It means the platform is broken, so a human has to be told - unlike a
    caller error, which must never file a ticket."""
    from opteryx_catalog.exceptions import Alertable
    from opteryx_catalog.exceptions import CatalogError
    from opteryx_catalog.exceptions import MaterializedViewError
    from opteryx_catalog.exceptions import MaterializedViewOwnerMissing

    assert issubclass(MaterializedViewOwnerMissing, Alertable)
    assert issubclass(MaterializedViewOwnerMissing, CatalogError)
    # ...and distinct from the ordinary caller-error type, which is not.
    assert not issubclass(MaterializedViewError, Alertable)


def test_fire_submits_the_refresh_through_jobs():
    """The catalog no longer writes the job document or enqueues its own task; it
    hands jobs everything jobs cannot derive. The facts asserted here are the same
    ones this test checked when they were written into Firestore directly - they
    have moved from a document we wrote to a payload we send."""
    catalog = _catalog_stub(triggers=[_refresh_trigger()])

    with (
        patch.object(
            trigger_firing, "_submit_refresh_job", return_value=("exec-1", "enqueued")
        ) as submit,
    ):
        fire_triggers(catalog, "src.a", author="alice", snapshot_id=123)

    kwargs = submit.call_args.kwargs
    assert kwargs["sql_text"] == "REFRESH MATERIALIZED VIEW ws.mart.daily"
    assert "SELECT" not in kwargs["sql_text"]
    # Provenance only. The acting identity, policies, billing account and dedup
    # window are deliberately ABSENT: jobs resolves all four from the statement
    # and the trigger record the provenance names, so this library cannot be
    # wrong about them.
    assert kwargs["fired_by"] == "alice"
    # Fully qualified, though the commit path hands over `collection.dataset`:
    # the job document's provenance is checked against `ws.coll.*` grants by
    # the run-history listing, and a two-part name matched none of them.
    assert kwargs["source_dataset"] == "ws.src.a"
    assert kwargs["snapshot_id"] == 123
    assert kwargs["target_view"] == "ws.mart.daily"
    assert "runs_as" not in kwargs
    assert "billing_account" not in kwargs
    assert "policies" not in kwargs
    assert "task_id" not in kwargs

    catalog.mark_trigger_fired.assert_called_once_with(
        "src.a", "refresh__mart__daily", status="enqueued"
    )


def test_qualified_source_leaves_a_qualified_name_alone():
    """Idempotent, like `_qualify` on the catalog: a caller that already has
    the workspace on the name must not gain a second one."""
    catalog = _catalog_stub()
    assert trigger_firing._qualified_source(catalog, "src.a") == "ws.src.a"
    assert trigger_firing._qualified_source(catalog, "ws.src.a") == "ws.src.a"


def _workspace_doc(data):
    """A `workspaces/{name}` snapshot stub. `data is None` means no document."""
    snapshot = MagicMock()
    snapshot.exists = data is not None
    snapshot.to_dict.return_value = data
    client = MagicMock()
    client.collection.return_value.document.return_value.get.return_value = snapshot
    return client


def test_duplicate_targets_fire_once():
    catalog = _catalog_stub(
        triggers=[
            _refresh_trigger("t1"),
            _refresh_trigger("t2"),  # same target view
            {"name": "other", "kind": "something_else", "target-view": "mart.x"},
        ]
    )
    with (
        patch.object(trigger_firing, "_submit_refresh_job", return_value=("exec-1", "enqueued")) as enq,
    ):
        fire_triggers(catalog, "src.a", author="alice")

    assert enq.call_count == 1
    assert catalog.get_materialized_view.call_count == 1


def test_dedup_outcome_recorded():
    catalog = _catalog_stub(triggers=[_refresh_trigger()])
    with (
        patch.object(trigger_firing, "_submit_refresh_job", return_value=("exec-1", "deduplicated")),
    ):
        fire_triggers(catalog, "src.a", author="alice")

    catalog.mark_trigger_fired.assert_called_once_with(
        "src.a", "refresh__mart__daily", status="deduplicated"
    )


def test_failure_is_alerted_and_audited_but_not_raised():
    catalog = _catalog_stub(triggers=[_refresh_trigger()])
    catalog.get_materialized_view.side_effect = RuntimeError("stale trigger")

    with (
        patch.object(trigger_firing, "_alert") as alert,
        patch.object(trigger_firing, "write_audit_record") as audit,
    ):
        fire_triggers(catalog, "src.a", author="alice")  # must not raise

    assert alert.call_count == 1
    assert audit.call_args.args[0]["event"] == "trigger.fire_failed"


def test_one_bad_trigger_does_not_stop_the_rest():
    catalog = _catalog_stub(
        triggers=[_refresh_trigger("t1", "mart.broken"), _refresh_trigger("t2", "mart.ok")]
    )

    def mv_lookup(target):
        if target == "mart.broken":
            raise RuntimeError("boom")
        return {
            "identifier": "ws.mart.ok",
            "name": "ok",
            "collection": "mart",
            "sql": "SELECT 1",
        }

    catalog.get_materialized_view.side_effect = mv_lookup
    with (
        patch.object(trigger_firing, "_alert"),
        patch.object(trigger_firing, "_submit_refresh_job", return_value=("exec-1", "enqueued")) as enq,
    ):
        fire_triggers(catalog, "src.a", author="alice")

    assert enq.call_count == 1


def test_kill_switch(monkeypatch):
    monkeypatch.setenv("OPTERYX_TRIGGER_FIRING", "0")
    catalog = _catalog_stub(triggers=[_refresh_trigger()])
    fire_triggers(catalog, "src.a", author="alice")
    catalog.list_triggers.assert_not_called()


# --- OIDC identity -------------------------------------------------------


def _metadata_response(status=200, text="svc@project.iam.gserviceaccount.com"):
    response = MagicMock()
    response.status_code = status
    response.text = text
    return response


def test_refuses_to_submit_without_a_secret(monkeypatch):
    """No identity is a loud failure, not a request jobs will 401.

    Same guarantee as when this library minted its own OIDC token for Cloud
    Tasks - only who it authenticates as changed. A missing secret must stop
    the refresh here, where `fire_triggers` turns it into an alert and a
    recorded fire failure, rather than produce an unauthenticated call with a
    much dimmer trail.
    """
    monkeypatch.delenv(trigger_firing.FEDERATOR_CLIENT_SECRET_ENV, raising=False)
    trigger_firing._token_cache["access_token"] = None

    with pytest.raises(MaterializedViewError, match=trigger_firing.FEDERATOR_CLIENT_SECRET_ENV):
        trigger_firing._federator_token()


def test_a_failed_submission_audits_instead_of_breaking_the_commit():
    """The commit has already landed. A refresh that cannot be submitted is a
    fire failure - alerted and audited - and must never propagate into the write
    that triggered it. Previously forced by removing the OIDC identity; now by
    the submission itself failing, which is the same class of fault."""
    catalog = _catalog_stub(triggers=[_refresh_trigger()])
    with (
        patch.object(
            trigger_firing,
            "_submit_refresh_job",
            side_effect=MaterializedViewError("no credential"),
        ),
        patch.object(trigger_firing, "_alert") as alert,
        patch.object(trigger_firing, "write_audit_record") as audit,
    ):
        fire_triggers(catalog, "src.a", author="alice")  # must not raise

    assert alert.call_count == 1
    assert audit.call_args.args[0]["event"] == "trigger.fire_failed"


# --- _after_commit guard -------------------------------------------------


def _dataset_with_catalog():
    dataset = object.__new__(SimpleDataset)
    dataset.identifier = "src.a"
    dataset.catalog = MagicMock()
    dataset.catalog.workspace = "ws"
    return dataset


def test_after_commit_fires_for_user_snapshots():
    dataset = _dataset_with_catalog()
    with patch.object(trigger_firing, "fire_triggers") as fire:
        dataset._after_commit("alice", _snapshot(user_created=True))
    # The parent is threaded through as well: a task's window is this commit and
    # the one before it, bound now rather than resolved when the job runs.
    fire.assert_called_once_with(
        dataset.catalog,
        "src.a",
        author="alice",
        snapshot_id=123,
        parent_snapshot_id=None,
    )


def test_after_commit_skips_housekeeping_snapshots():
    """refresh_manifest / compaction snapshots must not re-run every MV."""
    dataset = _dataset_with_catalog()
    with patch.object(trigger_firing, "fire_triggers") as fire:
        dataset._after_commit("alice", _snapshot(user_created=False))
        dataset._after_commit("alice", _snapshot(user_created=None))
    fire.assert_not_called()


def test_after_commit_never_breaks_the_commit():
    dataset = _dataset_with_catalog()
    with (
        patch.object(trigger_firing, "fire_triggers", side_effect=RuntimeError("boom")),
        patch("opteryx_catalog.catalog.dataset._alert") as alert,
    ):
        dataset._after_commit("alice", _snapshot(user_created=True))
    assert alert.call_count == 1


# --- fire_trigger (manual "Run now") ----------------------------------------


def _task_trigger(name="task__mart__rollup", target="mart.rollup", interval=None):
    trigger = {
        "name": name,
        "kind": trigger_firing.TASK_TRIGGER_KIND,
        "target-task": target,
        "runs-as": "olive",
    }
    if interval is not None:
        trigger["minimum-interval-seconds"] = interval
    return trigger


def _task_catalog(trigger, last_window_to=None, head=500):
    catalog = _catalog_stub(triggers=[trigger])
    catalog.head_snapshot_id.return_value = head
    task = {
        "identifier": "ws.mart.rollup",
        "sql": "INSERT INTO ws.mart.out SELECT * FROM ws.src.a "
        "WHERE v > :parent_version AND v <= :current_version",
        "writes": [],
    }
    if last_window_to is not None:
        task["last-window-to"] = last_window_to
    catalog.get_task.return_value = task
    return catalog


def test_manual_refresh_fires_as_the_trigger_with_the_caller_recorded():
    catalog = _catalog_stub(triggers=[_refresh_trigger()])
    catalog.head_snapshot_id.return_value = 777

    with patch.object(
        trigger_firing, "_submit_refresh_job", return_value=("exec-9", "enqueued")
    ) as submit:
        result = trigger_firing.fire_trigger(catalog, "src.a", "refresh__mart__daily", "bob")

    assert result["status"] == "enqueued"
    assert result["execution_id"] == "exec-9"
    kwargs = submit.call_args.kwargs
    assert kwargs["sql_text"] == "REFRESH MATERIALIZED VIEW ws.mart.daily"
    assert kwargs["fired_by"] == "bob"
    assert kwargs["snapshot_id"] == 777
    catalog.mark_trigger_fired.assert_called_once_with(
        "src.a", "refresh__mart__daily", status="enqueued"
    )


def test_manual_fire_ignores_the_minimum_interval():
    """The floor damps bursts of commits. A person asking for one run is not a
    burst: the fire is neither refused nor does it claim the interval, so the
    next commit's fire is not silenced by it."""
    trigger = _refresh_trigger()
    trigger["minimum-interval-seconds"] = 3600
    catalog = _catalog_stub(triggers=[trigger])
    catalog.head_snapshot_id.return_value = 777

    with patch.object(trigger_firing, "_submit_refresh_job", return_value=("exec-9", "enqueued")):
        result = trigger_firing.fire_trigger(catalog, "src.a", "refresh__mart__daily", "bob")

    assert result["status"] == "enqueued"
    catalog.claim_trigger_fire.assert_not_called()


def test_the_commit_path_still_takes_the_floor():
    trigger = _refresh_trigger()
    trigger["minimum-interval-seconds"] = 3600
    catalog = _catalog_stub(triggers=[trigger])
    catalog.claim_trigger_fire.return_value = MagicMock(granted=False, interval_seconds=3600, at_ms=1)

    with (
        patch.object(trigger_firing, "_submit_refresh_job") as submit,
        patch.object(trigger_firing, "_submit_throttled_record"),
        patch.object(trigger_firing, "write_audit_record"),
    ):
        fire_triggers(catalog, "src.a", author="alice", snapshot_id=123)

    submit.assert_not_called()
    catalog.mark_trigger_fired.assert_called_once_with(
        "src.a", "refresh__mart__daily", status="throttled"
    )


def test_manual_refresh_of_a_suspended_view_says_so():
    mv = dict(_catalog_stub().get_materialized_view.return_value)
    mv["suspended-at-ms"] = 1
    catalog = _catalog_stub(triggers=[_refresh_trigger()], mv=mv)
    catalog.head_snapshot_id.return_value = 777

    with patch.object(trigger_firing, "_submit_refresh_job") as submit:
        result = trigger_firing.fire_trigger(catalog, "src.a", "refresh__mart__daily", "bob")

    submit.assert_not_called()
    assert result["status"] == "suspended"


def test_manual_task_run_windows_everything_since_the_last_success():
    catalog = _task_catalog(_task_trigger(interval=3600), last_window_to=300, head=500)

    with patch.object(
        trigger_firing, "_submit_task_job", return_value=("exec-7", "enqueued")
    ) as submit:
        result = trigger_firing.fire_trigger(catalog, "src.a", "task__mart__rollup", "bob")

    assert result["status"] == "enqueued"
    kwargs = submit.call_args.kwargs
    assert kwargs["sql_text"] == (
        "EXECUTE ws.mart.rollup USING 300 AS parent_version, 500 AS current_version"
    )
    assert kwargs["fired_by"] == "bob"
    catalog.claim_trigger_fire.assert_not_called()


def test_manual_task_run_that_never_succeeded_takes_everything():
    catalog = _task_catalog(_task_trigger(), last_window_to=None, head=500)

    with patch.object(
        trigger_firing, "_submit_task_job", return_value=("exec-7", "enqueued")
    ) as submit:
        trigger_firing.fire_trigger(catalog, "src.a", "task__mart__rollup", "bob")

    assert submit.call_args.kwargs["sql_text"] == (
        f"EXECUTE ws.mart.rollup USING {trigger_firing.NO_PARENT_VERSION_FLOOR} "
        "AS parent_version, 500 AS current_version"
    )


def test_manual_task_run_with_nothing_new_is_superseded():
    catalog = _task_catalog(_task_trigger(), last_window_to=500, head=500)

    with patch.object(trigger_firing, "_submit_task_job") as submit:
        result = trigger_firing.fire_trigger(catalog, "src.a", "task__mart__rollup", "bob")

    submit.assert_not_called()
    assert result["status"] == "superseded"


def test_manual_task_run_on_an_empty_source_fires_nothing():
    catalog = _task_catalog(_task_trigger(), head=None)

    with patch.object(trigger_firing, "_submit_task_job") as submit:
        result = trigger_firing.fire_trigger(catalog, "src.a", "task__mart__rollup", "bob")

    submit.assert_not_called()
    assert result["status"] == "superseded"
    catalog.mark_trigger_fired.assert_not_called()


def test_manual_fire_of_an_unknown_trigger_is_not_found():
    from opteryx_catalog.exceptions import TriggerNotFound

    catalog = _catalog_stub(triggers=[_refresh_trigger()])
    with pytest.raises(TriggerNotFound):
        trigger_firing.fire_trigger(catalog, "src.a", "nope", "bob")


def test_manual_fire_refuses_a_non_commit_trigger():
    from opteryx_catalog.exceptions import TriggerNotFound

    trigger = _refresh_trigger()
    trigger["event-kind"] = trigger_firing.SCHEDULE_EVENT_KIND
    catalog = _catalog_stub(triggers=[trigger])
    with pytest.raises(TriggerNotFound):
        trigger_firing.fire_trigger(catalog, "src.a", "refresh__mart__daily", "bob")


def test_manual_fire_failure_is_returned_not_raised():
    catalog = _catalog_stub(triggers=[_refresh_trigger(runs_as=None)])
    catalog.head_snapshot_id.return_value = 777

    with patch.object(trigger_firing, "_alert"), patch.object(trigger_firing, "_submit_refresh_job") as submit:
        result = trigger_firing.fire_trigger(catalog, "src.a", "refresh__mart__daily", "bob")

    submit.assert_not_called()
    assert result["status"] == "owner-missing"
    assert "no runs-as identity" in result["detail"]
