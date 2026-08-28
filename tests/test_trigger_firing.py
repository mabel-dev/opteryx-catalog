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
        "runs-as": "olive",
    }
    return catalog


def _refresh_trigger(name="refresh__mart__daily", target="ws.mart.daily"):
    return {"name": name, "kind": "materialized_view_refresh", "target-view": target}


# --- pure helpers --------------------------------------------------------


# --- fire_triggers flow --------------------------------------------------


def test_a_view_with_no_owner_refuses_to_fire():
    """A registered view with no `runs-as` is a damaged record, not a caller
    error, and the tempting default - the committing user - is the one answer
    guaranteed to be wrong: it silently reinstates invoker semantics, so the
    loss resurfaces hours later as a baffling permission denial.

    Nothing is enqueued, the trigger records why, and it alerts.
    """
    mv = dict(_catalog_stub().get_materialized_view.return_value)
    mv.pop("runs-as")
    catalog = _catalog_stub(triggers=[_refresh_trigger()], mv=mv)
    jobs_collection = MagicMock()
    jobs_client = MagicMock()
    jobs_client.collection.return_value = jobs_collection

    with (
        patch.object(trigger_firing, "_alert") as alert,
        patch.object(trigger_firing, "_submit_refresh_job", return_value=("exec-1", "enqueued")) as enq,
    ):
        # Never raises into the commit path, whatever it finds.
        fire_triggers(catalog, "src.a", author="alice", snapshot_id=123)

    jobs_collection.document.return_value.set.assert_not_called()
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
    # and the view's own definition, so this library cannot be wrong about them.
    assert kwargs["fired_by"] == "alice"
    assert kwargs["source_dataset"] == "src.a"
    assert kwargs["snapshot_id"] == 123
    assert kwargs["target_view"] == "ws.mart.daily"
    assert "runs_as" not in kwargs
    assert "billing_account" not in kwargs
    assert "policies" not in kwargs
    assert "task_id" not in kwargs

    catalog.mark_trigger_fired.assert_called_once_with(
        "src.a", "refresh__mart__daily", status="enqueued"
    )


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
            "runs-as": "olive",
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
