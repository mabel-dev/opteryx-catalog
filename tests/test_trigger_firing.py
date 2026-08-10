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
from opteryx_catalog.trigger_firing import _enqueue_refresh_task
from opteryx_catalog.trigger_firing import _make_job_id
from opteryx_catalog.trigger_firing import _oidc_service_account
from opteryx_catalog.trigger_firing import _runtime_service_account
from opteryx_catalog.trigger_firing import _task_id
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
        "name": "daily",
        "collection": "mart",
        "sql": "SELECT * FROM src.a",
        "statement-id": "1",
        "source-tables": ["src.a"],
    }
    return catalog


def _refresh_trigger(name="refresh__mart__daily", target="mart.daily"):
    return {"name": name, "kind": "materialized_view_refresh", "target-view": target}


# --- pure helpers --------------------------------------------------------


def test_job_id_shape():
    job_id = _make_job_id()
    prefix, _, rand = job_id.partition("-")
    assert len(prefix) == 14 and prefix.isdigit()
    assert len(rand) == 16 and rand.isalnum() and rand == rand.lower()


def test_task_id_is_stable_within_window_and_rolls_over():
    a = _task_id("ws", "refresh__mart__daily", now_s=960.0)
    b = _task_id("ws", "refresh__mart__daily", now_s=1019.9)
    c = _task_id("ws", "refresh__mart__daily", now_s=1020.0)
    assert a == b
    assert a != c


def test_task_id_sanitized():
    task_id = _task_id("w s", "trig.ger/name", now_s=0)
    assert all(ch.isalnum() or ch in "-_" for ch in task_id)


# --- fire_triggers flow --------------------------------------------------


def test_fire_writes_job_doc_and_enqueues():
    catalog = _catalog_stub(triggers=[_refresh_trigger()])
    jobs_collection = MagicMock()
    jobs_client = MagicMock()
    jobs_client.collection.return_value = jobs_collection

    with (
        patch.object(trigger_firing, "_jobs_client", return_value=jobs_client),
        patch.object(trigger_firing, "_enqueue_refresh_task", return_value="enqueued") as enq,
        patch.object(
            trigger_firing, "_policies_for", return_value=[{"role": "owner", "pattern": "*"}]
        ),
    ):
        fire_triggers(catalog, "src.a", author="alice", snapshot_id=123)

    (execution_id,) = jobs_collection.document.call_args.args
    job_doc = jobs_collection.document.return_value.set.call_args.args[0]

    assert job_doc["execution_id"] == execution_id
    # The statement names the intent. It is not the CoRTAS it desugars to, and
    # it does not carry the definition - the engine re-reads that from the
    # catalog when the refresh runs, so a view redefined between firing and
    # execution refreshes as its current self.
    assert job_doc["sql_text"] == "REFRESH MATERIALIZED VIEW ws.mart.daily"
    assert "SELECT" not in job_doc["sql_text"]
    assert job_doc["status"] == "SUBMITTED"
    assert job_doc["submitted_by"] == "alice"  # invoker semantics
    assert job_doc["billing_account"] == "alice"
    assert job_doc["origin"] == "trigger"
    assert job_doc["policies"] == [{"role": "owner", "pattern": "*"}]
    assert job_doc["trigger"]["source_dataset"] == "src.a"
    assert job_doc["trigger"]["snapshot_id"] == 123

    enq.assert_called_once()
    catalog.mark_trigger_fired.assert_called_once_with(
        "src.a", "refresh__mart__daily", status="enqueued"
    )


def test_duplicate_targets_fire_once():
    catalog = _catalog_stub(
        triggers=[
            _refresh_trigger("t1"),
            _refresh_trigger("t2"),  # same target view
            {"name": "other", "kind": "something_else", "target-view": "mart.x"},
        ]
    )
    with (
        patch.object(trigger_firing, "_jobs_client", return_value=MagicMock()),
        patch.object(trigger_firing, "_enqueue_refresh_task", return_value="enqueued") as enq,
        patch.object(trigger_firing, "_policies_for", return_value=None),
    ):
        fire_triggers(catalog, "src.a", author="alice")

    assert enq.call_count == 1
    assert catalog.get_materialized_view.call_count == 1


def test_dedup_outcome_recorded():
    catalog = _catalog_stub(triggers=[_refresh_trigger()])
    with (
        patch.object(trigger_firing, "_jobs_client", return_value=MagicMock()),
        patch.object(trigger_firing, "_enqueue_refresh_task", return_value="deduplicated"),
        patch.object(trigger_firing, "_policies_for", return_value=None),
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
        return {"name": "ok", "collection": "mart", "sql": "SELECT 1"}

    catalog.get_materialized_view.side_effect = mv_lookup
    with (
        patch.object(trigger_firing, "_alert"),
        patch.object(trigger_firing, "_jobs_client", return_value=MagicMock()),
        patch.object(trigger_firing, "_enqueue_refresh_task", return_value="enqueued") as enq,
        patch.object(trigger_firing, "_policies_for", return_value=None),
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


def test_runtime_service_account_from_metadata_server(monkeypatch):
    monkeypatch.setattr(trigger_firing, "_sa_cache", None)
    with patch.object(trigger_firing.requests, "get", return_value=_metadata_response()) as get:
        assert _runtime_service_account() == "svc@project.iam.gserviceaccount.com"
    assert get.call_args.kwargs["headers"] == {"Metadata-Flavor": "Google"}


def test_runtime_service_account_caches_only_success(monkeypatch):
    """A slow metadata server must not stick as 'no identity' for the process."""
    monkeypatch.setattr(trigger_firing, "_sa_cache", None)
    with patch.object(
        trigger_firing.requests, "get", side_effect=trigger_firing.requests.RequestException
    ):
        assert _runtime_service_account() is None
    with patch.object(trigger_firing.requests, "get", return_value=_metadata_response()) as get:
        assert _runtime_service_account() == "svc@project.iam.gserviceaccount.com"
        assert _runtime_service_account() == "svc@project.iam.gserviceaccount.com"
    assert get.call_count == 1


def test_env_overrides_runtime_identity(monkeypatch):
    monkeypatch.setenv("TASKS_OIDC_SA", "explicit@project.iam.gserviceaccount.com")
    with patch.object(trigger_firing, "_runtime_service_account") as runtime:
        assert _oidc_service_account() == "explicit@project.iam.gserviceaccount.com"
    runtime.assert_not_called()


def test_enqueue_mints_oidc_for_the_runtime_identity(monkeypatch):
    monkeypatch.delenv("TASKS_OIDC_SA", raising=False)
    monkeypatch.delenv("TASKS_OIDC_AUDIENCE", raising=False)
    client = MagicMock()
    client.queue_path.return_value = "projects/p/locations/l/queues/worker-dispatch"
    with (
        patch.object(trigger_firing, "_project_id", return_value="p"),
        patch.object(
            trigger_firing,
            "_runtime_service_account",
            return_value="runtime@project.iam.gserviceaccount.com",
        ),
        patch("google.cloud.tasks_v2.CloudTasksClient", return_value=client),
    ):
        assert _enqueue_refresh_task(MagicMock(), "exec-1", "task-1") == "enqueued"

    token = client.create_task.call_args.kwargs["task"]["http_request"]["oidc_token"]
    assert token.service_account_email == "runtime@project.iam.gserviceaccount.com"
    assert token.audience == "https://worker.opteryx.app/api/v1/submit"


def test_enqueue_refuses_to_send_an_unauthenticated_task(monkeypatch):
    """No identity is a loud failure, not a task the worker will 401."""
    monkeypatch.delenv("TASKS_OIDC_SA", raising=False)
    client = MagicMock()
    with (
        patch.object(trigger_firing, "_project_id", return_value="p"),
        patch.object(trigger_firing, "_runtime_service_account", return_value=None),
        patch("google.cloud.tasks_v2.CloudTasksClient", return_value=client),
        pytest.raises(MaterializedViewError, match="OIDC"),
    ):
        _enqueue_refresh_task(MagicMock(), "exec-1", "task-1")

    client.create_task.assert_not_called()


def test_missing_identity_audits_instead_of_breaking_the_commit(monkeypatch):
    monkeypatch.delenv("TASKS_OIDC_SA", raising=False)
    catalog = _catalog_stub(triggers=[_refresh_trigger()])
    with (
        patch.object(trigger_firing, "_jobs_client", return_value=MagicMock()),
        patch.object(trigger_firing, "_policies_for", return_value=None),
        patch.object(trigger_firing, "_runtime_service_account", return_value=None),
        patch("google.cloud.tasks_v2.CloudTasksClient", return_value=MagicMock()),
        patch.object(trigger_firing, "_project_id", return_value="p"),
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
    fire.assert_called_once_with(dataset.catalog, "src.a", author="alice", snapshot_id=123)


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
