"""Telling a task's subscribers what happened - the producer side.

Two emit points, because a task fails in two disjoint places: a run that ran
and failed (worker.opteryx), and a run that never started (the catalog's own
`_dispatch`). A subscriber told about only the first learns nothing in the case
that hurts most - an egress block or a missing owner, where the task silently
never runs while `last-fired-status` keeps reading `enqueued`.

Every failure body is INSTRUCTIVE: it names the statement that fixes it.
"""

from __future__ import annotations

import json
import re
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from opteryx_catalog import task_notifications as tn


class _Catalog:
    """Just the one method the producer reads."""

    def __init__(self, listeners=(), raises=None):
        self._listeners = list(listeners)
        self._raises = raises

    def list_listeners(self, identifier):
        if self._raises is not None:
            raise self._raises
        return list(self._listeners)


@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setenv("CONTROL_URL", "https://control.opteryx.app/")
    monkeypatch.setenv("CONTROL_ADMIN_TOKEN", "sekrit")
    monkeypatch.setenv("OPTERYX_NOTIFICATIONS_QUEUE", "projects/p/locations/l/queues/notifications")


def _enqueued(mock_enqueue):
    """The payloads handed to the queue, in order."""
    return [call.args[3] for call in mock_enqueue.call_args_list]


# --- routing


def test_a_failed_run_reaches_an_error_subscriber(configured):
    catalog = _Catalog([{"user": "alice", "outcome": "ERROR"}])

    with patch.object(tn, "_enqueue") as enqueue:
        sent = tn.notify_run_finished(catalog, "ws.ops.rollup", "failed")

    assert sent == 1
    assert _enqueued(enqueue)[0]["client_id"] == "alice"


def test_a_failed_run_does_not_reach_a_success_subscriber(configured):
    catalog = _Catalog([{"user": "alice", "outcome": "SUCCESS"}])

    with patch.object(tn, "_enqueue") as enqueue:
        sent = tn.notify_run_finished(catalog, "ws.ops.rollup", "failed")

    assert sent == 0
    assert enqueue.call_count == 0


def test_everything_takes_both(configured):
    catalog = _Catalog([{"user": "alice", "outcome": "EVERYTHING"}])

    with patch.object(tn, "_enqueue") as enqueue:
        assert tn.notify_run_finished(catalog, "ws.ops.rollup", "failed") == 1
        assert tn.notify_run_finished(catalog, "ws.ops.rollup", "succeeded") == 1
    assert enqueue.call_count == 2


def test_a_fire_that_never_started_is_always_an_error(configured):
    """Nothing ran, so there is no success reading of it - and a SUCCESS-only
    subscriber is not told, because nothing succeeded."""
    catalog = _Catalog(
        [{"user": "alice", "outcome": "ERROR"}, {"user": "bobby", "outcome": "SUCCESS"}]
    )

    with patch.object(tn, "_enqueue") as enqueue:
        sent = tn.notify_fire_failed(catalog, "ws.ops.rollup", "egress-blocked")

    assert sent == 1
    assert _enqueued(enqueue)[0]["client_id"] == "alice"


def test_an_unrecognised_subscription_filter_sends_nothing(configured):
    """A filter that cannot be read is not an invitation to send everything:
    that turns one bad record into a stream nobody asked for."""
    catalog = _Catalog([{"user": "alice", "outcome": "SOMETIMES"}])

    with patch.object(tn, "_enqueue") as enqueue:
        assert tn.notify_run_finished(catalog, "ws.ops.rollup", "failed") == 0
    assert enqueue.call_count == 0


def test_an_unknown_run_status_is_treated_as_an_error(configured):
    """Silence about a run that did not work is worse than an unhelpful bell."""
    catalog = _Catalog([{"user": "alice", "outcome": "ERROR"}])

    with patch.object(tn, "_enqueue") as enqueue:
        assert tn.notify_run_finished(catalog, "ws.ops.rollup", "vanished") == 1
    assert "vanished" in _enqueued(enqueue)[0]["body"]


# --- the payload


def test_the_payload_matches_the_routes_contract(configured):
    catalog = _Catalog([{"user": "alice", "outcome": "EVERYTHING"}])

    with patch.object(tn, "_enqueue") as enqueue:
        tn.notify_run_finished(catalog, "ws.ops.rollup", "failed")

    payload = _enqueued(enqueue)[0]
    assert set(payload) == {"client_id", "id", "kind", "title", "body", "severity", "target"}
    # The idempotency key control.opteryx uses as the document id, so a Cloud
    # Tasks retry of this same body is a no-op rather than a second bell.
    assert re.match(r"^[A-Za-z0-9_-]{8,64}$", payload["id"])
    assert payload["kind"] == tn.KIND_FAILED
    assert payload["severity"] == tn.SEVERITY_ACTION
    assert payload["target"] == {
        "kind": "task",
        "workspace": "ws",
        "collection": "ops",
        "object": "rollup",
    }
    # It has to survive the trip as JSON: Cloud Tasks carries the body encoded.
    json.dumps(payload)


def test_each_notification_carries_its_own_idempotency_key(configured):
    """Fixed before the request is queued, so retries reuse it - but distinct
    per recipient and per event, so two real failures are two bells."""
    catalog = _Catalog(
        [{"user": "alice", "outcome": "ERROR"}, {"user": "bobby", "outcome": "ERROR"}]
    )

    with patch.object(tn, "_enqueue") as enqueue:
        tn.notify_run_finished(catalog, "ws.ops.rollup", "failed")
        tn.notify_run_finished(catalog, "ws.ops.rollup", "failed")

    keys = [payload["id"] for payload in _enqueued(enqueue)]
    assert len(keys) == 4
    assert len(set(keys)) == 4


def test_a_success_is_informational_not_actionable(configured):
    catalog = _Catalog([{"user": "alice", "outcome": "SUCCESS"}])

    with patch.object(tn, "_enqueue") as enqueue:
        tn.notify_run_finished(catalog, "ws.ops.rollup", "succeeded")

    payload = _enqueued(enqueue)[0]
    assert payload["severity"] == tn.SEVERITY_INFO
    assert payload["kind"] == tn.KIND_SUCCEEDED


@pytest.mark.parametrize(
    "status,statement",
    [
        ("egress-blocked", "ALTER WORKSPACE ws SET SECURE ws.ops.rollup TO"),
        ("owner-missing", "ALTER TRIGGER nightly ON ws.raw.events OWNER TO"),
        ("window-unbound", "OVER <table>"),
        ("denied", "ALTER TRIGGER nightly ON ws.raw.events OWNER TO"),
        ("failed", "SHOW CREATE TASK ws.ops.rollup"),
    ],
)
def test_every_failure_names_the_statement_that_fixes_it(status, statement):
    """Instructive, not merely informative (architect ruling 2026-09-02)."""
    _, body = tn._compose(
        status, "ws.ops.rollup", trigger="nightly", holder="ws.raw.events", detail=""
    )

    assert statement in body


def test_the_error_text_is_carried_through():
    """The difference between a fixable error and a status code."""
    _, body = tn._compose(
        "failed",
        "ws.ops.rollup",
        trigger="nightly",
        holder="ws.raw.events",
        detail="DatasetNotFound: ws.marts.daily",
    )

    assert "DatasetNotFound: ws.marts.daily" in body


def test_the_title_and_body_are_clipped_to_the_routes_limits():
    _, body = tn._compose(
        "failed", "ws.ops.rollup", trigger="n", holder="h", detail="x" * 5000
    )

    assert len(body) == tn.MAX_BODY


def test_a_title_says_the_task_is_not_running_when_it_never_started():
    """The distinction a reader acts on: a failed run will be retried by the
    next commit; a blocked fire will not."""
    blocked, _ = tn._compose("egress-blocked", "ws.ops.rollup", trigger="n", holder="h", detail="")
    failed, _ = tn._compose("failed", "ws.ops.rollup", trigger="n", holder="h", detail="")

    assert "is not running" in blocked
    assert "is not running" not in failed


# --- failure posture


def test_nothing_is_sent_when_notifications_are_not_configured(monkeypatch):
    monkeypatch.delenv("CONTROL_URL", raising=False)
    monkeypatch.delenv("CONTROL_ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("OPTERYX_NOTIFICATIONS_QUEUE", raising=False)
    catalog = _Catalog([{"user": "alice", "outcome": "EVERYTHING"}])

    with patch.object(tn, "_enqueue") as enqueue:
        assert tn.notify_run_finished(catalog, "ws.ops.rollup", "failed") == 0
    assert enqueue.call_count == 0


def test_an_unreadable_subscriber_list_is_alerted_not_raised(configured):
    """The caller has already recorded the run's outcome; a bell that could not
    be rung must not turn that into something worse."""
    catalog = _Catalog(raises=RuntimeError("firestore is down"))

    with patch.object(tn, "_alert") as alert:
        assert tn.notify_run_finished(catalog, "ws.ops.rollup", "failed") == 0

    assert alert.call_count == 1


def test_a_queue_failure_is_alerted_and_does_not_stop_the_others(configured):
    """One recipient's failure must not cost the rest theirs."""
    catalog = _Catalog(
        [
            {"user": "alice", "outcome": "ERROR"},
            {"user": "bobby", "outcome": "ERROR"},
        ]
    )

    with patch.object(tn, "_alert") as alert:
        with patch.object(tn, "_enqueue", side_effect=[RuntimeError("no queue"), None]):
            sent = tn.notify_run_finished(catalog, "ws.ops.rollup", "failed")

    assert sent == 1
    assert alert.call_count == 1


def test_a_subscriber_the_feed_cannot_address_is_alerted(configured):
    """`client_id` is the `sub` of an authenticate.opteryx token. A recorded
    subscriber that is not one means the identity space drifted, and the route
    would 400 - which Cloud Tasks would retry five times and then discard."""
    catalog = _Catalog([{"user": "not a valid client id", "outcome": "ERROR"}])

    with patch.object(tn, "_alert") as alert:
        with patch.object(tn, "_enqueue") as enqueue:
            assert tn.notify_run_finished(catalog, "ws.ops.rollup", "failed") == 0

    assert enqueue.call_count == 0
    assert alert.call_count == 1


def test_the_request_targets_the_internal_route_with_the_admin_token(configured):
    """The one place the wire format is asserted: everything else patches
    `_enqueue` out."""
    catalog = _Catalog([{"user": "alice", "outcome": "ERROR"}])
    client = MagicMock()

    with patch("google.cloud.tasks_v2.CloudTasksClient", return_value=client):
        with patch("google.cloud.tasks_v2.HttpRequest") as http_request:
            with patch("google.cloud.tasks_v2.Task"), patch(
                "google.cloud.tasks_v2.CreateTaskRequest"
            ):
                tn.notify_run_finished(catalog, "ws.ops.rollup", "failed")

    kwargs = http_request.call_args.kwargs
    assert kwargs["url"] == "https://control.opteryx.app/v1/internal/notifications"
    assert kwargs["headers"]["X-Admin-Token"] == "sekrit"
    assert json.loads(kwargs["body"])["client_id"] == "alice"
