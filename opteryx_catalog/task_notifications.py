"""Telling a task's subscribers what happened to it.

`LISTEN TO <task>` records who wants to know; this is what tells them. The feed
is control.opteryx's (`POST /v1/internal/notifications`), which is deliberately
the only writer to `notifications/{client_id}/items` - so this enqueues one
Cloud Tasks request per recipient rather than writing there itself. That route
"resolves no audiences": one request, one principal, so the fan-out is ours.

TWO EMIT POINTS, because a task fails in two disjoint places and a subscriber
told about only one of them learns nothing in the worse case:

  - **The run ran and failed** - `succeeded`/`failed`/`denied`, stamped by
    worker.opteryx in `_stamp_fired_task`. Call `notify_run_finished`.
  - **The run never started** - `egress-blocked`, `owner-missing`,
    `window-unbound`, `error`, raised in `trigger_firing._dispatch`. These never
    reach a worker, so nothing there could notify about them. Call
    `notify_fire_failed`.

The second class is the one that hurts. A task blocked by egress protection
silently never runs while its trigger keeps reporting `enqueued` - the exact
shape of the cross-workspace billing failure - so a subscriber hearing only
about failed RUNS would hear nothing at all for weeks.

INSTRUCTIVE, NOT INFORMATIVE (architect ruling 2026-09-02). Every failure
carries the statement that fixes it, and `target` carries where to go - which is
control.opteryx's own standard for the field: "a notification you cannot act on
from is a log line". Failures are severity `action`; a success is `info`.

NEVER RAISES. Both call sites have already done the thing being reported - the
run failed, or the fire did - and a bell that could not be rung must not turn
either into something worse. Delivery reliability belongs to the queue (5
attempts, 10s doubling backoff); what this owes is an honest enqueue, so a
failure to enqueue is alerted rather than swallowed.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any
from typing import Optional

from .alerts import report as _alert

logger = logging.getLogger(__name__)

# Kinds, in control.opteryx's `<noun>.<verb>` vocabulary (`query.finished`,
# `grant.added`). Two rather than one so the Studio can style a failure
# differently without parsing the title; both fall to that service's 30-day
# fallback TTL until it lists them.
KIND_FAILED = "task.failed"
KIND_SUCCEEDED = "task.succeeded"

# control.opteryx's `SEVERITIES`. `action` is what puts an actionable badge on
# the bell, which is the right claim only because every failure below carries
# the statement that resolves it.
SEVERITY_ACTION = "action"
SEVERITY_INFO = "info"

# The route's own field constraints, restated so a payload that would be
# rejected fails HERE, loudly, at the producer. Cloud Tasks retries any non-2xx,
# so a 400 would otherwise burn all five attempts and vanish silently.
CLIENT_ID_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]{3,31}$")
MAX_TITLE = 200
MAX_BODY = 2000

# What a subscription asked to hear about, against what happened. EVERYTHING
# takes both; the two named outcomes take one each.
_ERROR = "ERROR"
_SUCCESS = "SUCCESS"
_EVERYTHING = "EVERYTHING"


def _wants(subscribed_to: Optional[str], outcome: str) -> bool:
    """Whether a subscription recorded as `subscribed_to` covers `outcome`.

    An unrecognised value covers NOTHING. A subscription whose filter cannot be
    read is not an invitation to send everything - that turns one bad record
    into a stream of notifications nobody asked for.
    """
    if subscribed_to == _EVERYTHING:
        return True
    return subscribed_to == outcome


# --- what to DO about it -------------------------------------------------------
#
# One entry per status either emit point can produce. A failure's body is the
# REMEDY, in the second person, naming the statement that fixes it - not a
# restatement of the status, which the title already carries. Success is the one
# entry that instructs nothing, because there is nothing to do about it.

_BODIES = {
    "succeeded": (
        "The run completed. SELECT * FROM {workspace}.information_schema.tasks WHERE "
        "task_name = '{task_name}' shows when it last ran and how far its window has "
        "been consumed."
    ),
    # Never started.
    "egress-blocked": (
        "This task writes into another workspace, and {workspace}'s egress protection "
        "refuses it - so the run was never submitted. Sanction this one task with "
        "ALTER WORKSPACE {workspace} SET SECURE {task} TO <destination workspace>. "
        "Turning egress protection off would unlock every copy out of {workspace}, "
        "not just this one."
    ),
    "owner-missing": (
        "The trigger that fires this task has no owner recorded, and an unattended run "
        "has no identity of its own to execute as - so nothing was submitted. Record "
        "one with ALTER TRIGGER {trigger} ON {holder} OWNER TO <principal>; the run "
        "will execute as, and be billed to, that principal."
    ),
    "window-unbound": (
        "This task consumes a window (:parent_version / :current_version) but the event "
        "that fired it carries none, so there was nothing to bind and no run was "
        "submitted. Either give the trigger a source to take its window from - "
        "CREATE OR REPLACE TRIGGER {trigger} ON ... OVER <table> - or redefine the task "
        "with a statement that takes no window."
    ),
    "error": (
        "The trigger could not submit a run. SELECT * FROM "
        "{workspace}.information_schema.tasks WHERE task_name = '{task_name}' shows the "
        "task's last-fired status; the error is below."
    ),
    # Ran and did not succeed.
    "failed": (
        "The run was submitted and the statement failed. SHOW CREATE TASK {task} is what "
        "ran; the error is below. Nothing was consumed, so the next run will cover this "
        "window as well as its own."
    ),
    "denied": (
        "The run was refused on permissions. An unattended run executes as the trigger's "
        "owner, not as you, so it is that principal that lacks access - grant it what the "
        "statement needs, or move the trigger with ALTER TRIGGER {trigger} ON {holder} "
        "OWNER TO <principal>."
    ),
}

_TITLES = {
    "egress-blocked": "Task {task_name} is blocked and is not running",
    "owner-missing": "Task {task_name} has no owner and is not running",
    "window-unbound": "Task {task_name} cannot bind its window and is not running",
    "error": "Task {task_name} could not be started",
    "failed": "Task {task_name} failed",
    "denied": "Task {task_name} was refused on permissions",
    "succeeded": "Task {task_name} succeeded",
}


def _split(task_identifier: str) -> tuple[str, str, str]:
    """`(workspace, collection, task)` from a task identifier.

    Accepts the qualified spelling a trigger records and the local one a caller
    writes; a missing workspace is returned empty rather than guessed, because
    the notification names it back to the reader.
    """
    parts = str(task_identifier).split(".")
    if len(parts) >= 3:
        return parts[0], parts[1], ".".join(parts[2:])
    if len(parts) == 2:
        return "", parts[0], parts[1]
    return "", "", parts[0]


def _compose(status: str, task_identifier: str, *, trigger: str, holder: str, detail: str) -> tuple:
    """`(title, body)` for one outcome - the whole of what a reader sees."""
    workspace, collection, task_name = _split(task_identifier)
    fields = {
        "task": task_identifier,
        "task_name": task_name,
        "collection": collection,
        "workspace": workspace or "<workspace>",
        "trigger": trigger or "<trigger>",
        "holder": holder or "<table>",
    }

    title = _TITLES.get(status, "Task {task_name}: {status}").format(status=status, **fields)

    body_template = _BODIES.get(status)
    if body_template is None:
        # A status with no body written for it. Named plainly rather than
        # dressed up as advice - a made-up instruction is worse than none.
        body = f"The task's run finished with status '{status}'."
    else:
        body = body_template.format(**fields)

    if detail:
        body = f"{body}\n\n{detail}"
    return title[:MAX_TITLE], body[:MAX_BODY]


# --- delivery ------------------------------------------------------------------


def _config() -> tuple[Optional[str], Optional[str], Optional[str]]:
    """`(control_url, admin_token, queue_path)`, each None when unset."""
    url = os.getenv("CONTROL_URL")
    return (
        url.rstrip("/") if url else None,
        os.getenv("CONTROL_ADMIN_TOKEN"),
        os.getenv("OPTERYX_NOTIFICATIONS_QUEUE"),
    )


def _enqueue(url: str, token: str, queue_path: str, payload: dict[str, Any]) -> None:
    """Put one notification on the queue. Raises; the caller reports."""
    from google.cloud import tasks_v2

    client = tasks_v2.CloudTasksClient()
    client.create_task(
        request=tasks_v2.CreateTaskRequest(
            parent=queue_path,
            task=tasks_v2.Task(
                http_request=tasks_v2.HttpRequest(
                    http_method=tasks_v2.HttpMethod.POST,
                    url=f"{url}/v1/internal/notifications",
                    headers={
                        "Content-Type": "application/json",
                        "X-Admin-Token": token,
                        "User-Agent": "opteryx-catalog-task-notifications/1.0",
                    },
                    body=json.dumps(payload).encode(),
                )
            ),
        )
    )


def _notify(catalog, task_identifier: str, status: str, outcome: str, **context) -> int:
    """Tell every subscriber who asked about `outcome`. Returns how many were told.

    Never raises: see the module docstring. Returns 0 for every reason there is
    nothing to send - no subscribers, notifications not configured on this
    deployment - which are not the same thing but are the same non-event here.
    """
    url, token, queue_path = _config()
    if not (url and token and queue_path):
        # Not configured. Not an error: a deployment without the notification
        # queue - a local run, a test - has no bell to ring. Logged at debug so
        # it does not become noise on every fire.
        logger.debug("task notifications are not configured; nothing sent")
        return 0

    try:
        listeners = catalog.list_listeners(task_identifier)
    except Exception as exc:  # noqa: BLE001 - the caller's outcome is already recorded
        _alert(
            exc,
            note="task subscribers could not be read; nobody was notified",
            fingerprint=("task-listeners-unreadable", str(task_identifier)),
            context={"task": task_identifier, "status": status},
        )
        return 0

    title, body = _compose(
        status,
        task_identifier,
        trigger=context.get("trigger") or "",
        holder=context.get("holder") or "",
        detail=context.get("detail") or "",
    )
    workspace, collection, task_name = _split(task_identifier)
    severity = SEVERITY_INFO if outcome == _SUCCESS else SEVERITY_ACTION

    sent = 0
    for listener in listeners:
        if not _wants(listener.get("outcome"), outcome):
            continue
        client_id = str(listener.get("user") or "")
        if not CLIENT_ID_PATTERN.match(client_id):
            # A subscriber whose identity the feed cannot address. Alerted, not
            # skipped quietly: LISTEN recorded it, so either the identity space
            # drifted or a record is corrupt, and both are ours to know about.
            _alert(
                ValueError(f"subscriber {client_id!r} is not a valid notification recipient"),
                note="a task subscriber cannot be addressed and was not notified",
                fingerprint=("task-listener-unaddressable", str(task_identifier), client_id),
                context={"task": task_identifier},
            )
            continue

        payload = {
            "client_id": client_id,
            "kind": KIND_SUCCEEDED if outcome == _SUCCESS else KIND_FAILED,
            "title": title,
            "body": body,
            "severity": severity,
            # Where clicking it goes. The task's own page - which is where every
            # remedy above is carried out.
            "target": {
                "kind": "task",
                "workspace": workspace,
                "collection": collection,
                "task": task_name,
            },
        }
        try:
            _enqueue(url, token, queue_path, payload)
            sent += 1
        except Exception as exc:  # noqa: BLE001 - Cloud Tasks client boundary
            _alert(
                exc,
                note="a task outcome notification could not be queued",
                fingerprint=("task-notification-unqueueable", str(task_identifier)),
                context={"task": task_identifier, "status": status, "client_id": client_id},
            )
    return sent


def notify_fire_failed(catalog, task_identifier: str, status: str, **context) -> int:
    """A run that NEVER STARTED. Called from `trigger_firing._dispatch`.

    Always an error outcome: every status reaching here is a fire that raised,
    and the task did not run.
    """
    return _notify(catalog, task_identifier, status, _ERROR, **context)


def notify_run_finished(catalog, task_identifier: str, status: str, **context) -> int:
    """A run that STARTED. Called from worker.opteryx's `_stamp_fired_task`.

    `succeeded` is the only success; `failed` and `denied` are errors. A status
    this does not recognise is treated as an error, because the one thing worse
    than an unhelpful notification is silence about a run that did not work.
    """
    outcome = _SUCCESS if status == "succeeded" else _ERROR
    return _notify(catalog, task_identifier, status, outcome, **context)
