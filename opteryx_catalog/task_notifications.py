"""Telling a subscriber what happened to the thing they subscribed to.

`LISTEN TO <name>` records who wants to know; this is what tells them. The name
is whatever a TRIGGER TARGETS - a TASK it runs with EXECUTE, or a MATERIALIZED
VIEW it refreshes. The two fire paths differ only in the statement they build:
they share the suspension check, the egress enforcement, the required `runs-as`
and the dispatch contract, so they share this too. Only `window-unbound` is
task-only, because only a task binds a window. The feed
is control.opteryx's (`POST /v1/internal/notifications`), which is deliberately
the only writer to `notifications/{client_id}/items` - so this enqueues one
Cloud Tasks request per recipient rather than writing there itself. That route
"resolves no audiences": one request, one principal, so the fan-out is ours.

TWO EMIT POINTS, because a task fails in two disjoint places and a subscriber
told about only one of them learns nothing in the worse case:

  - **The run ran and failed** - `succeeded`/`failed`/`denied`. For a task that
    is worker.opteryx's `_stamp_fired_task`; for a view it is
    `mark_materialized_view_refreshed`, which BOTH outcomes funnel through (the
    worker stamps failures, the engine stamps success from inside the refresh)
    and which a manual REFRESH reaches too. Call `notify_run_finished`.
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

IDEMPOTENT. Every request carries an `id` that control.opteryx uses as the
document id, so the queue's retries cannot turn one event into several bells.

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
import uuid
import uuid
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
#
# The wording follows the object's KIND, which the subscription itself records.
# Nothing here re-derives it.

_NOUNS = {"task": "Task", "materialized_view": "Materialized view"}

_DEFINITION_STATEMENTS = {
    "task": "SHOW CREATE TASK {object}",
    "materialized_view": "SHOW CREATE MATERIALIZED VIEW {object}",
}

# Who to point at for a missing owner. A view's refresh triggers must all share
# one identity, and ALTER MATERIALIZED VIEW ... OWNER TO repoints every one of
# them in a single batch - so sending a reader to the per-trigger statement
# would have them fix one of N and wonder why it still fails.
_OWNER_STATEMENTS = {
    "task": "ALTER TRIGGER {trigger} ON {holder} OWNER TO <principal>",
    "materialized_view": "ALTER MATERIALIZED VIEW {object} OWNER TO <principal>",
}

# Where the object's own state is readable. Two tables, because a task is not a
# relation and a view is.
_STATE_STATEMENTS = {
    "task": (
        "SELECT * FROM {workspace}.information_schema.tasks WHERE task_name = '{name}'"
    ),
    "materialized_view": (
        "SELECT * FROM {workspace}.information_schema.views WHERE table_name = '{name}'"
    ),
}

_BODIES = {
    "succeeded": (
        "The run completed. {state_statement} shows when it last ran."
    ),
    # Never started.
    "egress-blocked": (
        "This {noun_lower} writes into another workspace, and {workspace}'s egress "
        "protection refuses it - so the run was never submitted. Sanction this one "
        "object with ALTER WORKSPACE {workspace} SET SECURE {object} TO "
        "<destination workspace>. Turning egress protection off would unlock every "
        "copy out of {workspace}, not just this one."
    ),
    "owner-missing": (
        "The trigger that fires this {noun_lower} has no owner recorded, and an "
        "unattended run has no identity of its own to execute as - so nothing was "
        "submitted. Record one with {owner_statement}; the run will execute as, and "
        "be billed to, that principal."
    ),
    "window-unbound": (
        "This task consumes a window (:parent_version / :current_version) but the "
        "event that fired it carries none, so there was nothing to bind and no run "
        "was submitted. Either give the trigger a source to take its window from - "
        "CREATE OR REPLACE TRIGGER {trigger} ON ... OVER <table> - or redefine the "
        "task with a statement that takes no window."
    ),
    "error": (
        "The trigger could not submit a run. {state_statement} shows the last recorded "
        "status; the error is below."
    ),
    # Ran and did not succeed.
    "failed": (
        "The run was submitted and the statement failed. {definition_statement} is "
        "what ran; the error is below."
    ),
    "denied": (
        "The run was refused on permissions. An unattended run executes as the "
        "trigger's owner, not as you, so it is that principal that lacks access - "
        "grant it what the statement needs, or move it with {owner_statement}."
    ),
}

_TITLES = {
    "egress-blocked": "{noun} {name} is blocked and is not running",
    "owner-missing": "{noun} {name} has no owner and is not running",
    "window-unbound": "{noun} {name} cannot bind its window and is not running",
    "error": "{noun} {name} could not be started",
    "failed": "{noun} {name} failed",
    "denied": "{noun} {name} was refused on permissions",
    "succeeded": "{noun} {name} succeeded",
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


def _compose(
    status: str,
    identifier: str,
    *,
    kind: str = "task",
    trigger: str = "",
    holder: str = "",
    detail: str = "",
) -> tuple:
    """`(title, body)` for one outcome - the whole of what a reader sees."""
    workspace, collection, name = _split(identifier)
    noun = _NOUNS.get(kind, "Object")
    fields = {
        "object": identifier,
        "name": name,
        "collection": collection,
        "workspace": workspace or "<workspace>",
        "trigger": trigger or "<trigger>",
        "holder": holder or "<table>",
        "noun": noun,
        "noun_lower": noun.lower(),
    }
    # Resolved before the body, because the bodies interpolate them.
    fields["definition_statement"] = _DEFINITION_STATEMENTS.get(kind, "").format(**fields)
    fields["owner_statement"] = _OWNER_STATEMENTS.get(kind, "").format(**fields)
    fields["state_statement"] = _STATE_STATEMENTS.get(kind, "").format(**fields)

    title = _TITLES.get(status, "{noun} {name}: {status}").format(status=status, **fields)

    body_template = _BODIES.get(status)
    if body_template is None:
        # A status with no body written for it. Named plainly rather than
        # dressed up as advice - a made-up instruction is worse than none.
        body = f"The run finished with status '{status}'."
    else:
        body = body_template.format(**fields)

    if detail:
        body = f"{body}\n\n{detail}"
    return title[:MAX_TITLE], body[:MAX_BODY]


# --- delivery ------------------------------------------------------------------


def _setting(key: str) -> Optional[str]:
    """One configuration value, from wherever this deployment serves it.

    `opteryx_shared_services` resolves ENVVAR first and then the fleet's config
    DOCUMENT, and the fleet is migrating settings out of per-service ENVVARs
    into that document - so reading `os.environ` alone would go quiet on any
    service that has already moved, with `_notify` reporting "not configured"
    for a setting that is configured.

    Detected rather than depended on, exactly as `alerts/_secrets` detects
    `google-cloud-secret-manager` and `_enqueue` detects Cloud Tasks: this
    library is also used where that package is not installed, and there ENVVARs
    are the whole of the configuration.
    """
    try:
        from opteryx_shared_services.config import get_config
    except ImportError:
        return os.getenv(key)
    return get_config(key) or os.getenv(key)


def _config() -> tuple[Optional[str], Optional[str], Optional[str]]:
    """`(control_url, admin_token, queue_path)`, each None when unset."""
    url = _setting("CONTROL_URL")
    return (
        url.rstrip("/") if url else None,
        _setting("CONTROL_ADMIN_TOKEN"),
        _setting("OPTERYX_NOTIFICATIONS_QUEUE"),
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


def _notify(catalog, identifier: str, status: str, outcome: str, **context) -> int:
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
        #
        # Named individually rather than as one blanket message: a deployment
        # that has two of the three set is not "unconfigured", it is one
        # setting away, and a caller who raises this logger to DEBUG to find
        # out why the bell is silent should not also have to go read this
        # function's source to learn which key `_config()` actually missed.
        missing = [
            name
            for name, value in (("CONTROL_URL", url), ("CONTROL_ADMIN_TOKEN", token),
                                 ("OPTERYX_NOTIFICATIONS_QUEUE", queue_path))
            if not value
        ]
        logger.debug("task notifications are not configured; missing %s; nothing sent", missing)
        return 0

    try:
        listeners = catalog.list_listeners(identifier)
    except Exception as exc:  # noqa: BLE001 - the caller's outcome is already recorded
        _alert(
            exc,
            note="subscribers could not be read; nobody was notified",
            fingerprint=("listeners-unreadable", str(identifier)),
            context={"object": identifier, "status": status},
        )
        return 0

    workspace, collection, name = _split(identifier)
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
                note="a subscriber cannot be addressed and was not notified",
                fingerprint=("listener-unaddressable", str(identifier), client_id),
                context={"object": identifier},
            )
            continue

        # Composed per subscriber because the KIND comes off the subscription
        # record. Every subscription to one object carries the same kind, so
        # this is the same text each time - read from the record rather than
        # from a second lookup, which is the whole reason the kind is stored.
        kind = listener.get("kind") or "task"
        title, body = _compose(
            status,
            identifier,
            kind=kind,
            trigger=context.get("trigger") or "",
            holder=context.get("holder") or "",
            detail=context.get("detail") or "",
        )

        payload = {
            "client_id": client_id,
            # THE IDEMPOTENCY KEY, fixed here - before the request is queued -
            # which is the whole of what makes it work. Cloud Tasks retries any
            # non-2xx and replays this exact body, so a request that landed,
            # wrote the document and then lost its response addresses the same
            # document on the retry instead of adding a second identical bell.
            # control.opteryx uses it as the document id and treats a repeat as
            # a no-op rather than an overwrite, so a notification already read
            # or dismissed is not resurrected.
            #
            # Fresh per recipient and per event, deliberately NOT derived from
            # the outcome: two real failures of the same object are two things
            # that happened, and collapsing them would silence the second.
            "id": uuid.uuid4().hex,
            "kind": KIND_SUCCEEDED if outcome == _SUCCESS else KIND_FAILED,
            "title": title,
            "body": body,
            "severity": severity,
            # Where clicking it goes. The object's own page - which is where
            # every remedy above is carried out.
            "target": {
                "kind": kind,
                "workspace": workspace,
                "collection": collection,
                "object": name,
            },
        }
        try:
            _enqueue(url, token, queue_path, payload)
            sent += 1
        except Exception as exc:  # noqa: BLE001 - Cloud Tasks client boundary
            _alert(
                exc,
                note="an outcome notification could not be queued",
                fingerprint=("notification-unqueueable", str(identifier)),
                context={"object": identifier, "status": status, "client_id": client_id},
            )
    return sent


def notify_fire_failed(catalog, identifier: str, status: str, **context) -> int:
    """A run that NEVER STARTED. Called from `trigger_firing._dispatch`.

    Always an error outcome: every status reaching here is a fire that raised,
    so nothing ran - whether the trigger was going to EXECUTE a task or REFRESH
    a view.
    """
    return _notify(catalog, identifier, status, _ERROR, **context)


def notify_run_finished(catalog, identifier: str, status: str, **context) -> int:
    """A run that STARTED - a task's, from worker.opteryx's `_stamp_fired_task`,
    or a view's, from `mark_materialized_view_refreshed`.

    `succeeded` is the only success; `failed` and `denied` are errors. A status
    this does not recognise is treated as an error, because the one thing worse
    than an unhelpful notification is silence about a run that did not work.
    """
    outcome = _SUCCESS if status == "succeeded" else _ERROR
    return _notify(catalog, identifier, status, outcome, **context)
