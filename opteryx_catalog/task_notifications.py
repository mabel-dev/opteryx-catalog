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

MARKDOWN. A body is written in the limited subset the Studio renders
(/docs/core-concepts/markdown-in-descriptions): every SQL statement is an
inline code span and a carried error is a fenced block, so the statement a
reader is meant to copy is visually separate from the prose telling them to.
The subset is deliberate - it is the same one comments and descriptions use,
and anything outside it renders as literal text rather than as markup, so a
body is still readable where nothing renders it (an alert, a log line).

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
from urllib.parse import urlencode
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
CLIENT_ID_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]{3,63}\Z")
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
        "object with `ALTER WORKSPACE {workspace} SET SECURE {object} TO "
        "<destination workspace>`. Turning egress protection off would unlock every "
        "copy out of {workspace}, not just this one."
    ),
    "owner-missing": (
        "The trigger that fires this {noun_lower} has no owner recorded, and an "
        "unattended run has no identity of its own to execute as - so nothing was "
        "submitted. Record one with {owner_statement}; the run will execute as, and "
        "be billed to, that principal."
    ),
    "window-unbound": (
        "This task consumes a window (`:parent_version` / `:current_version`) but the "
        "event that fired it carries none, so there was nothing to bind and no run "
        "was submitted. Either give the trigger a source to take its window from - "
        "`CREATE OR REPLACE TRIGGER {trigger} ON ... OVER <table>` - or redefine the "
        "task with a statement that takes no window."
    ),
    "error": (
        "The trigger could not submit a run. {state_statement} shows the last recorded "
        "status."
    ),
    # Ran and did not succeed.
    "failed": (
        "The run was submitted and the statement failed. {definition_statement} is "
        "what ran."
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


def _code(statement: str) -> str:
    """One SQL statement as an inline code span, or "" if there is none.

    Empty in, empty out: a kind with no statement recorded for it interpolates
    nothing rather than an empty pair of backticks, which would render as two
    literal characters in the middle of a sentence.
    """
    return f"`{statement}`" if statement else ""


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


# The Studio page every task and view is managed from - the same link its
# workflow map draws (web.opteryx `static/js/workflows.js`, `datasetUrl`). The
# bell follows `target.url` and nothing else, so a target without one is a bell
# that goes nowhere when clicked.
STUDIO_OBJECT_PAGE = "/studio/manage-dataset.html"


def _studio_url(workspace: str, collection: str, name: str) -> Optional[str]:
    """The object's Studio page, or None if the identifier is not qualified.

    That page reads `dataset` as `workspace.collection.name`; a local name has no
    workspace to put first, and a link built from it would open the wrong object.
    """
    if not (workspace and collection and name):
        return None
    return f"{STUDIO_OBJECT_PAGE}?{urlencode({'dataset': f'{workspace}.{collection}.{name}'})}"


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
    # Resolved before the body, because the bodies interpolate them, and each
    # is code-spanned HERE rather than in the templates: every one of them is
    # interpolated mid-sentence, and backticks written around `{field}` in a
    # template would produce a bare pair of them for a kind that has no
    # statement to name.
    fields["definition_statement"] = _code(_DEFINITION_STATEMENTS.get(kind, "").format(**fields))
    fields["owner_statement"] = _code(_OWNER_STATEMENTS.get(kind, "").format(**fields))
    fields["state_statement"] = _code(_STATE_STATEMENTS.get(kind, "").format(**fields))

    title = _TITLES.get(status, "{noun} {name}: {status}").format(status=status, **fields)

    body_template = _BODIES.get(status)
    if body_template is None:
        # A status with no body written for it. Named plainly rather than
        # dressed up as advice - a made-up instruction is worse than none.
        body = f"The run finished with status '{status}'."
    else:
        body = body_template.format(**fields)

    if detail:
        # The promise of an error is made HERE, alongside the error itself, and
        # not in the body templates: a caller that emits a failure without one
        # would otherwise render a sentence pointing at nothing.
        # Fenced, not appended as prose: an engine error carries its own line
        # breaks, quoting and identifiers, and running it into the paragraph
        # above loses the boundary between what we are saying and what the
        # engine said. A fence also renders the text verbatim, so nothing in
        # an error message is read as markup.
        body = f"{body} The error is below.\n\n```\n{detail}\n```"
    # Clipped to the route's limit, which can cut mid-fence or mid-span. The
    # renderer closes an unterminated fence at the end of the text, so the
    # worst case is a trailing backtick shown literally - which is the right
    # trade against refusing to send a bell because its error was long.
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
    """`(control_url, admin_token, queue_path)`, each None when unset.

    `admin_token` is the interim shared secret (`notifications-producer-token`,
    under its historical name). It is OPTIONAL now: see `_producer_identity`.
    """
    url = _setting("CONTROL_URL")
    return (
        url.rstrip("/") if url else None,
        _setting("CONTROL_ADMIN_TOKEN"),
        _setting("OPTERYX_NOTIFICATIONS_QUEUE"),
    )


# --- who is sending ------------------------------------------------------------
#
# control.opteryx accepts a producer by its Google identity (SEC-06 CTRL-7,
# opteryx-infrastructure docs/sec-02/admin-identity-proposal.md §4): an OIDC
# token for this deployment's own runtime service account, whose numeric `sub`
# control holds in an allow-list. Cloud Tasks mints that token itself, fresh
# for every attempt, from `oidc_token` on the task - so nothing secret is
# stored in the queue, and the creator of the task must be allowed to act as
# the service account it names (`iam.serviceAccounts.actAs`). Naming our OWN
# account needs that permission on ourselves only.

# What control verifies the token against: the route's identity, not the
# transport's URL, so it is the same whichever address CONTROL_URL holds.
# Must equal control's `NOTIFICATIONS_OIDC_AUDIENCE`.
DEFAULT_OIDC_AUDIENCE = "https://control.opteryx.app/v1/internal/notifications"

# The metadata server answers this on Cloud Run and on Compute Engine alike, with
# the service account the code is running as.
_METADATA_SA_EMAIL_URL = (
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email"
)
_runtime_sa: Optional[str] = None


def _runtime_service_account() -> Optional[str]:
    """This process's own service account email, from the metadata server.

    Only a SUCCESS is cached. A lookup that fails (a laptop, a test, a metadata
    blip) is tried again on the next bell rather than pinning this process to
    sending none.
    """
    global _runtime_sa
    if _runtime_sa:
        return _runtime_sa
    import urllib.request

    try:
        request = urllib.request.Request(
            _METADATA_SA_EMAIL_URL, headers={"Metadata-Flavor": "Google"}
        )
        with urllib.request.urlopen(request, timeout=2) as response:  # noqa: S310 - fixed URL
            email = response.read().decode().strip()
    except Exception as exc:  # noqa: BLE001 - no metadata server is an answer, not a fault
        logger.debug("no runtime service account from the metadata server: %s", exc)
        return None
    if "@" not in email:
        return None
    _runtime_sa = email
    return email


def _producer_identity() -> tuple[Optional[str], str]:
    """`(service_account_email, audience)` for the task's OIDC token.

    The email is `NOTIFICATIONS_PRODUCER_SA` when a deployment sets it, and
    otherwise the runtime service account the metadata server reports - which
    is what it should be on every deployment, so the setting exists for the
    exception (a deployment that must sign as an account other than the one it
    runs as) rather than as something every service has to carry. None when
    neither is available: then no token is attached.
    """
    audience = _setting("NOTIFICATIONS_OIDC_AUDIENCE") or DEFAULT_OIDC_AUDIENCE
    return _setting("NOTIFICATIONS_PRODUCER_SA") or _runtime_service_account(), audience


def _enqueue(
    url: str,
    token: Optional[str],
    queue_path: str,
    payload: dict[str, Any],
    *,
    service_account: Optional[str] = None,
    audience: str = DEFAULT_OIDC_AUDIENCE,
) -> None:
    """Put one notification on the queue. Raises; the caller reports.

    Authenticates by OIDC when `service_account` is given. `X-Admin-Token` is
    added only while `token` is configured - control's dual-accept phase. A
    deployment leaves it by having `CONTROL_ADMIN_TOKEN` removed, with no
    release; the header and the setting are then deleted from this code.
    """
    from google.cloud import tasks_v2

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "opteryx-catalog-task-notifications/1.0",
    }
    if token:
        headers["X-Admin-Token"] = token
    http_request = tasks_v2.HttpRequest(
        http_method=tasks_v2.HttpMethod.POST,
        url=f"{url}/v1/internal/notifications",
        headers=headers,
        body=json.dumps(payload).encode(),
    )
    if service_account:
        http_request.oidc_token = tasks_v2.OidcToken(
            service_account_email=service_account, audience=audience
        )

    client = tasks_v2.CloudTasksClient()
    client.create_task(
        request=tasks_v2.CreateTaskRequest(
            parent=queue_path,
            task=tasks_v2.Task(http_request=http_request),
        )
    )


def _notify(catalog, identifier: str, status: str, outcome: str, **context) -> int:
    """Tell every subscriber who asked about `outcome`. Returns how many were told.

    Never raises: see the module docstring. Returns 0 for every reason there is
    nothing to send - no subscribers, notifications not configured on this
    deployment - which are not the same thing but are the same non-event here.
    """
    url, token, queue_path = _config()
    service_account, audience = (None, DEFAULT_OIDC_AUDIENCE)
    if url and queue_path:
        service_account, audience = _producer_identity()
    if not (url and queue_path and (token or service_account)):
        # Not configured. Not an error: a deployment without the notification
        # queue - a local run, a test - has no bell to ring. Logged at debug so
        # it does not become noise on every fire.
        #
        # Named individually rather than as one blanket message: a deployment
        # that has two of the three set is not "unconfigured", it is one
        # setting away, and a caller who raises this logger to DEBUG to find
        # out why the bell is silent should not also have to go read this
        # function's source to learn which key `_config()` actually missed.
        #
        # A credential is one of two: the runtime service account (OIDC) or,
        # while control still accepts it, CONTROL_ADMIN_TOKEN.
        missing = [
            name
            for name, value in (("CONTROL_URL", url), ("OPTERYX_NOTIFICATIONS_QUEUE", queue_path))
            if not value
        ]
        if url and queue_path:
            # The queue is configured, so this deployment MEANS to ring bells
            # and has no way to prove who it is: no metadata server answered
            # and no shared token is set. That is a fault, not a local run, so
            # it is a warning rather than debug.
            logger.warning(
                "task notifications have no credential: no runtime service account "
                "from the metadata server and no CONTROL_ADMIN_TOKEN; nothing sent"
            )
            return 0
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
            # every remedy above is carried out. `url` is what the bell
            # follows; control.opteryx accepts only a Studio path there. The
            # other keys name the object for anything that reads the feed.
            "target": {
                "kind": kind,
                "workspace": workspace,
                "collection": collection,
                "object": name,
            },
        }
        studio_url = _studio_url(workspace, collection, name)
        if studio_url:
            payload["target"]["url"] = studio_url
        try:
            _enqueue(
                url,
                token,
                queue_path,
                payload,
                service_account=service_account,
                audience=audience,
            )
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
