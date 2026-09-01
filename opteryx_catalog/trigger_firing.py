"""Fire dataset triggers after a user-created commit.

The commit path calls `fire_triggers()`, which reads the committed dataset's
`triggers` subcollection and, for each materialized-view refresh trigger,
submits `REFRESH MATERIALIZED VIEW <name>` to jobs.opteryx - because the
refresh IS a query, and jobs is the one control point through which work
reaches the workers. This library used to write the `jobs/{execution_id}`
document and enqueue the Cloud Task itself; both moved into jobs, which
recognises the statement by its query class and resolves the acting identity,
policies, billing and dedup window there. What travels from here is the
statement and provenance, nothing a submitter could be wrong about.

Invoker semantics (settled 2026-08-06): the job runs as the commit's `author`.
Their current policies are read from the workspace's `$policies/access`
collection at fire time - the same Firestore database the catalog already
holds - so a revoked role stops refreshes at the next fire, enforced by the
engine's binder in the worker. A non-owner invoker's refresh is *denied
visibly* (the job fails; `last-refresh-status` records it), by design.

Egress lock: before the job document is written, the view's sources are put
through `enforce_materialized_view_egress`. Re-checking at fire time is
the point of checking here at all - registration already checked, but a source
workspace can take the lock afterwards, and the refresh is what turns one grant
into a standing copy. A blocked refresh is a fire failure like any other: alert,
audit, `last-fired-status: egress-blocked`, commit untouched.

The same gate covers a task run, inverted: the source is the dataset that fired
it and the destinations are what its statement writes (`enforce_task_egress`).
A cross-workspace task write is refused again by the engine when the run binds,
but only this end can report it as a fire failure rather than as an error in a
job nobody is reading.

The job document carries `origin: "trigger"`, which is what keeps these off
`/jobs/recent` (filtered in jobs.opteryx) and tells the worker to stamp the
MV's refresh state on completion.

Failure here must be loud but must never break the commit that triggered it:
every failure emits an audit record and an alert, and `fire_triggers` never
raises. A missed fire is a silently stale MV - the one outcome this module
exists to avoid - so "swallow and return False" (the old webhook sender's
pattern) is exactly what this does NOT do.
"""

from __future__ import annotations

import json
import logging
import os
import re
import secrets
import string
import threading
import time
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any

import urllib.parse
import urllib.request

import requests

from .alerts import report as _alert
from .audit import write_audit_record
from .exceptions import EgressRestricted
from .exceptions import MaterializedViewError
from .exceptions import MaterializedViewOwnerMissing
from .exceptions import TaskError
from .exceptions import TaskOwnerMissing

# Trigger kind for a task run. Defined in `opteryx_catalog` beside the refresh
# kind and re-exported here, so the creation gate that refuses a platform
# identity as a task trigger's owner and the firing path that reads that owner
# are keyed off one string. The MV refresh kind is still spelled inline where it
# is matched; it becomes a task in the next phase, at which point one kind is
# left and this constant is the only one.
from .opteryx_catalog import TASK_TRIGGER_KIND  # noqa: F401

# The `parent_version` bound for a dataset's FIRST commit, which has no parent.
# The window is then everything up to and including that commit, so this only
# has to sit below every real snapshot id - those are millisecond timestamps,
# so 1 is comfortably beneath all of them.
#
# NOT 0, which the engine reserves: `VERSION AS OF 0` is the rewriter's sentinel
# for `VERSION AS OF PREVIOUS`, resolved against the chain as it stands WHEN THE
# QUERY RUNS. Binding 0 into a task that time-travels on `:parent_version` would
# quietly restore the very race this design exists to remove - and quietly is
# the operative word, because the rewriter's refusal of a literal 0 happens on
# the SQL text, before parsing, while binding happens after it. 1 is not a
# sentinel: in a predicate it admits everything, and in a time-travel clause it
# names a snapshot that does not exist and fails loudly, which is the truth -
# a first commit has no predecessor to travel to.
NO_PARENT_VERSION_FLOOR = 1

# Bounds one HTTP call - minting a token, or submitting one refresh - not the
# refresh itself, which jobs runs asynchronously long after this returns.
HTTP_TIMEOUT_SECONDS = 30

_KILL_SWITCH_ENV = "OPTERYX_TRIGGER_FIRING"

logger = logging.getLogger(__name__)

# The service's own identity, from the metadata server. Same endpoint and
# timeout as `alerts/_secrets.py` uses for the project id.
_METADATA_SA_URL = (
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email"
)
_METADATA_TIMEOUT_SECONDS = 1.0

_sa_lock = threading.Lock()
_sa_cache: str | None = None


def firing_enabled() -> bool:
    """Whether commit-time trigger firing is on. Defaults to ON.

    Set OPTERYX_TRIGGER_FIRING=0 to silence it (local scripts, test runs in
    environments with no Cloud Tasks / jobs-collection access).
    """
    value = os.environ.get(_KILL_SWITCH_ENV, "1").strip().lower()
    return value not in ("0", "false", "no", "off")


HOUSE_BILLING_ACCOUNT = "opteryx"


def policies_for(catalog, principal: str | None) -> list[dict] | None:
    """The principal's current access policies, in job-document shape.

    Read from `{workspace}/$policies/access` - policy.opteryx's storage,
    which lives in the same Firestore database as the catalog - and shaped
    exactly as `normalize_policies_for_storage` writes them onto job docs:
    `[{"role", "pattern", "policy"}]`.

    Public because jobs.opteryx is the caller now: the control point resolves
    the refresh's acting identity and policies itself rather than trusting a
    submission to carry them, and this is the data-access half of that. Read at
    submission time deliberately: a revoked role stops the very next refresh.
    """
    if not principal:
        return None
    from google.cloud.firestore_v1 import FieldFilter

    access = (
        catalog.firestore_client.collection(catalog.workspace)
        .document("$policies")
        .collection("access")
    )
    policies = []
    query = access.where(filter=FieldFilter("principal", "in", [principal, "*"]))
    for doc in query.stream():
        data = doc.to_dict() or {}
        role, pattern = data.get("role"), data.get("pattern")
        if role and pattern:
            policies.append({"role": role, "pattern": pattern, "policy": doc.id})
    return policies or None


# --- Submitting through jobs.opteryx -------------------------------------------
#
# This module used to write the `jobs/{execution_id}` document itself and enqueue
# its own Cloud Task straight at worker.opteryx. That made it a second
# implementation of jobs' contract, and - because this is a LIBRARY, embedded in
# every service that commits a dataset - it meant work reached the workers from
# several places with nothing able to see, meter or refuse it in one spot. jobs
# is the control point; a refresh is a query; so a refresh goes through jobs.
#
# The submission authenticates as `federator`, the platform's service-to-service
# identity. That is ALL it does: jobs resolves a refresh's acting identity,
# payer, policies and dedup window from the statement and the view's own
# definition, so this credential carries no authority over any of them - it
# proves "a platform service, not the open internet", nothing more. That is
# what makes it safe to ship FEDERATOR_CLIENT_SECRET into every committing
# service: holding it unlocks nothing that any authenticated caller lacks.
#
# The secret's home mirrors xb500's `app/federator.py`, which owns rotation:
# ENVVAR first (local runs), then Secret Manager - same entry, same name for
# both, one name to grep for when the credential is the thing that broke.
# Deployed services leave the ENVVAR unset on purpose, exactly as federator.py
# documents: an env-injected secret is resolved at instance start and never
# sees a rotation, while the Secret Manager read happens at every MINT - so a
# rotated secret lands on the next token, not the next restart. The mint is
# already throttled by the token cache below, so this is not a per-refresh
# Secret Manager call.
FEDERATOR_CLIENT_ID_ENV = "FEDERATOR_CLIENT_ID"
FEDERATOR_CLIENT_SECRET_ENV = "FEDERATOR_CLIENT_SECRET"

# Re-minting per refresh would be one auth round trip per fired trigger, and a
# commit can fire several. Margin so a token is never spent in its last seconds.
_TOKEN_EXPIRY_MARGIN_SECONDS = 60
_token_cache: dict = {"access_token": None, "expires_at": 0.0}


def _auth_url() -> str:
    return os.environ.get("AUTH_URL", "https://authenticate.opteryx.app")


def _federator_token() -> str:
    """A `federator` bearer token, minted via client_credentials and cached.

    Raises rather than returning None: a refresh that cannot authenticate has
    not happened, and `fire_triggers` turns that into an alert and a recorded
    fire failure. Returning a falsy token would instead produce a 401 at jobs
    and a much less obvious trail.
    """
    now = time.time()
    cached = _token_cache.get("access_token")
    if cached and _token_cache.get("expires_at", 0) > now:
        return cached

    secret = os.environ.get(FEDERATOR_CLIENT_SECRET_ENV)
    if not secret:
        from .alerts._secrets import access_secret

        secret = access_secret(FEDERATOR_CLIENT_SECRET_ENV)
    if not secret:
        raise MaterializedViewError(
            f"cannot submit a materialized view refresh: {FEDERATOR_CLIENT_SECRET_ENV} "
            "is neither in the environment nor readable from Secret Manager. Without "
            "it this service cannot authenticate to the jobs API - check the runtime "
            "service account's secretAccessor grant on that secret."
        )
    client_id = os.environ.get(FEDERATOR_CLIENT_ID_ENV, "federator")

    payload = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": secret,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        f"{_auth_url()}/token",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    # The secret is in the request body, so nothing from the request is echoed
    # into an error - only what came back.
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        body = json.loads(response.read().decode("utf-8"))

    access_token = body.get("access_token")
    if not access_token:
        raise MaterializedViewError(f"auth service returned no access_token for {client_id}")
    _token_cache["access_token"] = access_token
    _token_cache["expires_at"] = (
        now + max(0, int(body.get("expires_in") or 0)) - _TOKEN_EXPIRY_MARGIN_SECONDS
    )
    return access_token


def _jobs_url() -> str:
    return os.environ.get("JOBS_URL", "https://jobs.opteryx.app")


def _post_job(payload: dict) -> dict:
    """POST one job submission to jobs as federator, and return its body.

    The transport only. What is submitted, and what a given response means, stay
    with each caller: a refresh reads `SKIPPED` as the read gate declining it,
    which is a decision particular to views and not something a task shares.
    """
    request = urllib.request.Request(
        f"{_jobs_url()}/api/v1/jobs",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {_federator_token()}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


def _submit_task_job(
    catalog,
    *,
    sql_text: str,
    source_dataset: str,
    trigger_name: str,
    target_task: str,
    snapshot_id: Any | None,
    fired_by: str | None,
) -> tuple[str, str]:
    """Submit one task run to jobs. Returns `(execution_id, outcome)`.

    Like the refresh path, the payload is the statement plus provenance and
    nothing more. `EXECUTE <task> USING ...` is self-identifying - it names a
    catalog object - so jobs resolves the task's pinned `runs-as`, that
    principal's policies and billing account, and routes the work, none of which
    this library asserts. A submission that could name the actor or the payer
    is one that could name them wrongly.
    """
    payload = {
        "sql_text": sql_text,
        "client_info": {
            "trigger": {
                "source_dataset": source_dataset,
                "trigger_name": trigger_name,
                "target_task": target_task,
                "snapshot_id": snapshot_id,
                "fired_by": fired_by,
            }
        },
    }
    body = _post_job(payload)

    execution_id = body.get("execution_id")
    if not execution_id:
        raise TaskError(f"jobs accepted the run of {target_task} but returned no execution_id")
    return execution_id, "enqueued"


def _submit_refresh_job(
    catalog,
    *,
    sql_text: str,
    source_dataset: str,
    trigger_name: str,
    target_view: str,
    snapshot_id: Any | None,
    fired_by: str | None,
) -> tuple[str, str]:
    """Submit the refresh to jobs. Returns `(execution_id, outcome)`.

    The payload is the REFRESH statement plus provenance, nothing more. The
    statement is self-identifying - `REFRESH MATERIALIZED VIEW <name>` parses
    as its own query class - so jobs recognises it, resolves the view's
    pinned `runs-as`, that principal's policies and billing account, routes it
    to the background worker, and derives the dedup window. None of those are
    this library's to assert: a submission that could name the actor or the
    payer is a submission that could name them WRONGLY, and the control point
    exists so that nothing below it has to be trusted about either.

    `client_info.trigger` is provenance for the audit trail - which commit
    fired this - not instructions. jobs derives the authoritative workspace
    and target view from the statement itself.
    """
    payload = {
        "sql_text": sql_text,
        "client_info": {
            "trigger": {
                "source_dataset": source_dataset,
                "trigger_name": trigger_name,
                "target_view": target_view,
                "snapshot_id": snapshot_id,
                "fired_by": fired_by,
            }
        },
    }
    body = _post_job(payload)

    # jobs may decline the refresh outright: its read gate skips a view nobody
    # has read since the last refresh. That is a decision, not a failure - the
    # trigger records it (SHOW TRIGGERS shows "skipped-unread"), no job exists,
    # and the view's next reader triggers the catch-up.
    if body.get("status") == "SKIPPED":
        return None, "skipped-unread"

    execution_id = body.get("execution_id")
    if not execution_id:
        raise MaterializedViewError(
            f"jobs accepted the refresh of {target_view} but returned no execution_id"
        )
    # "enqueued", not "submitted": this string is recorded on the trigger and
    # shown by SHOW TRIGGERS. The work still ends up on a queue; only which
    # service puts it there changed, and the operator vocabulary should not
    # shift underneath them for that.
    return execution_id, "enqueued"


def _fire_refresh(
    catalog,
    dataset_identifier: str,
    trigger: dict,
    author: str | None,
    snapshot_id: Any | None,
) -> None:
    target_view = trigger["target-view"]
    mv = catalog.get_materialized_view(target_view)
    # The definition is not sent to the worker (REFRESH re-reads it), but a view
    # with none can never refresh, and finding that out here means it lands in
    # the audit log next to the commit that tried, rather than as a job failure
    # nobody is watching.
    if not mv.get("sql"):
        raise MaterializedViewError(f"materialized view has no defining SQL: {target_view}")

    # Suspended by an operator. Not an error and not alerted - a suspended refresh
    # is the setting working, the same reasoning as a blocked egress copy. The
    # trigger still records that it fired and why nothing came of it, so the
    # suppression is visible where someone looks for the view's staleness rather
    # than only in whatever they remember pausing.
    if mv.get("suspended-at-ms"):
        catalog.mark_trigger_fired(dataset_identifier, trigger["name"], status="suspended")
        return

    # Re-checked here and not only at creation: a source workspace can take the
    # egress lock long after the view was registered, and every refresh writes
    # a fresh copy. Before the job document, so a blocked refresh leaves no
    # job for the worker to pick up.
    #
    # Surfaced like any other fire failure - `fire_triggers` alerts and audits
    # it, and never lets it reach the commit - plus a status on the trigger
    # document, so the block is visible where an operator looks for the view's
    # staleness rather than only in the alert stream.
    try:
        catalog.enforce_materialized_view_egress(target_view, mv.get("source-tables") or [])
    except EgressRestricted:
        raise

    # REFRESH MATERIALIZED VIEW, not the CoRTAS it desugars to. The statement
    # says what is happening, which is what the job list, the audit record and
    # anyone reading the query history all see - and it is now the only way in:
    # a plain CREATE OR REPLACE TABLE aimed at a view is refused by the engine,
    # because a view is not a table.
    #
    # It also means the definition is no longer copied into the job. The engine
    # reads it from the catalog when the refresh runs, so a view redefined
    # between firing and execution refreshes as its current self rather than as
    # a snapshot of what it was when someone committed to a source.
    sql_text = f"REFRESH MATERIALIZED VIEW {mv['identifier']}"

    # The refresh runs as the view's pinned owner, NOT as whoever's commit fired
    # it. A committer is incidental - an ingest account with writer on a source
    # collection and nothing on the view's would make the view permanently
    # unrefreshable, and which principal happened to write last would decide
    # whether a refresh worked. The owner is the identity that chose to create
    # the standing cost, and the only one it makes sense to charge and check.
    #
    # An owner whose grants have been revoked yields no policies, the binder
    # denies, and `last-refresh-status` records it. That is the intended
    # failure.
    #
    # A MISSING owner is a different thing entirely - a damaged record - and is
    # refused rather than defaulted. Defaulting to the committer is the one
    # answer guaranteed to be wrong: it silently reinstates invoker semantics,
    # so a field lost by some unrelated write reappears hours later as a
    # baffling permission denial (or, if the committer happens to be
    # privileged, as a refresh running with authority the view never had).
    runs_as = mv.get("runs-as")
    if not runs_as:
        raise MaterializedViewOwnerMissing(
            f"materialized view {target_view} has no runs-as identity; refusing to "
            "refresh it as the committing user. Set one with ALTER MATERIALIZED "
            f"VIEW {target_view} OWNER TO <principal>."
        )

    execution_id, outcome = _submit_refresh_job(
        catalog,
        sql_text=sql_text,
        source_dataset=dataset_identifier,
        trigger_name=trigger["name"],
        target_view=target_view,
        snapshot_id=snapshot_id,
        fired_by=author,
    )
    catalog.mark_trigger_fired(dataset_identifier, trigger["name"], status=outcome)

    write_audit_record(
        {
            "event": "trigger.fired",
            "workspace": catalog.workspace,
            "dataset": dataset_identifier,
            "trigger": trigger["name"],
            "target_view": target_view,
            "execution_id": execution_id,
            "outcome": outcome,
            "author": author,
        }
    )


def _fire_task(
    catalog,
    dataset_identifier: str,
    trigger: dict,
    author: str | None,
    snapshot_id: Any | None,
    parent_snapshot_id: Any | None,
) -> None:
    """Enqueue one task run for the commit that just landed.

    The window is bound HERE, at fire time, as the committing snapshot and its
    parent - not left to resolve when a worker picks the job up. Execution is
    asynchronous, so a relative window (`VERSION AS OF PREVIOUS`) would mean
    whatever the snapshot chain looked like minutes later: with two commits in
    flight, one window is processed twice and another never, silently. Bound
    boundaries make a run's window exactly the commit that fired it, however
    late it runs, and replayable afterwards by naming the same two versions.

    Because the window is a snapshot and its own parent it spans exactly one
    commit, so a compaction can only ever be one of its endpoints - never
    something inside it whose rewritten files would read as new rows. The one
    exception is a window widened over a gap (below), which spans back to the
    last commit a run actually consumed.

    The window is then checked against `last-window-to` before anything is
    submitted: a commit already consumed is skipped as superseded, and one that
    starts beyond where the last success ended is widened back to it. Both
    comparisons are within ONE dataset's version sequence, which is what the
    one-trigger rule guarantees and what makes them mean anything.
    """
    target_task = trigger["target-task"]
    task = catalog.get_task(target_task)

    # A task with no statement can never run; found here so it lands in the
    # audit log beside the commit that tried, rather than as a job failure
    # nobody is watching.
    if not task.get("sql"):
        raise TaskError(f"task has no statement recorded: {target_task}")

    # The identity this unattended run executes as - the TRIGGER's, never the
    # committer's and never the task's. A task carries no identity: a person
    # running EXECUTE runs it as themselves, and the trigger is what makes a run
    # unattended, so the trigger is what must say whose authority it uses. See
    # `_fire_refresh` for why a missing one is refused rather than defaulted.
    runs_as = trigger.get("runs-as")
    if not runs_as:
        raise TaskOwnerMissing(
            f"trigger {trigger['name']} on {dataset_identifier} has no runs-as "
            f"identity; refusing to run {target_task} as the committing user."
        )

    # A dataset's first commit has no parent; the window is then everything up
    # to and including it. Skipping instead would silently drop the rows of the
    # very first commit - and provisioning a task before any data lands is the
    # normal order, so that case is reached in practice, not in theory.
    # See NO_PARENT_VERSION_FLOOR for why this is emphatically not 0.
    parent_version = (
        NO_PARENT_VERSION_FLOOR if parent_snapshot_id is None else int(parent_snapshot_id)
    )
    current_version = int(snapshot_id)

    # THE WINDOW GUARD. `last-window-to` is the `current_version` the last
    # SUCCESSFUL run consumed to. It is usable as a guard - rather than the
    # breadcrumb it was - only because a task has exactly one trigger
    # (`create_trigger`'s one-trigger rule): one trigger is one source, so this
    # scalar and the window being bound here are readings of the SAME version
    # sequence. With two sources they were interleaved ids from two incomparable
    # ones, and either comparison below would have been a coin toss - skipping
    # every fire from whichever dataset's ids ran lower, forever and silently.
    #
    # None for a task that has never succeeded, which is the ordinary state of a
    # new one: no floor, nothing to compare, and the first run takes its window
    # as bound.
    last_window_to = task.get("last-window-to")
    if last_window_to is not None:
        last_window_to = int(last_window_to)

        if current_version <= last_window_to:
            # SUPERSEDED: a run has already consumed through this commit. Reached
            # by a fire that queued behind a later one and outlived the dedup
            # window - re-running it would reprocess rows a successful run
            # already took. Recorded on the task rather than dropped silently,
            # so "nothing happened, deliberately" is visible where someone looks
            # for the run they expected.
            catalog.mark_task_fired(target_task, status="superseded")
            catalog.mark_trigger_fired(dataset_identifier, trigger["name"], status="superseded")
            write_audit_record(
                {
                    "event": "task.superseded",
                    "workspace": getattr(catalog, "workspace", None),
                    "dataset": dataset_identifier,
                    "trigger": trigger["name"],
                    "target_task": target_task,
                    "current_version": current_version,
                    "last_window_to": last_window_to,
                    "author": author,
                }
            )
            return

        if parent_version > last_window_to:
            # GAP: the run that should have covered `last_window_to ->
            # parent_version` never succeeded, so the commits in between were
            # consumed by nobody. Widen this window back to the last covered
            # point rather than starting at this commit's parent and leaving
            # them behind for good. Because `mark_task_fired` stamps only on
            # SUCCESS, the gap stays visible - and keeps widening the next
            # window - until a run actually covers it.
            parent_version = last_window_to

    # The egress gate, on the same terms as a refresh: before the job document,
    # so a blocked run leaves nothing for a worker to pick up, and re-checked
    # here because the SOURCE workspace's protection - this one's, the firing
    # dataset's - can be taken, or the task repointed at a foreign target, long
    # after the trigger was armed.
    #
    # A task's write into ANOTHER workspace is the textbook standing copy: this
    # fires on every commit, forever. The engine refuses it again when the run
    # binds; without this the refusal only ever surfaced there, inside a job,
    # while the trigger recorded `enqueued` and looked healthy.
    catalog.enforce_task_egress(target_task, task.get("writes") or ())

    # Both versions are integers off the catalog's own records, coerced here so
    # nothing but a number can reach the statement text.
    sql_text = (
        f"EXECUTE {task['identifier']} "
        f"USING {parent_version} AS parent_version, "
        f"{current_version} AS current_version"
    )

    execution_id, outcome = _submit_task_job(
        catalog,
        sql_text=sql_text,
        source_dataset=dataset_identifier,
        trigger_name=trigger["name"],
        target_task=target_task,
        snapshot_id=snapshot_id,
        fired_by=author,
    )
    catalog.mark_trigger_fired(dataset_identifier, trigger["name"], status=outcome)

    write_audit_record(
        {
            "event": "task.fired",
            "workspace": getattr(catalog, "workspace", None),
            "dataset": dataset_identifier,
            "trigger": trigger["name"],
            "target_task": target_task,
            "parent_version": parent_version,
            "current_version": snapshot_id,
            "execution_id": execution_id,
            "outcome": outcome,
            "author": author,
        }
    )


# Statuses for the failures that RAISE. Recorded in one place - the handler in
# `fire_triggers` - rather than at each raise site: stamping in both meant the
# handler's generic status overwrote the specific one, turning "owner-missing"
# into "error" and losing the only useful part. Arms that RETURN instead of
# raising (a suspended trigger) still stamp themselves, because no handler sees
# them.
_FAILURE_STATUSES = (
    (EgressRestricted, "egress-blocked"),
    (MaterializedViewOwnerMissing, "owner-missing"),
    (TaskOwnerMissing, "owner-missing"),
)


def _failure_status(exc: BaseException) -> str:
    """What `last-fired-status` should say about a fire that raised."""
    for kind, status in _FAILURE_STATUSES:
        if isinstance(exc, kind):
            return status
    return "error"


def fire_triggers(
    catalog,
    dataset_identifier: str,
    author: str | None,
    snapshot_id: Any | None = None,
    parent_snapshot_id: Any | None = None,
) -> None:
    """Fire every refresh trigger on a dataset that just took a user commit.

    One refresh per distinct target view, however many triggers point at it.
    Never raises: each trigger's failure is alerted and audited individually,
    and one bad trigger does not stop the rest firing.
    """
    if not firing_enabled():
        return

    try:
        triggers = catalog.list_triggers(dataset_identifier)
    except Exception as exc:  # noqa: BLE001 - commit path must survive
        _alert(
            exc,
            note="reading triggers subcollection failed - refreshes NOT fired",
            fingerprint=("trigger-list-failed", dataset_identifier),
            context={"dataset": dataset_identifier},
        )
        return

    seen_targets = set()
    for trigger in triggers:
        kind = trigger.get("kind")
        if kind == "materialized_view_refresh":
            target_view = trigger.get("target-view")
            fire = lambda: _fire_refresh(  # noqa: E731
                catalog, dataset_identifier, trigger, author, snapshot_id
            )
            note = "materialized view refresh NOT enqueued - the view is going stale"
        elif kind == TASK_TRIGGER_KIND:
            target_view = trigger.get("target-task")
            fire = lambda: _fire_task(  # noqa: E731
                catalog, dataset_identifier, trigger, author, snapshot_id, parent_snapshot_id
            )
            note = "task NOT enqueued - its output is going stale"
        else:
            continue
        if not target_view or target_view in seen_targets:
            continue

        # Suspended by an operator. Not an error and not alerted - the trigger
        # records that it was reached and why nothing came of it, so the
        # suppression is visible where someone looks for the staleness rather
        # than only in whatever they remember pausing.
        if trigger.get("suspended-at-ms"):
            catalog.mark_trigger_fired(dataset_identifier, trigger["name"], status="suspended")
            continue

        seen_targets.add(target_view)
        try:
            fire()
        except Exception as exc:  # noqa: BLE001 - commit path must survive
            # Stamp the trigger BEFORE alerting. Every expected outcome above
            # records itself - suspended, egress-blocked, owner-missing - but an
            # unexpected exception used to record nothing at all, so a trigger
            # failing on every commit was indistinguishable from one that had
            # never fired: `last-fired-at-ms` and `last-fired-status` both stayed
            # null, and SHOW TRIGGERS / information_schema showed a healthy-looking
            # row. That is how a TaskNotFound went unnoticed while it fired hourly.
            #
            # Guarded because this is the failure path of the failure path: if
            # stamping also fails, the alert below is what must still happen.
            try:
                catalog.mark_trigger_fired(
                    dataset_identifier, trigger["name"], status=_failure_status(exc)
                )
            except Exception:  # noqa: BLE001 - never displace the real failure
                logger.warning(
                    "could not record the fire failure on trigger %s", trigger.get("name")
                )
            _alert(
                exc,
                note=note,
                fingerprint=("trigger-fire-failed", dataset_identifier, trigger.get("name")),
                context={
                    "dataset": dataset_identifier,
                    "trigger": trigger.get("name"),
                    "target_view": target_view,
                },
            )
            write_audit_record(
                {
                    "event": "trigger.fire_failed",
                    "workspace": getattr(catalog, "workspace", None),
                    "dataset": dataset_identifier,
                    "trigger": trigger.get("name"),
                    "target_view": target_view,
                    "error": str(exc),
                    "author": author,
                }
            )
