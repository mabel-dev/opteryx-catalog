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

import hashlib
import hmac
import json
import logging
import os
import threading
import time
import urllib.parse
import urllib.request
from collections.abc import Iterable
from typing import Any

from .alerts import report as _alert
from .audit import write_audit_record
from .exceptions import EgressRestricted
from .exceptions import MaterializedViewError
from .exceptions import MaterializedViewOwnerMissing
from .exceptions import TaskError
from .exceptions import TaskOwnerMissing
from .exceptions import TaskWindowUnbound
from .exceptions import TriggerNotFound

# Trigger kind for a task run. Defined in `opteryx_catalog` beside the refresh
# kind and re-exported here, so the creation gate that refuses a platform
# identity as a task trigger's owner and the firing path that reads that owner
# are keyed off one string. The MV refresh kind is still spelled inline where it
# is matched; it becomes a task in the next phase, at which point one kind is
# left and this constant is the only one.
from .opteryx_catalog import COMMIT_EVENT_KIND
from .opteryx_catalog import DATASET_HOLDER
from .opteryx_catalog import SCHEDULE_EVENT_KIND
from .opteryx_catalog import SIGNAL_EVENT_KIND
from .opteryx_catalog import TASK_HOLDER
from .opteryx_catalog import TASK_TRIGGER_KIND
from .opteryx_catalog import TRIGGERS_SUBCOLLECTION
from .opteryx_catalog import WINDOW_PARAMETER_PATTERN
from .task_notifications import notify_fire_failed

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


def _qualified_source(catalog, dataset_identifier: str) -> str:
    """The firing dataset's name as `workspace.collection.dataset`.

    The commit path hands `fire_triggers` the dataset's own identifier, which
    is `collection.dataset` - the workspace is the catalog handle's, not part
    of the name. That spelling went onto the job document as
    `trigger.source_dataset` verbatim, and it is the one name in the
    provenance block that is NOT how anything else spells a resource: policy
    patterns, the target, and every catalog record carry the workspace. The
    run-history listing in jobs checks the caller may READ the source, and a
    two-part name matches no `ws.coll.*` grant, so every fired run was
    filtered out of its own history.

    Qualified here, once, where the workspace is known for certain: this
    handle's, because a trigger fires on a commit to a dataset in the
    workspace the handle is bound to. Idempotent for a name that already
    carries one, in the same way `_qualify` on the catalog is.
    """
    parts = dataset_identifier.split(".")
    if len(parts) >= 3:
        return dataset_identifier
    return f"{catalog.workspace}.{dataset_identifier}"

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

# The roles a policy may carry into a run, and the ONLY place this list is
# applied on the unattended path. Must stay in sync with
# authenticate.opteryx's `app/policies.VALID_ROLES`, control.opteryx's
# create/update validation, and `opteryx_access.roles.ROLES` - the other points
# where a role string is gated before or after it reaches Firestore.
#
# No `admin`: it is a BILLING role, never a data role. The engine's
# `ACTION_ROLES` has no entry for it, so a run carrying one held a role that
# authorised nothing while reading, to anyone looking at the job document, as
# though it authorised everything. authenticate drops it before minting a
# token; this drops it before it reaches a job, so the two paths hand the
# engine the same vocabulary.
VALID_ROLES = frozenset({"owner", "writer", "reader"})

# Policies written against this principal grant every HUMAN user. A service
# principal is not "any user" - see `policies_for`'s `include_wildcard`.
WILDCARD_PRINCIPAL = "*"


def policies_for(
    catalog,
    principal: str | None,
    workspaces: Iterable[str] | None = None,
    include_wildcard: bool = False,
) -> list[dict] | None:
    """The principal's current access policies, in job-document shape.

    Read from `{workspace}/$policies/access` - policy.opteryx's storage,
    which lives in the same Firestore database as the catalog - and shaped
    exactly as `normalize_policies_for_storage` writes them onto job docs:
    `[{"role", "pattern", "policy"}]`.

    Public because jobs.opteryx is the caller now: the control point resolves
    the refresh's acting identity and policies itself rather than trusting a
    submission to carry them, and this is the data-access half of that. Read at
    submission time deliberately: a revoked role stops the very next refresh.

    `workspaces` NAMES THE WORKSPACES TO READ, and defaults to this catalog's
    own - which was the only behaviour, and was a silent authority ceiling on
    any statement that crossed a workspace. A minted token carries the
    principal's policies from EVERY workspace (authenticate.opteryx builds the
    claim with a collection-group query), so a person running a statement
    interactively is bound against all of them; an unattended run of the SAME
    statement was bound against one, and every relation outside it was denied
    however the principal was actually granted. A task writing another
    workspace therefore failed on every fire, as a job error, with the trigger
    reporting `enqueued`.

    Deliberately a NAMED SET rather than "everywhere". The caller knows which
    relations the statement touches, so the run is bound against exactly those
    workspaces - which keeps the least-privilege posture `opteryx_access.store`
    states when it refuses a general cross-workspace policy dump, and keeps the
    cost one already-indexed query per workspace involved rather than a
    collection-group scan. Read in the order given, deduplicated, so a job
    document is stable across submissions.

    Any workspace's `$policies/access` is readable through any handle in this
    database - workspaces are sibling root collections, the same property
    `_foreign_properties_ref` relies on for the egress flag - so this needs no
    handle per workspace, and constructs none.

    `include_wildcard` mirrors `authenticate.opteryx.fetch_policies_for_principal`,
    name, default and meaning: wildcard-principal grants reach a HUMAN's token
    and deliberately not a client_credentials caller's, because a service
    account is not "any user" and granting it broad access implicitly is exactly
    what nobody would notice. It was unconditionally on here, so an unattended
    run could carry grants the same principal's own token would not. Off by
    default, as it is there; the caller opts in where it can say the acting
    identity is a real account.

    Roles are filtered to `VALID_ROLES` for the same reason authenticate filters
    them: this is the sole path by which policies reach an unattended run, so a
    role written some other way must not travel silently.
    """
    if not principal:
        return None
    from google.cloud.firestore_v1 import FieldFilter

    names = list(dict.fromkeys(workspaces)) if workspaces else [catalog.workspace]

    principals = [principal]
    if include_wildcard and principal != WILDCARD_PRINCIPAL:
        principals.append(WILDCARD_PRINCIPAL)

    policies: list[dict] = []
    seen: set = set()
    for workspace in names:
        access = (
            catalog.firestore_client.collection(workspace)
            .document("$policies")
            .collection("access")
        )
        query = access.where(filter=FieldFilter("principal", "in", principals))
        for doc in query.stream():
            data = doc.to_dict() or {}
            role, pattern = data.get("role"), data.get("pattern")
            if not (role and pattern):
                continue
            if role not in VALID_ROLES:
                logger.warning(
                    "policies_for: skipping policy %s in %s for principal=%s - "
                    "invalid role=%r",
                    doc.id,
                    workspace,
                    principal,
                    role,
                )
                continue
            # Document ids are unique per workspace, not across them, so the id
            # alone cannot be the key: two workspaces can hold a policy of the
            # same id saying different things, and either dropping one or
            # emitting both as duplicates would be wrong.
            key = (workspace, doc.id, role, pattern)
            if key in seen:
                continue
            seen.add(key)
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
    holder: str | None = None,
    holder_kind: str = DATASET_HOLDER,
    event_kind: str = COMMIT_EVENT_KIND,
) -> tuple[str, str]:
    """Submit one task run to jobs. Returns `(execution_id, outcome)`.

    `holder` and `holder_kind` name where the firing trigger lives - the
    source dataset for a commit trigger, the task itself for a schedule or
    signal - which is what jobs reads the trigger's `runs-as` back through.
    `source_dataset` is the older spelling of the same thing for a commit
    trigger and is kept populated for it, so jobs can move to `holder` in its
    own release; for a task-held trigger it is None, because there is no
    source dataset, and a jobs that still resolves identity through it will
    refuse the run rather than guess.

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
                "holder": holder if holder is not None else source_dataset,
                "holder_kind": holder_kind,
                "event_kind": event_kind,
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
    as its own query class - so jobs recognises it, resolves the firing
    trigger's pinned `runs-as` from the trigger record the provenance names,
    that principal's policies and billing account, routes it to the background
    worker, and derives the dedup window. None of those are this library's to
    assert: a submission that could name the actor or the payer is a
    submission that could name them WRONGLY, and the control point exists so
    that nothing below it has to be trusted about either.

    `client_info.trigger` is provenance for the audit trail - which commit
    fired this, through which trigger - not instructions. jobs derives the
    authoritative workspace and target view from the statement itself, and
    reads the trigger's identity from the catalog rather than from anything
    sent here.
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

    # The refresh runs as the TRIGGER's pinned `runs-as` - never as whoever's
    # commit fired it, and never as anything on the view. The view is stored
    # SQL with no identity of its own, exactly as a task is: a person running
    # REFRESH runs it as themselves, and an unattended refresh carries the
    # identity of the trigger that started it, the same rule `_fire_task`
    # applies. A committer is incidental - an ingest account with writer on a
    # source collection and nothing on the view's would make the view
    # permanently unrefreshable, and which principal happened to write last
    # would decide whether a refresh worked.
    #
    # The identity used to live on the view, on the argument that a view's N
    # refresh triggers must share one identity and only one record can be
    # changed atomically. They still share one: `set_materialized_view_owner`
    # repoints every refresh trigger of a view in one batch, and
    # `create_materialized_view` pins a new trigger to the author without
    # touching the ones that exist. What moved is where the answer is read.
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
    # Nor is the view's record consulted as a fallback: a trigger written
    # before the identity moved onto triggers is what the backfill script is
    # for, and reading the old field here would keep the old model alive.
    runs_as = trigger.get("runs-as")
    if not runs_as:
        raise MaterializedViewOwnerMissing(
            f"trigger {trigger['name']} on {dataset_identifier} has no runs-as identity; "
            f"refusing to refresh {target_view} as the committing user. Set one with "
            f"ALTER TRIGGER {trigger['name']} ON {dataset_identifier} OWNER TO "
            f"<principal>, or move every refresh trigger of the view at once with "
            f"ALTER MATERIALIZED VIEW {target_view} OWNER TO <principal>."
        )

    execution_id, outcome = _submit_refresh_job(
        catalog,
        sql_text=sql_text,
        source_dataset=_qualified_source(catalog, dataset_identifier),
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


def _holder_kwargs(holder_kind: str) -> dict:
    """The keyword a task-held trigger adds to every catalog trigger call.

    Empty for a dataset holder so the commit path's calls are exactly what they
    were - and what every test of them asserts - and a catalog that predates
    holders is called the way it expects.
    """
    return {} if holder_kind == DATASET_HOLDER else {"holder_kind": holder_kind}


def _fire_task(
    catalog,
    dataset_identifier: str,
    trigger: dict,
    author: str | None,
    snapshot_id: Any | None,
    parent_snapshot_id: Any | None,
    *,
    holder_kind: str = DATASET_HOLDER,
) -> tuple[str, str | None]:
    """Enqueue one task run. Returns `(status, execution_id)`.

    `dataset_identifier` is the trigger's HOLDER: the committed dataset for a
    commit trigger, the task itself for a schedule or signal (`holder_kind`).
    `author` is what fired it - the committer, `"schedule"`, or the principal
    who signalled - and is provenance only. The run's identity is the
    trigger's `runs-as`, whichever event fired it.

    THE WINDOW. Bound HERE, at fire time - not left to resolve when a worker
    picks the job up. Execution is asynchronous, so a relative window
    (`VERSION AS OF PREVIOUS`) would mean whatever the snapshot chain looked
    like minutes later: with two commits in flight, one window is processed
    twice and another never, silently. Bound boundaries make a run's window
    exactly what fired it, however late it runs, and replayable afterwards by
    naming the same two versions.

    - A COMMIT binds the committing snapshot and its parent, spanning exactly
      one commit, so a compaction can only ever be one of its endpoints.
    - A SCHEDULE or SIGNAL windowed OVER a dataset binds that dataset's head
      at fire time against `last-window-to`, spanning everything since the
      last successful run - the batch form of the same window. Nothing new
      since then is `superseded`, deliberately not an error: it is the normal
      outcome of most ticks on a quiet dataset.
    - A SCHEDULE or SIGNAL with no OVER binds nothing, and the statement must
      consume nothing: `create_trigger` refused to arm one that does, and this
      refuses again (`TaskWindowUnbound`, recorded as `window-unbound`)
      because the statement can be replaced after arming.

    The window is then checked against `last-window-to` before anything is
    submitted: a commit already consumed is skipped as superseded, and one that
    starts beyond where the last success ended is widened back to it. Both
    comparisons are within ONE dataset's version sequence, which is what the
    one-trigger rule guarantees and what makes them mean anything.
    """
    target_task = trigger["target-task"]
    name = trigger["name"]
    holder_args = _holder_kwargs(holder_kind)
    event_kind = trigger.get("event-kind") or COMMIT_EVENT_KIND
    task = catalog.get_task(target_task)

    # A task with no statement can never run; found here so it lands in the
    # audit log beside the event that tried, rather than as a job failure
    # nobody is watching.
    if not task.get("sql"):
        raise TaskError(f"task has no statement recorded: {target_task}")

    # The identity this unattended run executes as - the TRIGGER's, never the
    # committer's, never the signaller's and never the task's. A task carries
    # no identity: a person running EXECUTE runs it as themselves, and the
    # trigger is what makes a run unattended, so the trigger is what must say
    # whose authority it uses. See `_fire_refresh` for why a missing one is
    # refused rather than defaulted.
    runs_as = trigger.get("runs-as")
    if not runs_as:
        raise TaskOwnerMissing(
            f"trigger {name} on {dataset_identifier} has no runs-as "
            f"identity; refusing to run {target_task} as whoever fired it."
        )

    # None for a task that has never succeeded, which is the ordinary state of a
    # new one: no floor, nothing to compare, and the first run takes its window
    # as bound.
    last_window_to = task.get("last-window-to")
    if last_window_to is not None:
        last_window_to = int(last_window_to)

    def _superseded(detail: str) -> tuple[str, None]:
        # Recorded on the task rather than dropped silently, so "nothing
        # happened, deliberately" is visible where someone looks for the run
        # they expected.
        catalog.mark_task_fired(target_task, status="superseded")
        catalog.mark_trigger_fired(dataset_identifier, name, status="superseded", **holder_args)
        write_audit_record(
            {
                "event": "task.superseded",
                "workspace": getattr(catalog, "workspace", None),
                "dataset": dataset_identifier,
                "trigger": name,
                "target_task": target_task,
                "event_kind": event_kind,
                "current_version": current_version,
                "last_window_to": last_window_to,
                "detail": detail,
                "author": author,
            }
        )
        return "superseded", None

    if event_kind == COMMIT_EVENT_KIND:
        # A dataset's first commit has no parent; the window is then everything
        # up to and including it. Skipping instead would silently drop the rows
        # of the very first commit - and provisioning a task before any data
        # lands is the normal order, so that case is reached in practice, not
        # in theory. See NO_PARENT_VERSION_FLOOR for why this is emphatically
        # not 0.
        parent_version = (
            NO_PARENT_VERSION_FLOOR if parent_snapshot_id is None else int(parent_snapshot_id)
        )
        current_version = int(snapshot_id)
        window_source = _qualified_source(catalog, dataset_identifier)
    else:
        window_source = trigger.get("window-source")
        if window_source is None:
            wanted = sorted(set(WINDOW_PARAMETER_PATTERN.findall(task["sql"])))
            if wanted:
                raise TaskWindowUnbound(
                    f"task {target_task} consumes a window ("
                    + ", ".join(f":{w}" for w in wanted)
                    + f") but its {event_kind} trigger {name} names no OVER dataset to "
                    "bind one from. Recreate the trigger with OVER <table>, or remove "
                    "the window parameters from the statement."
                )
            parent_version = current_version = None
        else:
            head = catalog.head_snapshot_id(window_source)
            if head is None:
                current_version = None
                return _superseded(f"{window_source} has no snapshot; nothing to consume")
            current_version = int(head)
            parent_version = NO_PARENT_VERSION_FLOOR if last_window_to is None else last_window_to

    # THE WINDOW GUARD. `last-window-to` is the `current_version` the last
    # SUCCESSFUL run consumed to. It is usable as a guard - rather than the
    # breadcrumb it was - only because a task has exactly one trigger
    # (`create_trigger`'s one-trigger rule): one trigger is one source, so this
    # scalar and the window being bound here are readings of the SAME version
    # sequence. With two sources they were interleaved ids from two incomparable
    # ones, and either comparison below would have been a coin toss - skipping
    # every fire from whichever dataset's ids ran lower, forever and silently.
    if current_version is not None and last_window_to is not None:
        if current_version <= last_window_to:
            # SUPERSEDED: a run has already consumed through this version.
            # Reached by a commit fire that queued behind a later one and
            # outlived the dedup window, or by a tick on a dataset nothing has
            # landed in since the last run - re-running would reprocess rows a
            # successful run already took.
            return _superseded("already consumed by an earlier successful run")

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
    # here because the SOURCE workspace's protection - this one's - can be
    # taken, or the task repointed at a foreign target, long after the trigger
    # was armed.
    #
    # A task's write into ANOTHER workspace is the textbook standing copy: this
    # fires on every event, forever. The engine refuses it again when the run
    # binds; without this the refusal only ever surfaced there, inside a job,
    # while the trigger recorded `enqueued` and looked healthy.
    catalog.enforce_task_egress(target_task, task.get("writes") or ())

    # Both versions are integers off the catalog's own records, coerced above so
    # nothing but a number can reach the statement text.
    if current_version is None:
        sql_text = f"EXECUTE {task['identifier']}"
    else:
        sql_text = (
            f"EXECUTE {task['identifier']} "
            f"USING {parent_version} AS parent_version, "
            f"{current_version} AS current_version"
        )

    holder = (
        _qualified_source(catalog, dataset_identifier)
        if holder_kind == DATASET_HOLDER
        else trigger.get("holder") or f"{getattr(catalog, 'workspace', '')}.{dataset_identifier}"
    )
    execution_id, outcome = _submit_task_job(
        catalog,
        sql_text=sql_text,
        source_dataset=window_source if event_kind == COMMIT_EVENT_KIND else None,
        trigger_name=name,
        target_task=target_task,
        snapshot_id=snapshot_id if event_kind == COMMIT_EVENT_KIND else current_version,
        fired_by=author,
        holder=holder,
        holder_kind=holder_kind,
        event_kind=event_kind,
    )
    catalog.mark_trigger_fired(dataset_identifier, name, status=outcome, **holder_args)

    write_audit_record(
        {
            "event": "task.fired",
            "workspace": getattr(catalog, "workspace", None),
            "dataset": dataset_identifier,
            "trigger": name,
            "target_task": target_task,
            "event_kind": event_kind,
            "window_source": window_source,
            "parent_version": parent_version,
            "current_version": snapshot_id if event_kind == COMMIT_EVENT_KIND else current_version,
            "execution_id": execution_id,
            "outcome": outcome,
            "author": author,
        }
    )
    return outcome, execution_id


def _minimum_interval_seconds(trigger: dict) -> int:
    """The trigger's firing floor as listed; 0 for a record that has none."""
    try:
        return max(0, int(trigger.get("minimum-interval-seconds") or 0))
    except (TypeError, ValueError):
        return 0


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
    (TaskWindowUnbound, "window-unbound"),
)


def _failure_status(exc: BaseException) -> str:
    """What `last-fired-status` should say about a fire that raised."""
    for kind, status in _FAILURE_STATUSES:
        if isinstance(exc, kind):
            return status
    return "error"


def _dispatch(
    catalog,
    holder: str,
    holder_kind: str,
    trigger: dict,
    target: str | None,
    fire,
    note: str,
    author: str | None,
) -> tuple[str, str | None, str | None]:
    """Run one trigger's fire under the contract every event shares.

    Suspension, the firing floor, and the never-raise failure handling are the
    same whether a commit, a tick or a signal reached the trigger, so they are
    here once and `fire_triggers`, `fire_signal` and `fire_due_schedules` differ
    only in how they found the trigger and what `fire` binds. Returns
    `(status, execution_id, detail)`; `detail` is the error text of a fire that
    raised, for a caller with someone to show it to.
    """
    name = trigger["name"]
    holder_args = _holder_kwargs(holder_kind)

    # Suspended by an operator. Not an error and not alerted - the trigger
    # records that it was reached and why nothing came of it, so the
    # suppression is visible where someone looks for the staleness rather
    # than only in whatever they remember pausing.
    if trigger.get("suspended-at-ms"):
        catalog.mark_trigger_fired(holder, name, status="suspended", **holder_args)
        return "suspended", None, None

    claim = None
    try:
        # THE FLOOR. A trigger carrying `minimum-interval-seconds` fires at most
        # once per interval. The right to fire is CLAIMED in a transaction
        # on the trigger document - see `claim_trigger_fire` for why a
        # read-then-stamp would let a burst of commits all through - and
        # taken before the submission so the stamp cannot be keyed on an
        # outcome. A refused claim is recorded like a suspension: not an
        # error, not alerted, but visible where someone looks for the run
        # they expected. A record with no floor is never claimed, so a
        # trigger that predates the field costs exactly what it did.
        if _minimum_interval_seconds(trigger) > 0:
            claim = catalog.claim_trigger_fire(holder, name, **holder_args)
            if not claim.granted:
                catalog.mark_trigger_fired(holder, name, status="throttled", **holder_args)
                write_audit_record(
                    {
                        "event": "trigger.throttled",
                        "workspace": getattr(catalog, "workspace", None),
                        "dataset": holder,
                        "trigger": name,
                        "target_view": target,
                        "minimum_interval_seconds": claim.interval_seconds,
                        "last_claimed_at_ms": claim.at_ms,
                        "author": author,
                    }
                )
                return "throttled", None, None
        result = fire()
    except Exception as exc:  # noqa: BLE001 - the caller's path must survive
        # A claim whose fire raised is handed back first, so the failure
        # does not also silence the next interval. Guarded like the stamp
        # below: the alert is what must still happen.
        if claim is not None and claim.granted:
            try:
                catalog.release_trigger_fire(holder, name, claim, **holder_args)
            except Exception:  # noqa: BLE001 - never displace the real failure
                logger.warning("could not release the fire claim on trigger %s", name)
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
        status = _failure_status(exc)
        try:
            catalog.mark_trigger_fired(holder, name, status=status, **holder_args)
        except Exception:  # noqa: BLE001 - never displace the real failure
            logger.warning("could not record the fire failure on trigger %s", name)
        _alert(
            exc,
            note=note,
            fingerprint=("trigger-fire-failed", holder, name),
            context={"dataset": holder, "trigger": name, "target_view": target},
        )
        write_audit_record(
            {
                "event": "trigger.fire_failed",
                "workspace": getattr(catalog, "workspace", None),
                "dataset": holder,
                "trigger": name,
                "target_view": target,
                "error": str(exc),
                "author": author,
            }
        )
        # THE RUN NEVER STARTED, so nothing downstream can report it: the worker
        # only ever sees runs that were submitted. This is the only place a
        # subscriber can be told that an egress block, a missing owner or an
        # unbindable window is quietly stopping their task - the failure mode
        # where `last-fired-status` keeps reading `enqueued` and the output just
        # goes stale. Tasks only; a materialized view carries no subscriptions.
        if trigger.get("kind") == TASK_TRIGGER_KIND and target:
            notify_fire_failed(
                catalog,
                target,
                status,
                trigger=name,
                holder=holder,
                detail=str(exc),
            )
        return status, None, str(exc)

    if isinstance(result, tuple):
        status, execution_id = result
        return status, execution_id, None
    return "enqueued", None, None


def fire_triggers(
    catalog,
    dataset_identifier: str,
    author: str | None,
    snapshot_id: Any | None = None,
    parent_snapshot_id: Any | None = None,
) -> None:
    """Fire every trigger on a dataset that just took a user commit.

    One refresh per distinct target view, however many triggers point at it.
    Never raises: each trigger's failure is alerted and audited individually,
    and one bad trigger does not stop the rest firing.

    A trigger with a firing floor (`minimum-interval-seconds`) is claimed before it
    fires and skipped as `throttled` inside the interval after a granted claim.
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
            fire = lambda: _fire_refresh(
                catalog, dataset_identifier, trigger, author, snapshot_id
            )
            note = "materialized view refresh NOT enqueued - the view is going stale"
        elif kind == TASK_TRIGGER_KIND:
            target_view = trigger.get("target-task")
            fire = lambda: _fire_task(
                catalog, dataset_identifier, trigger, author, snapshot_id, parent_snapshot_id
            )
            note = "task NOT enqueued - its output is going stale"
        else:
            continue
        if not target_view or target_view in seen_targets:
            continue
        # A dataset holds commit triggers only; a record that says otherwise is
        # misfiled, and firing it on a commit would bind a window it was never
        # meant to have. Left for integrity to report.
        if (trigger.get("event-kind") or COMMIT_EVENT_KIND) != COMMIT_EVENT_KIND:
            continue

        # Marked as seen even when suspended or throttled: a second trigger at
        # the same target must not fire in its place.
        seen_targets.add(target_view)
        _dispatch(catalog, dataset_identifier, DATASET_HOLDER, trigger, target_view, fire, note, author)


# --- Firing from the clock and from a signal ------------------------------------
#
# Both are dispatch.opteryx's to call. Neither has a commit: the task's own
# trigger record says how the run is windowed (`window-source`) and as whom it
# runs (`runs-as`), and the event supplies only provenance - `"schedule"`, or
# the principal who signalled.


def _task_trigger(catalog, task_identifier: str, expected_event: str) -> tuple[dict, dict]:
    """The task and its one held trigger, which must be of `expected_event`.

    Found through the back-pointer rather than by listing: the pointer is
    what the one-trigger rule maintains, so a task with a commit trigger on
    some dataset reports that - `TriggerNotFound` naming what it has - rather
    than an empty subcollection that looks like "no trigger".
    """
    task = catalog.get_task(task_identifier)
    held = task.get("trigger") or {}
    if not held.get("name"):
        raise TriggerNotFound(f"task {task['identifier']} has no trigger")
    if held.get("source") != task["identifier"]:
        raise TriggerNotFound(
            f"task {task['identifier']} is fired by {held['name']} ON {held.get('source')}, "
            f"a commit trigger, not a {expected_event}"
        )
    triggers = {
        t.get("name"): t for t in catalog.list_triggers(task_identifier, holder_kind=TASK_HOLDER)
    }
    trigger = triggers.get(held["name"])
    if trigger is None:
        raise TriggerNotFound(
            f"task {task['identifier']} points at trigger {held['name']}, which does not exist"
        )
    if (trigger.get("event-kind") or COMMIT_EVENT_KIND) != expected_event:
        raise TriggerNotFound(
            f"trigger {held['name']} on {task['identifier']} is a "
            f"{trigger.get('event-kind') or COMMIT_EVENT_KIND} trigger, not a {expected_event}"
        )
    return task, trigger


def signal_signature(token: str, task_identifier: str, identity: str) -> str:
    """The signature a signed invoke URL carries: HMAC-SHA256 over the task and identity.

    "A hash of the token, the task name and the identity" - in the HMAC form
    rather than a bare digest of the concatenation, so a signature cannot be
    extended into one for a longer task name, and with a separator so
    `("ab", "c")` and `("a", "bc")` sign differently. Hex, URL-safe as it is.
    `task_identifier` is the fully-qualified name as the catalog stores it;
    `token` is the task's signal token (`rotate_signal_token`).
    """
    message = f"{task_identifier}\n{identity}".encode()
    return hmac.new(token.encode(), message, hashlib.sha256).hexdigest()


def signal_signature_matches(token: str | None, task_identifier: str, identity: str, presented: str | None) -> bool:
    """Constant-time check of a presented signature; False when there is no token."""
    if not token or not presented:
        return False
    return hmac.compare_digest(signal_signature(token, task_identifier, identity), presented.strip().lower())


def fire_signal(catalog, task_identifier: str, caller: str, channel: str = "bearer") -> dict:
    """Fire a task's signal trigger because `caller` asked.

    The caller is the EVENT, not the context: recorded as `fired_by`, and the
    run assumes the trigger's `runs-as` exactly as a commit-fired run does.
    Whether the caller may do this - SIGNAL on the task - is the endpoint's
    question, answered before this is reached; this trusts the name it is
    handed the way `fire_triggers` trusts the committer's.

    `channel` says how the request arrived - `bearer` (a principal's token,
    checked for SIGNAL by the endpoint) or `signed-url` (a URL the task's
    owner minted; `caller` is then the identity the URL was minted for, and
    the authority is the owner's who minted it). Provenance only.

    Never raises for a fire that failed: that is recorded on the trigger,
    alerted, and returned as the status, so a webhook sender sees the same
    outcome an operator would find on the record. Raises `TaskNotFound` and
    `TriggerNotFound` for a request that names nothing to fire, which are
    the caller's to see as a 404.
    """
    task, trigger = _task_trigger(catalog, task_identifier, SIGNAL_EVENT_KIND)
    holder = task_identifier
    status, execution_id, detail = _dispatch(
        catalog,
        holder,
        TASK_HOLDER,
        trigger,
        task["identifier"],
        lambda: _fire_task(catalog, holder, trigger, caller, None, None, holder_kind=TASK_HOLDER),
        "signalled task NOT enqueued",
        caller,
    )
    return {
        "status": status,
        "execution_id": execution_id,
        "trigger": trigger["name"],
        "task": task["identifier"],
        "detail": detail,
        # How the signal arrived: `bearer` for an authenticated principal,
        # `signed-url` for a URL minted by the task's owner. Provenance for the
        # endpoint's log line; the run's identity is the trigger's either way.
        "channel": channel,
    }


def _due_schedule_triggers(client, now_ms: int) -> list[tuple[str, str, str, str, dict]]:
    """`(workspace, collection, task, trigger name, record)` for every due schedule.

    ONE collection-group query across every workspace in the database, which
    is what one clock wants: workspaces are sibling root collections and their
    `triggers` subcollections share a name. Suspended records are filtered
    here rather than in the query - Firestore cannot ask "is null" cheaply -
    and are stamped `suspended` by `_dispatch` so the suppression is visible.

    A trigger not under a task is not a schedule, whatever its record says
    (`create_trigger` refuses the combination); skipped, and left for
    integrity to report.
    """
    from google.cloud.firestore_v1 import FieldFilter

    query = (
        client.collection_group(TRIGGERS_SUBCOLLECTION)
        .where(filter=FieldFilter("event-kind", "==", SCHEDULE_EVENT_KIND))
        .where(filter=FieldFilter("next-due-at-ms", "<=", now_ms))
    )
    due = []
    for doc in query.stream():
        path = getattr(getattr(doc, "reference", None), "path", None) or ""
        parts = path.split("/")
        # {workspace}/{collection}/tasks/{task}/triggers/{name}
        if len(parts) != 6 or parts[2] != "tasks" or parts[4] != TRIGGERS_SUBCOLLECTION:
            logger.warning("schedule trigger at an unexpected path, not fired: %s", path)
            continue
        data = doc.to_dict() or {}
        data.setdefault("name", parts[5])
        due.append((parts[0], parts[1], parts[3], parts[5], data))
    return due


def _default_catalog_factory(client):
    def _build(workspace: str):
        from .opteryx_catalog import OpteryxCatalog

        return OpteryxCatalog(
            workspace=workspace,
            firestore_project=getattr(client, "project", None),
            firestore_database=getattr(client, "_database", None),
        )

    return _build


def fire_due_schedules(client, now_ms: int | None = None, catalog_factory=None) -> list[dict]:
    """Fire every schedule trigger that is due, across every workspace. Never raises.

    The clock's tick. Holds nothing between calls: which triggers are due is
    asked of Firestore every time, so a restart, a redeploy or a stalled
    minute costs nothing and the next tick finds the same due records. Each
    due trigger is CLAIMED (`claim_schedule_tick`) before it fires, which is
    what lets two ticks overlap - a rollout serving two revisions - without
    double-firing; a claim the fire could not honour is handed back.

    `catalog_factory(workspace)` builds the per-workspace handle a fire needs;
    the default constructs one on `client`'s project and database. One handle
    per workspace per tick, however many triggers it holds.

    Returns one outcome per due trigger, in the shape `fire_signal` returns
    plus `workspace` and `skipped_occurrences`, for the tick's log line.
    """
    now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    if not firing_enabled():
        return []

    try:
        due = _due_schedule_triggers(client, now_ms)
    except Exception as exc:  # noqa: BLE001 - the loop must survive
        _alert(
            exc,
            note="reading due schedule triggers failed - NOTHING fired this tick",
            fingerprint=("schedule-scan-failed",),
            context={"now_ms": now_ms},
        )
        return []

    build = catalog_factory or _default_catalog_factory(client)
    handles: dict = {}
    outcomes = []
    for workspace, collection, task_name, name, trigger in due:
        task_identifier = f"{collection}.{task_name}"
        outcome = {
            "workspace": workspace,
            "task": f"{workspace}.{task_identifier}",
            "trigger": name,
            "status": "error",
            "execution_id": None,
            "detail": None,
            "skipped_occurrences": 0,
        }
        try:
            catalog = handles.get(workspace)
            if catalog is None:
                catalog = handles[workspace] = build(workspace)

            if trigger.get("suspended-at-ms"):
                # Not claimed: the due instant stays where it is, so RESUME's
                # recompute-from-now is what moves it, and the record shows
                # every tick that reached it while paused.
                catalog.mark_trigger_fired(task_identifier, name, status="suspended", holder_kind=TASK_HOLDER)
                outcome["status"] = "suspended"
                outcomes.append(outcome)
                continue

            claim = catalog.claim_schedule_tick(task_identifier, name, now_ms)
            if not claim.granted:
                # Another loop's tick took it between the scan and now.
                outcome["status"] = "claimed-elsewhere"
                outcomes.append(outcome)
                continue
            outcome["skipped_occurrences"] = claim.skipped_occurrences

            status, execution_id, detail = _dispatch(
                catalog,
                task_identifier,
                TASK_HOLDER,
                trigger,
                outcome["task"],
                lambda catalog=catalog, task_identifier=task_identifier, trigger=trigger: _fire_task(
                    catalog, task_identifier, trigger, "schedule", None, None, holder_kind=TASK_HOLDER
                ),
                "scheduled task NOT enqueued",
                "schedule",
            )
            if detail is not None:
                # The fire raised: the slot was not consumed, so the tick is
                # handed back and the next loop finds it due again.
                try:
                    catalog.release_schedule_tick(task_identifier, name, claim)
                except Exception:  # noqa: BLE001 - never displace the real failure
                    logger.warning("could not release the tick on trigger %s", name)
            outcome.update({"status": status, "execution_id": execution_id, "detail": detail})
            write_audit_record(
                {
                    "event": "schedule.ticked",
                    "workspace": workspace,
                    "task": outcome["task"],
                    "trigger": name,
                    "due_at_ms": claim.previous_due_ms,
                    "next_due_at_ms": claim.next_due_ms,
                    "skipped_occurrences": claim.skipped_occurrences,
                    "status": status,
                    "execution_id": execution_id,
                }
            )
        except Exception as exc:  # noqa: BLE001 - one bad trigger must not stop the tick
            outcome["detail"] = str(exc)
            _alert(
                exc,
                note="schedule trigger NOT fired - its task is going stale",
                fingerprint=("schedule-tick-failed", workspace, task_identifier, name),
                context={"workspace": workspace, "task": task_identifier, "trigger": name},
            )
        outcomes.append(outcome)
    return outcomes
