"""Fire dataset triggers after a user-created commit.

The commit path calls `fire_triggers()`, which reads the committed dataset's
`triggers` subcollection and, for each materialized-view refresh trigger,
does what jobs.opteryx does when a user submits a query - because the refresh
IS a query, executed by the same worker:

1. Write a `jobs/{execution_id}` document. worker.opteryx ignores the Cloud
   Tasks payload beyond `execution_id` and re-reads everything (sql_text,
   submitted_by, policies, ...) from this document, so the document is the
   whole contract.
2. Enqueue a Cloud Task targeting worker.opteryx's `/api/v1/submit`, OIDC-
   authenticated, with a *named* task so rapid commits within the dedup
   window collapse into one refresh.

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

# Refreshes fired inside one window share a task name, and Cloud Tasks
# rejects a name it has already seen - that rejection IS the debounce.
DEDUP_WINDOW_SECONDS = 60

# Bounds one HTTP call - minting a token, or submitting one refresh - not the
# refresh itself, which jobs runs asynchronously long after this returns.
HTTP_TIMEOUT_SECONDS = 30

# Mirrors jobs.opteryx's JOB_TTL_DAYS: how long a refresh job document
# lingers before the purge sweep may remove it.
JOB_TTL_DAYS = int(os.environ.get("JOB_TTL_DAYS", "14"))

# Cloud Tasks task ids allow letters, digits, hyphens and underscores.
_TASK_ID_UNSAFE = re.compile(r"[^A-Za-z0-9_-]+")

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


def _project_id(catalog) -> str | None:
    """The GCP project for the jobs collection and task queue.

    Env wins (the deployed convention, same names jobs.opteryx reads);
    the catalog's own Firestore project is the fallback.
    """
    for env in ("GCP_PROJECT_ID", "GCP_PROJECT", "GOOGLE_CLOUD_PROJECT"):
        value = os.environ.get(env)
        if value:
            return value
    return getattr(getattr(catalog, "firestore_client", None), "project", None)


def _jobs_client(catalog):
    """Firestore client for the `jobs` collection.

    jobs.opteryx and worker.opteryx use the project's *default* database
    (`db.collection("jobs")`), not the catalog's `catalogs` database, so the
    catalog's own client cannot be reused. OPTERYX_JOBS_DATABASE overrides
    for tests/emulators.
    """
    from google.cloud import firestore

    database = os.environ.get("OPTERYX_JOBS_DATABASE")
    kwargs = {"project": _project_id(catalog)}
    if database:
        kwargs["database"] = database
    return firestore.Client(**kwargs)


HOUSE_BILLING_ACCOUNT = "opteryx"


def _billing_account_for_workspace(catalog, workspace: str) -> str:
    """The account that pays for work done on behalf of `workspace`.

    A refresh used to bill the view's `runs-as` identity, which for every
    platform-owned view is `federator` - an identity, not an account, and by
    volume the largest single thing on the meter. The party that benefits from
    a materialized view is the workspace holding it, so that is who pays.

    `workspaces/{name}` lives in the project's DEFAULT database - the one
    `_jobs_client` already talks to. The catalog's own `catalogs` database does
    not hold it.

    A workspace with no billing record falls back to the house account rather
    than to `runs-as`. Neither answer is knowably right, but this one is wrong
    in a bounded way: the house absorbs it, and the event carries `workspace`,
    so the gap is findable by name. Billing `runs-as` instead invents a payer
    out of a service identity, which is the failure this replaces.

    Never raises. A refresh must not fail because a billing lookup did.
    """
    try:
        snapshot = _jobs_client(catalog).collection("workspaces").document(workspace).get()
        account = (snapshot.to_dict() or {}).get("billing_account") if snapshot.exists else None
    except Exception as exc:  # noqa: BLE001 - see docstring
        logger.warning(f"could not read the billing account for workspace {workspace!r}: {exc}")
        account = None

    if not account:
        logger.warning(
            f"workspace {workspace!r} has no billing account; billing this refresh to "
            f"{HOUSE_BILLING_ACCOUNT!r}"
        )
        return HOUSE_BILLING_ACCOUNT
    return account


def _policies_for(catalog, principal: str | None) -> list[dict] | None:
    """The principal's current access policies, in job-document shape.

    Read from `{workspace}/$policies/access` - policy.opteryx's storage,
    which lives in the same Firestore database as the catalog - and shaped
    exactly as `normalize_policies_for_storage` writes them onto job docs:
    `[{"role", "pattern", "policy"}]`. Read at fire time deliberately: a
    revoked role stops the very next refresh.
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


def _task_id(workspace: str, trigger_name: str, now_s: float | None = None) -> str:
    """Deterministic per-window task id - the dedup key.

    Two commits inside the same window produce the same id; Cloud Tasks
    rejects the second create with AlreadyExists, which callers treat as a
    successful (deduplicated) fire. Window rollover changes the id, so the
    ~1h task-name tombstone never suppresses a later refresh.
    """
    if now_s is None:
        now_s = time.time()
    bucket = int(now_s // DEDUP_WINDOW_SECONDS)
    raw = f"mvrefresh--{workspace}--{trigger_name}--{bucket}"
    return _TASK_ID_UNSAFE.sub("-", raw)[:500]


# --- Submitting through jobs.opteryx -------------------------------------------
#
# This module used to write the `jobs/{execution_id}` document itself and enqueue
# its own Cloud Task straight at worker.opteryx. That made it a second
# implementation of jobs' contract, and - because this is a LIBRARY, embedded in
# every service that commits a dataset - it meant work reached the workers from
# several places with nothing able to see, meter or refuse it in one spot. jobs
# is the control point; a refresh is a query; so a refresh goes through jobs.
#
# The credential is the catalog's OWN, not `federator`'s. federator is
# compaction's identity and its secret stays in xb500: this library runs inside
# upload, worker and worker-lo, so handing it federator's secret would put
# compaction's identity in three more places. What is NOT avoided is
# distribution - the catalog's own secret does ship to all three, because that
# is where the code runs. What is contained is WHICH identity a compromise
# there yields.
#
# Read from the environment only, with no Secret Manager path here. Cloud Run's
# `--set-secrets` resolves the secret at instance start, which keeps this
# library free of a Secret Manager dependency it would otherwise carry into
# every host. The consequence is stated rather than hidden: a rotated secret is
# picked up when instances restart, not on the next call.
CLIENT_ID_ENV = "CATALOG_CLIENT_ID"
CLIENT_SECRET_ENV = "CATALOG_CLIENT_SECRET"

# Re-minting per refresh would be one auth round trip per fired trigger, and a
# commit can fire several. Margin so a token is never spent in its last seconds.
_TOKEN_EXPIRY_MARGIN_SECONDS = 60
_token_cache: dict = {"access_token": None, "expires_at": 0.0}


def _auth_url() -> str:
    return os.environ.get("AUTH_URL", "https://authenticate.opteryx.app")


def _jobs_url() -> str:
    return os.environ.get("JOBS_URL", "https://jobs.opteryx.app")


def _catalog_token() -> str:
    """A platform bearer token for this library, minted and cached.

    Raises rather than returning None: a refresh that cannot authenticate has
    not happened, and `fire_triggers` turns that into an alert and a recorded
    fire failure. Returning a falsy token would instead produce a 401 at jobs
    and a much less obvious trail.
    """
    now = time.time()
    cached = _token_cache.get("access_token")
    if cached and _token_cache.get("expires_at", 0) > now:
        return cached

    secret = os.environ.get(CLIENT_SECRET_ENV)
    if not secret:
        raise MaterializedViewError(
            f"cannot submit a materialized view refresh: {CLIENT_SECRET_ENV} is not set. "
            "It is injected by the deployment (Cloud Run --set-secrets); without it this "
            "service cannot authenticate to the jobs API."
        )
    client_id = os.environ.get(CLIENT_ID_ENV, "catalog")

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
    _token_cache["expires_at"] = now + max(0, int(body.get("expires_in") or 0)) - _TOKEN_EXPIRY_MARGIN_SECONDS
    return access_token


def _submit_refresh_job(
    catalog,
    *,
    sql_text: str,
    runs_as: str,
    policies: list | None,
    source_dataset: str,
    trigger_name: str,
    target_view: str,
    snapshot_id: Any | None,
    billing_account: str,
    fired_by: str | None,
    task_id: str,
) -> tuple[str, str]:
    """Submit the refresh to jobs. Returns `(execution_id, outcome)`.

    Everything that used to be written into the job document by hand travels in
    the `platform` block, which jobs refuses from callers not on its
    submitter allowlist. Two of those fields are the reason the block is gated:
    a refresh ACTS as the view's pinned owner and BILLS the workspace holding
    the view - two parties, and neither of them is this library.

    `task_id` becomes jobs' idempotency key, which is what carries the dedup
    window across the move. It used to be a Cloud Tasks task NAME chosen here,
    and Cloud Tasks refusing a name it had seen was the debounce. Submitting
    through jobs without passing it would have silently turned one refresh per
    window back into one refresh per commit.
    """
    payload = {
        "sql_text": sql_text,
        "platform": {
            "submitted_by": runs_as,
            "billing_account": billing_account,
            "workspace": catalog.workspace,
            "origin": "trigger",
            "policies": policies or None,
            "trigger": {
                "workspace": catalog.workspace,
                "source_dataset": source_dataset,
                "trigger_name": trigger_name,
                "target_view": target_view,
                "snapshot_id": snapshot_id,
                "fired_by": fired_by,
            },
            "idempotency_key": task_id,
        },
    }
    request = urllib.request.Request(
        f"{_jobs_url()}/api/v1/jobs",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {_catalog_token()}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        body = json.loads(response.read().decode("utf-8"))

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
        catalog.mark_trigger_fired(dataset_identifier, trigger["name"], status="egress-blocked")
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
        catalog.mark_trigger_fired(dataset_identifier, trigger["name"], status="owner-missing")
        raise MaterializedViewOwnerMissing(
            f"materialized view {target_view} has no runs-as identity; refusing to "
            "refresh it as the committing user. Set one with ALTER MATERIALIZED "
            f"VIEW {target_view} OWNER TO <principal>."
        )

    billing_account = _billing_account_for_workspace(catalog, catalog.workspace)
    execution_id, outcome = _submit_refresh_job(
        catalog,
        sql_text=sql_text,
        runs_as=runs_as,
        policies=_policies_for(catalog, runs_as),
        source_dataset=dataset_identifier,
        trigger_name=trigger["name"],
        target_view=target_view,
        snapshot_id=snapshot_id,
        billing_account=billing_account,
        fired_by=author,
        task_id=_task_id(catalog.workspace, trigger["name"]),
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
            "billing_account": billing_account,
        }
    )


def fire_triggers(
    catalog,
    dataset_identifier: str,
    author: str | None,
    snapshot_id: Any | None = None,
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
        if trigger.get("kind") != "materialized_view_refresh":
            continue
        target_view = trigger.get("target-view")
        if not target_view or target_view in seen_targets:
            continue
        seen_targets.add(target_view)
        try:
            _fire_refresh(catalog, dataset_identifier, trigger, author, snapshot_id)
        except Exception as exc:  # noqa: BLE001 - commit path must survive
            _alert(
                exc,
                note="materialized view refresh NOT enqueued - the view is going stale",
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
