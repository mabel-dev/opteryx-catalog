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

import requests

from .alerts import report as _alert
from .audit import write_audit_record
from .exceptions import EgressRestricted
from .exceptions import MaterializedViewError

# Refreshes fired inside one window share a task name, and Cloud Tasks
# rejects a name it has already seen - that rejection IS the debounce.
DEDUP_WINDOW_SECONDS = 60

# Mirrors jobs.opteryx's JOB_TTL_DAYS: how long a refresh job document
# lingers before the purge sweep may remove it.
JOB_TTL_DAYS = int(os.environ.get("JOB_TTL_DAYS", "14"))

# Cloud Tasks task ids allow letters, digits, hyphens and underscores.
_TASK_ID_UNSAFE = re.compile(r"[^A-Za-z0-9_-]+")

_KILL_SWITCH_ENV = "OPTERYX_TRIGGER_FIRING"

# The service's own identity, from the metadata server. Same endpoint and
# timeout as `alerts/_secrets.py` uses for the project id.
_METADATA_SA_URL = (
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email"
)
_METADATA_TIMEOUT_SECONDS = 1.0

_sa_lock = threading.Lock()
_sa_cache: str | None = None


def _runtime_service_account() -> str | None:
    """This process's own service account, or None when not on GCP.

    Only successful lookups are cached: a failure here is fatal to the fire
    (see `_enqueue_refresh_task`), so caching a None would turn one slow
    metadata response into permanently stale views for the process lifetime.
    """
    global _sa_cache

    with _sa_lock:
        if _sa_cache is not None:
            return _sa_cache

    try:
        response = requests.get(
            _METADATA_SA_URL,
            headers={"Metadata-Flavor": "Google"},
            timeout=_METADATA_TIMEOUT_SECONDS,
        )
    except requests.RequestException:
        # Not on GCP, or the metadata server did not answer in time.
        return None
    if response.status_code != 200:
        return None
    email = response.text.strip()
    if not email:
        return None

    with _sa_lock:
        _sa_cache = email
    return email


def _oidc_service_account() -> str | None:
    """The service account the refresh task is minted for.

    worker.opteryx pins one OIDC subject (`WORKER_OIDC_SUBJECT`, defaulting to
    the numeric id of the project's Cloud Run runtime identity), so the answer
    is always "whoever this process already is" - every Opteryx service runs as
    that same account, which is why jobs.opteryx's enqueue is accepted today.
    Asking the metadata server is therefore both correct and self-configuring;
    there is nothing for an operator to set, and nothing to keep in step with
    the worker when the platform identity changes.

    TASKS_OIDC_SA remains as an escape hatch for the case the runtime identity
    and the enqueuing identity are deliberately split.
    """
    return os.environ.get("TASKS_OIDC_SA") or _runtime_service_account()


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


def _make_job_id(now: datetime | None = None) -> str:
    """Job id of the form YYYYMMDDHHMMSS-{16 lowercase alphanums}.

    Same shape `jobs.opteryx._make_job_id` mints, so refresh jobs are
    indistinguishable infrastructure-wise from user submissions.
    """
    if now is None:
        now = datetime.now(UTC)
    prefix = now.strftime("%Y%m%d%H%M%S")
    chars = string.ascii_lowercase + string.digits
    rand = "".join(secrets.choice(chars) for _ in range(16))
    return f"{prefix}-{rand}"


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


def _enqueue_refresh_task(catalog, execution_id: str, task_id: str) -> str:
    """Enqueue the Cloud Task push to worker.opteryx. Returns the outcome.

    Same wiring as jobs.opteryx's enqueue (`interface.py`, create_job): plain
    dict task, OIDC token for the worker's pinned service account, audience
    defaulting to the target URL. The worker reads only `execution_id` from
    the body.
    """
    from google.api_core.exceptions import AlreadyExists
    from google.cloud import tasks_v2

    project = _project_id(catalog)
    location = os.environ.get("TASKS_LOCATION", "us-east1")
    queue = os.environ.get("TASKS_QUEUE", "worker-dispatch")
    target_url = os.environ.get("TASKS_TARGET_URL", "https://worker.opteryx.app/api/v1/submit")

    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(project, location, queue)

    http_request: dict = {
        "http_method": tasks_v2.HttpMethod.POST,
        "url": target_url,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"execution_id": execution_id}).encode("utf-8"),
    }
    # worker.opteryx pins one OIDC subject; the SA here must be the same one
    # jobs.opteryx enqueues as (decision 4 - no worker-side auth changes).
    # No token means a task the worker answers with 401, which Cloud Tasks
    # then retries until it expires - a stale view whose only evidence is in
    # the queue's logs. Failing here instead puts it in the audit log.
    oidc_sa = _oidc_service_account()
    if not oidc_sa:
        raise MaterializedViewError(
            "cannot mint an OIDC token for the refresh task: no TASKS_OIDC_SA "
            "and no runtime service account from the metadata server. Set "
            "OPTERYX_TRIGGER_FIRING=0 where commits happen off-platform."
        )
    http_request["oidc_token"] = tasks_v2.OidcToken(
        service_account_email=oidc_sa,
        audience=os.environ.get("TASKS_OIDC_AUDIENCE", target_url),
    )

    task = {"name": f"{parent}/tasks/{task_id}", "http_request": http_request}
    try:
        client.create_task(parent=parent, task=task)
    except AlreadyExists:
        return "deduplicated"
    return "enqueued"


def _write_refresh_job(
    catalog,
    execution_id: str,
    sql_text: str,
    author: str | None,
    policies: list[dict] | None,
    source_dataset: str,
    trigger_name: str,
    target_view: str,
    snapshot_id: Any | None,
) -> None:
    """Write the jobs/{execution_id} document the worker will execute from."""
    from google.cloud import firestore

    now = datetime.now(UTC)
    job_doc = {
        "execution_id": execution_id,
        "sql_text": sql_text,
        "status": "SUBMITTED",
        "created_at": firestore.SERVER_TIMESTAMP,
        "updated_at": firestore.SERVER_TIMESTAMP,
        # The invoker: the author of the commit that fired the trigger. This
        # is the identity the worker hands the engine, the one the binder
        # authorizes, and the one billed - matching jobs.opteryx's fallback
        # of billing_account to the submitting identity.
        "submitted_by": author,
        "billing_account": author,
        "entitlements": [],
        "purge_at": now + timedelta(days=JOB_TTL_DAYS),
        # `origin` keeps this off /jobs/recent and tells the worker to stamp
        # the MV's refresh state when the job finishes.
        "origin": "trigger",
        "trigger": {
            # workspace travels too: target_view is workspace-relative, and
            # the worker needs both to stamp refresh state on the right MV.
            "workspace": catalog.workspace,
            "source_dataset": source_dataset,
            "trigger_name": trigger_name,
            "target_view": target_view,
            "snapshot_id": snapshot_id,
        },
        "description": (
            f"materialized view refresh of {target_view} "
            f"(trigger {trigger_name} on {source_dataset})"
        ),
        "describer": "trigger",
    }
    if policies:
        job_doc["policies"] = policies
    _jobs_client(catalog).collection("jobs").document(execution_id).set(job_doc)


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
    qualified_target = f"{catalog.workspace}.{mv['collection']}.{mv['name']}"
    sql_text = f"REFRESH MATERIALIZED VIEW {qualified_target}"

    execution_id = _make_job_id()
    _write_refresh_job(
        catalog,
        execution_id=execution_id,
        sql_text=sql_text,
        author=author,
        policies=_policies_for(catalog, author),
        source_dataset=dataset_identifier,
        trigger_name=trigger["name"],
        target_view=target_view,
        snapshot_id=snapshot_id,
    )
    outcome = _enqueue_refresh_task(
        catalog, execution_id, _task_id(catalog.workspace, trigger["name"])
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
