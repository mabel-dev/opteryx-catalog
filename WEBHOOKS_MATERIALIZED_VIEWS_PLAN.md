# Webhooks + Materialized Views — Implementation Plan

Status: draft. **Phases 2, 4 and 6 are withdrawn — see "Revision: subscriptions are
triggers" below.** Phases 1, 3 and 5 stand.
Owners: catalog (this repo), subscriber.opteryx.app (new)
Out of scope: C(R)TAS refresh mechanics — owned by another agent/workstream. This plan treats
"run the defining query and (re)populate the target" as an external capability it calls, not
something it designs.

## Context

The webhook system in `opteryx_catalog/webhooks/` is ~8 months old and was never completed or
delivered — it fires on dataset/view **create** only, has no delivery-status tracking, and was
designed before the permissions system (policy.opteryx) established the per-workspace,
pattern-matched Firestore convention this plan now follows. `router.opteryx.app`, mentioned in
the webhook README as the delivery target, was never built and has since been deliberately
dropped — confirming this is unfinished work being picked back up, not a working system being
extended.

This plan finishes it, and builds the new pieces needed to support materialized views as a
consumer of it:

1. Complete the catalog's CUD event coverage and give webhook subscriptions a real home in
   Firestore, following the policy.opteryx pattern.
2. Add `materialized_view` as a third catalog resource type.
3. Stand up two new services: **webhooks.opteryx.app** (subscription management) and
   **subscriber.opteryx.app** (event execution).
4. Extend web.opteryx's existing "manage" pages with a webhook-management surface.

## Naming

- Third resource type: **`materialized_view`**.
- Management API (CRUD for webhook subscriptions): **webhooks.opteryx.app**.
- Event listener/executor (receives Cloud Tasks pushes, performs the reaction):
  **subscriber.opteryx.app**. (Alternative if you want something more action-flavored:
  `reactor.opteryx.app` — not pushing this, `subscriber` reads fine and pairs naturally
  with `webhooks`.)

## Architecture overview

```
catalog mutation (any client: xb500, jobs.opteryx, worker.opteryx, ad-hoc script, ...)
   |
   v
opteryx_catalog emits event -----> Cloud Tasks queue -----> subscriber.opteryx.app
   (send_webhook, all CUD paths)                             (executes the reaction,
                                                                e.g. re-run the MV's
                                                                defining query)
   ^
   |
webhook subscription lookup ($hooks collection, pattern-matched to workspace + resource)
   ^
   |
webhooks.opteryx.app (user-facing CRUD: create/list/delete subscriptions, view last-enqueued status)
   ^
   |
web.opteryx UI (new page under static/app/, added to existing manage/settings surface)
```

Key point from the discussion: **xb500.opteryx does not receive webhooks.** It's a catalog
client like any other — its housekeeping jobs (compaction, expiration, backfills) perform normal
catalog CRUD, and those mutations fire webhooks the same way any other client's would. No
special-casing needed for it anywhere in this plan.

**No router.** Cloud Tasks pushes directly to the subscriber's HTTP endpoint (OIDC-authenticated),
the same pattern jobs.opteryx already uses against worker.opteryx
(`jobs.opteryx/app/routes/v1/interface.py:371` → `worker.opteryx/app/routes/v1/interface.py`
`POST /api/v1/submit`). That pair is the concrete template for the Cloud Tasks producer/consumer
wiring in Phase 3.

## Phase 1 — Catalog: resource type + event completeness

Repo: `opteryx-catalog`.

1. Introduce a real `ResourceType` (today `"dataset"` / `"view"` / `"collection"` are bare
   strings repeated at each call site — `opteryx_catalog/opteryx_catalog.py:293,540,750,850` and
   `opteryx_catalog/audit.py:102`). Add `materialized_view` as the fourth value. This is the
   forcing function to stop hand-repeating strings — touch every existing call site once while
   adding the new one.
2. Store a materialized view as a normal dataset document (same schema/location fields as any
   other dataset — this is what "treated as tables/datasets" means structurally) plus three
   extra fields: `defining_sql`, `source_pattern` (what it should be refreshed in reaction to),
   `last_refreshed_at`.
3. Wire `send_webhook` into the CUD paths that currently skip it:
   - `update_dataset_description` (`opteryx_catalog.py:951`)
   - `update_view_execution_metadata` (`opteryx_catalog.py:903`)
   - `update_view_description` (`opteryx_catalog.py:925`)
   - `create_collection` (`opteryx_catalog.py:602-638`) — `collection_created_payload()` already
     exists unused at `webhooks/events.py:110`
   - **Commit events**: `save_snapshot` / `write_parquet_manifest` — `dataset_commit_payload()`
     already exists unused at `webhooks/events.py:85`. This is the one that actually matters for
     materialized views: it's the "this dataset's content changed" signal a refresh reacts to.
     Without it there is nothing to trigger a refresh on.
4. No changes needed to xb500's log-routing (`xb500.opteryx/app/operations/__init__.py`,
   `is_catalog_change_record`) — it keys off `event == "catalog.mutation"` and passes
   `resource_type` through untouched, so `materialized_view` flows through existing audit/log
   partitioning automatically.

## Phase 2 — WITHDRAWN: a subscription is a trigger

This phase proposed a `$hooks/subscriptions` collection mirroring `$policies/access`, with
`pattern` / `resource_types` / `actions` / `target` fields. It is a re-implementation of
machinery this repo already has.

A trigger is already "when this dataset takes a commit, enqueue the reaction described by
`kind`" (`create_trigger`, `opteryx_catalog.py:3129`), `kind` is already a discriminator with
two values dispatched at `trigger_firing.py:604` (`materialized_view_refresh`, `task`), and the
target field is already selected by kind — "one of them None — so a reader never has to know
the kind to find the field" (`opteryx_catalog.py:3153`).

Outbound delivery is a **third kind**, not a second subscription system:

```
kind: "http_endpoint"     target_secret: <workspace>.<secret name>
```

It inherits `CREATE TRIGGER` / `DROP TRIGGER` / `ALTER TRIGGER … OWNER TO`, the cycle check,
the writer-on-the-dataset permission gate, and the existing firing path — all of it already
reachable from SQL, which is where catalog objects are managed.

`last_enqueued_at` survives the withdrawal as a field on the trigger document; the reasoning
was right. Cloud Tasks owns retry/delivery from the point of enqueue, so the catalog can only
truthfully report "attempted," not "delivered." Delivery success/failure visibility, if wanted
later, is a Cloud Tasks dashboard/DLQ concern, not something to fake here.

**The target is a secret name, never a URL.** For a Slack incoming webhook the URL *is* the
credential, so a subscription document holding a plaintext `target` URL — served back by a list
endpoint — is a stored credential in the clear. See `jobs.opteryx/docs/design/secrets.md`, which
owns the storage, encryption and dispatch of the referenced secret. This also closes this plan's
open item about arbitrary external URLs as targets: the question does not arise.

**What is lost, and why that is acceptable:**

- *Wildcard subscriptions.* `$hooks` patterns would have covered datasets that do not exist
  yet. Triggers are per-dataset and explicit. For permissions a pattern is right; for
  reactions it is the opposite — a wildcard that starts POSTing data to an external endpoint
  because someone created a dataset matching a glob is a surprise nobody asked for.
- *Metadata events.* Triggers fire on user-created data commits, not on `create_collection` or
  `update_dataset_description`. Phase 1 still wires `send_webhook` into those paths for the
  audit/event stream; what changes is that no subscription reacts to them yet. Phase 1 itself
  says the commit event "is the one that actually matters for materialized views." If a
  metadata reaction is wanted later, it is another trigger kind, not a parallel system.

## Phase 3 — Catalog: Cloud Tasks as the only delivery path

Repo: `opteryx-catalog`.

`WebhookManager.send()` (`opteryx_catalog/webhooks/__init__.py:109-113`) already branches to
`_send_via_cloud_tasks` (lines 165-205) when `OPTERYX_WEBHOOK_QUEUE` is set, falling back to
synchronous `_send_direct` (lines 140-163) otherwise. Per the direction that Cloud Tasks is how
delivery becomes async/non-blocking: make the Cloud Tasks path mandatory in any deployed
environment — `OPTERYX_WEBHOOK_DOMAIN` alone should no longer be sufficient to enable webhooks;
`OPTERYX_WEBHOOK_QUEUE` becomes required config wherever this runs for real. Keep `_send_direct`
only if local dev needs a no-emulator fallback — otherwise delete it.

## Phase 4 — WITHDRAWN: webhooks.opteryx.app (new service)

Withdrawn with Phase 2. Subscriptions are triggers, and triggers are created, listed and
dropped in SQL; there is no CRUD surface left for this service to own. Audit entries for
subscription mutations come from the catalog's existing trigger paths and
`catalog_audit_log_contract`, as they already do for the other two kinds.

The original text follows for reference.

### (withdrawn) webhooks.opteryx.app

New repo, following the existing service shape (Dockerfile/Makefile/pyproject.toml, FastAPI —
same skeleton as `policy.opteryx` or `worker.opteryx`).

Responsibilities:
- CRUD for `$hooks` subscriptions (create/list/delete), workspace-scoped, owner/admin-gated the
  same way `policy.opteryx/app/routes/v1/access.py` gates policy mutations.
- Emits audit-log-shaped entries for subscription mutations, per the existing
  `catalog_audit_log_contract` convention (`severity: "AUDIT"`, kept off Cloud Logging's real
  severity enum so it stays out of the general log stream — see xb500's `is_audit_record`).
- Read-only status view: last-enqueued time per subscription.

Not responsible for: creating materialized views themselves (that's a normal SQL statement —
CTAS today, possibly `CREATE MATERIALIZED VIEW` syntax later — executed through the normal query
path, not a REST endpoint on this service).

## Phase 5 — subscriber.opteryx.app (new service)

New repo, same shape. Single job: receive the Cloud Tasks push, look up which materialized
view(s) the triggering event's pattern/resource matches, and invoke the refresh capability
(external dependency — the C(R)TAS work owned elsewhere) against opteryx. Update
`last_refreshed_at` on success.

Deliberately not folded into xb500: xb500 is explicitly "not end-user facing" housekeeping and,
per the discussion, is a webhook **emitter** via its normal catalog CRUD, not a receiver — giving
it a second, unrelated responsibility (receiving pushes and executing arbitrary reactions) would
blur that boundary rather than reuse it.

## Phase 6 — WITHDRAWN: web.opteryx UI

Withdrawn with Phase 4 — it was a page for that service's API. The Studio manages triggers the
way it manages every other catalog object: by running SQL.

The original text follows for reference.

### (withdrawn) web.opteryx UI

Repo: `web.opteryx`.

Closest existing template: `static/app/permissions.html` — workspace-scoped, two-column
`.settings-layout`, `PolicyApi`-style client hitting a per-service `*_BASE_URL`
(`window.OPTERYX_CONFIG.POLICY_BASE_URL` today). Add a new page the same way, hitting a new
`WEBHOOKS_BASE_URL` pointed at webhooks.opteryx.app. No redesign — copy the structure.

Wiring points:
- New menu entry in `static/app/index.html`, `#user-menu-dropdown` (lines 243-332), sibling to
  `permissions-btn`.
- Click-handler in `static/js/app.js:2431-2470`, copying the `permissions-btn` pattern exactly
  (owner-only visibility check via `window.opteryx_policies`, full-page nav fallback).

## Revision: subscriptions are triggers

Phases 2, 4 and 6 are withdrawn. What they proposed — a subscription store, a service to CRUD
it, and a page to drive that service — is the trigger machinery this repo already has, reached
through DDL that already exists. Outbound delivery becomes `kind: "http_endpoint"` on a trigger,
with the destination held as a secret rather than as a plaintext URL.

That removes one new service, one new Firestore convention, one web page, and the open question
about external URLs as targets, and it leaves the plan as three phases of work in one repo plus
one new consumer service.

## Dependency order

Phase 1 (events) and the `http_endpoint` trigger kind can proceed together — both catalog-side,
no external dependents.
Phase 3 (Cloud Tasks mandatory) can land independently at any point.
Phase 5 (subscriber.opteryx.app) can be scaffolded in parallel, but can't be exercised
end-to-end until Phase 1's commit-event wiring exists and the refresh capability (external) is
ready to be called.

## Open items

- Whether `_send_direct` is deleted outright or kept behind a dev-only flag (Phase 3).
- Exact shape of the "look up which materialized view(s) match this event" step in
  subscriber.opteryx.app — depends on how source_pattern/materialized_view metadata ends up
  structured in Phase 1, point 2.
- The SQL spelling for an outbound trigger. `CREATE TRIGGER <name> ON <dataset> CALL SECRET
  <name>` reads closest to the existing forms, but it is not designed here.
- Whether MV refresh subscriptions were ever going to need patterns. The withdrawal assumes not:
  a materialized view names its source explicitly, so a per-dataset trigger is exactly right.
