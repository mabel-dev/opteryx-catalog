# Webhooks + Materialized Views — Implementation Plan

Status: draft, awaiting review
Owners: catalog (this repo), webhooks.opteryx.app (new), subscriber.opteryx.app (new), web.opteryx
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

## Phase 2 — Catalog: webhook subscription storage

Repo: `opteryx-catalog`.

Mirror policy.opteryx's Firestore shape (`policy.opteryx/app/routes/v1/access.py:92-94`,
`db.collection(workspace).document("$policies").collection("access")`) into a parallel `$hooks`
collection:

```
db.collection(workspace).document("$hooks").collection("subscriptions")
```

Each subscription document:

```
{
  pattern: str,            # fnmatch pattern over collection.dataset, same semantics as policies
  resource_types: [str],   # e.g. ["dataset"], or ["materialized_view"] for reflection-specific hooks
  actions: [str],          # e.g. ["commit"], ["create", "delete"]
  target: str,             # subscriber.opteryx.app URL (or, later, an arbitrary external URL)
  created_by: str,
  last_enqueued_at: timestamp | None,
}
```

`last_enqueued_at` is the honest field to track — Cloud Tasks owns retry/delivery from the point
of enqueue, so the catalog can only truthfully report "attempted," not "delivered." (Delivery
success/failure visibility, if wanted later, is a Cloud Tasks dashboard/DLQ concern, not something
to fake here.)

## Phase 3 — Catalog: Cloud Tasks as the only delivery path

Repo: `opteryx-catalog`.

`WebhookManager.send()` (`opteryx_catalog/webhooks/__init__.py:109-113`) already branches to
`_send_via_cloud_tasks` (lines 165-205) when `OPTERYX_WEBHOOK_QUEUE` is set, falling back to
synchronous `_send_direct` (lines 140-163) otherwise. Per the direction that Cloud Tasks is how
delivery becomes async/non-blocking: make the Cloud Tasks path mandatory in any deployed
environment — `OPTERYX_WEBHOOK_DOMAIN` alone should no longer be sufficient to enable webhooks;
`OPTERYX_WEBHOOK_QUEUE` becomes required config wherever this runs for real. Keep `_send_direct`
only if local dev needs a no-emulator fallback — otherwise delete it.

## Phase 4 — webhooks.opteryx.app (new service)

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

## Phase 6 — web.opteryx UI

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

## Dependency order

Phase 1 and Phase 2 can proceed together (both catalog-side, no external dependents yet).
Phase 3 (Cloud Tasks mandatory) can land independently at any point.
Phase 4 and Phase 5 (the two new services) can be scaffolded in parallel with Phase 1/2, but
can't be exercised end-to-end until Phase 1's commit-event wiring exists and the refresh
capability (external) is ready to be called.
Phase 6 depends on Phase 4's API existing.

## Open items

- Whether `_send_direct` is deleted outright or kept behind a dev-only flag (Phase 3).
- Whether webhooks.opteryx.app subscriptions support arbitrary external URLs as `target`, or are
  restricted to subscriber.opteryx.app only for v1 (recommend restricting to v1 — generalize only
  if a real second consumer shows up).
- Exact shape of the "look up which materialized view(s) match this event" step in
  subscriber.opteryx.app — depends on how source_pattern/materialized_view metadata ends up
  structured in Phase 1, point 2.
