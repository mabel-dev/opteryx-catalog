# Materialized Views via Table Triggers — Implementation Plan

Status: approved — decisions settled 2026-08-06.
Delivered: Phase 1-2 (catalog storage/API + commit-time firing), Phase 3
(engine CREATE/DROP MATERIALIZED VIEW, incl. LocalStoreConnector sidecar for
GCP-free tests), Phase 4 (DROP TRIGGER / SHOW TRIGGERS FOR &lt;table&gt; /
information_schema.triggers), Phase 5 (jobs recent-queries filter + worker
refresh stamping), Phase 6 docs. Pending: the end-to-end emulator test, and
deployment: worker-dispatch queue IAM (`cloudtasks.enqueuer`) for committing
identities, deploy updated worker/jobs, release + pin a new opteryx-catalog
(services pin `>=0.4.78`; triggers first shipped in 0.4.81).

`TASKS_OIDC_SA` is **not** a setting to deploy (2026-08-09). Every Opteryx
Cloud Run service runs as `762690895289-compute@developer.gserviceaccount.com`,
whose numeric id is the `GPC_SUBJECT` the worker pins - so the account to mint
for is always "whoever this process already is", read from the metadata server
(`trigger_firing._oidc_service_account`). The env var survives only as an
escape hatch. No token now means a hard failure into the audit log rather than
a task the worker 401s and Cloud Tasks silently retries into expiry.

Live E2E (2026-08-06, dev/mabeldev): registration → trigger → commit →
job-doc write all verified against the real catalog; enqueue 403'd on queue
IAM and failed loudly (alert + trigger.fire_failed audit) as designed. Test
artifacts left in `opteryx.tests_temp.mv_e2e_*`; a sample trigger also sits
on `personal/bastian/create`.
Supersedes: the materialized-view portions of `WEBHOOKS_MATERIALIZED_VIEWS_PLAN.md` (the
`$hooks` pattern-matched subscriptions, the subscriber.opteryx.app service, and the
webhooks.opteryx.app management surface are **not** used by this design — triggers are
per-dataset documents and the executor is the existing worker.opteryx). The webhook-completion
work in that doc remains valid as a separate, unrelated workstream.

## Design summary

```
CREATE MATERIALIZED VIEW ws.coll.mv AS SELECT ... FROM ws.src.a JOIN ws.src.b ...
   |
   |  (opteryx-core: CTAS the backing table, then register in catalog)
   v
catalog: mv stored as a dataset document (readable as a table by the FE, no engine
         changes needed to query it) + defining SQL in its statement subcollection
         + one TRIGGER document under EACH referenced source dataset:
           {ws}/{coll}/datasets/{a}/triggers/{trigger-name}
   |
   |  (later: any user-created data commit on src.a or src.b)
   v
catalog commit path reads the dataset's triggers subcollection
   → writes a jobs/{execution_id} document (REFRESH statement, principal, origin: "trigger")
   → enqueues a Cloud Task (OIDC) targeting https://worker.opteryx.app/api/v1/submit
   |
   v
worker.opteryx executes the REFRESH exactly like any other job — the engine's binder
enforces permissions from the job's policies; if the principal lacks rights the plan
fails and the MV goes stale (visibly, via last-refreshed-at)
   |
jobs.opteryx /jobs/recent filters origin == "trigger" → never shown as a recent query
```

Key properties:
- **No new services.** worker.opteryx is the executor; jobs.opteryx needs a one-filter change.
- **The MV is just a dataset.** The FE and engine read it like any table; only creation,
  refresh, and drop know it's special.
- **Refresh is `REFRESH MATERIALIZED VIEW`**, which desugars at plan time to the CoRTAS it
  always was — `CREATE OR REPLACE TABLE ... AS SELECT`, already atomic: files written
  durably first, then a single `truncate_and_add_files` snapshot commit
  (`opteryx-core/opteryx/operators/insert/insert.pyx:70-105`,
  `opteryx-core/opteryx/connectors/opteryx_connector.py:852-861`). The statement is the
  honest name for what happens; the desugar keeps the proven write path. **A materialized
  view is not a table**: a user-written CTAS, INSERT, TRUNCATE or ALTER TABLE aimed at one
  is refused at bind time, so REFRESH (and the CREATE that made it) are the only writes that
  can land on a view.

## 1. Statement surface (opteryx-core)

### CREATE MATERIALIZED VIEW — parser already done, planner gated

sqlparser 0.62 already parses it: `plan_create_view` receives `materialized: true` and
currently rejects it at `opteryx/planner/logical_planner/logical_planner.py:1791-1801`.
Lift that gate into a new path:

- When `materialized=True`, do **not** build a plain CreateView node. Instead delegate to
  the CTAS machinery: `_plan_ctas` (`logical_planner.py:2204-2234`) with
  `relation_name = view_name`, reusing the Insert node with `create_target=True` /
  `or_replace`. The serial engine's InsertNode-head special case
  (`managers/execution/serial_engine.py:105-132`) then executes the SELECT and writes the
  backing table unchanged.
- MV registration (defining SQL, source tables, trigger creation) happens at the **end of
  the insert operator's catalog mutation**, not in a second plan node — the serial engine
  assumes InsertNode is the plan head, so don't stack a DDL node above it. Add an
  `is_materialized_view` flag to the Insert node; when set, `opteryx_connector`'s
  create/replace path additionally calls the new catalog API (§2) with the defining SQL
  (re-rendered via `sqloxide.ast_to_sql`, same trick as
  `operators/view_management/view_management.pyx:81-104`) and the list of referenced tables.
- **Referenced-table extraction**: the bound SELECT subtree already knows every relation it
  scans — collect fully-qualified names from the bound scan nodes rather than re-parsing the
  SQL. Only 3-part catalog-resident names get triggers; a reference to `$planets`,
  `information_schema.*`, or a non-catalog source is an error for an MV (nothing can ever
  fire its refresh).
- `CREATE OR REPLACE MATERIALIZED VIEW` follows CoRTAS semantics (below, permissions §4).

### DROP MATERIALIZED VIEW — parses today, dies in plan_drop

`{"Drop": {"object_type": "MaterializedView"}}` parses fine and hits the else-branch at
`logical_planner.py:2201`. Add a `"MaterializedView"` arm to `plan_drop`
(`logical_planner.py:2114-2201`) → `DropRelation`-shaped node with an MV flag. Execution
drops the backing dataset **and removes this MV's trigger documents from every source
dataset** (catalog API, §2). Plain `DROP TABLE` against an MV should be rejected with a
pointer to `DROP MATERIALIZED VIEW` (and vice versa for plain tables) — `locate_object`-style
type guard, same pattern as `drop_view`'s View check
(`view_management.pyx:106-129`).

### DROP TRIGGER — parser gap, use the pre-parse intercept pattern

`OpteryxDialect` is not in sqlparser's `dialect_of!` allowlist for trigger statements
(`parser/mod.rs:6136-6141`), so `DROP TRIGGER` fails to parse. Don't fork the Rust: follow
`_intercept_drop_statistics` (`opteryx/planner/__init__.py:196-219`), which regex-matches the
raw SQL and synthesizes the AST node directly. Synthesize
`[{"DropTrigger": {"trigger_name", "table_name", "if_exists"}}]`, then:

- `"DropTrigger": plan_drop_trigger` in `QUERY_BUILDERS` (`logical_planner.py:2642-2664`);
  new `LogicalPlanStepType.DropTrigger`.
- Syntax: `DROP TRIGGER <name> ON <table>` (table required — trigger names are only unique
  per dataset, and it makes the permission target explicit). Reject CASCADE/RESTRICT the
  same way `plan_drop` rejects them for everything (`logical_planner.py:2124-2136`).
- Binder visitor is **mandatory** — `binder/common.py:103-113` raises
  `InvalidInternalStateError` for unbound node types, precisely so DDL can't ship
  unauthorized. `visit_drop_trigger` checks `can_perform_action(..., table, action="WRITE")`
  (§4).
- Physical: new action on the View Management operator or a small sibling; registration in
  `operators/catalog.py:429-465`.

Note: dropping an MV's auto-created trigger orphans the MV (it stops refreshing but stays
queryable and keeps `last-refreshed-at` honest). That's allowed — it's the documented way to
"pause" an MV — but `information_schema.triggers` must make the situation visible.

`CREATE TRIGGER` (user-defined, arbitrary) is **out of scope** — triggers exist only as the
automatic artifact of `CREATE MATERIALIZED VIEW` in v1. The storage model doesn't preclude
adding it later.

### SHOW TRIGGERS + information_schema.triggers — do both, they're cheap

- `SHOW TRIGGERS` already parses as `ShowVariable` with variable `TRIGGERS` — add an arm in
  `plan_show_variables` (`logical_planner.py:1691-1747`) desugaring to
  `_plan_virtual_dataset_scan`, the same shape as `SHOW USER` / `SHOW GRANTS`
  (`logical_planner.py:1624-1653`).
- `information_schema.triggers`: copy `InformationSchemaViewsTable`
  (`opteryx/connectors/information_schema.py:593-688`) — new class + one entry in
  `_TABLE_CLASSES` (`information_schema.py:756-761`). Columns:
  `trigger_catalog, trigger_collection, trigger_name, event_object_table (the source),
  action_statement (the CoRTAS), target_view, created_by, created_at, last_fired_at,
  last_fired_status`. Row-level security identical to the views table: per-row
  `can_perform_action(..., READ)` on the **source table**, deny-all without an execution
  context (`information_schema.py:67-79`).

## 2. Catalog storage (this repo)

### The MV document

An MV is a normal dataset document (`{ws}/{coll}/datasets/{name}` —
`opteryx_catalog.py:320-321`) so it gets `location`, schemas, snapshots, and FE readability
for free. Additions, following the repo's kebab-case field convention:

- `dataset-type: "materialized_view"` — and introduce the `ResourceType` enum while at it
  (the plan-doc Phase 1 item stands: bare strings today at `opteryx_catalog.py:293,540,750,850`,
  `audit.py:102`).
- `statement-id` + a `statement` subcollection for the defining SQL, exactly as views do
  (`opteryx_catalog.py:1467-1477`) — versioned, per-version `author`, so "which SQL produced
  this refresh" is answerable.
- `source-tables: [str]` — fully-qualified, as extracted at creation.
- `last-refreshed-at`, `last-refresh-status`, `last-refresh-execution-id` — stamped by the
  refresh job. (`refresh-frequency-mins` already exists and stays inert/optional — a later
  scheduled-refresh fallback can consume it without schema change.)

New catalog API: `create_materialized_view(identifier, sql, source_tables, author, ...)`,
`drop_materialized_view(identifier, author)` (drops dataset + removes its triggers from all
source datasets), `list/load` variants.

### The triggers subcollection

`{ws}/{coll}/datasets/{src}/triggers/{trigger-name}` — sits beside the existing
`snapshots`, `schemas`, and `maintenance` subcollections. The best template is the
quarantine subcollection (`catalog/orphan_quarantine.py:128-153`), chosen for the same
reason its docstring gives: `load_dataset` reads the dataset document on every call, so
opt-in state must live in a subcollection, not on the doc.

Trigger document:

```
{
  name: str,               # doc id; auto-generated: "refresh__{view_coll}__{view_name}"
  kind: "materialized_view_refresh",
  target-view: str,        # fully-qualified MV identifier
  statement-id: str,       # which SQL version to run (re-read at fire time from the MV)
  created-by: str,         # author who created the MV
  created-at: timestamp,
  last-fired-at: timestamp | None,
  last-fired-status: str | None,
}
```

Firestore does not cascade deletes (`opteryx_catalog.py:346-354`), so:
- `drop_dataset` (`opteryx_catalog.py:734-736`) must delete the `triggers` subcollection —
  and if the dropped dataset is a **source** of MVs, those triggers die with it (their MVs
  go stale-but-queryable; visible in information_schema).
- `rename_dataset` (`opteryx_catalog.py:926-947`) copies `schemas`/`snapshots` today and
  must copy `triggers` too. Renaming a *source* also strands the `source-tables` entries on
  its MVs — v1: reject renaming a dataset that has refresh triggers attached, rather than
  chasing the graph.

New API: `create_trigger`, `drop_trigger`, `list_triggers(dataset)` — all requiring
`author`, all emitting audit (`audit.py:99-140`) with the new resource type.

## 3. Firing (this repo → worker.opteryx)

### Hook site

The five real commit sites are in `catalog/dataset.py` — `append` (:531-534), `overwrite`
(:828-831), `add_files` (:985-988), `truncate_and_add_files` (:1166-1169), `truncate`
(:1862-1865). Factor a `_after_commit()` beside the existing `_emit_audit` helper
(`dataset.py:637-650`) and call it from all five. **Only fire for user-created snapshots**
(`Snapshot.user_created`, set at `dataset.py:518`, authoritative per `dataset.py:295`) —
`refresh_manifest`, compaction, and expiration also land snapshots and must not re-run every
MV on every housekeeping pass.

`_after_commit` reads the dataset's `triggers` subcollection (one keyed read — this is why
per-dataset beats the old plan's workspace-wide `$hooks` query) and enqueues one refresh per
trigger. Debounce within the call: a multi-trigger commit fires each distinct MV once.

**MV-over-MV / loops**: a refresh commit on an MV's backing table is itself a user-created
commit, so MVs stacking on MVs works mechanically, and it is **allowed**. The outer view is
a refresh behind the inner one and a failed inner refresh pins the tower at its last good
data — both visible in `last-refresh-status`, not silent. Loops are what is rejected, at
**creation** time (walk `source-tables` of any source that is itself an MV) — cheaper and
more debuggable than runtime loop detection. Creation-time checking cannot see two
registrations racing to close a loop from opposite ends, so `fsck` re-walks the graph and
reports `mv-source-cycle`.

### Enqueue mechanics — copy jobs.opteryx, not the webhook sender

The existing `_send_via_cloud_tasks` (`webhooks/__init__.py:165-205`) is **not** reusable
as-is: it sends no OIDC token (worker rejects unauthenticated pushes), hardcodes a single
`https://{domain}/event` URL, and swallows every exception. The working template is
`jobs.opteryx/app/routes/v1/interface.py:414-441`: `CloudTasksClient`, `queue_path(project,
location, queue)`, `tasks_v2.OidcToken(service_account_email=TASKS_OIDC_SA,
audience=target_url)`, POST body.

Critical integration facts (verified in worker.opteryx):
- **The worker ignores the task payload except `execution_id`** — it re-reads everything
  from the Firestore `jobs/{execution_id}` document
  (`worker.opteryx/app/worker.py:186-203`). So the catalog must **write a jobs document
  first**, then enqueue. Job doc fields the worker consumes: `statement` (the CoRTAS),
  `submitted_by`, `policies`, `entitlements`, `billing_account`, `parameters`, `status`.
- **Worker auth pins one OIDC subject** — `GPC_SUBJECT` hardcoded at
  `worker.opteryx/app/auth.py:14-50` and duplicated at `routes/v1/interface.py:19`, with
  audience exactly `https://worker.opteryx.app/api/v1/submit`. The catalog's enqueue must
  mint OIDC for that same service account (or worker's subject check becomes a small
  allowlist). Deployment decision, not code complexity.
  (Pre-existing issue, noted in passing: `validate_token` uses `get_unverified_claims` —
  the signature is never actually verified. Worth fixing while touching this file.)
- The refresh statement is
  `CREATE OR REPLACE TABLE {backing} AS {defining_sql}` — plus stamping
  `last-refreshed-at` / `last-refresh-status` on the MV doc when the job completes
  (worker-side, keyed off a job-doc field, see below).

**Environment caveat**: commits happen wherever `opteryx_catalog` runs — worker, upload
service, xb500, or a laptop. Firing requires Firestore write access to `jobs` and Cloud
Tasks enqueue rights; environments without them must fail **loudly into the audit log**
(`audit.py:73-88` `write_audit_record`), never silently (the current webhook sender's
`except Exception: return False` is the anti-pattern). A missed fire = a silently stale MV.

### Identity, permissions at refresh time, and recent-queries suppression

The job document is where all three live:

- `submitted_by`: **the invoker** — the `author` of the commit that fired the trigger
  (decision 1). This is the field the worker passes as the engine session user and that
  gates every per-job endpoint.
- `policies`: the catalog can derive these itself — policy documents live in the same
  `catalogs` Firestore database at `{ws}/$policies/access`
  (`policy.opteryx/app/routes/v1/access.py:446`), so at fire time read the principal's
  current policies and write them onto the job doc, exactly the shape jobs.opteryx stores
  (`interface.py:301-302,361`). No token needed, no staleness: revoking someone's writer
  role stops their MVs refreshing at the next fire, which is the correct behavior — the
  engine's binder then denies the plan (`relation.py:357-370`) and the job fails visibly.
- `origin: "trigger"` (new field): jobs.opteryx's `/jobs/recent` is a single
  `submitted_by == sub` query (`interface.py:1166-1206`) — add a client-side skip of
  `origin == "trigger"` docs in **both** result loops (`:1207-1233` unfiltered, `:1259-1281`
  filtered search). Client-side, not a `.where()`, to avoid a composite index and
  legacy-doc-missing-field exclusion. `QueryJob` has `extra: "ignore"` so the new field is
  harmless (`app/models/v1.py:264+`). The same field is how the worker knows to stamp
  `last-refreshed-at` on completion.

## 4. Permissions (creation time)

Enforced where all DDL is enforced — the opteryx-core **binder**, from the session's
`access_policies` (`managers/permissions/__init__.py`; WRITE tier = writer/owner,
`ACTION_MAP` at `:12-32`):

- **Each referenced source table: `WRITE`** — creating a trigger is an update to that table
  (per direction). New check in the MV creation path over the extracted source list.
- **The MV target: owner** (decision 2) — the MV creation path checks the `DROP` tier on
  the target regardless of whether it exists yet (stricter than the CTAS default of
  `CREATE` for fresh targets, `binder/relation.py:366-370`; the replace path is already
  owner-only at `binder/relation.py:357-364`). Refresh CoRTAS inherits the same tier
  unmodified — a non-owner invoker's fire is denied, by design.
- **`DROP TRIGGER ... ON table`: `WRITE` on that table** (symmetric with creation — it's an
  update to that table). `DROP MATERIALIZED VIEW`: `DROP` tier on the MV itself, consistent
  with `DROP TABLE`.

### The egress lock — `egress_protection` (not a permission)

Granting `reader` on `ichnos.landing.*` transitively means "may copy all of landing
anywhere I can write, permanently, on a refresh schedule". Nobody means that when they
grant read. `egress_protection` is an optional workspace property that refuses the automated
copy — MV creation, MV refresh, and CTAS — of that workspace's data **into a different
workspace**.

- **The source workspace's flag decides**, never the destination's. The property protects
  data leaving; a copy that stays inside the source workspace is not egress.
- **Enforced at creation and at every refresh.** A workspace can be locked long after a
  view that reads it was registered, and each refresh is a fresh copy.
- **On by default.** Tri-state: absent, null, and a workspace with no `$properties` document
  at all read as restricted; only an explicit `OFF` clears it (`ALTER WORKSPACE ichnos SET
  egress_protection TO OFF`). Sharing data out is opted into, never defaulted into, and a
  workspace nobody has thought about is not wide open. `deletion_protection` was moved to the
  same default-on tri-state in the same change, via the shared `_guard_is_on` helper.
  Still an **ordinary settable property**, deliberately NOT in
  `_RESERVED_WORKSPACE_PROPERTIES` — that set is for lifecycle fields with dedicated methods.
- **Named `*_protection`, uniformly.** `delete_protection` was renamed to
  `deletion_protection` in the same change so both workspace guards are noun-phrase
  protections with the same polarity. The point is scannability: every `..._protection`
  property is safe when ON, so a list of workspace settings can be read for `OFF` without
  reasoning about which direction each rule points.
- **Not containment, and must never be described as such.** Anyone with read can SELECT and
  paste. It is leaky by construction, the same way a VPC Service Controls perimeter is an
  egress boundary rather than a permission. What it stops is the systematic, automated,
  recurring copy, which is where the leakage volume actually is.

### Deployment: no migration

`delete_protection` → `deletion_protection` is read with no fallback, and needs none: no
workspace has ever had either protection set, so there is nothing stored under the old key
anywhere. The rename is code-only. Same for `egress_protection`, which is new.

This is why both defaults could be flipped to ON at all, and why it had to happen now: with
no stored values, "absent" is every workspace's state, so the default *is* the behaviour and
changing it costs nothing. Once anyone has set either flag deliberately, a change like this
stops being free — a later flip would silently override real decisions, and a later rename
would silently discard them.

Catalog surface (`opteryx_catalog.py`): `EGRESS_PROTECTION_PROPERTY`,
`is_egress_restricted(workspace=None)`, `enforce_egress_policy(source_workspaces,
destination_workspace, operation)` — the shared gate — and `enforce_materialized_view_egress`,
which resolves an MV's source workspaces and calls it. `EgressRestricted` is its own
exception type (not a `MaterializedViewError`) because CTAS raises it too, and it is
deliberately not `Alertable`: a blocked copy is the setting working. `enforce_` rather than
`_assert_` because both are called across a duck-typed catalog boundary, where
`unittest.mock` rejects any attribute named `assert*`.

A blocked refresh in `trigger_firing._fire_refresh` is checked before the job document is
written and surfaces exactly like any other fire failure — `_alert`, a `trigger.fire_failed`
audit record, `last-fired-status: egress-blocked` — and never raises into the commit.

**Guard first, feature second.** Cross-workspace MV sources are not representable yet — an
MV's sources are workspace-relative (`_relative_identifier`) and opteryx-core's
`register_materialized_view` rejects a cross-workspace source outright — but they are
planned, and cross-workspace MVs are a driver for this work. Landing the boundary ahead of
the capability is deliberate: because the flag defaults to ON, the day a view can read
another workspace it needs that workspace's owner to have opted out first, and there is no
release in which the capability ships ahead of the guard. When cross-workspace sources land,
`_relative_identifier` and `_source_workspace` should be revisited together — once a source
carries its workspace explicitly, resolution stops being a guess and the local-dataset probe
becomes a compatibility path for names stored under the old rule.

### Engine wiring (delivered, opteryx-core)

The load-bearing path is the write path, which lives in opteryx-core, and it is now wired:

- **`WORKSPACE_PROPERTIES`** (`planner/logical_planner/logical_planner.py`) — carries
  `egress_protection` with `_parse_boolean_workspace_property`. Without it the flag could not
  be cleared through SQL at all, which under default-on means no opt-out route exists.
- **`Writable.enforce_egress_policy(target_relation, source_relations)`**
  (`connectors/capabilities/writable.py`) — a **no-op by default**, not a
  `NotImplementedError` like its neighbours: a connector with no workspace concept has no
  boundary to cross, so "nothing to check" is the complete answer, and filesystem CTAS is
  untouched.
- **`OpteryxConnector.enforce_egress_policy`** — resolves relation names to workspaces, drops
  same-workspace sources before asking (so the common case costs no Firestore read), and
  translates the catalog's `EgressRestricted` into the engine's
  `EgressRestrictedError(SecurityError)`. Deliberately **not** a `PermissionsError`: the
  caller may hold every grant the statement needs; what is refused is the destination.
  **Fails closed** if the installed opteryx-catalog is too old to carry the gate — opteryx
  resolves opteryx-catalog from site-packages, not a sibling checkout, so version skew is
  real and a silently-unenforced control is worse than a refused copy.
- **`planner/binder/relation.py::visit_insert`** — calls it at bind time, after the target's
  permission check (so a caller who may not write the target cannot use this to probe another
  workspace's protection state) and before any schema work or write.

**Scope: every write, not just CTAS.** The check runs for CTAS, CREATE OR REPLACE, CREATE
MATERIALIZED VIEW *and* plain `INSERT ... SELECT`. Covering only CTAS would not be a
boundary — the same copy is two statements away (`CREATE TABLE mine.x AS SELECT ... LIMIT 0`,
then `INSERT INTO mine.x SELECT ...`), and a control with a two-statement bypass teaches
people to route around it. `INSERT ... VALUES` scans nothing and drops straight through.

**MV refresh needs no separate statement.** There is no `REFRESH MATERIALIZED VIEW`;
`_fire_refresh` submits `CREATE OR REPLACE TABLE <view> AS <sql>`, which reaches the binder
as an ordinary CTAS. So a refresh is checked twice on the same setting, in two processes: by
the catalog at fire time before the job document is written, and by the engine at bind time
in the worker. That redundancy is wanted for a flag that can be switched on long after the
view was created.

Remaining, not in scope here:

- **opteryx-core** — `opteryx_connector.register_materialized_view` still rejects a
  cross-workspace source with a flat `ValueError`. When cross-workspace MVs land, that
  rejection is replaced by the egress gate, not simply deleted.
- **odata.opteryx / flight.opteryx / upload.opteryx** — these are read/serve surfaces, not
  copy-into-a-workspace surfaces, so the lock does not apply to them as written. If any of
  them grows a "materialize this result into a workspace" path, it takes the same gate.
- **Release ordering** — the engine wiring requires an opteryx-catalog carrying
  `enforce_egress_policy` and `EgressRestricted`. Until that is released and installed, every
  cross-workspace write fails closed with the "catalog too old" message.

Compatible with, and deliberately not foreclosing, the open questions: definer's rights for
refresh (an identity question — orthogonal to whether the copy may cross the boundary at
all), and a `SECURE` flag marking a sanctioned declassification, which would be a documented
exemption checked inside `enforce_egress_policy`.

## 5. Delivery phases

1. **Catalog: storage + API** — `ResourceType` enum; MV dataset fields + statement
   subcollection reuse; `triggers` subcollection with create/drop/list; cascade handling in
   `drop_dataset`/`rename_dataset`; audit events. Testable standalone against the Firestore
   emulator.
2. **Catalog: firing** — `_after_commit` on the five commit sites (user-created snapshots
   only); jobs-doc write + OIDC Cloud Tasks enqueue (jobs.opteryx pattern); policy
   derivation from `$policies/access`; loud audit on enqueue failure.
3. **opteryx-core: CREATE/DROP MATERIALIZED VIEW** — lift the planner gate, CTAS
   delegation, insert-operator registration hook, referenced-table extraction, creation-time
   permission checks, cycle guard, plan_drop arm.
4. **opteryx-core: trigger visibility** — DROP TRIGGER (pre-parse intercept + binder
   visitor + operator action), SHOW TRIGGERS desugar, `information_schema.triggers`.
5. **jobs.opteryx + worker.opteryx** — `/jobs/recent` origin filter (both loops); worker
   OIDC subject allowlist (or shared SA) and completion-time MV stamping; (opportunistic:
   real signature verification in `auth.py`).
6. **Docs + slt tests** — `test_ddl_sql.py` / `test_ctas.py` siblings, an end-to-end
   emulator test: create MV → commit to source → assert job doc + task enqueued → simulate
   worker run → assert MV refreshed and absent from recent queries.

Phases 1-2 (this repo) and 3-4 (opteryx-core) can run in parallel; 5 is small and
independent; nothing end-to-end until 2+3 land.

## Decisions (settled 2026-08-06)

1. **Invoker semantics.** `submitted_by` = the committing user (the `author` at the commit
   site) — "we have their perms because they just invoked." Policies are derived for that
   principal at fire time from `{ws}/$policies/access`.
2. **Owner-only on the MV target, for both creation and refresh.** Creating an MV requires
   owner on the MV target; the CoRTAS-replace DROP tier stands unmodified for refresh.
   Recorded consequence (accepted, by design — "the work can be denied"): a source-table
   commit by a principal who is not an owner of the MV target produces a **denied refresh**
   — the job fails visibly, `last-refresh-status` records the denial, and the MV stays
   stale until an owner's commit fires it. The writer requirement applies to source tables
   only (trigger creation = update to that table).
3. **Refresh coalescing**: Cloud Tasks named-task dedup, ~60s window
   (`task_id = trigger + time-bucket`). `refresh-frequency-mins` stays inert for later.
4. **Worker auth**: no worker changes for authorization — worker.opteryx is the platform's
   main query worker and already holds all needed permissions; catalog enqueues use the
   same OIDC service account / audience the jobs.opteryx path uses.

Additional platform note: the username **`trigger` is reserved** (claimed at the platform
level) so no real user can hold it — system-originated job documents and audit lines can
reference it without impersonation risk or confusion with `personal.trigger.*` semantics.
