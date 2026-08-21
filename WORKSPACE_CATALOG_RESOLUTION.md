# Workspace Catalog Resolution

Moving the workspace → catalog binding out of `worker.py` and into the catalog store,
resolved at query time.

- **Status:** Proposed (design agreed; see companion plan `WORKSPACE_CATALOG_RESOLUTION_PLAN.md`)
- **Date:** 2026-08-20
- **Repos:** opteryx-core · opteryx-catalog · opteryx-iceberg · worker.opteryx · control.opteryx · odata.opteryx

## 0. Problem

Which catalog service backs a workspace is currently decided by hardcoded registration at
worker import (`worker.opteryx/app/worker.py:64-116`): the native Firestore/GCS catalog as
the per-query-refreshed default, plus two baked-in prefixes — `mabel_data` (line 86) and
`tarchia`, an external Iceberg REST catalog on Google BigLake (lines 96-106). The `tarchia`
registration is proven in production, but adding a second Iceberg workspace, rotating its
endpoint, or onboarding any customer-owned external catalog means editing Python and
redeploying every worker.

The target: the worker asks, per query, *"here is the workspace name — what kind of catalog
backs it, and what config and credentials do I need?"* — and the answer lives in data, not code.

Scope note: opteryx-core's workspace/connector machinery may be completely rewritten. The
only hard requirements carried forward are (1) local disk remains the terminal fallback and
(2) a global default connector can still be set.

## 1. Registry: where the binding lives

**Decision:** a `catalog` block on the workspace's existing `$properties` document in the
catalogs Firestore database. No block (or `kind: "native"`) means the native catalog — every
existing workspace is grandfathered with zero writes. Workspaces backed by an external
catalog get a *shell* `$properties` doc: the block plus the standard lifecycle fields, no
datasets.

Why here and not the alternatives:

- **A dedicated registry collection** would create a second source of truth for "does this
  workspace exist." The `$properties` doc already *is* the existence gate
  (`OpteryxCatalog.__init__` raises `WorkspaceNotFound` when it's absent), already carries
  lifecycle state (`locked-by`, `billing-account-id`, …), and is already deleted by
  `DROP WORKSPACE` — so binding lifecycle rides along for free.
- **control.opteryx's own `workspaces/{name}` doc** would couple the hot query path to the
  governance service's `(default)` database, which the worker doesn't touch today.
  Governance *writes* the binding (§6), but the engine reads it from the catalogs DB it
  already talks to.
- The "you must already know the workspace name to find its doc" property of `$properties`
  is exactly right for this use: query-time resolution always starts from a named workspace.
  There is deliberately still no global workspace enumeration on the query path.

### Schema

```jsonc
// <workspace>/$properties — new block alongside timestamp-ms,
// billing-account-id, owner, locked-by, locked-at-ms, …
"catalog": {
  "kind": "iceberg",            // allowlisted name, never a class path (§4)
  "config": {                   // arbitrary nested dict; passed through verbatim
    "catalog_type": "rest",
    "uri": "https://biglake.googleapis.com/iceberg/v1/restcatalog",
    "warehouse": "bl://projects/mabeldev/catalogs/opteryx-iceberg-tier1-test",
    "google_auth_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
    "header.x-goog-user-project": "mabeldev"
  },
  "auth": {
    "mode": "ambient",          // "ambient" (ADC — first-class, no secret) | "stored"
    "ciphertext": null,         // stored only: KMS-envelope-encrypted secret
    "kms-key": null,            // stored only: wrapping key resource name
    "inject-as": null           // stored only: config key (dotted path) that receives plaintext
  },
  "preserve-sql-case": false,
  "version": 1,                 // bumped on every write; drives cache invalidation (§3)
  "updated-at-ms": 1755648000000,
  "updated-by": "justin.joyce@joocer.com"
}
```

Constraints:

- `config` may be an **arbitrary nested dict**, stored and passed through verbatim. The old
  flat-hashable-kwargs discipline was purely an artifact of the kwargs-hash connector cache,
  which the core rewrite replaces with a cache keyed by workspace name (§3) — so
  opteryx-iceberg's flattened `auth_type`/`google_auth_scopes` shape is retired in favor of
  pyiceberg's native nested config (§6 change list).
- `config` must never contain `workspace`, `connector`, or `prefix` — `OpteryxConnector`
  injects `workspace=` itself. The resolver rejects entries carrying reserved keys rather
  than silently dropping them.
- `tarchia`'s case — ADC with per-request token refresh, no stored secret — is
  `mode: "ambient"`, a first-class mode, not a stored credential with an empty value.

The block above *is* `tarchia`'s entry verbatim — and `tarchia` is the only workspace
migrating. **`mabel_data` stays hardcoded (decided):** it has a single client who is already
migrating off the legacy mabel tables, so its registration will simply be deleted from
`worker.py` when that migration completes — it never becomes a registry kind.

**Billing for bound (shell) workspaces is the existing cost model, unchanged (decided):**
processing and query execution are charged exactly as for native workspaces; storage never
appears on the bill because nothing is stored. No new billing plumbing is needed for shell
workspaces.

## 2. Credentials

**Decision:** same mechanism the approved external-tables plan already fixed at dataset
level: KMS envelope encryption with ciphertext stored on the doc — not Secret Manager. The
worker decrypts at resolution time; plaintext exists only in-process. `mode: "ambient"`
stores nothing and means "authenticate as the worker's own identity" (ADC / attached
service account).

- The encrypt/decrypt implementation is the external-tables plan's Phase 1 module
  (`opteryx_catalog/security/kms.py`, optional `kms` extra) — one envelope helper serves
  both dataset-level and workspace-level ciphertext. Whichever plan lands first builds it;
  the other reuses it.
- `inject-as` names the config key the plaintext is delivered through — a dotted path now
  that configs are nested (e.g. `token`, or `auth.credential`). This keeps the registry
  schema agnostic to what each catalog kind calls its secret.
- Plaintext never appears in logs, telemetry, error messages, or Firestore. Resolution
  errors report the workspace name and kind, never config values.

> **Note:** opteryx-iceberg today only wires up Google ADC auth. With the flattening
> workaround retired (§3), `IcebergMetastore` forwards nested config to pyiceberg's catalog
> loader, which natively understands `token`/`credential` — so stored-credential Iceberg
> catalogs become configuration (`inject-as` pointing at the right config path) rather than
> new opteryx-iceberg code. Not a blocker for migrating `tarchia`, which is ambient.

## 3. Resolution flow

**Decision:** opteryx-core's workspace/connector machinery is **rewritten
resolution-first**: `connector_factory` becomes *resolve, then reuse-or-build*. A resolution
chain is asked "what backs this workspace?" on demand; connector instances are cached **by
workspace name** and validated by a **version compare** on every lookup. `register_workspace`
survives as sugar over a static table at the head of the chain; `set_default_connector`
remains the settable global default; local disk stays the terminal fallback. Two workarounds
cease to exist rather than being preserved: the flat-hashable-kwargs constraint (an artifact
of the old kwargs-hash cache key) and worker.py's per-query `register_default_connector()`
re-registration (an artifact of config frozen at import).

### The resolution chain

`connector_factory(dataset, …)` extracts the workspace segment and walks the chain; the
first answer wins. Each answer is a `Resolution` — `{connector, config, version}`, where
`config` is an ordinary nested dict (nothing hashes it any more):

1. **Static table** — entries from `register_workspace`. Kept for embedded users, tests, and
   `mabel_data`; during migration, a static entry shadows the registry.
2. **Installed resolver** — `set_workspace_resolver(fn)`; in production this is the worker's
   registry resolver, including its native-default branch (below).
3. **Static default** — `set_default_connector`, the explicit home for hard requirement 2.
   The worker doesn't use this slot: its resolver's no-binding branch *is* the default, with
   config re-read per call — which is what makes the per-query re-registration workaround
   unnecessary instead of replicated.
4. **Disk** — the local-filesystem connector, the explicit home for hard requirement 1 and
   the terminal branch, exactly as today.

### Cache and invalidation

`_connectors[workspace] = (version, instance)` — one entry per workspace. Resolution runs on
every lookup; if the returned `version` matches the cached one, the instance is reused,
otherwise it's rebuilt and *replaces* the entry, so rotated-config instances can't
accumulate. `version` is the binding doc's `version` field when the registry answered, and a
cheap fingerprint of the resolved config otherwise — so a plain config change (a new
`GCS_BUCKET`, say) rotates even the native connector on the next query, preserving the
config-goes-live-without-redeploy property the per-query re-registration existed to provide.
No cross-process signaling: every worker converges on its next query.

### Flow

```
planner: SELECT … FROM tarchia.interop_ns.people
  └─ connector_factory extracts workspace "tarchia", walks the chain
       1. static table: no entry (post-migration)
       2. installed resolver (worker.opteryx):
            one Firestore doc get: tarchia/$properties
            (database from get_config, resolved per call — config changes stay live)
            ├─ doc has catalog block → map kind through code-side allowlist,
            │    decrypt stored credential if any,
            │    return Resolution(connector, nested config, version=N)
            ├─ doc exists, no block → Resolution(OpteryxConnector, catalog=OpteryxCatalog,
            │    config re-read from get_config, version=config fingerprint)  # native default
            ├─ doc missing → same native default; OpteryxCatalog.__init__ raises
            │    WorkspaceNotFound, exactly as today
            └─ Firestore read fails / unknown kind / decrypt fails → RAISE (fail loudly)
  └─ _connectors["tarchia"]: cached version == resolved version → reuse;
       else build connector(workspace, config, telemetry) and replace the entry
  └─ OpteryxConnector._get_catalog("tarchia") →
       IcebergMetastore(workspace="tarchia", **config)   # config nested, passed as-is
```

### Why this shape

- **Resolution over registration:** the worker can't know which workspaces a query touches
  without parsing it; `connector_factory` is the exact point where the engine already knows.
  Only workspaces actually referenced pay the lookup, and nothing is frozen at import.
- **Per-call doc read, no TTL cache (v1):** one Firestore doc get per referenced workspace
  per query. There's direct precedent for this cost: `OpteryxConnector._get_catalog` already
  does a `$properties` liveness read on every cache hit (`opteryx_connector.py:608-623`),
  for the same reason — config must go live without a redeploy. Add a short TTL later only
  if measurement says so.
- **Nested configs end to end:** keying the cache by workspace name removes the only reason
  configs had to be flat and hashable.

### Failure modes

| Condition | Behavior |
|---|---|
| `$properties` unreadable (Firestore error) | Resolver raises `CatalogResolutionError`; the query fails. **Never** falls through to the default — that would silently route one workspace's query at another catalog. |
| Doc exists, no `catalog` block | The resolver's native-default branch: `OpteryxConnector` + `OpteryxCatalog` with config re-read from `get_config`. The normal case for every existing workspace. |
| Doc missing | Same native-default branch → `WorkspaceNotFound` from `OpteryxCatalog.__init__`. Today's behavior, preserved. |
| Unknown `kind` / reserved key in `config` | Resolver raises. A malformed entry must be loud, not quietly native. |
| KMS decrypt failure | Resolver raises with workspace + kind only; no config values, no ciphertext in the message. |

## 4. Security boundary

The worker executes user SQL, so the rule is: **a query can choose *which workspace*, never
*what a workspace means*.**

- The only query-controlled input to resolution is the relation's first identifier segment,
  already constrained by the engine's identifier rules. It selects a Firestore document by
  id — a point get, no query construction, no path traversal (the resolver additionally
  rejects segments containing `/` or leading `$`).
- `kind` is a name looked up in a **code-side allowlist in worker.py** — a one-entry table
  to start: `"iceberg"` → `OpteryxConnector` + `IcebergMetastore`. The registry never
  carries an import path or class name, so a compromised or malformed entry cannot make the
  worker load arbitrary code.
- Binding writes are governance-gated (§6): workspace-owner tier via control.opteryx, the
  same stricter tier the external-tables plan chose for credential capture. An entry's blast
  radius is its own workspace — it only affects queries that name it.
- **Decrypted credentials live in-process only (decided).** Plaintext in memory can only be
  pushed so far — deferring decryption into each metastore would buy little while spreading
  the KMS dependency across backends. After the core rewrite the exposure surface actually
  shrinks: cache keys are workspace names, so plaintext exists only in the resolved config
  held by the connector/catalog instance. Configs aren't logged today; the change list adds
  a guard-rail test that resolution logging/telemetry never includes config or auth values.

## 5. Discovery: permissions vs. listing

"How does anything know which datasets exist under a bound workspace?" splits into two
concerns that behave very differently.

### Enforcement needs no enumeration

opteryx-access grants are *patterns* — `workspace[.collection[.dataset]]`, with `*` covering
the whole subtree below it and the workspace segment always literal (`patterns.py:1-22`). A
check is a normalized `fnmatch` of the relation name the query actually used against the
stored patterns; `grants.py`'s module docstring explicitly disclaims resource-existence
checking ("not a permissions concern — call [the catalog] before `grant()`"). So the policy
layer never enumerates a catalog, native or external, and **externals are not limited to
`<workspace>.*` grants**: `tarchia.interop_ns.people` is a perfectly good fine-grained grant
today, enforced without the policy store ever knowing whether the table exists.
Workspace-subtree grants will be the common case (genesis policies are workspace-scoped
anyway), but that's convention, not a limitation this design imposes.

One consequence for control.opteryx: the "validate the resource exists before `grant()`"
courtesy pre-check that callers are told to perform can't use a Firestore dataset lookup for
a bound workspace. For v1, the pre-check should downgrade to a warning (or be skipped) when
the workspace carries a `catalog` block.

### Engine-side `information_schema` already routes correctly

`information_schema` is served per-workspace by `OpteryxConnector` itself, against whatever
catalog the connector resolved to — for an Iceberg-bound workspace that's `IcebergMetastore`,
whose `list_datasets(namespace)` wraps pyiceberg's `list_tables` (`metastore.py:112-115`).
So in-engine discovery flows through the same resolution path as reads and needs no extra
design — with one verification item: confirm Tier 1 exposes namespace enumeration (not just
per-namespace table listing) for whatever `information_schema` views need it.

### odata.opteryx is the real gap

**Decision:** bound workspaces publish lightweight *stub dataset docs* into their shell
workspace in Firestore — name-only projections of the external catalog's listing, marked
`external-catalog: true`, refreshed **only by an explicit, user-initiated sync** (the
control endpoint) — never automatically. When a failure smells like a stale listing (a
dataset present externally but absent from the stubs, or the reverse), the error
*recommends* running the refresh; a sync re-lists the external catalog and is
cost-impacting, so the system suggests and the user decides. odata keeps a single discovery
surface and gains no pyiceberg dependency, no external round-trips, and no credential access.

The gap: odata enumerates entity sets with a Firestore collection-group query over dataset
docs filtered by `workspace == X` (`service_document.py`, `_query_collection_group`). A
bound workspace has a shell `$properties` and zero dataset docs, so its datasets would be
invisible to the OData service document and `$metadata` even though the engine can query
them. Options considered:

- **Live enumeration in odata** — read the binding, call the external catalog per request.
  Rejected: it drags pyiceberg, KMS access, and external-catalog latency into a service that
  deliberately avoids even constructing catalog handles.
- **Stub projection docs** — chosen. Same philosophy as the external-tables plan's
  register-from-listing: Firestore holds a cheap, possibly-stale index; the external catalog
  stays authoritative for reads. Staleness is a listing-only concern — queries and
  permissions never consult the stubs.
- **Punt** — acceptable fallback for v1 if no OData consumer needs bound workspaces yet; the
  stub mechanism can land later without schema changes.

Stub docs carry only `workspace`/`collection`/`name`/`external-catalog: true` — no
snapshots, schemas, or sort orders. The marker keeps anything snapshot-hungry (compaction,
sweeps, `DatasetInfo` sort-order resolution) from treating a stub as a real dataset.
Permission filtering of the listing is unchanged: it pattern-matches the stub-derived
resource names exactly as it does native ones.

The marker is also the reconciler's delete guard: a sync only ever removes documents that
carry it, and never overwrites one that doesn't, so a projection cannot eat a dataset
document this catalog owns.

Because the refresh is user-initiated and cost-impacting, the binding block also carries
`listing-synced-at-ms` and `listing-count`, written by the same call that reconciles and
surfaced by control.opteryx's `GET …/catalog`. Stating the age of the list is the ONLY
automatic behaviour permitted here — without it, a refresh control has no "last refreshed"
beside it and gets pressed repeatedly just to find out whether it needed pressing, which is
the cost this decision was avoiding. Absent fields mean "never refreshed"; a binding write
replaces the block and so resets them, deliberately, since a rebind may point at an
entirely different catalog.

## 6. Per-repo change list

### opteryx-core — resolution-first rewrite of the connector layer

- `connectors/__init__.py` rewritten around §3's model: a `Resolution` type (`connector`,
  nested `config`, `version`); the four-slot chain (static table → installed resolver →
  static default → disk); `_connectors[workspace] = (version, instance)` replacing the
  kwargs-hash cache. `register_workspace` and `set_default_connector` survive as thin API
  compatibility over slots 1 and 3 — existing callers and tests keep working — and the disk
  fallback is the explicit terminal branch. `connector_factory`'s signature is unchanged for
  the planner; resolver exceptions propagate.
- `OpteryxConnector`: accept a nested config dict instead of flat kwargs (it already pops
  `connector`/`prefix` and injects `workspace=`; that carries over). Its per-hit
  `$properties` liveness re-check (`opteryx_connector.py:608-623`) stays — it guards a
  different thing (dropped workspaces) than the version compare (rotated config).
- No Firestore, no KMS, no catalog knowledge lands in core — the resolver is a callable,
  nothing more.

### opteryx-iceberg — retire the flattening workaround

- `IcebergMetastore` accepts pyiceberg's natural nested config and forwards it to the
  catalog loader, instead of rebuilding it from flat `auth_type`/`google_auth_scopes`/
  `header.*` kwargs. One caller, unpublished package — a clean break, with the README's
  flat-kwargs section rewritten.
- Free consequence: `token`/`credential` stored-credential auth needs no new code —
  pyiceberg understands those keys natively once config passes through (§2 note).

### opteryx-catalog — binding read/write + crypto

- New lightweight module (e.g. `opteryx_catalog/binding.py`):
  `read_catalog_binding(firestore_client, workspace) → Binding | None` and
  `write_catalog_binding(...)` / `clear_catalog_binding(...)`. Deliberately *not* methods on
  `OpteryxCatalog` — the resolver must not pay full handle construction (storage client,
  parquet-engine check, existence gating) for one doc read, and for shell workspaces there's
  no data plane to construct. Writers bump `version` atomically and validate reserved-key
  rules at write time, so malformed entries are rejected where the author can see the error.
- `write_catalog_binding` creates the shell `$properties` doc if missing (explicit
  provisioning, the `create_if_missing=True` spirit) — with the standard lifecycle fields so
  locking and `DROP WORKSPACE` work uniformly.
- `security/kms.py`: the external-tables plan's Phase 1 envelope module, shared verbatim.
- Verify `drop_workspace`'s storage sweep no-ops cleanly on shell workspaces (no datasets,
  no GCS locations) rather than erroring — likely already true, needs a test.

### worker.opteryx — the resolver + allowlist; deletes the hardcoding

- New `app/catalog_resolver.py`: the `KIND` allowlist table and
  `resolve_workspace(name) → Resolution` — doc read via `read_catalog_binding` (Firestore
  client/database resolved per call, matching the `_firestore_database()` pattern at
  `worker.py:50`); KMS decrypt for `mode: "stored"`; binding's `version` as the Resolution
  version. Its no-binding branch *is* the native default: `OpteryxConnector` +
  `OpteryxCatalog` with `firestore_database`/`gcs_bucket` re-read from `get_config` per
  call, versioned by config fingerprint.
- `worker.py`: `set_workspace_resolver(resolve_workspace)` once at import — stateless-fresh
  per call, so nothing needs re-installing per query. *End state:* delete lines 96-106
  (`tarchia`) **and** `register_default_connector` entirely (lines 64-85 plus its per-query
  call site) — the workaround it existed for (config frozen at import) no longer exists.
  Line 86 (`mabel_data`) stays, untouched, until its one remaining client finishes migrating
  off the legacy mabel tables — then it is deleted outright, never registry-managed.
- Prerequisite for prod: `opteryx-iceberg` is currently reachable only via the local
  `sys.path` sibling-checkout convention and isn't published. Publishing or vendoring it is
  a hard gate before the resolver can serve Iceberg kinds from a deployed worker — same gate
  the hardcoded `tarchia` registration already sits behind.

### control.opteryx — binding CRUD

- New `app/routes/v1/workspace_catalog.py` following the `workspaces.py` conventions
  (`Depends(require_bearer_token)`, handler-body permission checks):
  - `GET /v1/workspaces/{name}/catalog` — binding with `auth.ciphertext` redacted (mode and
    key name shown).
  - `PUT /v1/workspaces/{name}/catalog` — body `{kind, config, auth: {mode, secret?,
    inject_as?}}`; encrypts via the catalog lib before any write; bumps `version`.
    **Precondition (decided): the workspace must already exist in governance with an owner**
    — `workspaces/{name}` with an owner member. An unowned workspace is not queryable and
    cannot be bound; enforcing this at bind time is sufficient, because query-time access is
    already gated by access policies, which only come into existence through the
    owned-workspace genesis flow. The check lives in this handler, not in `opteryx_catalog`,
    per this repo's convention that the library doesn't enforce identity rules.
  - `DELETE /v1/workspaces/{name}/catalog` — reverts to native.
- **API-only (decided): no SQL surface.** There is no `ALTER WORKSPACE … SET CATALOG` verb —
  a binding write can carry a secret, and secrets in query text would end up wherever query
  text goes. Delete-protection's SQL precedent doesn't carry a secret, so it doesn't transfer.
- Permission tier: `_require_workspace_owner` (`workspaces.py:107-114`) — binding writes can
  carry credentials and redefine where a workspace's data comes from.
- Same in-transit-secret discipline as the external-tables plan's Phase 5: `AuditMiddleware`
  logs method/path/status only — the diff must not add body logging; the raw secret exists
  in memory only until the encrypt call.
- Grant pre-checks (§5): where access-policy endpoints validate that a grant's target
  resource exists, skip or soft-warn when the workspace carries a `catalog` block.
- `POST /v1/workspaces/{name}/catalog/sync` — refresh the stub dataset docs from the
  external catalog's listing (§5). Owner-or-admin tier; returns `(added, removed)` like
  `REFRESH EXTERNAL TABLE`. **User-initiated only (decided)** — nothing in the system
  triggers it automatically; stale-listing failures recommend it in their error message
  instead.

### odata.opteryx — no code change (by design)

- The stub-projection decision (§5) exists precisely so odata's collection-group listing
  keeps working unmodified. Only follow-up: verify the listing renders a stub
  (`external-catalog: true`, no snapshots/sort-orders) as a plain `Table` without warnings,
  and add that as a test case.

## 7. Migration

Every step is independently deployable and reversible; the hardcoded registrations shadow
the registry until cutover because the static table is slot 1 of the resolution chain.

1. **opteryx-catalog:** binding module + KMS helper. Inert — nothing calls it.
2. **opteryx-core:** the resolution-first rewrite. Behavior-compatible by construction —
   `register_workspace`, `set_default_connector`, and the disk fallback all keep working,
   held to that by core's existing test suite. Inert beyond that: no resolver installed, so
   slots 1/3/4 reproduce today's behavior. This is the largest step; land and soak it before
   anything depends on slot 2.
3. **worker.opteryx:** install the resolver *alongside* the existing registrations, keeping
   `register_default_connector` temporarily as the slot-3 belt-and-braces. Native workspaces
   now resolve through the resolver's native branch — which reads the same config the old
   default did, so behavior is identical — and its failure paths get proven on healthy
   traffic.
4. **Backfill:** write the `tarchia` registry entry (script or the control endpoint once
   step 6 lands). Still shadowed by the static-table entry — verifiable in a REPL by calling
   the resolver directly.
5. **Cutover:** delete the `tarchia` `register_workspace` call and
   `register_default_connector` (function + per-query call) from `worker.py`; deploy; verify
   in prod with the known-good probe (`SELECT … FROM tarchia.interop_ns.people`) plus a
   native-workspace read. `mabel_data`'s registration is untouched. Rollback is reverting
   this deploy — the registry entry is harmless while shadowed.
6. **control.opteryx:** CRUD endpoints. Can land any time after step 1; required for
   self-serve binding management but not for the migration itself.

## 8. Open questions

- **Read cost at scale.** If per-query doc reads show up in latency or Firestore billing,
  add a short-TTL resolver cache (keyed by workspace, invalidated by version mismatch on
  next miss). Deliberately deferred — measure first.

Resolved since first draft: plaintext credentials in process memory — accepted (§4; after
the core rewrite they live only in instance config, since cache keys are workspace names);
no SQL surface for binding management (§6); billing for bound workspaces is the unchanged
processing/query cost model with no storage line (§1); `mabel_data` stays hardcoded until
its one client finishes migrating off legacy mabel tables (§1, §6); a workspace must have an
owner before it can be bound or queried — enforced at bind time in control.opteryx (§6);
stub sync is user-initiated only, with stale-listing failures recommending the refresh
rather than triggering it (§5, §6).
