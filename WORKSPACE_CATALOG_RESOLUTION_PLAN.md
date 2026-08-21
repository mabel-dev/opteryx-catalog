# Workspace Catalog Resolution — Cross-Repo Implementation Plan

Implements `WORKSPACE_CATALOG_RESOLUTION.md` (the design doc; section references below are
to it). Read that first — this file only sequences the work, it does not restate the
rationale or the decisions, which are settled there.

Six repos are touched: `opteryx-core`, `opteryx-catalog`, `opteryx-iceberg`,
`worker.opteryx`, `control.opteryx`, `odata.opteryx` (the last one deliberately with no
code change). Shares one module with the external-tables plan
(`~/.claude/plans/bind-time-this-atomic-hamster.md`): `opteryx_catalog/security/kms.py` —
whichever plan lands first builds it, the other reuses it.

## Dependency graph

Two independent spines, converging at the worker:

- **Engine spine:** Phase 0 (core rewrite) → Phase 2 (iceberg nested config) → Phase 3
  (worker resolver) → Phase 4 (backfill + cutover).
- **Registry spine:** Phase 1 (binding module + KMS) → Phase 3, and → Phase 5
  (control CRUD) → Phase 6 (stub sync).

Phases 0 and 1 have no dependency on each other and can be built in parallel. Phase 5's
hard gate mirrors the external-tables plan: no credential-capturing endpoint before the
encrypt path exists.

---

## Phase 0 — opteryx-core: resolution-first rewrite
**Repo:** opteryx-core. **The largest and riskiest phase — land and soak first.**
**Status: implemented 2026-08-20** (uncommitted in the opteryx-core working tree) —
`opteryx/connectors/__init__.py` rewritten, new `tests/unit/connectors/test_resolution_chain.py`
(16 tests). Verified non-breaking: full `tests/unit` + `tests/storage` failure set is
byte-identical to HEAD's (121 pre-existing failures, zero introduced). One deliberate
refinement vs. the design text: the slot-3 static default caches under a single shared
`"_default"` key (one gateway instance, as today — `OpteryxConnector` keys catalogs per
workspace internally); slot-1/slot-2 answers cache per workspace/prefix. Also kept
`_storage_prefixes` / `_default_connector` / `_connector_cache` (values = bare instances,
versions in a parallel `_connector_versions` dict) since existing tests reach into them.

Rewrite `opteryx/connectors/__init__.py` around the §3 model:

- A `Resolution` type: `{connector, config (nested dict), version}`.
- `set_workspace_resolver(fn)` and the four-slot chain in `connector_factory`:
  static table → installed resolver → static default → disk. First answer wins; resolver
  exceptions propagate (never swallowed into a fallback).
- Cache: `_connectors[workspace] = (version, instance)` replacing the kwargs-hash cache
  (`cache_key = tuple(sorted(connector_entry.items()))` and its flat-hashable constraint
  both go away). Version match → reuse; mismatch → rebuild and replace the entry.
- API compatibility shims: `register_workspace(prefix, connector, **kwargs)` writes slot 1;
  `set_default_connector(connector, **kwargs)` writes slot 3; the `$`-prefixed
  virtual-dataset branch and the disk terminal branch are preserved verbatim.
- `OpteryxConnector.__init__`: accept nested config (it already pops `connector`/`prefix`
  and injects `workspace=` in `_get_catalog` — keep both). Do NOT touch the per-hit
  `$properties` liveness re-check (`opteryx_connector.py:608-623`); it guards dropped
  workspaces, orthogonal to version-based config rotation.

**Hard requirements to hold, by test:** (1) no registrations, no default → disk connector,
exactly today's last branch; (2) `set_default_connector` still works as the global default.

**Verification:**
- Core's existing connector/test suite green, unmodified — that's the
  behavior-compatibility gate.
- New tests: chain precedence (static beats resolver beats default beats disk); version
  rotation (same version → same instance id, bumped version → new instance, old one
  replaced); resolver exception propagates to the query; resolver returning `None`
  falls through to slots 3/4; nested-dict config reaches the connector intact.

**Depends on:** nothing. **Unblocks:** Phases 2, 3.

---

## Phase 1 — opteryx-catalog: binding module + KMS
**Repo:** opteryx-catalog. Parallel with Phase 0.
**Status: implemented 2026-08-20** — `opteryx_catalog/binding.py` (read/write/clear +
`CatalogBinding`), `opteryx_catalog/security/kms.py` (envelope: per-secret AES-256-GCM DEK,
KMS-wrapped, key name bound as GCM AAD), `InvalidCatalogBinding` in exceptions.py, `kms`
extra in pyproject. Tests: `test_catalog_binding.py`, `test_kms_envelope.py`,
`test_drop_shell_workspace.py` (26 new, all passing; full suite otherwise unchanged — the
only failures are venv gaps: missing `hypothesis` and `google-cloud-tasks`). One design
refinement the tests forced: `clear_catalog_binding` leaves a `catalog-version-floor` field
so clear-then-rebind inside one millisecond cannot regress the version — wall clock alone
was not monotonic there.

- New `opteryx_catalog/binding.py`: `read_catalog_binding(firestore_client, workspace)`,
  `write_catalog_binding(...)`, `clear_catalog_binding(...)`. Plain functions, not
  `OpteryxCatalog` methods (§6 explains why). Writers: atomic `version` bump
  (Firestore transaction or read-modify-write with precondition), reserved-key validation
  (`workspace`/`connector`/`prefix` rejected), shell `$properties` creation when missing
  (with the standard lifecycle fields — mirror the seed block in
  `opteryx_catalog.py`'s `__init__`).
- `opteryx_catalog/security/kms.py`: envelope encrypt/decrypt, optional `kms` extra in
  `pyproject.toml` (lazy-import pattern like `s3`/`webhooks`/`alerts`). **Coordinate with
  the external-tables plan** — same module, build once.
- Test: `drop_workspace` on a shell workspace (binding block, zero datasets, no GCS
  locations) completes cleanly.

**Verification:** round-trip read/write/clear including version bump; malformed entry
(reserved key, unknown top-level field) rejected at write; encrypt → decrypt round-trip;
shell doc carries all lifecycle fields.

**Depends on:** nothing (KMS module may already exist from the external-tables plan —
check before building). **Unblocks:** Phases 3, 5.

---

## Phase 2 — opteryx-iceberg: nested config passthrough
**Repo:** opteryx-iceberg.
**Status: implemented 2026-08-20** — `IcebergMetastore.__init__` forwards `**properties`
verbatim to `load_catalog` (flat `auth_type`/`google_auth_scopes` now raise a ValueError
with the nested-form hint instead of silently producing unauthenticated requests); README
flat-kwargs section rewritten; `tests/test_google_auth_config.py` rewritten (nested auth
passthrough, `token`/`credential` passthrough, retired-kwargs guard) — 9/9 passing incl.
the live SqlCatalog read tests. `worker.opteryx/app/worker.py`'s hardcoded `tarchia`
registration was updated to the nested `auth={...}` form in the same stroke (legal now
that Phase 0 removed kwargs hashing) so local runs keep working. The publish/vendor gate
for Phase 4 remains open.

- `IcebergMetastore` accepts pyiceberg's natural nested config and forwards it to
  `pyiceberg.catalog.load_catalog`, deleting the flat
  `auth_type`/`google_auth_scopes`/`header.*` reconstruction. Keep the `workspace=`
  injection contract with `OpteryxConnector`.
- Rewrite the README's flat-kwargs section (it documents the retired constraint at length).
- Add a test that `token`/`credential` keys pass through to pyiceberg (mock the loader —
  no live catalog needed).
- **Separate but gating task:** publish or vendor `opteryx-iceberg` so a deployed
  `worker.opteryx` can import it (today it's `sys.path` sibling-checkout only). This gates
  Phase 4's cutover, not this phase's code.

**Verification:** existing tests against pyiceberg's local `SqlCatalog` green; one manual
run against the kept BigLake catalog (`tarchia` config in nested form) before Phase 4.

**Depends on:** Phase 0 (nested config only flows once core stops hashing kwargs).
**Unblocks:** Phase 3 (iceberg kind), Phase 4.

---

## Phase 3 — worker.opteryx: the resolver
**Repo:** worker.opteryx.
**Status: implemented 2026-08-20 (shadow mode)** — `app/catalog_resolver.py` (KINDS
allowlist, `resolve_workspace`, `CatalogResolutionError`; Firestore clients cached per
(project, database) with the database NAME re-read from config each call; native-default
branch versioned by config fingerprint) and `set_workspace_resolver(resolve_workspace)`
installed once at import in `worker.py`, with the static registrations and
`register_default_connector` deliberately kept as the migration safety net.
`tests/test_catalog_resolver.py` covers every failure-mode-table row plus the
no-secrets-in-logs/errors guard-rails (16 tests); `test_worker.py` gained a
recorder-based wiring assertion mirroring the permissions-capability one, and both
stub-carrying test files' `opteryx`/`opteryx_catalog` stubs were extended
(`Resolution`, `set_workspace_resolver`, `opteryx_catalog.binding` submodule). Full worker
suite: 106 passed, failure set identical to baseline (the pre-existing
`test_config_promotion` failure only). Remaining before Phase 4: deployed shadow-mode soak
with error rates watched, and the opteryx-iceberg publish/vendor gate.

- New `app/catalog_resolver.py`:
  - `KIND` allowlist: `{"iceberg": (OpteryxConnector, IcebergMetastore)}` — one entry.
  - `resolve_workspace(name) → Resolution` implementing the §3 branch table exactly,
    including: segment sanity check (reject `/`, leading `$`); per-call Firestore
    client/database via `get_config` (the `_firestore_database()` pattern); KMS decrypt for
    `mode: "stored"` with `inject-as` dotted-path insertion; `CatalogResolutionError` on
    read/kind/decrypt failure — messages carry workspace + kind only.
  - The no-binding branch returns the native default (`OpteryxConnector` +
    `OpteryxCatalog`, config re-read per call, version = config fingerprint).
- `worker.py`: `set_workspace_resolver(resolve_workspace)` at import. **Keep** the
  `mabel_data` and `tarchia` registrations and `register_default_connector` in this phase —
  slot-1 shadowing and the slot-3 belt-and-braces are the migration safety net.
- Guard-rail test: capture logging/telemetry through a resolution (including a failing one)
  and assert no config values or auth material appear.

**Verification:** unit tests per branch of the failure-mode table (§3); integration:
resolver returns the native default for an existing native workspace and its config
fingerprint changes when `GCS_BUCKET` config changes; deployed shadow-mode soak with error
rates watched.

**Depends on:** Phases 0, 1 (and 2 for the iceberg kind to actually work). **Unblocks:** Phase 4.

---

## Phase 4 — backfill + cutover
**Repos:** worker.opteryx + a one-off script. **Gate: opteryx-iceberg published/vendored.**

1. Script writes the `tarchia` binding (§1 schema, `mode: "ambient"`) via
   `write_catalog_binding`. Verify in a REPL: `resolve_workspace("tarchia")` returns the
   iceberg Resolution while the deployed worker still serves it from the static table.
2. Delete from `worker.py`: the `tarchia` registration (lines 96-106) and
   `register_default_connector` (lines 64-85 + per-query call site). `mabel_data` (line 86)
   stays.
3. Deploy. Probes: `SELECT … FROM tarchia.interop_ns.people`; a native-workspace read; a
   `mabel_data` read (still static). Rollback = revert this deploy; the binding entry is
   harmless while shadowed.

**Depends on:** Phases 2, 3, and the publish/vendor gate.

---

## Phase 5 — control.opteryx: binding CRUD
**Repo:** control.opteryx. **Hard gate: Phase 1's encrypt path must exist first** (same
reasoning as the external-tables plan — never create pressure to store raw secrets).
**Status: implemented 2026-08-20** — `app/routes/v1/workspace_catalog.py` (GET redacted /
PUT with the full precondition ladder / DELETE), registered in `routes/v1/__init__.py`;
grant pre-check softened in `access.py` (`_workspace_is_bound` skips `_resource_exists`
for bound workspaces, read-failure falls through to the normal check rather than waiving
it); `tests/test_workspace_catalog_endpoints.py` covers the owner/non-owner/unowned
matrix, stored-credential encrypt-only-handoff, GET ciphertext redaction, DELETE
revert-to-native, and the no-secret-in-any-log-line capture test (18 tests; full suite
327 passed, zero failures). Two implementation notes: the binding/kms library imports are
LAZY with a clean 501 — control's venv carries a pip-installed opteryx-catalog that
pre-dates `binding.py`, so app startup stays safe until the dependency is released and
upgraded (a Phase 5 deploy prerequisite, analogous to the iceberg publish gate); and
`mode: "stored"` additionally requires the `CATALOG_KMS_KEY` config (the CryptoKey
resource name) — unset yields 501, not a broken write.

- New `app/routes/v1/workspace_catalog.py` per §6: GET (ciphertext redacted) / PUT / DELETE.
- PUT preconditions, in order: `require_bearer_token` → workspace exists in governance
  (`workspaces/{name}`) **with an owner member** (the decided queryability gate) →
  `_require_workspace_owner` for the caller → validate kind against a mirrored allowlist of
  *names* (control validates the name; only the worker maps names to classes) → encrypt via
  the catalog lib → `write_catalog_binding`.
- No body logging anywhere in the diff (`AuditMiddleware` currently logs method/path/status
  only — keep it that way); log-inspection test that a PUT body containing a secret never
  appears in any log line.
- Grant pre-check softening (§5): where access-policy endpoints validate a grant target
  exists, skip/soft-warn for workspaces with a `catalog` block.

**Verification:** owner vs. non-owner vs. unowned-workspace matrix for PUT; GET redaction;
DELETE reverts to native (subsequent resolution takes the native branch); the log test.

**Depends on:** Phase 1. **Unblocks:** Phase 6.

---

## Phase 6 — stub projection sync
**Repos:** opteryx-catalog (primitive) + control.opteryx (endpoint). Lowest urgency — the
engine and permissions work without it; only OData listing needs it (§5).
**Status: implemented 2026-08-21** — `opteryx_catalog/stub_projection.py` (+18 tests in
`tests/test_stub_projection.py`, opteryx-catalog 0.4.102);
`POST /v1/workspaces/{name}/catalog/sync` in control.opteryx's
`app/routes/v1/workspace_catalog.py` (+21 tests); the odata stub-listing test as
`tests/test_service_document_external_catalog_stubs.py`.

- opteryx-catalog: a stub-writing primitive — given a workspace and a listing
  (`[(collection, name), …]`), reconcile stub docs (`workspace`/`collection`/`name`/
  `external-catalog: true`) under the shell workspace, returning `(added, removed)`.
  The listing itself is produced by the caller.
- control.opteryx: `POST /v1/workspaces/{name}/catalog/sync` — reads the binding, builds
  the external catalog's listing via `IcebergMetastore.list_datasets`, calls the primitive.
  **Noted decision:** this puts `opteryx-iceberg` (and pyiceberg) into control.opteryx's
  dependency set — acceptable for a FastAPI service that already carries pydantic — and
  duplicates the one-entry kind table there, matching that repo's documented
  "kept local rather than shared" convention. Revisit both if kinds multiply.
  User-initiated only; nothing schedules or auto-triggers it.
- **Added to this phase during implementation — two schema fields the plan did not carry:**
  `listing-synced-at-ms` and `listing-count` on the `catalog` block, written by the sync
  and returned by `GET …/catalog` as `listing_synced_at_ms`/`listing_count`. Without them
  no UI can state the age of the list, and a refresh control with no "last refreshed"
  beside it is one people press repeatedly to find out whether they needed to — the exact
  cost the user-initiated-only rule exists to avoid. The sync writer is already writing the
  document, so it was cheap now and awkward to retrofit. A binding write replaces the whole
  block and therefore drops both, deliberately: a rebind can point at a different catalog,
  and carrying an old stamp forward would report freshness for someone else's listing.
- Stale-listing failure messaging: where a bound workspace's query fails with
  dataset-not-found, the error recommends running the sync — it must not trigger it
  (cost-impacting; the user decides). **Still outstanding** — the endpoint and the
  freshness stamp it needs now exist; the engine/Studio-side wording does not.
- odata.opteryx: no code change; add the test that a stub renders as a plain `Table` in the
  service document without warnings.

**Implementation notes, where reality differed from the text above:**
- The primitive returns a `StubSyncResult` NamedTuple (`added`, `removed`, `total`,
  `synced_at_ms`) rather than a bare `(added, removed)`. The endpoint has to report `total`
  and `synced_at_ms` to the UI, and those are the values it just WROTE to the binding
  block — recomputing them in the caller invites a quiet disagreement with the document.
- Reconciliation only ever deletes documents carrying `external-catalog: true`, and never
  overwrites one that doesn't. A projection cannot eat a real dataset document.
- `§5`'s open verification item — "confirm Tier 1 exposes namespace enumeration" — is still
  open: `IcebergMetastore` has `list_datasets(namespace)` but no `list_collections()`. The
  endpoint prefers `list_collections()` when the installed opteryx-iceberg has one and
  otherwise walks pyiceberg's `list_namespaces` on the wrapped catalog handle (bounded
  depth). Adding the method upstream retires the fallback with no change at the call site.

**Depends on:** Phases 1, 2, 5.

---

## Live verification — the stored-credential path (2026-08-20)

**Status: verified end to end against a real external catalog.** Everything above
this line had been exercised against `tarchia` (Google BigLake), which
authenticates by **ambient identity** — so `mode: "ambient"` was proven in
production while `mode: "stored"` (§2's KMS-envelope branch) had unit tests and
no service that would actually reject it. Ambient auth cannot fail the way a
stored credential fails.

A standing Apache Polaris 1.7 fixture now closes that gap — Cloud Run
`polaris-opteryx-fixture` in `mabeldev`, metastore on an external Postgres,
warehouse on `gs://opteryx-polaris-fixture`, catalog `opteryx_fixture`, table
`interop_ns.people` seeded by plain `pyiceberg.catalog.rest.RestCatalog`. It is
**kept, not torn down**, on the same precedent as the BigLake tier-1 catalog.
Deployment, seeding and the test harness live in the `polaris.opteryx` repo;
standing cost is under $1/month (Cloud Run scales to zero — the bill is a KMS
key version, five Secret Manager secrets and a little Artifact Registry).

Polaris was the right adversary because it implements the Iceberg REST spec's
OAuth2 client-credentials flow, which pyiceberg drives natively from the
`credential` property — so `inject-as: "credential"` needed no opteryx-iceberg
code, exactly as §2's note predicted.

The workspace `polaris_test` (owner `bastian`, billing account `opteryx`) was
created via `PUT /v1/workspaces/{name}` and bound via
`PUT /v1/workspaces/{name}/catalog` with `kind: "iceberg"`, `mode: "stored"`,
`inject_as: "credential"`. All three tests ran against the **deployed** platform
— real token from authenticate.opteryx, SQL through jobs.opteryx, executed by
worker.opteryx, bound through control.opteryx. Nothing stubbed.

1. **Happy path.** `SELECT id, name, team, score FROM
   polaris_test.interop_ns.people` returns the 5 seeded rows; a `GROUP BY`
   aggregate over the same table also works. Proves decrypt → inject → OAuth2
   end to end, and that a nested/dotted config (`header.X-Iceberg-Access-
   Delegation`, `oauth2-server-uri`) survives the round trip — the retired
   flat-hashable-kwargs constraint is genuinely gone.
2. **A credential the catalog rejects.** Binding a credential Polaris refuses
   makes every query fail with
   `OAuthError: unauthorized_client: The client is not authorized` — 4 probes
   for 4 failures, in 2.5–3.2s each, never a hang, and unmistakably an
   authorization failure rather than the `WorkspaceNotFound`/dataset-not-found
   that a silent fall-through to the native catalog would produce. §3's "never
   fall through on a resolution failure" holds. The harness probes repeatedly
   so that *every* live worker instance is shown to reject it — one instance
   quietly succeeding would itself be a fall-through.

   **This is not the same as revoking the secret at the catalog, and the
   difference is a real finding — see below.**
3. **Rotation — the version compare, across processes.** Publishing a working
   secret with a second `PUT .../catalog` bumps `version`; the next query
   authenticates with it **with no worker restart and no redeploy**. The
   harness proves this rather than assuming it. Cloud Run chooses which
   instance serves a query, so instance identity is read back out of Cloud
   Logging: test 2 records the instances that *demonstrably failed* with the
   rejected credential, and test 3 keeps querying until one of exactly those
   instances answers successfully. That instance is known to have been holding
   a connector built from the rejected credential, so a success on it can only
   mean `_connector_cache`/`_connector_versions` invalidated and rebuilt it. A
   success on a never-seen instance is explicitly *not* accepted — a fresh
   process builds its connector from scratch and never exercises the compare.
   In the recorded run this landed on the first attempt: the instance that had
   just failed four consecutive queries returned all 5 rows immediately after
   the binding write, with no restart and no redeploy.

### Finding: cutting off a bound workspace means writing the binding

The obvious way to write test 2 is "reset the principal's secret in Polaris,
then query". **That does not reliably fail**, and the reason is worth writing
down — not because anything is broken, but because the intuitive operational
move is the ineffective one.

Nothing here is misbehaving. Polaris is honouring a **still-valid, unexpired**
bearer token whose backing secret was deleted afterwards, which is exactly what
OAuth2 says should happen: revoking a client secret stops new tokens being
minted, it does not retroactively invalidate issued ones. Whether a catalog
honours an already-issued token is the catalog's business and not something
Opteryx can control or should try to.

What *is* ours is how long we hold one. `IcebergMetastore.__init__` calls
`load_catalog` once (`opteryx-iceberg/opteryx_iceberg/metastore.py:72`), so the
pyiceberg REST session — and its bearer token — is built once per connector
instance and lives exactly as long as that instance does. The resolver re-reads
the binding doc on every query, exactly as §3 specifies, but an unchanged
`version` means the cached connector *and its token* are reused, and the freshly
decrypted credential is discarded unused. So a far-end revocation is invisible
to a warm worker for up to the token's lifetime — **3600 seconds** on this
fixture.

Demonstrated directly rather than inferred: a long-lived `RestCatalog` keeps
reading after its secret is reset, while a *new* `RestCatalog` built from the
same dead secret gets an immediate `OAuthError`. The first attempt at test 2
also caught it live — the query succeeded against a revoked credential because
Cloud Run routed it to an instance with a warm connector. Earlier runs that
*did* fail had been routed to instances with no warm connector, which had to
re-authenticate; the pass was real but had been mis-attributed to revocation
being detected.

The one actionable consequence, which the design already provides for:
**revoking access to a bound workspace means writing the binding** (or
`DELETE .../catalog`), not just disabling the credential at the far end. A
binding write bumps `version` and every worker rebuilds on its next query —
§3's "every worker converges on its next query" holds exactly as specified. It
is only *far-end* revocation, which the registry never learns about, that
converges on the token lifetime instead. Worth stating wherever binding
management is documented, because "revoke the secret at the catalog" is the
intuitive move and it is the slow one.

**Secrets in logs (§4's guard-rail, checked live).** 2000 worker and control
log entries across the whole session contain no client secret, no client id, no
warehouse name, no KMS key name, no ciphertext and no metastore hostname. The
only config value that ever reached the logs came from a **pyiceberg
`DeprecationWarning`** — not from Opteryx's resolution logging — naming the
catalog URI because `oauth2-server-uri` was being inferred from `uri`. Setting
it explicitly on the binding silenced it: zero occurrences after that change,
every one of them before it. Worth noting as a limit on the guarantee: the
design's "resolution logging never includes config values" is a property of
*our* code, and a dependency writing to stderr can still put a config value in
the logs — the guard-rail test in worker.opteryx cannot catch that class.

### Gaps this surfaced

- **`opteryx-catalog[kms]` was missing from two dependency sets.** Both
  `control.opteryx` and `worker.opteryx` depended on `opteryx-catalog` without
  the `kms` extra, so `google-cloud-kms` was absent in both. In control that
  surfaced honestly as the intended 501; in the worker the import is lazy, so a
  `mode: "stored"` binding would have failed its query with
  `CatalogResolutionError` while ambient bindings kept working — a failure
  confined to the one path it breaks. Both pins are now
  `opteryx-catalog[kms]>=0.4.101` and both services were redeployed. This is
  the deploy prerequisite Phase 5's status note anticipated, on both sides.
- **`CATALOG_KMS_KEY`** is now set in the platform config document
  (`config/mabeldev`) to
  `projects/mabeldev/locations/us-east1/keyRings/opteryx-catalog-secrets/cryptoKeys/workspace-bindings`.
  Encrypt and decrypt are granted to `762690895289-compute@`, which control and
  worker **both** run as in this deployment — so §2's intended split (control
  encrypts, worker `useToDecrypt`) is expressed in the grant but not actually
  enforced by distinct identities here.
- **The worker's `KINDS` allowlist needed no change**, as expected — `"iceberg"`
  already covered it.
- **Polaris 1.7 rejects per-catalog `polaris.*` property overrides**
  ("matches reserved prefix"), so credential vending is opted out of client
  side, by sending an empty `X-Iceberg-Access-Delegation` header rather than
  the server's `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION`. The worker reads data
  files with its own ADC; Polaris serves metadata only.
- **Polaris `/principals/{name}/rotate` is self-service only** — an admin
  rotating another principal gets a 403. `/principals/{name}/reset` is the
  administrative equivalent.

---

## Recommended execution order

```
Phase 0 (core rewrite)        ─┐
Phase 1 (binding + KMS)       ─┼─→ Phase 3 (worker resolver, shadow) → Phase 4 (cutover)
Phase 2 (iceberg nested)      ─┘
Phase 5 (control CRUD, after 1) → Phase 6 (stub sync)
publish/vendor opteryx-iceberg  → gates Phase 4 only
```

Phase 0 first and alone — it's the only phase that rewrites load-bearing shared machinery,
and its behavior-compatibility gate (core's existing suite, untouched) is the cheapest
place to catch a regression. Everything after it is additive.

## Out of scope (settled in the design doc, listed to prevent re-litigating)

- No SQL surface for binding management.
- No TTL resolver cache in v1 — measure first (§8).
- No `mabel_data` registry kind — stays hardcoded until deleted.
- No automatic stub sync — user-initiated only.
