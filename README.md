# opteryx-catalog

A Firestore + Google Cloud Storage (GCS) backed implementation of a
lightweight catalog interface. This package provides an opinionated
catalog implementation for storing table metadata documents in Firestore and
consolidated Parquet manifests in GCS.

**Important:** This library is *modelled after* Apache Iceberg but is **not
compatible** with Iceberg; it is a separate implementation with different
storage conventions and metadata layout. This library is the catalog and
metastore used by [opteryx.app](https://opteryx.app/) and uses **Firestore** as the primary
metastore and **GCS** for data and manifest storage.

---

## Features ✅

- Firestore-backed catalog and collection storage
- GCS-based table metadata storage; export/import utilities available for artifact conversion
- Table creation, registration, listing, loading, renaming, and deletion
- Commit operations that write updated metadata to GCS and persist references in Firestore
- Simple, opinionated defaults (e.g., default GCS location derived from catalog properties)
- Lightweight schema handling (supports pyarrow schemas)

## Quick start 💡

1. Ensure you have GCP credentials available to the environment. Typical approaches:
   - Set `GOOGLE_APPLICATION_CREDENTIALS` to a service account JSON key file, or
   - Use `gcloud auth application-default login` for local development.

2. Install locally (or publish to your package repo):

```bash
python -m pip install -e ".[parquet]"
```

   `rugo` (the Parquet engine) is an optional dependency so it can be supplied by
   whatever already provides it — the `parquet` extra installs it directly, and
   `opteryx-core` bundles a matching build. Do not install the separately
   published `draken` distribution: rugo vendors its own, and pip overwrites it.

3. Create an `OpteryxCatalog` and use it in your application:

```python
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel

from opteryx_catalog import OpteryxCatalog

catalog = OpteryxCatalog(
    workspace="my_workspace",
    firestore_project="my-gcp-project",
    gcs_bucket="my-default-bucket",
)

# Create a collection
catalog.create_collection("example_collection", author="me")

# Schemas are described by an empty Morsel carrying the column types
schema = Morsel()
schema.append_vector("id", vector_from_sequence([], dtype="INTEGER"))
schema.append_vector("name", vector_from_sequence([], dtype="VARCHAR"))

# Create a new dataset (metadata written to a GCS path derived from the bucket property)
dataset = catalog.create_dataset("example_collection.users", schema, author="me")

# Or register a table if you already have a metadata JSON in GCS
catalog.register_table(
    ("example_namespace", "events"), "gs://my-bucket/path/to/events/metadata/00000001.json"
)

# Load a table
tbl = catalog.load_dataset(("example_namespace", "users"))
print(tbl.metadata)
```

## Configuration and environment 🔧

- GCP authentication: Use `GOOGLE_APPLICATION_CREDENTIALS` or Application Default Credentials
- `firestore_project` and `firestore_database` can be supplied when creating the catalog
- `gcs_bucket` is recommended to allow `create_dataset` to write metadata automatically; otherwise pass `location` explicitly to `create_dataset`
 - The catalog writes consolidated Parquet manifests and does not write manifest-list artifacts in the hot path. Use the provided export/import utilities for artifact conversion when necessary.

Example environment variables:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export GOOGLE_CLOUD_PROJECT="my-gcp-project"
```

### Trigger firing (materialized-view refresh) ⚡

A user-created commit on a dataset carrying triggers enqueues a refresh job
for each target materialized view: a `jobs/{execution_id}` document plus an
OIDC-authenticated Cloud Task to worker.opteryx (see
`MATERIALIZED_VIEWS_TRIGGERS_PLAN.md`). Firing environments need:

| Variable | Default | Purpose |
| --- | --- | --- |
| `OPTERYX_TRIGGER_FIRING` | on | `0` disables commit-time firing entirely (local scripts, tests) |
| `GCP_PROJECT_ID` / `GCP_PROJECT` / `GOOGLE_CLOUD_PROJECT` | catalog's Firestore project | Project holding the `jobs` collection and the task queue |
| `OPTERYX_JOBS_DATABASE` | *(default database)* | Firestore database for the `jobs` collection — jobs.opteryx/worker.opteryx use the project default, not `catalogs` |
| `TASKS_LOCATION` | `us-east1` | Cloud Tasks queue location |
| `TASKS_QUEUE` | `worker-dispatch` | Cloud Tasks queue name |
| `TASKS_TARGET_URL` | `https://worker.opteryx.app/api/v1/submit` | Where the task is pushed |
| `TASKS_OIDC_SA` | — | Service account for the task's OIDC token. **Must be the same SA jobs.opteryx enqueues as** — worker.opteryx pins that OIDC subject. Without it the task is enqueued unauthenticated and the worker rejects it |
| `TASKS_OIDC_AUDIENCE` | the target URL | OIDC audience — the worker checks exact equality with its submit URL |
| `JOB_TTL_DAYS` | `14` | `purge_at` horizon on refresh job documents, matching jobs.opteryx |

Enqueue failures never break the commit that triggered them — they alert and
write a `trigger.fire_failed` audit record instead. A missed fire is a stale
materialized view, so keep alerting configured wherever firing is enabled.

#### The firing floor (`minimum-interval-seconds`)

A trigger fires at most once per `minimum-interval-seconds`. A commit inside the
interval after a firing is recorded on the trigger as `last-fired-status:
throttled` and audited as `trigger.throttled`, but enqueues nothing — it is not
an error and does not alert, like a suspension. It is a throttle, not a
debounce: the *first* commit in a burst fires, and later ones in the interval
are dropped, so the target is refreshed as of that first commit until the next
commit after the interval fires again. Throttled task fires stay correct
because the next fire widens its window back over the skipped commits.

- **New triggers get 120 seconds** (`DEFAULT_MINIMUM_INTERVAL_SECONDS`), written
  onto the record at `create_trigger`. Pass `minimum_interval_seconds=0` for none,
  or any non-negative whole number of seconds.
- **Existing triggers are untouched.** The floor is read from the record and
  never defaulted at fire time, so a trigger document without the field fires
  on every commit as before and pays no extra Firestore round trip. Give one a
  floor with `set_trigger_minimum_interval(dataset, trigger, seconds)`, or in SQL
  with `ALTER TRIGGER <name> ON <table> SET MINIMUM INTERVAL TO <n> [SECONDS|MINUTES]`;
  `0` removes it. Re-registering a trigger (`CREATE OR REPLACE MATERIALIZED VIEW` rewrites
  every source trigger) keeps the floor the record already holds.
- **Two commits milliseconds apart cannot both fire.** The right to fire is
  claimed in a Firestore transaction on the trigger document
  (`claim_trigger_fire`), which stamps `last-claimed-at-ms` in the same
  transaction that reads it. Of two concurrent claims one commits and the other
  is retried against the fresh stamp and refused. The claim is keyed on its own
  field rather than `last-fired-at-ms`, which records failures too, and a fire
  that raises after claiming hands the claim back (`release_trigger_fire`) so an
  outage does not also silence the next interval.
- The floor is per **trigger**, not per target: a view with two sources has two
  triggers, each with its own floor. Coalescing what gets through stays with
  jobs' dedup window.

### Alerting 🚨

When the catalog detects a *platform inconsistency* — a state that should be impossible, like a
snapshot summary disagreeing with the manifest it describes — it raises or reports an exception
carrying the `Alertable` mixin (`opteryx_catalog/exceptions.py`). Those are delivered by
`opteryx_catalog/alerts/`.

Caller errors (`DatasetNotFound`, `DatasetLocked`, …) are deliberately **not** alertable — otherwise
every 404 files a ticket.

Delivery is by sink. **stdout is the guarantee**: one structured JSON line, written synchronously, so
the record survives the process being killed. Everything else is an addition and is best-effort.

| Sink | What it does | Default |
|---|---|---|
| `stdout` | One GCP-structured JSON line per alert, routed to `ops.stdout_logs` by its severity | on |
| `github` | Files, or folds into, one issue per distinct failure | off |
| `discord` | Posts to a channel webhook. Severity-gated — it interrupts people | off |

```bash
# Which channels. Comma-separated; 'both' is an alias for 'stdout,github'.
export OPTERYX_ALERTS_SINK="stdout,discord"

# Identifies the reporting job. Prefixes titles, becomes a label, and is SALTED
# INTO THE FINGERPRINT - changing it later gives every failure a new identity,
# orphaning open issues and re-alerting everything once. Pick it and leave it.
export OPTERYX_ALERTS_COMPONENT="catalog-maintenance"
export OPTERYX_ALERTS_ENVIRONMENT="production"
```

Everything below has a working default; set it only to change that default.

| Variable | Default | Notes |
|---|---|---|
| `OPTERYX_ALERTS_ENABLED` | on | `false` silences alerting entirely |
| `OPTERYX_ALERTS_COOLOFF_HOURS` | `24` | How long a known failure stays quiet. Applies to every sink |
| `OPTERYX_ALERTS_LABELS` | — | Extra labels, comma separated |
| `OPTERYX_ALERTS_REPO` | — | `owner/repo`, or a GitHub URL. Required by the `github` sink |
| `OPTERYX_ALERTS_TOKEN_SECRET` | `GITHUB_TOKEN` | Secret Manager secret holding the token. A `GITHUB_TOKEN` env var wins — the dev path |
| `OPTERYX_ALERTS_API_URL` | `https://api.github.com` | For GitHub Enterprise |
| `OPTERYX_ALERTS_DISCORD_WEBHOOK` | — | The webhook URL directly. Skips Secret Manager, so no IAM grant needed |
| `OPTERYX_ALERTS_DISCORD_WEBHOOK_SECRET` | `DISCORD_NOTIFICATION_WEBHOOK` | Used when the URL isn't set directly |
| `OPTERYX_ALERTS_DISCORD_MIN_SEVERITY` | `CRITICAL` | `WARNING`, `ERROR` or `CRITICAL` |
| `OPTERYX_ALERTS_DISCORD_MENTION` | — | `<@&ROLE_ID>` or `@here`. **Without this a Discord message posts silently and won't reach a phone.** Get the role ID from Discord → Settings → Advanced → Developer Mode, then right-click the role → Copy ID |

The legacy `PLATFORM_ISSUES_*` names are still read as a fallback, with a one-time warning — the
GitHub reporter these were moved from was configured that way. Check for stale ones on a deployed
service: an old `PLATFORM_ISSUES_COMPONENT` silently wins over an unset `OPTERYX_ALERTS_COMPONENT`
and changes your fingerprints.

Reading a secret needs `roles/secretmanager.secretAccessor` on it for the runtime service account.
Setting `OPTERYX_ALERTS_DISCORD_WEBHOOK` directly avoids that entirely.

To verify delivery end to end against a real channel:

```bash
python3 scripts/send_test_alert.py
```

The `alerts` extra installs `google-cloud-secret-manager`, needed only for the Secret Manager path:

```bash
pip install "opteryx-catalog[alerts]"
```

### Manifest format

This catalog writes consolidated Parquet manifests for fast query planning and stores table metadata in Firestore. Manifests and data files are stored in GCS. If you need different artifact formats, use the provided export/import utilities to convert manifests outside the hot path.

## API overview 📚

The package's entry point is the `OpteryxCatalog` class; there is no factory
helper. Alongside it, the top level exports the metastore interface (`Metastore`,
`Dataset`, `View`), the dataset and metadata types (`SimpleDataset`,
`DatasetMetadata`, `Snapshot`, `DataFile`, `ManifestEntry`), and `ResourceType`.
`DatasetCompactor` is exported from `opteryx_catalog.catalog`.

### Workspaces are not created implicitly

`OpteryxCatalog(workspace=...)` is a read: the workspace must already exist, or
construction raises `WorkspaceNotFound`. This keeps a mistyped workspace name in
a query from bringing an empty workspace into existence — in Firestore a
collection exists only because a document in it does, so writing the workspace's
`$properties` document *is* creating the workspace.

Provisioning is explicit:

```python
OpteryxCatalog(workspace="new_workspace", create_if_missing=True)
```

Key methods include:
- `create_collection(collection, properties={}, exists_ok=False)`
- `drop_namespace(namespace)`
- `list_namespaces()`
- `create_dataset(identifier, schema, location=None, partition_spec=None, sort_order=None, properties={})`
- `register_table(identifier, metadata_location)`
- `load_dataset(identifier)`
- `list_datasets(namespace)`
- `drop_dataset(identifier)`
- `rename_table(from_identifier, to_identifier)`
- `commit_table(table, requirements, updates)`
- `create_view(identifier, sql, schema=None, author=None, description=None, properties={})`
- `load_view(identifier)`
- `list_views(namespace)`
- `view_exists(identifier)`
- `drop_view(identifier)`
- `update_view_execution_metadata(identifier, row_count=None, execution_time=None)`
- `create_materialized_view(identifier, sql, source_tables, author, update_if_exists=False)`
- `get_materialized_view(identifier)` / `list_materialized_views(collection)`
- `drop_materialized_view(identifier, author)`
- `mark_materialized_view_refreshed(identifier, status, execution_id=None, author=None)`
- `create_trigger(dataset_identifier, name, target_view, statement_id=None, author=None)`
- `list_triggers(dataset_identifier)` / `drop_trigger(dataset_identifier, name, author, missing_ok=False)`
- `mark_trigger_fired(dataset_identifier, name, status)`

### Views 👁️

Views are SQL queries stored in the catalog that can be referenced like tables. Each view includes:
- **SQL statement**: The query that defines the view
- **Schema**: The expected result schema (optional but recommended)
- **Metadata**: Author, description, creation/update timestamps
- **Execution history**: Last run time, row count, execution time

Example usage:
```python
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import IntegerType, StringType

# Create a schema for the view
schema = Schema(
    NestedField(field_id=1, name="user_id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="username", field_type=StringType(), required=False),
)

# Create a view
view = catalog.create_view(
    identifier=("my_namespace", "active_users"),
    sql="SELECT user_id, username FROM users WHERE active = true",
    schema=schema,
    author="data_team",
    description="View of all active users in the system",
)

# Load a view
view = catalog.load_view(("my_namespace", "active_users"))
print(f"SQL: {view.sql}")
print(f"Schema: {view.metadata.schema}")

# Update execution metadata after running the view
catalog.update_view_execution_metadata(
    ("my_namespace", "active_users"), row_count=1250, execution_time=0.45
)
```

### Clock and signal triggers ⏰

A trigger is an EVENT plus a `runs-as`. A commit trigger is held by the dataset
whose commits fire it; a schedule or signal trigger has no dataset, so it is
held by the task it fires, under the task document's `triggers` subcollection.
Every trigger method takes `holder_kind="dataset"` (the default) or `"task"`.

```python
catalog.create_trigger(
    "ops.rollup", "hourly", target_task="ops.rollup", kind="task", author="alice",
    holder_kind="task", event_kind="schedule", schedule="0 * * * *",
    time_zone="Europe/London", window_source="src.events",   # OVER src.events
)
catalog.create_trigger(
    "ops.rollup", "on_demand", target_task="ops.rollup", kind="task", author="alice",
    holder_kind="task", event_kind="signal",
)
```

`window_source` names the dataset a run is windowed OVER: at fire time the
window is that dataset's head snapshot against the task's `last-window-to`,
with the same superseded/gap guard a commit-fired run has. Without one the
task must be windowless, refused at arming and again at fire time
(`window-unbound`). A task has one trigger whichever holder it lives under.

The dispatcher is `dispatch.opteryx`: `trigger_firing.fire_due_schedules(client)`
is its once-a-minute tick (a collection-group query for `next-due-at-ms <=
now`, each hit claimed with `claim_schedule_tick` so overlapping ticks cannot
double-fire), and `trigger_firing.fire_signal(catalog, task, caller)` is its
webhook. The caller of a signal is the event, not the context: the run assumes
the trigger's `runs-as` and the caller is recorded as `fired_by`. See
`SCHEDULE_SIGNAL_TRIGGERS_DESIGN.md`.

### Materialized views and triggers ⚡

A materialized view is a normal dataset document — readable as a table, with
its own location, schema and snapshots — that additionally carries
`dataset-type: "materialized_view"`, its defining SQL (versioned in the same
`statement` subcollection views use), and a `source-tables` list. Registration
also writes one **trigger** document under each source dataset:

```
{workspace}/{collection}/datasets/{source}/triggers/{trigger-name}
```

Triggers live in a subcollection, not on the dataset document, for the same
reason maintenance state does: `load_dataset` reads that document on every
call and must not pay for opt-in state.

The engine creates the backing table first (`CREATE MATERIALIZED VIEW` runs as
a CTAS), then registers it:

```python
catalog.create_materialized_view(
    "mart.daily_orders",
    "SELECT customer_id, COUNT(*) FROM sales.orders GROUP BY customer_id",
    source_tables=["sales.orders"],  # one refresh trigger per source
    author="data_team",
)

catalog.list_triggers("sales.orders")
# [{'name': 'refresh__mart__daily_orders', 'kind': 'materialized_view_refresh', ...}]
```

Refresh is event-driven. When a source dataset takes a **user-created** commit
(`append`, `overwrite`, `add_files`, `truncate_and_add_files`, committed
`truncate`), the commit path reads that dataset's triggers and enqueues one
refresh job per target view — see *Trigger firing* above for the environment it
needs. Housekeeping snapshots (compaction, expiration, `refresh_manifest`) are
excluded via `Snapshot.user_created`, so maintenance never re-runs every view.

Behavior worth knowing:
- **Invoker semantics**: the refresh runs as the author of the commit that
  fired it, with policies re-read at fire time. A committer without rights on
  the view gets a denied refresh — recorded in `last-refresh-status`, never
  silent.
- **Materialized views may stack.** A view's source can itself be a
  materialized view, and the chain refreshes a hop at a time: an inner
  refresh lands a user-created commit, which fires the triggers of the views
  above it. The cost is inherent to the shape — an outer view is always at
  least one refresh behind the inner one, and a failed inner refresh pins
  everything above it at the last good data (visible in each view's
  `last-refresh-status`).
- **The trigger graph is a DAG.** A node is a dataset, an edge `D -> V` a
  refresh trigger on `D` targeting view `V`. `create_trigger` walks forward
  from the target and refuses an edge that reaches back to the dataset the
  trigger sits on; `create_materialized_view` additionally rejects a cyclic
  `source-tables` graph up front, so a bad registration writes nothing at all.
  Enforcement is on the trigger graph rather than only on `source-tables`
  because that is the graph that fires — a trigger created directly, or left
  behind by a reconciliation that failed partway, is an edge no source list
  mentions. Two concurrent writes can each see an acyclic graph, so `fsck`
  re-walks it and reports `trigger-cycle` as the backstop.
- **Task triggers are outside all of this.** A task records its SQL and never
  what that SQL writes, so a task is not a node in the trigger graph and a loop
  that runs through one — a task fired by a commit on `a` whose statement
  writes `a` — is neither rejected nor detectable here.
- `drop_materialized_view` removes the triggers from every source before
  dropping the dataset; `drop_dataset` on a materialized view does the same
  cleanup, so a raw drop cannot strand triggers.
- Datasets carrying triggers, and materialized views themselves, **cannot be
  renamed** — trigger documents and source lists reference names. Drop and
  recreate instead.

#### The egress lock (`egress_protection`)

A workspace property that refuses **automated copies of that workspace's
datasets into a different workspace** — materialized-view refreshes, CTAS, and
`INSERT ... SELECT`. Enforced in the catalog and, since the engine wiring
landed, at bind time in opteryx-core before anything is written.

**On by default.** A workspace is restricted from birth: absent, null, and a
workspace with no `$properties` document at all all read as restricted, and only
an explicit `OFF` clears it. Sharing a workspace's data out is a decision
someone makes, not a state a workspace drifts into. It is an ordinary settable
property, not a reserved lifecycle field:

```sql
ALTER WORKSPACE ichnos SET egress_protection TO OFF;  -- opt out
ALTER WORKSPACE ichnos SET egress_protection TO ON;   -- opt back in
```

`deletion_protection` shares the same tri-state, default-on semantics
(`_guard_is_on`): a workspace is protected from birth, and deleting one is a
deliberate two-step — turn the flag off, then delete. For both guards only an
explicit falsey value clears them, so a hand-written `"OFF"` string keeps the
guard on rather than silently clearing it. Fail-closed is the right way for a
default-on flag to be wrong.

```python
catalog.is_egress_restricted()  # this workspace
catalog.is_egress_restricted("ichnos")  # any workspace in the database
catalog.enforce_egress_policy(  # the shared gate; raises EgressRestricted
    source_workspaces=["ichnos"],
    destination_workspace="sales",
    operation="create table sales.mart.copy",
)
```

The **source** workspace's flag decides, never the destination's — the property
protects data *leaving*. A copy that stays inside the source workspace is not
egress and is unaffected whatever the flag says, which is what makes a default-on
setting liveable: the ordinary same-workspace view or CTAS never touches it.

It is checked at both materialized-view creation and every refresh, because a
workspace can be restricted long after a view that reads it was registered, and
each refresh writes a fresh copy. A refresh blocked this way surfaces like any
other fire failure: alert, audit record, `last-fired-status: egress-blocked`, and
the commit that fired it is untouched.

#### SECURE — the sanctioned exemption

Clearing the lock is all-or-nothing: `ALTER WORKSPACE ichnos SET
egress_protection TO OFF` unlocks every copy out of `ichnos`, for everyone,
until somebody puts it back. Where the copy that has to be allowed is one known
statement — a platform pipeline moving billing events into another workspace,
say — marking that object SECURE is the narrow form: one named object, into
named destination workspaces, withdrawable on its own, with the lock left on for
everything else.

```python
# Run against the SOURCE workspace - the one being copied out of.
ichnos.mark_secure("ws.ops.billing_events_ingest", ["platform"], author="owner")
ichnos.list_secure()          # everything ichnos has sanctioned, with who and when
ichnos.clear_secure("ws.ops.billing_events_ingest", author="owner")
```

**Only the source can sanction, and that is structural.** The record lives in
the SOURCE workspace's `$properties`, under `secure_objects`, and a handle only
ever writes its own workspace's properties. A flag stored on the object would be
settable by whoever may edit the object — the party the lock protects against —
which makes it self-granting and the rule advisory. Here the destination cannot
sanction a copy into itself however hard it tries.

**Both the object and the destination must match.** A task's `writes` can be
changed by redefining it, so an exemption naming only the object would follow
that redefinition into a workspace its source never agreed to.

Anything malformed reads as NOT sanctioned. This is the permitting half of a
default-closed rule, so it has to be the conservative one about records it does
not understand — the opposite of `_guard_is_on`, and for the same reason.
`secure_objects` is a reserved property for that reason too: it is shaped, and an
unshaped one fails open, so it goes through `mark_secure` (which validates) or it
does not go.

**What it is not: containment.** Anyone with `reader` can still `SELECT` the
data and paste it wherever they like, and nothing here prevents that — it is
leaky by construction, in the way a VPC Service Controls perimeter is an egress
boundary rather than a permission. What it stops is the *systematic, automated,
recurring* copy: the standing view or CTAS that keeps a full mirror of someone
else's data fresh forever off the back of one read grant. Do not describe it,
or rely on it, as anything stronger.

**Where this stands relative to the feature it guards.** Cross-workspace MV
sources are not representable yet — `_relative_identifier` collapses a foreign
prefix into a relative name, and opteryx-core's `register_materialized_view`
rejects one outright — but they are planned, and this is the guard waiting for
them. That ordering is the point: because the flag defaults to ON, the day a view
can read another workspace it needs that workspace's owner to have said yes
first, and there is no release in which the capability ships ahead of the
boundary. Today the MV gate fires for a caller driving this library directly with
a foreign-qualified source; the load-bearing caller is CTAS, in the engine, which
is not wired up yet.

Notes about behavior:
- `create_dataset` will try to infer a default GCS location using the provided `gcs_bucket` property if `location` is omitted.
- `register_table` validates that the provided `metadata_location` points to an existing GCS blob.
- Views are stored as Firestore documents with complete metadata including SQL, schema, authorship, and execution history.
- Table transactions are intentionally unimplemented.

## Development & Linting 🧪

This package includes a small `Makefile` target to run linting and formatting tools (`ruff`, `isort`, `pycln`).

Install dev tools and run linters with:

```bash
python -m pip install --upgrade pycln isort ruff
make lint
```

Running tests (if you add tests):

```bash
python -m pytest
```

## Compaction 🔧

This catalog supports small file compaction to improve query performance. See [COMPACTION.md](COMPACTION.md) for detailed design documentation.

### Quick Start

```python
from opteryx_catalog import OpteryxCatalog
from opteryx_catalog.catalog import DatasetCompactor

catalog = OpteryxCatalog(workspace="my_workspace", gcs_bucket="my-bucket")

dataset = catalog.load_dataset("my_collection.my_dataset")

# `strategy=None` auto-detects: 'performance' when the dataset has a usable
# sort order, otherwise 'brute'.
compactor = DatasetCompactor(dataset, author="me")

# Each compact() call performs ONE read -> select -> execute -> commit pass.
# dry_run=True returns the plan dict (what would be compacted, and why);
# dry_run=False returns the committed Snapshot. Either returns None when
# nothing clears the size thresholds.
plan = compactor.compact(dry_run=True)
if plan is None:
    print("nothing to do")
else:
    print(plan["type"], plan["reason"])
    snapshot = compactor.compact(dry_run=False)
```

Scheduling is handled by the `xb500.opteryx` housekeeping service (Cloud
Scheduler → `/housekeeping/trigger_compaction`), which walks allowlisted
collections and audits every dataset it evaluates. See
[COMPACTION.md](COMPACTION.md).

Rules `brute` and `sort_aware` are independent. To attempt both in one tick,
call `compact()` twice in series rather than chaining them in a single call:

```python
compactor.compact(rule="brute")
compactor.compact(rule="sort_aware")
```

### Configuration

Compaction sizing is set by module constants in
`opteryx_catalog.catalog.compaction`, not by dataset properties — the target
output size is `TARGET_SIZE_BYTES` (4 GB), with `MIN_SIZE_BYTES` (3.5 GB) as
the lower bound of the acceptable band and `MAX_SIZE_BYTES` (4.1 GB) a hard
cap. The memory budget is the one runtime-tunable value, via the
`OPTERYX_COMPACTION_RAM_MB` environment variable (default 16384).

## Limitations & KNOWN ISSUES ⚠️

- No support for dataset-level transactions. `create_dataset_transaction` raises `NotImplementedError`.
- The catalog stores metadata location references in Firestore; purging metadata files from GCS is not implemented.
- This is an opinionated implementation intended for internal or controlled environments. Review for production constraints before use in multi-tenant environments.

## Contributing 🤝

Contributions are welcome. Please follow these steps:

1. Fork the repository and create a feature branch.
2. Run and pass linting and tests locally.
3. Submit a PR with a clear description of the change.

Please add unit tests and docs for new behaviors.

---

If you'd like, I can also add usage examples that show inserting rows using PyIceberg readers/writers, or add CI testing steps to the repository. ✅
