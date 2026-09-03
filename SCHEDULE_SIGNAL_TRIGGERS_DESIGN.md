# Clock and Signal Triggers, and dispatch.opteryx — The Dispatcher That Did Not Exist

Status: agreed 2026-09-02 (design discussion, Justin + Claude). IMPLEMENTED
2026-09-02 across `opteryx-catalog` (holder generalisation, `schedules.py`,
`fire_signal` / `fire_due_schedules`, the tick claim, integrity), `opteryx-core`
(the grammar, binder, both connectors, `information_schema.triggers`),
`opteryx-access` (the SIGNAL action) and the new `dispatch.opteryx` service.
Not yet done: jobs.opteryx reading `client_info.trigger.holder` (§7) - until
it does, a task-held trigger's run cannot resolve its identity there and is
refused, which is the safe direction; and the deployment itself (§11 step 5).
Companion to: `TASK_WINDOWING_DESIGN.md` (the one-trigger rule
and the window guard this reuses) and `MATERIALIZED_VIEWS_TRIGGERS_PLAN.md`
(the trigger substrate). The engine currently refuses these triggers by name
(`pre_parse.py`, `_TRIGGER_EVENT_LEAD`) with the message "Clock and signal
events need a dispatcher that does not exist". This document is that
dispatcher.

## 1. The principle: a trigger is an event plus an identity; the work has neither

Settled before any of the mechanics below, because the mechanics follow from it.

- **A trigger is an event and a `runs-as`.** The event is what fires it: a
  commit to a dataset today, a clock tick or an inbound signal after this. The
  event itself has no identity to offer — a clock has no user, and a
  committer was never the context either — so the trigger carries the
  identity that the work it starts will assume.
- **The work has no identity.** A task (and, once the in-flight change lands,
  a materialized view) is a statement. It receives an execution context from
  whatever started it: the submitter for a hand-run `EXECUTE` or `REFRESH`,
  the trigger's `runs-as` for a fired run.
- **The caller of a signal is the event, not the context.** Whoever hits the
  webhook is recorded as `fired_by`, exactly as a committer is today, and the
  run still assumes the trigger's `runs-as`. Authorizing a signal is therefore
  "may this principal fire this trigger", which is narrower than "may this
  principal run this work" — and is what lets a low-privilege service account
  kick off a pipeline that runs as someone else.

Tasks already follow this model (`create_trigger` pins `runs-as`;
`create_task` stores no identity; `_fire_task` refuses a trigger with none).
Materialized views are being brought into line separately (identity moves
from the view onto its refresh triggers). Nothing in this document changes
where identity lives.

## 2. The trigger record

### 2.1 Where it lives

Triggers are stored under the object that holds the event. A commit trigger
lives under its source dataset, because the dataset is where the commit
happens. A schedule or signal trigger has no source dataset: **it lives under
the task it fires**, in a `triggers` subcollection of the task document.

Consequences:

- The "holder" of a triggers subcollection becomes a dataset OR a task. The
  catalog methods keyed on `dataset_identifier` today — `list_triggers`,
  `mark_trigger_fired`, `claim_trigger_fire`, `release_trigger_fire`,
  `suspend_trigger`, `resume_trigger`, `set_trigger_owner`, `drop_trigger` —
  take a holder reference instead. Commit triggers are untouched by this.
- The one-trigger rule already fits. The task's back-pointer
  (`trigger: {source, name}`) names its single trigger; for a task-held
  trigger `source` is the task's own qualified identifier. A task cannot be
  both commit-fired and schedule-fired, which is correct: its window is one
  sequence, and §3 says which.
- `integrity.py`'s sweep, `information_schema.triggers` and `SHOW TRIGGERS`
  learn to walk task-held triggers as well as dataset-held ones.

### 2.2 The fields

Every trigger keeps what it has: `name`, `kind` (what it runs), the target,
`runs-as`, `created-by`, suspension, `min-interval-seconds` and its claim,
`last-fired-*`. It gains an event description:

| field               | commit           | schedule                         | signal   |
|---------------------|------------------|----------------------------------|----------|
| `event-kind`        | `commit`         | `schedule`                       | `signal` |
| `schedule`          | —                | cron expression, five fields     | —        |
| `time-zone`         | —                | IANA name; default `UTC`         | —        |
| `next-due-at-ms`    | —                | see §5.2                         | —        |
| `window-source`     | (the holder)     | optional dataset, see §3         | optional |

`kind` keeps meaning what the trigger runs (`task`, `materialized_view_refresh`)
because the firing path and `information_schema` branch on it. The event is a
separate axis.

### 2.3 Grammar

```sql
CREATE [OR REPLACE] TRIGGER <name>
    ON SCHEDULE '<cron>' [AT TIME ZONE '<zone>'] [OVER <table>]
    EXECUTE <task>;

CREATE [OR REPLACE] TRIGGER <name>
    ON SIGNAL [OVER <table>]
    EXECUTE <task>;

ALTER TRIGGER <name> ON <task> SUSPEND | RESUME | OWNER TO <principal>;
DROP TRIGGER [IF EXISTS] <name> ON <task>;
```

`ON <task>` in `ALTER`/`DROP` names the holder, the way `ON <table>` does for
a commit trigger. Creating any of these requires **AUTOMATE on the task** — the
tier `opteryx_access.actions` already assigns to "standing automation on a
relation", for the reasons given there.

`OR REPLACE` on a schedule trigger recomputes `next-due-at-ms` from the new
expression. `runs-as`, suspension and the claim are preserved across
re-registration, as they are today.

## 3. Windows: the design decision

A commit-fired run binds its window from the commit (`_fire_task`:
`parent_version`, `current_version`). A clock has no commit, and `plan_execute`
refuses to run a windowed task with no window. Two cases, decided by the
presence of `OVER`.

### 3.1 No `OVER`: the task must be windowless

The statement may not use `:parent_version` or `:current_version`. Enforced at
both ends of the trigger's life, as the egress rule is:

- **At arming**, `create_trigger` reads the task's statement and refuses if it
  consumes a window. Refused before any write, so nothing is left behind.
- **At fire time**, because the statement can be replaced after arming, a
  windowed statement under a sourceless trigger is a fire failure recorded as
  `window-unbound`, alerted like any other.

The run is `EXECUTE <task>` with no `USING`.

### 3.2 `OVER <table>`: windowed over the source's head

The window is bound at fire time from the named dataset:

```
current_version = head snapshot of <table> at fire time
parent_version  = task.last-window-to, or NO_PARENT_VERSION_FLOOR if none
```

Then the existing guard applies unchanged:

- `current_version <= last-window-to` — nothing landed since the last
  successful run. Recorded as `superseded` on trigger and task, no job. This is
  the normal outcome of most ticks on a quiet dataset and is deliberately not
  an error.
- A failed run leaves `last-window-to` where it was, so the next tick's window
  widens back over the gap. This is the gap-filling behaviour
  `TASK_WINDOWING_DESIGN.md` built, and it is exactly what a scheduled
  batch wants.

Because the window spans many commits rather than one, a compaction can sit
INSIDE it. That is already the case for a gap-widened commit window, and the
same answer applies: the windowed read is by version range, and a rewritten
file inside the range reads as its rows once.

`OVER` names a dataset in this workspace. Reading it at fire time is the
run's problem, as `runs-as`, in the engine — the dispatcher only reads the
head snapshot id from the catalog, which the trigger's creator already had to
be able to name.

### 3.3 Not in this version

- **Time-valued parameters** (`:window_start`, `:window_end`). A statement
  that wants wall-clock arithmetic can do it itself. If a real need appears,
  they can be added as a second parameter vocabulary with the same
  both-ends check.
- **Signal payloads bound as parameters.** The binding path is injection-safe
  (`parameter_dict_binder` substitutes after parse) but "any caller can
  choose the window" is a policy question this version does not open.

## 4. The service: `dispatch.opteryx` at `dispatch.opteryx.app`

A small Cloud Run service, **min instances 1, max instances 1, a quarter of a
CPU**, that does three things and holds no state:

1. **Signal endpoint** — authenticates the caller, checks SIGNAL on the task,
   calls the library's fire function, returns the outcome.
2. **Tick endpoint** — `POST /api/v1/tick`, called once a minute by a Cloud
   Scheduler job, runs the library's due-scan function inside the request.
3. **Health endpoint** — reports the time of the last completed tick.

Everything with logic in it lives in `opteryx_catalog.trigger_firing`, where it
is tested against the same fake Firestore the trigger tests already use. The
service is a FastAPI app and a Dockerfile. It authenticates to jobs as
`federator`, exactly as the commit path does (`_federator_token`).

### 4.1 The clock is a request, not a thread

The first design of this section had the clock as a background thread in an
always-on instance: "min instances 1, CPU always allocated, a fraction of a
CPU standing". Cloud Run does not sell that. A fractional CPU is only
available with request-based CPU allocation — `Total cpu < 1 is not supported
with cpu always allocated`, in either execution environment — and a
request-based instance is throttled to nothing between requests, which
starves a background thread. The choice was therefore a whole always-on core
(about $45 a month for something idle 59 seconds in every 60) or a fraction
of a core whose CPU is on only while a request is in flight (about $2).

The fraction, with the tick made into a request. Cloud Scheduler POSTs
`/api/v1/tick` once a minute with an OIDC identity token for the runtime
service account; the endpoint verifies the token against Google's
certificates, checks the audience and that the account is the one
configured, and runs `fire_due_schedules` synchronously before answering.
The in-process loop thread still exists (`DISPATCH_CLOCK_ENABLED`) for local
runs and for any platform that keeps the CPU on; in production it is off.

Nothing that made the loop safe lived in the thread, which is why this is a
one-flag change and not a redesign:

- **State lives in Firestore, never in memory.** A tick asks which triggers
  have `next-due-at-ms <= now`. A restart, a redeploy, a stalled minute or a
  Scheduler call that never arrives costs nothing; the next tick finds the
  same due records. A slot is lost only if no tick lands for the whole
  minute, and even then the record is still due when one does.
- **Every fire is claimed transactionally** (§5.2). Two ticks landing at once
  — a rollout serving two revisions, a Scheduler retry overlapping a slow
  tick — cannot double-fire. Max instances 1 keeps one place for the tick to
  land and one heartbeat to read; it is an optimisation, not what
  correctness rests on.
- **Concurrency 1**, because Cloud Run requires it for a fractional CPU. A
  tick and a webhook arriving together queue for a few hundred milliseconds.
  The request timeout is generous (five minutes) so a tick with a backlog
  behind it drains in one call rather than being cut off mid-fire.
- **A heartbeat.** A completed tick stamps a document; a tick that raised
  does not, so `/health` goes stale (503) rather than lying, and the existing
  alerting fires on a stale stamp. Scheduler records its own failed runs
  against the job as well.
- **The endpoint fails closed.** With no allowed caller configured it refuses
  every request before reading the token. An open tick endpoint would let
  anyone drive the clock; a closed one on a misdeployed service shows up as a
  stale heartbeat within three minutes.

### 4.2 Why not Cloud Tasks, and what became of the case against Scheduler

The earlier version of this section argued against Cloud Scheduler on two
grounds: the tick endpoint would have had to live on jobs, and it added a
resource to keep in step. Neither survives contact with the pricing above.
The endpoint lives on dispatch, where the clock was always meant to be, and
the Scheduler job is one `gcloud scheduler jobs create|update` step in the
same deploy workflow as every other piece of the service's configuration, so
it cannot drift from the service it drives.

The signal handler does not enqueue Cloud Tasks. Jobs is the one control point
through which work reaches the workers — it resolves the trigger's `runs-as`,
that principal's policies and billing, and the dedup window — and the commit
path deliberately posts straight to it. A Cloud Task in front of an HTTP call
that takes milliseconds would either bypass jobs, which the identity model
forbids, or buffer a call that does not need buffering. If jobs is down, the
fire is recorded as a failure on the trigger and alerted, which is what a
commit fire does today. A retry buffer can be added later if that proves too
brittle in practice.

### 4.3 Naming

`dispatch` because the thing being built is the dispatcher the engine's
refusal names. Peers are `jobs.opteryx.app`, `authenticate.opteryx.app`,
`router.opteryx.app`. `router` and `hook` are already spoken for by the
OUTBOUND webhook sender (`opteryx_catalog/webhooks`), so neither should be
reused for the inbound surface. `signals.opteryx.app` was the runner-up and
undersells the clock half.

## 5. The clock

### 5.1 The due scan

A Firestore **collection-group query** over every `triggers` subcollection:

```
collection_group("triggers")
    .where("event-kind", "==", "schedule")
    .where("next-due-at-ms", "<=", now_ms)
```

This spans every workspace in the database, which is what one clock wants. The
catalog already uses collection-group queries (relationships), so the pattern
and the composite index handling exist. Suspended records are filtered in
code rather than in the query (Firestore cannot express "is null" cheaply,
and a suspended trigger is still stamped `suspended` so the suppression is
visible, as `fire_triggers` does today).

### 5.2 The claim: compare-and-advance

For each due record, one transaction:

1. Re-read the record. If `next-due-at-ms` is no longer `<= now`, another
   loop took it; skip silently.
2. Compute the next occurrence strictly after `now` from `schedule` and
   `time-zone`, and write it to `next-due-at-ms`. Record how many occurrences
   fell between the old value and the new one.
3. Return the claim.

This is `claim_trigger_fire`'s shape — read and stamp in ONE transaction, so
two concurrent claims resolve to one grant with no window between them. The
fire then proceeds under the existing `min-interval-seconds` floor as well,
which is redundant for a clock but keeps every fire path uniform.

**A missed slot fires once.** A trigger due an hour ago after an outage fires
now and advances to the next future occurrence; it does not replay every
missed slot. An `OVER` window covers the gap by construction (§3.2), and a
windowless task has nothing to replay. The skipped count goes in the audit
record so the outage is visible where someone looks for the runs.

**RESUME recomputes from now.** A trigger suspended for a week does not fire
the moment it is resumed on a `next-due-at-ms` that went stale. Same rule for
`OR REPLACE`.

**If the fire raises**, the claim is released by restoring the previous
`next-due-at-ms` — the tick was not consumed. Mirrors `release_trigger_fire`.
A hard kill between the advance and the submission loses that one slot; the
next occurrence still fires. This is at-most-once per slot, which matches the
existing stance that a fire that did nothing must consume nothing, and is
preferable to fire-then-advance, which double-fires under overlap.

### 5.3 The fire

The trigger document's path names its workspace and task. The loop builds a
per-workspace catalog handle and calls the task fire path with a snapshot
bound per §3, `fired_by="schedule"`, and the same suspension, egress and
failure-stamping as the commit path. `fire_triggers`'s never-raise,
alert-per-trigger behaviour applies: one bad schedule does not stop the rest of
the tick.

Cron parsing: `croniter`, five-field expressions, timezone via `zoneinfo`.
Validated at creation so a malformed expression is refused before it is
stored.

## 6. The signal

```
POST https://dispatch.opteryx.app/invoke/{workspace}/{collection}/{task}
Authorization: Bearer <principal token>
```

`/invoke` is for firing; `/api` is for management, including the clock's
tick. The trigger is not named in the path: a task has one, and if it is not
a signal trigger the request is a 404.

- **Authentication** is the existing bearer token from `authenticate.opteryx`.
  The caller is a principal, human or service account.
- **Authorization** is a new action in `opteryx_access.actions`:
  `"SIGNAL": {"writer", "owner"}` on the task. A reader may not fire work;
  a writer may, and does not need AUTOMATE, which stays owner-only for
  creating the trigger in the first place.
- **The run assumes the trigger's `runs-as`.** The caller is recorded as
  `fired_by` and in the audit record. See §1.
- **Dedup** is the trigger's `min-interval-seconds` floor via
  `claim_trigger_fire`, which is the existing answer to a burst — here, a
  retrying webhook sender.
- **Window** per §3, identical to the clock.
- **No payload** in this version (§3.3). The body is ignored.

Responses:

| outcome                                   | status | body                       |
|-------------------------------------------|--------|----------------------------|
| run submitted                             | 202    | `execution_id`, `enqueued` |
| throttled / suspended / superseded        | 200    | the status, no run         |
| trigger not found, or not a signal trigger| 404    |                            |
| caller lacks SIGNAL on the task           | 403    |                            |
| fire failed (owner missing, egress, jobs) | 502    | the recorded status        |

Throttled, suspended and superseded are 200 because they are recorded
outcomes the trigger chose, not caller errors, and a sender that retries on
non-2xx must not retry them.

### 6.1 Signed URLs, for senders that cannot hold a token

A GitHub workflow or a SaaS webhook cannot do an OAuth flow; it can call a
URL. So a task's owner can mint one:

```
GET|POST https://dispatch.opteryx.app/invoke/{workspace}/{collection}/{task}/by/{identity}/{signature}
```

The signature is HMAC-SHA256 over the task's qualified name and the identity,
under a TOKEN the platform generates and keeps on the signal trigger
(`signal-token`; `information_schema.triggers` reports only when it was
rotated). The URL therefore fires exactly one task and is attributed to
exactly one identity - `fired_by` - and the sender holds nothing else. The
identity is attribution, not authentication: the authority is the owner's who
minted it, so minting (`POST /api/v1/tasks/{ws}/{coll}/{task}/mint`),
rotating and revoking (`POST`/`DELETE /api/v1/tasks/{ws}/{coll}/{task}/token`)
are AUTOMATE-tier, while firing through the URL needs no bearer token at all. The run assumes the trigger's
`runs-as`, is throttled by the same floor and windowed the same way, and every
refusal on the public door is the same 404 so the URL space cannot enumerate
tasks.

The scope of the token is one task. Rotating it invalidates every URL minted
from it, which is the remedy for a URL that has leaked - and a URL does leak,
into the request log, on every call. Per-identity
revocation, if it is ever wanted, is a revoked-identities list on the trigger
and no change to the URLs.

## 7. Jobs and the worker

Provenance today is `client_info.trigger = {source_dataset, trigger_name,
target_task, snapshot_id, fired_by}`, and jobs resolves the run's identity by
reading the trigger back through the source dataset. It gains one field:

```
client_info.trigger = {
    "holder":        "<workspace>.<collection>.<dataset or task>",
    "holder_kind":   "dataset" | "task",
    "trigger_name":  ...,
    "target_task":   ...,
    "snapshot_id":   ...,
    "fired_by":      "<principal>" | "schedule",
}
```

`source_dataset` is kept, populated for commit triggers, until jobs reads
`holder` — the two repos roll independently. Jobs resolves `runs-as` from the
trigger under the named holder and nothing else; a submission still cannot
name the actor or the payer. The worker's completion stamping
(`_stamp_fired_task`, `last-window-to` from the run's `current_version`) needs
no change.

## 8. Engine

- `pre_parse.py`: the two `CREATE TRIGGER` forms replace the refusal in
  `_TRIGGER_EVENT_LEAD`. `ON EVERY` and `ON EVENT` stay refused by name.
  `ALTER`/`DROP TRIGGER ... ON <task>` resolve the holder as a task when the
  name is one.
- `visit_create_trigger`: AUTOMATE on the task; the `OVER` table must exist
  and be in this workspace; the windowless check of §3.1.
- `information_schema.triggers`: `event_object_table` is the holder for a
  commit trigger and the `OVER` table otherwise (null if none); new columns
  `event_kind`, `schedule`, `time_zone`, `next_due_at`. `SHOW TRIGGERS`
  likewise.

## 9. Catalog

- Holder generalisation (§2.1) across the trigger methods; `create_trigger`
  gains `event_kind`, `schedule`, `time_zone`, `window_source`.
- `trigger_firing.py`: `fire_due_schedules(client, now_ms)` and
  `fire_signal(catalog, task, trigger_name, caller)`; `_fire_task` takes its
  window from a small binding function that covers both the commit form and
  the `OVER` form, so the guard has one implementation.
- `integrity.py`: task-held triggers in the sweep; a new finding for a
  schedule trigger whose `next-due-at-ms` is more than a day stale, which is
  the clock having stopped.
- `scripts/`: nothing to migrate; no existing record changes shape.

## 10. Failure modes

| failure                                  | outcome                                                         |
|------------------------------------------|-----------------------------------------------------------------|
| dispatch down for N minutes              | due triggers fire on return, once each; heartbeat alert fires   |
| two loops tick at once                   | one claim wins; the other skips                                 |
| jobs unreachable                         | claim released, `last-fired-status: error`, alert               |
| statement became windowed after arming   | `window-unbound`, alert, no run                                 |
| `runs-as` missing                        | `owner-missing`, as today                                       |
| cross-workspace write                    | `egress-blocked` at arming and at fire, as today                |
| quiet dataset under `OVER`               | `superseded` each tick, no run, no alert                        |
| malformed cron                           | refused at `CREATE`                                             |
| webhook retry storm                      | one run per `min-interval-seconds`, the rest `throttled` (200)  |

## 11. Order of work

1. **Catalog, no behaviour change:** holder generalisation, event fields,
   `information_schema` and integrity for task-held triggers.
2. **Engine and jobs:** grammar and binder; jobs resolving identity from
   `holder`.
3. **Signal path** in the library and the service. It exercises every new
   piece with no clock, and is the smaller half.
4. **Clock path:** due scan, compare-and-advance, the loop, the heartbeat.
5. **Deploy:** Cloud Run with min=max=1, a quarter CPU, request-based
   allocation and concurrency 1; a Cloud Scheduler job POSTing the tick every
   minute; `dispatch.opteryx.app` mapped; `croniter` added to the catalog's
   dependencies; the `triggers` collection-group index on `event-kind` and
   `next-due-at-ms`.

## 12. Decisions taken during implementation

- The keyword is `OVER <table>`; `FOR` and `OF` were the alternatives.
- A task has one trigger whichever holder it lives under: a task with a
  commit trigger refuses a schedule or signal and vice versa. "Fire on commit
  but also at least hourly" is served by a schedule `OVER` the same dataset,
  since a quiet tick is free.
- Trigger records carry `holder` and `holder-kind` explicitly, so a record
  read through a collection-group query (§5.1) says where it lives without
  parsing its path, and `event-kind` is a separate axis from `kind`.
- A schedule's `next-due-at-ms` is recomputed on `OR REPLACE` and on
  `RESUME`, never carried forward: the due instant is a function of the
  expression and now.
- The signal endpoint takes the trigger's name in the URL and checks it
  against the task's back-pointer before firing, so a stale webhook URL is a
  404 rather than a fire of whatever trigger the task has now.
- The token audience authenticate.opteryx mints is `opteryx`; the service
  defaults to it.
