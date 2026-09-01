# Tasks are Windowed, Materialized Views are Rewrites — Delta Semantics and the One-Trigger Rule

Status: agreed 2026-09-01 (design discussion, Justin + Claude). IMPLEMENTED
2026-09-01, except the "Related gaps" section below, which remains separate
work: the one-trigger rule and its back-pointer in `create_trigger` /
`drop_trigger`, the window guard in `_fire_task`, the refusal of a bare
`EXECUTE` of a windowed task in `plan_execute`, and the migration sweep
(`scripts/find_multiply_triggered_tasks.py`, which must be run and its findings
unwired - the rule cannot retroactively refuse what is already stored).
Companion to: `MATERIALIZED_VIEWS_TRIGGERS_PLAN.md` (the trigger substrate this
builds on). The workflows UI's view of these same gaps is
`web.opteryx/WORKFLOWS-DESIGN.md` §2.

## The flaw

A task's unattended run is windowed: `_fire_task` binds the committing
snapshot and its parent into the statement at fire time —

```
EXECUTE <task> USING <parent_version> AS parent_version,
               <current_version> AS current_version
```

— so a statement can process the delta of a commit rather than rescanning the
world. The binding is deliberate and right (see `_fire_task`'s docstring:
execution is asynchronous, so a window resolved at pickup time would drift
against queued and retried fires).

The flaw is that **nothing constrains how many sources feed a windowed task**.
Triggers on datasets A and B may both name the same task, and each fire binds
its own dataset's versions into the same two parameter names:

```sql
CREATE TASK t ON a AS INSERT INTO out SELECT * FROM a WHERE v > :parent_version;
CREATE TRIGGER also_b ON b EXECUTE t;   -- accepted today
```

Every fire from `b` now runs `t` with `b`'s snapshot ids in a predicate over
`a`'s versions. Snapshot ids from different datasets are not comparable, the
statement has no way to tell whose it was handed, and the result is plausible
wrong rows — no error, ever.

The damage extends to the task's own bookkeeping. `last-window-to` is a single
scalar on the task document, stamped on success. With two sources it holds
interleaved ids from two incomparable sequences, which is why it is currently
only a breadcrumb: any *guard* built on it (skip superseded runs, widen over
gaps) would be unsafe — if A's ids happen to run higher than B's, every B run
is skipped as "superseded" forever. Silent data loss, keyed on the accident of
which dataset's id sequence is numerically larger.

`EXECUTE`'s `USING` arguments are generic named constants substituted into the
statement text (`logical_planner.py`, `plan_execute`). The window can therefore
appear in any predicate, including one over joined columns — so **the engine
cannot reliably infer which table a delta applies to** from the AST. Whatever
fixes this must come from declared structure, not inference.

## Options considered

1. **A windowed statement declares a single delta table**, and wiring is
   constrained to match. Accepted, in the stronger form below.
2. **Pass 'latest' for every table the statement reads**, with a per-(task,
   source) frontier to derive each table's delta. Rejected: multi-way
   incremental algebra (ΔA ⋈ B ∪ A ⋈ ΔB double-counts ΔA ⋈ ΔB; aggregates need
   retractions) cannot be expressed in a plain SQL statement and cannot be
   checked by the system — handing users a delta vector invites plausibly-wrong
   pipelines no error will catch. Systems that do this properly own the
   maintenance plan themselves; the only surface from which Opteryx could ever
   generate one is the *declarative* form, which is the materialized view.
3. **Pass no versions; the statement resolves its own window at execution.**
   Rejected for the reason already written into `_fire_task`: execution-time
   resolution reintroduces exactly the drift fire-time binding was built to
   prevent. The only sound variant needs transactional frontier claims (CAS
   before run), which is heavy machinery replacing something that works.

## The decision: two kinds, honestly different

The earlier instinct — collapse materialized views into "a trigger plus a CTAS
task" — is dropped. The mechanical overlap is real (both fire through the same
loop, both submit references not text, both version statements the same way),
but the *processing model* differs, and the flaw above is the proof:

|                      | **Task**                                | **Materialized view**                   |
| -------------------- | --------------------------------------- | --------------------------------------- |
| Processing model     | **Delta / increment** — consumes the window | **Rewrite** — wholesale re-derivation |
| Triggers             | **Exactly one**                         | One per source, system-reconciled       |
| Window parameters    | Bound from its one trigger's source     | On the contract, never consumed         |
| Suspension           | On the trigger (its only one)           | On the view — atomic across N triggers  |
| Unattended identity  | The trigger's `runs-as`                 | The view's pinned owner                 |

A rewrite is order-N-sources because it reads everything fresh; which commit
fired it is irrelevant to what it produces. A delta is order-one-source by
construction: the window *is* a statement about one dataset's version sequence.
The trigger-count rule falls out of the semantics rather than being a policy
bolted on.

### The one-trigger rule

**A task may have at most one trigger.** Flat rule, all tasks — not just
window-consuming ones.

Why flat rather than the minimum (windowed-only): a flat rule needs no
statement classification at trigger-creation time, keeps the mental model one
sentence, and removes the ambiguity *by cardinality* — with one source there is
nothing to declare and nothing to mis-bind. `CREATE TASK … ON <table>` already
plants exactly one trigger ("the same bargain CREATE MATERIALIZED VIEW
strikes" — `relation_management.pyx`); this rule makes that the ceiling, not
just the convenience.

What it deliberately reverses — recorded so nobody rediscovers these as
regressions:

- `pre_parse.py`'s comment "a task's triggers are independent of each other, so
  pausing one is a coherent thing to want" and the `runs-as` rationale "one
  task fired by two triggers can legitimately run as two different principals".
  Both were written when N triggers per task was the model. Under this rule a
  task has one unattended identity and one pause point, which is simpler and
  loses nothing that the fan-in cost (below) doesn't already price in.

What it costs: fan-in. "Run t when either A or B changes" is no longer one
task. The honest spellings are two tasks (duplicated statement, each correctly
windowed on its own source) or a materialized view (if the work is a
derivation, it was a rewrite all along). This cost is accepted: the fan-in
case was exactly the broken case.

### Rules and enforcement points

1. **`create_trigger` (catalog) refuses a trigger for a task that already has
   one.** Error names the existing trigger and its source: *"task t is already
   fired by <name> ON <source>; a task has one trigger — its window is that
   source's version sequence."*
2. **A window-consuming statement with no trigger is legal** — it is a
   manual-only task, run by a person with explicit `USING`. The constraint
   binds wiring, not authorship.
3. **Manual `EXECUTE` of a window-consuming task without `USING` is refused**,
   not defaulted. Defaulting quietly to (mark, head) is how a hand-run doubles
   as an unlogged catch-up.
4. **Materialized views are untouched**: N triggers, reconciled on redefine,
   view-level suspend, owner identity — all as shipped.

Mechanics for rule 1: triggers live in subcollections under their *source*
datasets, so "does task t have a trigger" is a reverse lookup. Store a
back-pointer on the task document (`trigger: {source, name}`), written in the
same Firestore transaction as the trigger document — transactions across
documents are available and already the standard here for exactly this
partial-failure shape. A `collection_group` sweep (the pattern
`RELATIONSHIPS_SUBCOLLECTION` already uses) is the verifier, not the hot path.

`DROP TASK`'s deliberate non-sweeping of triggers is unchanged; with one
trigger the orphan case gets simpler to see, not different in kind.

### The window guard, now safe

With a single source, `last-window-to` is coherent as-is — one scalar, one
version sequence — and the fire path can finally use it as a guard rather than
a breadcrumb:

- **Superseded:** `current_version <= last-window-to` → skip, stamp
  `superseded`. Catches stale queued fires that dedup's window missed.
- **Gap:** commit's `parent_version > last-window-to` → bind
  `parent_version = last-window-to` instead, so the run covers the gap a
  failed predecessor left. Stamping only on success (already the behaviour) is
  what keeps the gap visible until it is covered.

Both comparisons are within one dataset's sequence, which is what makes them
meaningful at all.

### Migration

Rule 1 needs a sweep before it can be strict: enumerate triggers
(collection-group over the triggers subcollections), group by target task,
and surface every task with two or more. **These are live bugs, not
configurations to grandfather** — each is corrupting its own `last-window-to`
today if its statement is windowed, and ambiguous even if not. Expected count
is small; triggers shipped 2026-08.

## Related gaps this discussion surfaced (separate work)

- **Materialized views have no supersession guard at all.**
  `mark_materialized_view_refreshed` records a wall-clock timestamp, no
  version. Two refreshes race, the older-window one lands second, the view is
  stale while stamped fresh. The source's version is the wrong token for a
  rewrite; "superseded" for a rewrite is a property of the *target* — it needs
  a per-target completion token (target snapshot observed at submit, or a
  monotonic refresh id) checked before the CoRTAS commits.
- **`information_schema.triggers` still forks meaning by kind** for two
  columns: `suspended_at` reports only the trigger flag (a view suspended via
  `ALTER MATERIALIZED VIEW … SUSPEND` shows every wire live), and `runs_as` is
  null for refresh rows though the identity (the view's owner) is real and
  resolvable. Both are projection fixes; the reader already opens the MV
  record. See `web.opteryx/WORKFLOWS-DESIGN.md` §2.1 and §2.10 for the client
  side of the same asymmetry.
- **`writes` / `reads` derivation** (in flight, separate change): with reads
  derived the same way, a task gets the egress check MVs already have, and the
  wholesale/windowed classification used informally throughout this doc
  becomes a stored fact from the same AST pass.
