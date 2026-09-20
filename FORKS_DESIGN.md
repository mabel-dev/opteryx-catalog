# Dataset Forks — Design

Status: **DEPLOYED** (2026-09-20). The catalog, the SQL surface, the
`LOAD SAMPLE` removal, and the `samples` workspace - all five scale factors,
152.4M rows. Verified against production: cloning all eight tables of
`tpch_sf10` (94.6M rows over a 2.9 GB upstream) took 54 seconds and wrote
0.06 MB, which is the manifests and nothing else. What staging found on the
way is in S12.1.

Built in opteryx-catalog (rollout step 1, S12): the ownership guard
(`catalog/ownership.py`, `SnapshotExpiration._delete_file`), the `fork` block
(`Fork`/`ForkSource`/`ForkTarget` on `DatasetMetadata`, persisted both ways),
the `forks/` registry (`register_fork` / `deregister_fork` / `list_forks`),
fork pinning in `pinned_snapshot_ids`, `clone_dataset` / `resync_fork` /
`detach_fork`, `SimpleDataset.fork_state` / `current_sequence_number`, and the
drop/rename rules. Tests: `test_storage_ownership.py`, `test_fork_pinning.py`,
`test_fork_clone_mechanics.py`. Nothing calls any of it yet - there is no
statement that reaches it, which is what makes the step deployable alone.

Built in opteryx-core (rollout step 2, S12): `CREATE TABLE ... CLONE` planned
off the parser's own `clone` field, `ALTER TABLE ... RESYNC [FORCE] | DETACH`
through a `pre_parse` intercept, the three binders (with the egress gate on
CLONE), the `RelationManagementNode` actions, `clone_relation` /
`resync_relation` / `detach_relation` / `fork_state` on `OpteryxConnector`,
`information_schema.forks`, statement classification (including collecting the
clone's SOURCE, without which a permission pre-flight checks only half the
statement), and the clause catalog entries that regenerate the editor grammar
and the docs. Tests: `tests/unit/planner/test_fork_statements.py`. The
pre-existing `test_parse_error_keywords` failure is fixed in passing - `LOAD`
and `SAMPLE` had been missing from the typo detector's list since LOAD SAMPLE
shipped, and `CLONE`/`RESYNC`/`DETACH` would have joined them.

Deviations from what is described below, all deliberate:

* **Rename refuses rather than follows** (S5.3). The v1 escape hatch named
  there, taken: both ends record the other by name, the other end is often a
  document in another workspace, and `save_dataset_metadata` writes documents
  whole with `set()` - a read-modify-write against one that is committing can
  destroy the commit. `DETACH` then rename.
* **No new `operation_type` values.** Clone and resync both commit through
  `truncate_and_add_files`, whose `truncate-and-add-files` type is already a
  `REWRITE_OPERATION` - which is exactly the provenance classification both
  need, so the standing `sources` list comes out as `[upstream]` rather than
  accumulating. Adding `clone`/`resync` to that closed vocabulary would buy a
  nicer history label and cost a trace through every reader of the field; the
  commit message carries the label instead.
* **Egress is enforced in the engine, not the catalog** (S4.1). The catalog
  executes and the engine decides, which is the split every other
  cross-workspace write here already uses (`assert_egress_allowed` is called
  from the binder, not from the write path).
* **RESYNC and DETACH do not re-check egress** (S8.2, S8.3). The reader was
  authorized to copy the upstream once, at CLONE; resyncing re-reads the same
  relationship rather than establishing a new one. Re-checking would let an
  upstream owner strand existing forks half-updated by turning the guard back
  on - which should stop NEW forks, and should not reach inside ones already
  made. DETACH copies bytes the caller can already read into storage they then
  pay for, so refusing it would leave a fork permanently unable to stand alone.
* **`ALTER TABLE ... RESYNC | DETACH` is intercepted in `pre_parse`**, not added
  to the dialect. S8.2 costed the ALTER TABLE form as "one more branch in a
  rewriter that exists" - but that rewriter is the Rust dialect, and the tag DDL
  it handles has arguments to smuggle through `SetTblProperties` where these two
  clauses have none. `_intercept_alter_task` and `_intercept_alter_workspace_secure`
  are the precedent for an ALTER form living in `pre_parse`.
* **`CREATE COLLECTION ... CLONE` was brought forward** from S8.4. It was
  optional there, with "one statement per table" as the fallback; removing
  `LOAD SAMPLE` made it necessary, because Studio's dialog loads eight tables
  and its whole model is one statement in the editor. The parser carries
  `clone` on `CreateSchema` exactly as it does on `CreateTable`, so it was the
  same work one level up. Views are not cloned - a view is a query over names,
  and copying one where those names mean something else reads the wrong data.

Scope spans four repos and this document is the one place the whole shape is
written down: the catalog (registry, GC invariants, resync — §3–§7), opteryx-core
(SQL, binding, `information_schema` — §8, §10), odata.opteryx (`$metadata`
terms — §10.2) and web.opteryx (Studio — §10.3).

## 1. Problem

`LOAD SAMPLE TPCH INTO ws.coll AT SCALE 1` copies 272 MB server-side and then,
to register what it copied, downloads ~457 MB and decodes every row group to
rebuild statistics the staged files were written with. At scale 10 that is a
2.9 GB copy followed by a 3.9 GB download. The copy is the cheap part; the
statistics rebuild is the whole of the wait, and it recomputes numbers that
already existed the moment the bundle was generated.

It copies because pointing a manifest at shared files is unsafe today:
`SnapshotExpiration` deletes orphaned data files **by the path in the manifest
entry**, with no check that the path is one the dataset domiciles
(`expiration.py`, `_delete_file` → `io.delete(path)`). Fork a dataset, compact
the fork, let the superseded snapshot age out, and the sweep deletes the
original's bytes for everyone.

That one gap is the reason for the copy. Everything else already reads and
writes in terms of *entries whose path may lie outside the dataset's location*:

- `rename_dataset` remaps only paths under `old_location` and returns anything
  else unchanged — "externally-referenced files" is already a named case.
- `DatasetDeepClean` and `drop_workspace` reclaim by **listing the dataset's own
  location prefix**; a borrowed file is never a candidate.
- `drop_dataset` leaves storage to reconciliation, which compares the bucket
  against the locations the catalog claims — also prefix-scoped.
- `add_files` accepts arbitrary paths.

So a fork is one missing invariant, a registry, and a statement.

## 2. Concepts

A **fork** is a dataset whose initial content is another dataset's content at
one snapshot, made without moving a byte: its first manifest lists the same
files the **upstream**'s manifest listed. The upstream snapshot it was taken
from is the fork's **base**.

The **upstream is the real relation, not a staging prefix.** Sample data
becomes ordinary datasets in a `samples` workspace (§9) and `LOAD SAMPLE` goes
away; "load the TPC-H sample" is "fork these eight tables".

Writes to the fork land in the **fork's own location**, exactly as any write
does. The borrowed files are never rewritten in place — a compaction on the fork
writes new files under the fork and retires the borrowed entries from its
manifest; a delete on the fork writes a delete vector under the fork; a
`TRUNCATE` on the fork drops every entry. Nothing a fork does can alter bytes it
does not domicile, which is what "any updates happen on the fork" means
mechanically.

Two derived states, computed on read and never stored (§6):

- **behind** — the upstream has user commits after the fork's base. The fork can
  be **resynced** (§7).
- **drifted** — the fork has user commits of its own since it was forked.
  Resyncing would discard them, so it is refused without `FORCE`.

A fork that is neither is **in sync**. Maintenance commits (compaction,
statistics refresh) on either side change neither state: the data is the same
data, and `previous_user_snapshot()` already encodes that rule.

The relationship is **one-way**. Nothing ever flows from a fork to its upstream.
There is no merge and no pull request; the upstream's owner never sees a fork's
edits. A fork is a private branch, not a contribution.

## 3. Data model

### 3.1 On the fork: the `fork` block of the dataset document

```
fork: {
  source: {
    dataset:          "samples.tpch_sf1.lineitem",   # fully qualified, always
    snapshot-id:      1758300000123,                  # upstream snapshot forked from - THE PIN
    sequence-number:  41,                             # that snapshot's sequence on the upstream
  },
  target: {
    sequence-number:  1,                              # this dataset's clone / last-resync commit
    last-sync-ms:     1758300400000,                  # when it was taken
  },
  forked-at-ms:  1758300400000,
  forked-by:     "justin",
}
```

Two anchors, one per side, and each side's state is *its current sequence
against its anchor* (§6). `source.snapshot-id` is what the upstream pins
(§5.1); `source.sequence-number` is the same snapshot as a comparator, stored
so that "is the upstream ahead of me?" is one integer against another rather
than a history walk. `target.sequence-number` is the fork's own clone or
resync commit, so "have I been edited since?" is the same comparison on the
other side. `RESYNC` rewrites both anchors and `last-sync-ms`; nothing else
ever touches them.

Carried on `DatasetMetadata` for the reason every field in that block is
carried: `save_dataset_metadata` writes the document whole with `set()`, and a
field it does not know is destroyed by the fork's next commit. Absent on a
plain dataset.

`source.dataset` is a **name**. The catalog has no stable dataset id — identity
is the identifier — so a rename of the upstream must follow this reference the
way `rename_dataset` already follows inbound relationships (§5.3).

### 3.2 On the upstream: the `forks` subcollection

One document per live fork, under the upstream's dataset document, beside
`tags` and `relationships`:

```
datasets/<name>/forks/<fork-doc-id>:
  fork:             "personal.justin.lineitem"     # fully qualified
  pinned-snapshot:  1758300000123                  # the fork's current base
  created-at-ms:    1758300400000
```

This is a **protected input** in exactly the sense `list_tags` is one:
expiration reads it to learn which snapshots it may not retire (§5.1). It has
the same failure rule as tags — an unreadable registry aborts expiration with
`ManifestProtectionError`; it is never answered with "no forks".

`pinned-snapshot` mirrors the fork's `source.snapshot-id` and moves with it on
every successful `RESYNC` (the fork now rests on a newer base, so the old one
may go); it is deleted with the fork.

The document id is the fork's fully-qualified name with `.` replaced, so
registering is idempotent and a lookup by fork name is a document get, not a
query. Cross-workspace forks are documents in *another workspace's* Firestore
tree; that is already how `mark_secure` and `egress_verdict` reach a foreign
workspace's `$properties`.

### 3.3 Manifest entries: unchanged

A borrowed entry is any entry whose `file_path` is not under the dataset's own
`location`. No flag, no column. `rename_dataset` already defines "external" this
way and it is the one definition that cannot go stale: it is a property of the
path, computed, not a bit someone had to remember to set.

The fork's first manifest is the upstream's current manifest **verbatim** —
every entry, statistics included, delete-vector columns included. A borrowed
entry's `delete_file_path` points at a sidecar under the upstream's location;
that sidecar is borrowed too and read the same way. Nothing is recomputed.

### 3.4 Provenance

The fork's first snapshot carries the receipt the provenance design already
defines: `read-sources = [(upstream, source.snapshot-id, "version")]`. `"version"`
rather than `"current"` deliberately — a re-run of the same clone a day later
reads something else, and `RESOLVED_BY` exists to make that distinction. The
standing `sources` list on the fork therefore starts as `[upstream]`, and the
existing lineage UI shows the fork hanging off its upstream with no new
plumbing. A `RESYNC` commit carries the same receipt shape for the new base.

This is *also* why `fork` is a separate block rather than being inferred from
provenance: `sources` is capped, rewritten by overwrites, and describes what the
content is built from; the fork block describes a standing relationship with
obligations on both sides (§5). They answer different questions.

## 4. Creating a fork

`CREATE TABLE <target> CLONE <upstream>` (§8), in the catalog:

1. **Resolve** the upstream: load it, take its current snapshot id and manifest
   entries, its current schema, sort orders and clustering. A `VERSION AS OF` /
   `TIMESTAMP AS OF` on the upstream is honoured — a fork may be taken from any
   retained snapshot — and that snapshot becomes the base.
2. **Authorise** (§4.1). Refuse before anything is written.
3. **Register on the upstream first**: write the `forks/` document with
   `pinned-snapshot = base`. Ordering matters: from this point the base cannot
   expire, so the fork's manifest can never come to name a deleted file. If
   step 4 fails, an unregistered-fork document points at a fork that does not
   exist; the integrity sweep reports it and it is harmless in the meantime (it
   pins a snapshot, which costs storage the upstream was keeping anyway).
4. **Create** the target dataset with the upstream's schema and the `fork` block
   (§3.1), and commit its first snapshot with `add_files(entries=<upstream
   entries verbatim>, read_sources=[(upstream, base, "version")])`,
   `operation_type="clone"`. `entries=`, not `files=`: nothing is read back.

No bytes move. A fork of the 2.9 GB scale-10 bundle costs one Firestore write
per step and one manifest object.

### 4.1 Who may fork what

- `reader` on the upstream. Forking is a read: a fork exposes nothing a `SELECT
  *` would not.
- `writer` on the target collection — the same tier as `CREATE TABLE`.
- **Egress.** A fork is the standing, systematic copy that `egress_protection`
  exists to stop — a full mirror of someone else's data, kept off the back of a
  read grant, is precisely the case that guard's docstring describes. So a
  cross-workspace clone calls `assert_egress_allowed([upstream_ws], target_ws,
  "clone …")` and is refused while the upstream workspace's `egress_protection`
  is on. No `SECURE` exemption applies: `SECURE` sanctions a *named object*
  (a task, a materialized view), and a hand-run `CREATE TABLE … CLONE` is not
  one. The only way to allow forks out of a workspace is `ALTER WORKSPACE <ws>
  SET egress_protection TO OFF`, which is the decision it should be. The
  `samples` workspace is born with it off (§9). A fork *within* one workspace is
  not egress and is never refused on this ground.

### 4.2 Forking a fork

Allowed, with one rule: **a fork inherits its parent's pins.** When `P` (a fork
of `R`) is cloned to `C`, `C` registers on `P` (pinning `P`'s current snapshot)
*and* on every upstream `P` is registered on, at the same snapshots `P` pins.
`C`'s manifest may hold files domiciled by `R` (borrowed through `P`) and files
domiciled by `P` (written after `P` drifted); each owner must know. `C`'s
`fork.source.dataset` is `P` — `RESYNC` follows the direct parent only.

Without inheritance, `P` resyncing forward would release its pin on `R` while
`C` still names `R`'s old files. With it, `R` cannot retire that snapshot until
`C` too has moved on or gone.

## 5. Invariants

These are the point of the design. Each is a small change to code that exists.

### 5.1 An upstream never retires a snapshot a fork rests on

`pinned_snapshot_ids(catalog, identifier, metadata)` in `expiration.py` returns
the snapshot ids tags hold alive. It gains a second source: every
`pinned-snapshot` in the dataset's `forks/` subcollection. Same fail-closed
contract — a catalog that cannot list forks, or a listing that errors, raises
`ManifestProtectionError` and the run aborts. This one function is the whole
of "we don't delete a file that is referenced in a fork": a retained snapshot's
files are never orphans, so nothing downstream needs to know forks exist.

Compaction on the upstream is unaffected. It retires entries from the *current*
manifest; the pinned base still names the pre-compaction files, and they stay
until the pin moves.

### 5.2 A dataset never physically deletes a path outside its own location

`SnapshotExpiration._delete_file` gains the check `path.startswith(location +
"/")`; a path failing it is logged at debug and skipped, never deleted. Dropping
the entry from the manifest was the whole of the delete. This is the guard that
makes the fork side safe, and it is correct with no forks anywhere: a dataset
has no business deleting bytes it does not domicile.

Deep clean, reconciliation and `drop_workspace` already satisfy this by
construction (they list the location). The guard is added to them too, as a
cheap assertion, so the invariant is stated in one place per deleter rather
than being an accident of how three of them enumerate.

### 5.3 The upstream cannot be dropped or renamed out from under a fork

- `drop_dataset(upstream)` with a non-empty `forks/` registry **refuses**, naming
  the forks — the same shape and reason as the materialized-view refusal it sits
  beside: something else's content is built on this dataset's files. `DROP
  WORKSPACE` refuses for the same reason if any dataset in it has forks in
  *other* workspaces (forks inside the doomed workspace go with it and need no
  protection).
- `rename_dataset(upstream)` follows the reference: for each registered fork,
  rewrite `fork.source.dataset`; for each upstream the renamed dataset is registered
  on, rewrite the `fork` field of its registry document. This is
  `find_relationships_to` applied one more time. Renaming a *fork* rewrites its
  own registry entries on every upstream it is pinned on. The v1 escape hatch,
  if that proves fiddly, is the MV rule: refuse to rename a dataset with forks
  or that is a fork.

The way to drop an upstream that has forks is to `DETACH` them first (§8.3).

### 5.4 Dropping a fork

`drop_dataset(fork)` deletes its registry documents on every upstream **before**
its own document. Nothing else: its own files are reclaimed by reconciliation
as today; its borrowed entries were never its to reclaim (§5.2).

## 6. Sync state

Anchored by the two stored sequence numbers (§3.1), computed on request by
`SimpleDataset.fork_state()`:

```
behind   = upstream.current_sequence > fork.source.sequence-number
drifted  = fork.current_sequence     > fork.target.sequence-number
```

Each is one integer comparison against a value already on the document, so
the *question* costs nothing — and when both are false, which is the common
case, nothing else is read.

The **counts** are the raw differences, and they are reported as what they
are — upper bounds:

```
revisions_behind = upstream.current_sequence − fork.source.sequence-number
revisions_ahead  = fork.current_sequence     − fork.target.sequence-number
```

Sequence numbers advance on every commit, maintenance included, so a
difference of 3 may be three inserts or two compactions and a statistics
refresh. The number is therefore worded "at most 3 revisions behind", never
"3 commits behind", and it costs nothing: no snapshot is read to produce it.
Zero is exact — nothing whatsoever has happened on that side — which is the
answer that matters, and the one the common case gets.

Refining an upper bound into a count of user commits would mean reading the
snapshots above the anchor and classifying them the way
`previous_user_snapshot()` does. Nothing in this design needs that, and the
wording is chosen so that nothing has to: "at most" is true today and stays
true if a refinement is ever added underneath it.

A fork whose base snapshot has been dropped cannot happen (§5.1). A fork whose
upstream has been dropped cannot happen (§5.3). A fork can therefore always
answer.

## 7. Resync

`ALTER TABLE <fork> RESYNC [FORCE]` (§8.2):

1. Compute state. **Refuse** if `drifted` and not `FORCE`, naming the local
   commits that would be superseded. Refuse if not `behind` — there is nothing
   to do, and a no-op commit would be a lie in the history.
2. Read the upstream's current snapshot and entries.
3. Re-register: update `pinned-snapshot` on the upstream to the new base.
   (New pin before old pin release, as in §4 step 3.)
4. Commit a new snapshot on the fork whose manifest is the upstream's current
   entries verbatim, `operation_type="resync"`, receipt `[(upstream, new-base,
   "version")]`, and rewrite both anchors: `source.{snapshot-id,
   sequence-number}` to the new base, `target.sequence-number` to this commit,
   `target.last-sync-ms` to now. If the upstream's schema changed, this commit
   adopts it — a resync is "become the upstream again", schema included.

`FORCE` discards nothing physically. The fork's own files since the last base
are no longer referenced by its head, but every prior snapshot is still there:
`VERSION AS OF PREVIOUS` reads the pre-resync content until expiration retires
it under the fork's ordinary retention, and the fork's own files under its own
location are then reclaimed by the fork's own sweep — permitted by §5.2.

Resync is **manual only**. No trigger fires it, no schedule runs it. A fork that
resynced itself would be a materialized view of a table, which already exists
and has different guarantees.

## 8. SQL surface

### 8.1 `CREATE TABLE <target> CLONE <upstream>`

The vendored sqlparser already parses this — it is Snowflake's zero-copy clone
grammar — and yields a `CreateTable` with a `clone` field (verified 2026-09-19).
No `pre_parse` intercept, no regex, nothing to teach the classifier a new verb.
`VERSION AS OF` / `TIMESTAMP AS OF` on the source name follow the existing
time-travel path.

Not `CLONE <a> TO <b>`. A bare `CLONE` is outside every dialect: it needs a
`pre_parse` regex (as `LOAD SAMPLE` did), a `query_parser` classification, an
autocomplete entry, a docs page, and a `SHOW CREATE` form — for a statement
that already has a spelling the parser understands. The Snowflake spelling also
reads as what it is: a `CREATE TABLE` whose content is given by reference
rather than by query.

### 8.2 `ALTER TABLE <fork> RESYNC [FORCE]`

Joins the `ALTER TABLE … CREATE TAG` / `DROP TAG` family, which established
that `ALTER TABLE` is where a dataset's non-schema operations live. A bare
`RESYNC <dataset> [FORCE]` is possible through `pre_parse`, at the same cost as
a bare `CLONE`; the `ALTER TABLE` form costs one more branch in a rewriter that
exists.

### 8.3 `ALTER TABLE <fork> DETACH`

Materialises: copies every borrowed file into the fork's own location
(server-side, `_copy_object`), commits a snapshot with the remapped entries, and
deletes the fork's registry documents and `fork` block. Afterwards it is a plain
dataset. This is the escape hatch that lets an upstream with forks eventually be
dropped, and the one operation in this design that moves bytes — which is why
it is its own statement and nobody's default.

### 8.4 Collection clone, for samples

The sample flow forks eight tables. `CREATE COLLECTION <target> CLONE
<source-collection>` — every dataset in the source collection, each forked as
in §4, refused wholesale if any one would be refused. Whether sqlparser's
`CREATE SCHEMA … CLONE` covers it needs checking; if not, it is a small
`pre_parse` form, and Studio can issue one statement per table until it lands.

### 8.5 Retire `LOAD SAMPLE`

Remove: `_intercept_load_sample` and `_LOAD_SAMPLE_RE` (pre_parse.py),
`plan_load_sample` (logical_planner.py), the `LoadSample` entries in
`query_parser.py`, `OpteryxConnector.load_sample` and
`_read_parquet_schema`, `opteryx/managers/samples.py` and the manifest at
`gs://opteryx/samples.json`, the `load_sample` clause in `reference/clauses.json`
(which regenerates web.opteryx's editor grammar and docs.opteryx's definitions),
and the docs page. `SAMPLE_DATA_LOCATION` goes with them. Datasets already
loaded by it are plain copies and are unaffected.

## 9. Samples become datasets

### 9.1 Universally readable, never listed

Two properties, and they come from ONE mechanism rather than two.

`implicit_grants` (opteryx-access `checks.py`) gives every identity `reader` on
`samples.*`, exactly as it already does for `public.*`, and `samples` joins
`RESERVED_WORKSPACES` so no policy can ever be written over it. That is the
read: anyone can `SELECT` a sample or `CLONE` one with no policy issued to
them, and nobody but the platform identities can write one - the implicit grant
CAPS, so an issued policy cannot widen it.

The listing property falls out of the same fact. Implicit grants are never
minted into a token's `policies` claim (authenticate.opteryx builds it from the
stored `access` collection group), odata.opteryx builds its service document
from that claim, and Studio draws its catalog tree from the service document.
`public` appears in the tree only because the service document unions it in BY
NAME; `samples` deliberately is not unioned in, with a comment at both union
sites saying why.

`listed: false` (S9.2) is then the DECLARED form of the same intent, for the
case the trick does not stretch to.

### 9.2 `listed` - a workspace that is readable but not advertised

A tri-state workspace property on `$properties`, beside `deletion_protection`
and `egress_protection`, set with `ALTER WORKSPACE <ws> SET listed TO OFF` and
read by `OpteryxCatalog.is_listed`.

**It is not a permission**, and that is the thing to be clear about, because
somebody will reach for it as one. An unlisted workspace is still queryable by
name, still in `information_schema`, still named in provenance and in fork
relationships. Who may read it is decided by grants and nowhere else.

Honoured in ONE place - `_drop_unlistable_workspaces` in odata.opteryx, which
both the service document and `$metadata` call, and which already fetched every
workspace's `$properties` for the deleted-workspace check, so the second
question costs no extra read.

**Default ON, for the opposite reason its two siblings are.** Theirs is
fail-closed: unset must mean protect. This one follows the STATUS QUO, because
the failure modes are not symmetric - an unlisted workspace that shows up is
clutter, while a listed one that vanishes is somebody's data disappearing out
of their own catalog. So unset, an unrecognised value, and an unreachable
Firestore all read as listed.

Rejected on the way here: a **per-dataset `HIDDEN` attribute**. It is not one
boolean (hidden from the tree? from `$metadata`? from search? from provenance,
where a fork's upstream must always be nameable?), every consumer would have to
decide separately and they would drift, it would be mistaken for a security
boundary, and it is the wrong grain - five scale factors of TPC-H is forty
flags to keep right, and each new scale factor is eight more. The thing that is
"not your data" is the namespace, not each table in it.



A `samples` workspace — the name is already reserved in control.opteryx
(`reserve_workspace_names.py`) — with `egress_protection` **off** and public
`reader`, holding one collection per staged scale factor:

```
samples.tpch_sf001.{region, nation, …, lineitem}
samples.tpch_sf01.…
samples.tpch_sf1.…
samples.tpch_sf5.…
samples.tpch_sf10.…
```

`generate_tpch_samples.py` writes through rugo as it does now, and then
**commits the entries it just built** with `add_files(entries=…)`. Statistics
are computed once, at generation, in the writer that has the bytes in memory —
never again. This is the "catalog docs already written" the copy path lacked.

The inventory is then the catalog: what scale factors exist is what collections
exist, listable through OData like anything else. No manifest, no
`SAMPLE_DATA_LOCATION`, no hardcoded table in Studio.

Staged bundles stop being a special prefix that must never be touched. They are
datasets whose forked snapshots are pinned (§5.1), so restaging a scale factor is
an ordinary overwrite: existing forks keep their base, new forks take the new
content, and the old files leave when the last fork resyncs or detaches.

## 10. Surfaces

### 10.1 `information_schema.forks`

One row per (fork, upstream) pair visible from the workspace, beside
`information_schema.grants` (`connectors/information_schema.py`):

| column | |
|---|---|
| `fork` | fully qualified |
| `upstream` | fully qualified |
| `base_snapshot` | `source.snapshot-id` |
| `upstream_snapshot` | upstream's current |
| `revisions_behind` | `upstream.current_sequence − source anchor`; an upper bound, 0 = up to date |
| `revisions_ahead` | `fork.current_sequence − target anchor`; an upper bound, 0 = not drifted |
| `forked_at`, `forked_by`, `last_sync` | |

Filter by `fork` for a dataset's own state; by `upstream` for "who has forked
this" and the count. Both read the registry (§3.2) — the upstream side is a
subcollection read, the fork side a document get.

### 10.2 OData `$metadata`

BUILT, as ONE term rather than five. `Custom.Fork` is a single Record beside
`Custom.Sources` and `Custom.Consumers` carrying `Upstream`, `RevisionsBehind`,
`RevisionsAhead`, `BaseSnapshot`, `LastSyncMs` and `ForkedBy` - one annotation,
because they are one fact and five terms would be five things a client has to
find and keep in agreement. The service OMITS it entirely for a dataset nobody
cloned, so an absent term means "not a fork"; there is no companion
completeness flag, because there is no "older service" state to separate it
from.

Read by `odata-metadata.js`'s `readODataFork`, off the same `$metadata` fetch
everything else on the page comes from. The fork's divergence is part of the
metadata cache key - without it a resync would keep serving "at most 3
revisions behind" after it had become zero.

NOT built: `Custom.Forks.Count` on an upstream.

### 10.3 Studio

- **manage-dataset.html** — BUILT, as a LINE under the title and the version
  picker rather than a chip beside them, with the drift as DIFF COUNTS:
  `Forked from samples.tpch_sf1.lineitem  --3:++1` — red behind, green ahead,
  the way every tool that compares two refs shows it. The prose it replaced
  ("at most 3 revisions behind, edited here (at most 1 revision)") said the
  same thing in twelve words and had to be READ; this is scanned. The long
  form, with its "at most", moves to the `title`/`aria-label`, where a reader
  who does not know the convention can find it.

  A side that is zero is OMITTED rather than shown as `--0`, which reads as a
  quantity and is not one — so an in-sync fork shows just the name. "Up to
  date" is the unremarkable case, and saying it on every page is noise on a
  line whose job is to say something happened. The upstream is a link: a
  citation nobody can follow is most of the way to no provenance at all.

  Beside the title it did not work. The line carries a full dataset name of its
  own and competed with the name above it for the same row, which the title
  lost — `personal.bastian.lineitem` ellipsised to `perso…`. Neither name may
  be elided to make room for the other, so the two now stack:
  `.page-header-identity` is a column holding the title row and this line,
  inside a `.page-header` that is a flex row ending in the close button.

  NOT built: a `Resync` action on the line, and `4 forks` on an upstream
  linking to `information_schema.forks`. The data for both is live.
- **The Load-sample dialog** (`load-sample.js`) drops its hardcoded inventory
  and lists the `samples` workspace's collections from OData; the statement it
  writes into the editor becomes `CREATE COLLECTION personal.justin CLONE
  samples.tpch_sf01` (or one `CREATE TABLE … CLONE` per table until §8.4
  lands). The banner and the personal-collection lock are unchanged.
- **Catalog tree**: a fork carries a small mark, as external tables do.

## 11. Billing

A fork occupies no storage until it drifts, and then only its own new files:
the borrowed bytes are billed once, to the upstream. This is a change from the
copy design, where the caller owned and paid for the bytes, and it is the
intended one — sample data costs nothing to try. Compute and egress are
unchanged. `Custom.Forks.Count` on a dataset is also the honest answer to "why
is my storage not going down after that compaction" (§5.1).

## 12. Rollout

> **Step 3 is done for the small scale factors.** The `samples` workspace is
> real - `egress_protection` OFF, `listed` OFF, reservation marker cleared -
> and `scripts/stage_sample_workspace.py` registers the staged bundles as
> datasets. sf001, sf01 and sf1 are registered; sf5 and sf10 are the remaining
> ~4.3 GB and are the same command with `--scale 5 10`.
>
> Registration reads each file ONCE, to build its manifest entry, and that is
> the only time anything reads them: every fork afterwards is a manifest write.
> The script is resumable - an already-registered dataset is skipped - so an
> interrupted run is re-run rather than unpicked.


Order matters, and the catalog goes first:

1. **opteryx-catalog**: §5.2 guard (safe alone, and it is a bug fix without
   forks); the `fork` block and `forks/` registry; §5.1 pinning; `clone`,
   `resync`, `detach`, `fork_state`; drop/rename rules. Deployable with no
   caller — nothing creates a fork yet.
2. **opteryx-core**: bind `CreateTable.clone`, `ALTER TABLE … RESYNC | DETACH`,
   egress check, `information_schema.forks`, `reference/clauses.json`. Remove
   `LOAD SAMPLE` in the same release so the two never coexist.
3. **Staging**: create the `samples` workspace; regenerate the bundles as
   committed datasets. The old `gs://opteryx/tpch/` prefix is deleted once no
   `LOAD SAMPLE` engine is deployed.
4. **odata.opteryx**: `Custom.Fork.*` terms.
5. **web.opteryx**: `make sql-signatures` (editor grammar follows the clause
   catalog), Studio chips and dialog. Bump the cache stamp.
6. **docs.opteryx**: `make sql-docs`; a `CREATE TABLE … CLONE` page replaces
   `load-sample.md`; `ALTER TABLE` gains `RESYNC` / `DETACH`.

## 12.1 What staging against real infrastructure found

Two bugs that no fixture in this repository could have caught, because every
double here is a SINGLE-WORKSPACE double and both were in the cross-workspace
path - which is the only path that matters, since a fork of a sample is always
cross-workspace:

* `clone_dataset` loaded the upstream with `self.load_dataset(source_fq)`. That
  method takes a name LOCAL to its handle's workspace, so a three-part name
  naming another workspace was read as `collection.dataset` and raised
  `DatasetNotFound`. Fixed with `_catalog_for` / `_load_qualified`, which route
  to the owning workspace's handle - a real handle, not a document ref, because
  a clone needs the source's manifests, history and FileIO. Cached per
  workspace, since a collection clone asks for the same one eight times.
* All three of `clone_dataset`, `resync_fork` and `detach_fork` called
  `save_dataset_metadata(metadata)`, which takes `(identifier, metadata)`.

A third arrived later, from a user's own fork rather than a test: `load_dataset`
does NOT hydrate `metadata.schema` - it resolves `current_schema_id` and leaves
the object alone - so `clone_dataset` passed None to `create_dataset` and made
forks with six million rows, a correct manifest, a correct fork block, and NO
SCHEMA. Everything that describes a dataset showed blank. `_stored_schema_of`
now copies the upstream's stored column documents verbatim: the round trip
through this catalog's dependency-free `RelationSchema` is lossy in exactly
this direction, and a fork's schema is not merely similar to its upstream's -
it IS its upstream's.

All three are now covered by tests that assert the SHAPE of the call rather than
its effect, since the shape is what was wrong each time.

Also confirmed live, which is the point of having done it: a fork's manifest
names the staged file at `gs://opteryx/tpch/...` while the fork's own location
is under `gs://opteryx_data/...`, so the borrowed bytes are outside anything
that could reclaim them; the upstream's `pinned_snapshot_ids` reports the
fork's base; and dropping a forked upstream is refused by name.

## 13. Open questions

- **Fork of an external (bound) table.** A PostgreSQL-bound dataset has no
  manifest to borrow. Refuse in v1: "clone reads a manifest; that workspace's
  datasets have none". A CTAS is the copy for those.
- **Schema drift on resync.** §7 adopts the upstream's schema. If the fork added
  a column (which is drift, so `FORCE` was needed), it is gone after resync —
  consistent, but worth a line in the refusal text.
- **Registry and rename, v1.** §5.3 proposes following the reference. If the
  two-sided rewrite is more than a day, ship the refusal and follow in v2.
- **Fork count as a cap?** Nothing here limits how many forks an upstream may
  have. A pinned snapshot per fork is the only cost, and it is the upstream's
  storage, which its owner already controls through egress.
