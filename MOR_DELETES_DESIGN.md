# Merge-on-Read Deletes — Design

Status: **backend implemented, no SQL surface** (2026-08-25).

Built: the delete-vector sidecar (`catalog/deletes.py`), the two manifest
columns, `SimpleDataset.delete_rows` / `delete_files` / `delete_vectors`, GC
protection in deep-clean and expiration, compaction exclusion +
refresh-preservation, and read-side elimination in opteryx-core (FileEntry
delete fields resolved at binding, per-row-group subtraction in
ParquetReadNode, live `Manifest.get_record_count`, statistics-only MIN/MAX/
COUNT(col) declined under deletes, compiled fast-path scan sources gated to
the streaming path for delete-bearing scans). Tests:
`tests/test_mor_deletes.py` (catalog) and opteryx-core
`tests/integration/test_mor_deletes_local.py` (end to end).

Also built (2026-08-25, second pass): **compaction materialisation**.
Deleted rows are dropped wherever a merge reads an input file — the streaming
source cache rewrites delete-bearing bytes at fetch
(`deletes.materialise_live_parquet`), the hold-everything reader drops rows
per row group — so rules A/B materialise opportunistically whenever they
touch a delete-bearing file, and outputs carry no delete columns.
`_row_counts_balance` now asserts LIVE-rows-in == rows-out. A new **rule C
(`compact(rule="debt")`)** rewrites a file purely to shed debt when
`deleted_record_count / record_count >= 0.10` (per-dataset override:
`maintenance_policy["delete-debt-threshold"]`), one worst-ratio file per
pass, as an order-preserving single-file brute combine. Snapshot-id
allocation is collision-proof across all commit paths
(`_allocate_snapshot_id`), and `write_parquet_manifest` invalidates the
parsed-manifest cache on write. NOTE: the housekeeping service
(xb500.opteryx) must add `compact(rule="debt")` to its per-dataset rule
series for rule C to run on a schedule.

Not built: any SQL `DELETE` surface, `row_group_mask` (readers decode the
bitmap; whole-group skipping still happens when a group's slice is fully
deleted), CAS commits, and latmat/native-scan delete awareness.

## 1. Problem

Datasets in this catalog are append-mostly. The only ways to remove rows today are
whole-dataset (`truncate`, `truncate_and_add_files`) or a full copy-on-write rewrite
(`overwrite`) — both rewrite every byte to remove one row. There is no row-level
`DELETE`, and opteryx-core has no DELETE operator to plan one with.

What we want is the cheap half of the usual pair: a delete records *which rows are
gone* without touching the data files, and the read path filters them out. The
expensive half — actually rewriting the files — is deferred to compaction, which
already runs as a scheduled housekeeping job.

Target properties:

- A delete commit costs O(files touched by the predicate), not O(dataset).
- Readers pay a bounded, predictable cost per affected file.
- Time travel keeps working: an old snapshot still reads as it did.
- The existing GC sweeps (expiration, orphan quarantine, deep clean) do not
  eat the new files.

## 2. Shape of the choice

### 2.1 Positional deletes, not equality deletes

Iceberg supports both. We should support only **positional** deletes.

Equality deletes ("all rows where `id = 7`") are attractive for streaming CDC because
the writer doesn't need to find the rows. The cost is on every reader forever: an
equality delete applies to every data file with a lower sequence number, so each scan
must join the equality-delete keys against every candidate file. That join is the
single largest source of Iceberg MOR read regressions in practice, and we have no
CDC-shaped ingest that needs it.

Positional deletes name `(data file, row ordinal)`. The writer does the work once —
it must scan to find the rows — and every subsequent read is a bitmap test. Given
our reads are already row-group granular with late materialisation
(`opteryx/operators/parquet_read/parquet_read.pyx`), a per-file bitmap composes
naturally with the existing selection vector.

### 2.2 Delete vectors, not delete rows

Iceberg's v2 positional deletes are Parquet files of `(file_path, pos)` rows, one row
per deleted row. Iceberg v3 replaced this with delete *vectors* — one compressed
bitmap per data file — for good reason: the row form is enormous for large deletes,
requires a sort-merge at read time, and accumulates one file per commit.

We should go straight to delete vectors: **one bitmap per data file, holding every
row ordinal deleted from that file as of this snapshot**.

The decisive advantage falls out of a property this catalog already has: **manifests
are cumulative and fully rewritten on every commit** (`SimpleDataset.append` reads the
parent manifest and writes parent-rows + new-rows). So a delete commit can *merge* the
new positions into the previous vector for that file and write one fresh vector. There
is never a stack of delete files to reconcile at read time, and therefore no need for
per-entry sequence numbers or applicability rules — the hardest part of Iceberg MOR
disappears. Each snapshot's manifest is self-describing: one vector per file, already
merged, always applicable.

The cost is write amplification on the vector: deleting one more row from a file
rewrites that file's whole bitmap. A bitmap for a 4 GB file with 40 M rows is tens of
KB compressed. That is the right trade.

## 3. Physical layout

### 3.1 Delete vector file

One object per delete commit, holding the merged vectors for every file that commit
touched:

```
<dataset_location>/metadata/deletes-<snapshot_id>.parquet
```

Written with the existing rugo writer and `WRITE_PARQUET_OPTIONS`, so no new codec,
no new IO path, and `iops` needs no change. Schema:

| column                 | type      | meaning                                        |
|------------------------|-----------|------------------------------------------------|
| `data_file_path`       | VARCHAR   | the file these positions belong to             |
| `deleted_record_count` | INTEGER   | cardinality of the bitmap (denormalised)       |
| `row_group_mask`       | VARBINARY | one bit per row group: "this group has deletes"|
| `bitmap`               | VARBINARY | encoded row ordinals                           |

It lives under `metadata/` deliberately: `deep_clean.get_all_manifest_files` already
treats the manifest's *own* path as referenced, and both sweeps walk the whole dataset
prefix, so keeping delete state beside manifests keeps the two kinds of metadata in
one place.

`row_group_mask` is what makes whole-row-group skipping cheap: the reader can decide
"this row group is untouched, take the fast path" without decoding the bitmap.

### 3.2 Bitmap encoding

Self-describing, one header byte so the encoding can change later without a format
version bump:

- `0x00` — sorted `uint32` deltas, varint-encoded. Best for sparse deletes (the
  common case: a handful of rows out of millions).
- `0x01` — dense bitset over `[0, record_count)`. Best when most of a file is gone.
- `0x02` — reserved for Roaring, if we ever want the dependency.

Start with `0x00` and `0x01` and pick per file by whichever is smaller. Both are ~30
lines of Cython or plain Python; neither adds a dependency. Row ordinals are file-local
and zero-based, matching Parquet row order — the same order `parquet_read` produces.

### 3.3 Manifest changes

`ParquetManifestEntry` (and the column set in `write_parquet_manifest`) gains four
columns. Manifest rows stay strictly **one per data file** — no `content` discriminator
row for delete files, because every existing consumer (compaction's selectors,
`scan`, `describe`, `_totals_from_entries`, the planner's `Manifest`) iterates entries
assuming they are data files, and a second row kind would need a filter added in each
of them, with any omission a silent correctness bug.

| column                 | type    | default when absent | meaning                                   |
|------------------------|---------|---------------------|-------------------------------------------|
| `delete_file_path`     | VARCHAR | NULL                | vector file holding this file's deletes    |
| `delete_vector_row`    | INTEGER | NULL                | row index within that file                 |
| `deleted_record_count` | INTEGER | 0                   | rows deleted from this data file           |
| `row_group_mask`       | VARBINARY | NULL              | copy of the mask, so pruning needs no read |

Every one of these reads as "no deletes" when absent, so existing manifests, existing
readers, and a reader older than a writer all behave exactly as they do today. This is
the same backwards-compatibility discipline `field_ids` and `char_class_counts` already
follow.

`record_count` keeps meaning **physical rows in the file**. Live rows are
`record_count - deleted_record_count`. Keeping `record_count` physical is not
cosmetic: it is what `compaction._row_counts_balance` and the manifest-refresh path
compare against the file's own footer, and making it live would make the manifest
disagree with the bytes.

The price of the "one row per data file" rule is that `delete_file_path` is not
`file_path`, so the three path-collecting sweeps must learn about it — see §7.

## 4. Write path

### 4.1 Split of responsibility

The catalog cannot evaluate a SQL predicate; the engine cannot commit. So:

- **opteryx-core** plans the DELETE, scans the matching files, and produces, per file,
  the ordinals that matched.
- **opteryx-catalog** takes those ordinals and commits them.

New dataset API, mirroring `append`'s commit shape:

```python
dataset.delete_positions(
    positions: dict[str, Iterable[int]],   # data file path -> row ordinals
    author: str,
    commit_message: str | None = None,
    expected_snapshot_id: int | None = None,
) -> Snapshot
```

and a copy-on-write fast path for the case that matters most in practice:

```python
dataset.delete_files(paths: Iterable[str], author: str, ...) -> Snapshot
```

`delete_files` drops the manifest entries outright. It is what a predicate that
provably matches whole files (a partition-aligned `DELETE WHERE day = ...`, established
from manifest min/max bounds without reading anything) should compile to. It costs one
manifest write and no data read — cheaper than any MOR delete, and it leaves no debt.
The planner should always prefer it where the bounds prove containment.

### 4.2 Commit sequence for `delete_positions`

1. Load the current snapshot's manifest entries (`_parent_manifest_entries` — its
   fail-closed behaviour is exactly what we want here too).
2. For each named file, verify it is still in the manifest. A file that has vanished
   (compacted away underneath the scan) aborts the commit rather than dropping the
   delete on the floor.
3. Read the file's existing vector, if any, and OR the new ordinals into it. Reject
   any ordinal `>= record_count`.
4. Write one `deletes-<snapshot_id>.parquet` holding merged vectors for every touched
   file, and one *unchanged* row for every file that already had a vector and is not
   being touched — so a single vector file per snapshot holds all live delete state.
   (This copies untouched vectors forward; for pathological delete state, the copy is
   the same amplification the manifest already accepts.)
5. Write the new manifest: parent rows, with the touched rows' delete columns updated,
   and every row's `delete_file_path` repointed at the new vector file.
6. Commit a snapshot with `operation_type="delete"` and summary keys
   `deleted-records`, `delete-files-touched`, plus the standard totals.

Step 4's copy-forward is what makes a snapshot's delete state addressable by exactly
one object. The alternative — leaving untouched vectors pointing at older vector
files — saves the copy but makes expiration's reachability maths span snapshots, and
that is a class of bug we should not buy for a small write saving.

### 4.3 Concurrency

This is the weak point, and it exists already. `save_dataset_metadata` /
`save_snapshot` use Firestore `.set()`, last-writer-wins, with no compare-and-set.
Compaction defends itself with `_dataset_moved_under_us` — an after-the-fact check that
the current snapshot id is what it was when the pass started.

Deletes need at least the same guard, and the interaction matrix is:

| concurrent ops         | outcome                                                      |
|------------------------|--------------------------------------------------------------|
| delete ∥ append        | commutative — the appended file has no deletes. Safe to retry |
| delete ∥ delete        | commutative — merge both bitmaps. Safe to retry               |
| delete ∥ compaction    | **conflict** — positions refer to files compaction destroyed   |
| delete ∥ overwrite     | **conflict** — overwrite defines the whole file list          |

So: take `expected_snapshot_id`, re-read at commit, and on mismatch **rebase** rather
than fail — reapply the positions against the new manifest, and abort only if a named
file is gone. Compaction gets the reciprocal check: if any input file gained deletes
since the pass started, abort the pass (it is a scheduled job; the next pass picks it
up). Doing this properly argues for a real CAS on the dataset document — a Firestore
transaction asserting `current-snapshot-id` (the stored key for the head) — which
is worth doing regardless of MOR.

## 5. Read path (opteryx-core)

### 5.1 Binding

`FileEntry` gains `deleted_record_count`, `row_group_mask`, and a lazily-resolved
`delete_vector`. `FileEntry.from_datafile` populates them from the new manifest
columns. The vector file is fetched once per scan (one object read, shared across all
files in the scan) and sliced per entry — it must **not** be one read per file.

### 5.2 Statistics become one-sided

This is the correctness trap, and it needs writing down where the planner can see it:

- **min/max bounds stay valid** — a superset of live values is still a valid pruning
  bound. Deleting rows can only shrink the true range, never widen it. Pruning stays
  correct; it just gets slightly less selective.
- **null counts become upper bounds** — safe for pruning `IS NOT NULL`, unsafe for
  answering `COUNT(col)` from the manifest.
- **record counts are wrong** for anything that answers a query directly.
  `Manifest.get_record_count`, the `COUNT(*)`-from-manifest strategy, and LIMIT
  elimination must use `record_count - deleted_record_count`, and where a strategy
  needs an exact figure it must be *disabled* for any file with a non-zero
  `deleted_record_count` unless the whole file is provably covered.
- **sketches (min-k, histograms, char-class) become approximate over a superset.**
  They already are approximations feeding selectivity estimates, so this is a quality
  regression, not a correctness one — but a dataset with heavy delete debt will
  estimate badly, which is another argument for compacting on delete debt (§6).

### 5.3 Scan

Per row group, in the reader:

1. If `row_group_mask` says the group is untouched — current behaviour, byte for byte.
2. If the group is entirely deleted — skip it without reading. This is the case that
   makes large deletes cheap, and it is why the mask is worth carrying.
3. Otherwise, decode the group's slice of the bitmap into a selection vector and
   intersect it with the one late materialisation already produces. The delete filter
   applies **before** any pushed-down predicate result is used and before the row
   count is reported.

A file whose every row is deleted should never reach the reader at all — the delete
commit should drop the manifest entry instead of writing an all-ones vector.

### 5.4 Everything else that reads a manifest

`SHOW MANIFEST`, `describe`, snapshot history, the OData metadata path and the
information-schema views all report row counts. Each needs an explicit decision:
physical, live, or both. Default to reporting live counts to users and keeping the
physical count available for operators.

## 6. Compaction

Compaction becomes the *only* thing that materialises deletes, which makes it a
correctness-critical consumer rather than an optimisation:

- Any merge whose inputs carry deletes must drop the deleted rows while streaming, and
  emit outputs with no vectors.
- `_row_counts_balance` must compare **live** input rows against output rows. The
  current invariant (physical in == physical out) would fail every delete-bearing
  merge, and relaxing it wrongly would let a real row-loss bug through — so this
  change needs its own test.
- A new selection rule: **delete debt**. Any file whose
  `deleted_record_count / record_count` exceeds a threshold (start at 0.25, as a
  dataset property `compaction.delete-debt-threshold`) is a compaction candidate on
  its own, regardless of size — a 4 GB file that is 60% deleted is 2.4 GB of pure read
  waste on every scan. This is the mechanism that stops MOR debt accumulating forever.
- A file that is 100% deleted is dropped by the delete commit, so compaction never
  sees one.
- `refresh_manifest` rebuilds stats from the data files, which know nothing about
  deletes. It must preserve the delete columns rather than rebuild them, or it will
  silently resurrect deleted rows. Worth an explicit invariant test.

## 7. Garbage collection

Three sweeps decide "is this file referenced?" by collecting `file_path` from manifest
entries. Every one of them must also collect `delete_file_path`, or the first sweep
after a delete will quarantine and then delete the live vector — resurrecting deleted
rows across the whole dataset. This is the single highest-consequence change in the
design.

- `deep_clean.get_all_manifest_files` — add `delete_file_path` to the returned set.
- `orphan_quarantine` — same, and note its two-sighting rule gives us a safety margin
  here rather than an immediate loss.
- `expiration` — an expired snapshot's vector file is unreferenced only once no live
  snapshot names it. Because §4.2 copies vectors forward per snapshot, that is a
  simple per-snapshot reachability test with no cross-snapshot chasing.

Mitigation for the resurrection risk: gate the whole feature behind a dataset property
(`write.delete-mode = "copy-on-write" | "merge-on-read"`, default copy-on-write) so
that no dataset can grow vectors until its GC path is known-good, and add a sweep-side
assertion that refuses to delete anything under `metadata/` matching `deletes-*`.

## 8. Time travel

Falls out for free. Each snapshot's manifest names its own vector file and holds
already-merged state, so reading snapshot N applies exactly the deletes that existed at
N. No sequence-number filtering, no "which delete files apply to this data file"
question. This is the payoff for the copy-forward in §4.2.

## 9. Rollout

Each phase is independently useful and independently shippable.

1. **`delete_files` only** (catalog + engine). Whole-file COW deletes, no new file
   type, no reader change, no GC change. Serves partition-aligned deletes — likely a
   large share of real DELETE traffic — at near-zero risk.
2. **Manifest columns**, written as NULL/0 by every existing path. Pure plumbing; lets
   readers and sweeps be updated and deployed before anything writes a vector.
3. **GC sweeps** learn `delete_file_path`, plus the `deletes-*` protective assertion.
   Deployed and soaked *before* the first vector exists.
4. **Read path**: `FileEntry`, row-group skipping, selection-vector intersection,
   count corrections. Testable against hand-written manifests before any writer.
5. **`delete_positions` writer**, behind `write.delete-mode`, on one internal dataset.
6. **Compaction**: delete materialisation, live-row balance invariant, delete-debt
   rule.
7. **Engine DELETE operator** and planner rule choosing between `delete_files` and
   `delete_positions`.
8. Consider CAS commits (§4.3) — arguably should be earlier, since it is a pre-existing
   gap that MOR makes sharper.

## 10. Rejected alternatives

- **Equality deletes** — unbounded read-side cost, no ingest pattern that needs them.
  See §2.1.
- **Iceberg-compatible `(path, pos)` delete rows** — we are explicitly not Iceberg
  compatible (README), so the only reason to adopt the row form would be interop we
  don't have. It costs a read-side sort-merge and one file per commit.
- **Copy-on-write only** (rewrite the file on every delete) — simplest and always
  correct, and genuinely the right answer for small datasets. It is unusable for a
  4 GB file losing one row, which is exactly the shape compaction produces.
- **Deletes in Firestore** rather than in object storage — attractive for small deletes
  (no extra object, transactional with the commit) but the document size limit caps it
  hard, and it puts data-shaped state in the metastore. Could be a future optimisation
  for vectors under a few KB, encoded inline in the manifest instead.
- **A `content` discriminator column with delete rows in the manifest** (Iceberg's
  model) — every entry-iterating consumer would need a filter, and a missed one is a
  silent bug. See §3.3.

## 11. Open questions

- Does anything outside this repo read manifests directly (the vscode parquet
  inspector, `repair_manifests.py`, the odata service)? Each is a reader that would
  over-report rows until updated.
- Should `delete_positions` be expressible as a predicate the catalog evaluates against
  a single file (so the engine sends a filter, not ordinals)? It would cut the wire
  payload for large deletes considerably, at the cost of putting expression evaluation
  in the catalog.
- What is the interaction with materialized views — does a delete on a source table
  fire a refresh trigger? Today's triggers key off commits, so it would, but a refresh
  that reads a MOR dataset must be correct first.
- Should `UPDATE` be modelled as delete + append in one snapshot? The commit shape
  above supports it, and doing it as one snapshot avoids a window where the row exists
  neither before nor after.
