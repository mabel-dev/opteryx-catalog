# Metadata-only RENAME TABLE (decision D9)

Status: **2026-10-30 - rename AND create implemented (uncommitted).** Originally a design note;
Q1/Q2 were built on the recommendations below when the architect asked for the create change. The ruling (architect, D9) is fixed: rename is
read-doc / create-doc / drop-doc in ONE Firestore transaction and moves no bytes.
Everything under "Open questions" is not decided - it is for the architect.

## What is already true

A dataset's `location` is **already stored** on its Firestore doc. `create_dataset`
writes it, `_dataset_from_doc` reads it, and every reader and deleter
(`SimpleDataset`, expiration, deep clean, `is_own_path`, the engine's index and
write paths) goes through `metadata.location`. So "a stored location" is not new.

What is missing is the other half of the ruling: the stored location is **derived
from the name at creation** (`_dataset_location`), so it is not *never-reused*.
After a metadata-only rename the old name is free, `create_dataset(old_name)`
derives the same prefix, and two live datasets share one location - dropping either
one reclaims the other's files. That is the whole reason rename copies today
(`rename_dataset` docstring), and it is the hazard `drop_dataset`'s docstring
describes.

The tension with `drop_dataset` ("persist no note of a dropped location") dissolves
once a location can never be derived again: a dropped dataset's prefix is then
permanently dead, so there is nothing a stale note could re-bind to. Reclamation can
keep re-deriving from ground truth (bucket vs. claimed locations) and loses nothing.

## What changes

| | Before | Now |
|---|---|---|
| `rename_dataset` | copy files, rewrite manifests, write new entry, delete old, delete superseded files; not atomic | one transaction: read every doc, create under new name, delete old; `location` carried **verbatim**; no storage I/O |
| `create_dataset` | location = derived from `(collection, name)` | location = `gs://<bucket>/<workspace>/<uuid4 hex>`, minted (`_mint_location`), stored, never re-derived; optional `location=` accepted only if of that shape |
| `open_pending_data_file_writer` | wrote under the derived location | writes under a location RESERVED per identifier (`_reserve_location`, in-process); the following `create_dataset` takes it |
| `_dataset_from_doc` | `data.get("location") or <derive>` | unchanged - now serves only stubs, which own no files (Q3) |

A dataset created before this change keeps its name-derived location (nothing is
moved). That is safe to rename: no create produces that prefix again, so the vacated
name cannot bind it. `rename_dataset` refuses only a document that records **no**
location (`DatasetLocationReusable`), never guessing one.

**Deploy order.** The rename's safety rests on no writer deriving a location from a
name. A process still running the old `create_dataset` after a rename can recreate
the vacated name on the renamed dataset's prefix. Roll the catalog out to every
service that creates datasets (engine, jobs, control) before renames are used; the
new `duplicate-location` fsck finding (`integrity.py`) detects it afterwards.

## Sites that derive or assume location-from-name

Catalog (`opteryx-catalog`):

1. `_dataset_location` (opteryx_catalog.py ~1190) - the one derivation. Callers:
   `create_dataset` (~1288) and `open_pending_data_file_writer` (~1262).
2. `_dataset_from_doc` (~1641) - **silent fallback**: derives the location when the
   doc has none. Contradicts "no fallbacks".
3. `rename_dataset` (~2170) - re-derived `new_location` from the new name (removed
   by this change).
4. `drop_dataset` docstring (~1851) - states the rule that locations are derived;
   needs rewriting with Q1.
5. `drop_workspace` (~2530) - reads each doc's stored `location` before dropping.
   Already correct under a stored location.
6. Forks: `clone_dataset` calls `create_dataset`, so a clone gets its own location;
   borrowed entries keep the upstream's absolute paths (outside the fork's
   location, by design). `resync_fork`/`detach_fork` copy into
   `dataset.metadata.location`. All correct under a stored location. They record
   the upstream **by name** (`fork.source.dataset`, registry doc keyed by fork name)
   - a name reference, not a location one (Q4).
7. `clone_collection` - loops `clone_dataset`; no direct derivation.
8. Materialized views - the backing dataset is an ordinary dataset; triggers and
   `source-tables` reference datasets **by name** (not location). Rename stays
   refused for MVs and for datasets carrying triggers.
9. Orphan quarantine (`maintenance/orphan-quarantine`) holds paths under the
   location. With a stable location these stay valid across a rename.
10. `scripts/restore_snapshot.py` (~361) - derives `target_location` from a name.
11. `catalog/ownership.py` (`is_own_path`, `is_admissible_path`) - prefix tests on
    the stored location; correct as is.

Outside this repo:

12. `opteryx-core` `connectors/postgres_connector.py:stats_manifest_path` and
    `control.opteryx/app/routes/v1/workspace_catalog.py:_stats_manifest_path` -
    both derive `gs://<bucket>/<prefix>/<schema>/<table>/metadata/manifest-<table>.parquet`
    from the name for **Postgres-projected relations** (stubs; no data files). They
    are a writer/reader pair and must change together if the format changes.
13. `opteryx-core` `OpteryxConnector.open_data_file_writer(pending_schema=...)` -
    CTAS gets its location from `open_pending_data_file_writer` (item 1). Other
    engine paths read `catalog.load_dataset(...).metadata.location`. The engine's
    `rename_relation` docstring says "O(all bytes)" and needs updating.
14. Bucket-wide reconciliation of unclaimed prefixes: described in the
    `drop_dataset`/`ownership.py` docstrings and the engine's "24h reclamation
    sweep", but **I did not find it** in `opteryx-catalog`, `opteryx-core`,
    `jobs.opteryx`, `control.opteryx` or `dispatch.opteryx`. Where it lives is
    unverified (infrastructure repo?). Its claim set is the stored `location`s, which
    is unaffected by this change.

## Open questions (architect)

**Q1 - Location format. BUILT as recommended, minus the id field.** `location =
gs://<bucket>/<workspace>/<uuid4 hex>` - opaque, no collection or name in it, so it
stays true across a rename (including cross-collection). Costs: the bucket stops
being human-browsable by name; anything listing `<workspace>/<collection>/` by prefix
would break (none found, not exhaustive). Storage listings are all per dataset
location, and the workspace prefix still contains everything. **No separate
`dataset-id` field was added**: the location is the identity, and it is what a retry
could be keyed on. Adding an explicit `dataset-id` (for Q4/Q6) is still open.

**Q2 - CTAS needs a location before the doc exists. BUILT in catalog and engine.**
The pending writer reports its location as `DataFileWriter.location`. The engine's
`DataFileStream` reads it off the first file, hands it back to every later writer
(`pending_location` -> `open_pending_data_file_writer(location=...)`), and `Insert`
passes it to `create_relation(location=...)` -> `create_dataset(location=...)`. The
location travels with the statement. The catalog's in-process reservation stays as
the fallback for an engine that predates this (it still works, as long as writer and
create reach the same catalog instance - which `_get_catalog`'s cache gives it). Both
new arguments are only sent when there is a value, so an older catalog, LocalStore and
third-party connectors are unaffected. Tests: opteryx-core
`tests/storage/test_ctas_catalog_location.py`. Not yet verified against a live CTAS.

**Q3 - Backfill. Nothing run; no byte moves.** Datasets created before this change
keep their name-derived `location` and are rename-safe because no create derives it
again. **Correction to the earlier text:** the docs with no `location` are not (only)
legacy - stubs projected from an external catalog never record one
(`stub_projection.py` writes none), and `_dataset_from_doc` still derives a location
for them. That fallback is harmless (a stub owns no files, nothing is written there,
rename refuses it) but contradicts "no fallbacks". Removing it needs a ruling on what a
stub's location IS (absent? empty?) - not a backfill. For real datasets lacking
`location`, if any exist, a dry-run-first script would count them; it was not written.

**Q4 - Forks and clones.** Data-wise a stable location makes borrowed paths immune
to an upstream rename. What remains is the **name** references (fork block on the
fork, registry doc on the upstream, possibly in another workspace). This change
**keeps today's refusals** (either end of a fork, MVs, triggers). Lifting the fork
refusal means rewriting both ends transactionally across workspaces; with a
`dataset-id` the references could be by id instead. Separate decision.

**Q5 - Cross-collection rename. Supported, as built.** Under Q1's opaque location it costs nothing, so
recommendation is to keep it supported (as today). The implemented transaction
supports it. Confirm.

**Q6 - Retry after commit.** The transaction is atomic, so there is no partial
state. A re-run after commit raises `DatasetNotFound` for the source (and, if the
old name was recreated meanwhile, `DatasetAlreadyExists` for the target). Never
silent, but a retry cannot tell "already done" from "never existed". An exactly
idempotent retry needs an expected `dataset-id` (Q1). Ask: is `DatasetNotFound` on
retry acceptable for the worker's retry classification?

**Q7 - Interim. DECIDED 2026-10-30: ship gated.** The transactional rename ships
as implemented; RENAME is unavailable (`DatasetLocationReusable`) until Q1/Q2 land.
The copying rename is not kept as a fallback.

## Implemented

**Rename**: `rename_dataset` as one transaction (reads first, then writes), moving:
the dataset doc, `schemas`, `snapshots`, `tags`, `relationships` and suppressions (near
address rewritten), and the orphan quarantine (carried, since its paths stay valid).
The maintenance `lease` is not carried: it is a claim by a holder that knows the old
name. Guards preserved: locked, MV, triggers, fork either end, source missing, target
exists - and the target name is also checked against the view/task namespace (the old
code did not; `create_dataset` does). Post-commit and non-transactional, as before:
repointing inbound relationships on other datasets, the webhook, the audit record. A
crash between commit and those leaves stale inbound rows for `fsck` and no
webhook/audit; they are not retried by re-running rename.

Firestore transaction size limits apply to datasets with very many snapshot documents.
I could not confirm the current per-transaction write cap, so none is invented:
Firestore rejects an over-large transaction atomically and loudly.

**Create**: `_mint_location`, `_reserve_location` / `_take_reservation`,
`create_dataset(location=...)`. `_dataset_location` is gone. `scripts/restore_snapshot.py`
mints instead of deriving. `integrity.py` reports `duplicate-location`.

Tests: `tests/test_rename_dataset.py`, `tests/test_create_location.py`,
`tests/test_pending_data_file_writer.py` (fake Firestore keyed by path; rejects
read-after-write in a transaction).

## Remaining

- Roll out the catalog to every service that creates datasets BEFORE using RENAME
  (deploy order, above); run the `duplicate-location` fsck afterwards.
- Engine change is built (Q2) and `rename_relation`'s docstring updated. Verify CTAS end
  to end against a non-production workspace.
- Stubs and the `_dataset_from_doc` fallback (Q3): needs a ruling.
- Postgres stats manifest path pair (core + control) still derives from the name; it
  describes projected relations only, so it does not touch the location invariant.
- Rewrite the `drop_dataset` docstring ("location is derived from its name") and
  `ownership.py` prose now that it is not.
- Locate the bucket-wide reclamation sweep (item 14).
- Optional `dataset-id` for Q4/Q6 (forks by id; exactly idempotent rename retry).
- Concurrent writers: a commit in flight against the old name when the rename commits
  writes to a document that no longer exists. Pre-existing, not addressed here.
