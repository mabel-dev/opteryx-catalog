# External File Tables — Design

Status: **PROPOSED** (2026-10-26). Nothing is built.

An external file table is a dataset in this catalog whose data files live in
the customer's own object storage. The catalog holds the name, schema,
statistics, permissions and description; the customer holds the bytes. It is
created by pointing at either a glob (`gs://acme-lake/events/**/*.parquet`),
which keeps discovering new files, or an explicit list of files, which never
reads anything else.

## 1. Problem

The engine already reads a folder of parquet with no ceremony - the CLI does it,
and `READ_*` with a secret does it in the worker. What it cannot do is make that
folder a *table*: something with a name in a collection, that other people can
be granted, that has a description and statistics, that appears in
`information_schema`, OData and the Studio, and that a view or MV can be built
on.

Today the only way to get there is to copy the data in, which means we host
it, they pay us to store a second copy, and it is stale the moment their
pipeline writes the next file.

## 2. Why a dataset kind and not a catalog provider

A provider (`binding.py`) binds a WHOLE WORKSPACE to another catalog, and that
catalog is the authority for the table list, schemas and history; we project
stubs of it. Neither half fits:

* **Granularity.** People want one external table beside their native ones. A
  bound workspace cannot domicile native datasets at all
  (`stub_projection.py`, `_existing_datasets`), so a provider would
  mean a workspace per folder.
* **Authority.** For a folder of parquet there is no other catalog. Name,
  schema, statistics, secret, permissions and description have to be
  authoritative somewhere, and the only somewhere is here. A provider would
  have to grow a second metadata store to hold them.

Being a dataset also inherits, for nothing: permissions, audit, webhooks,
descriptions, favourites, relationship inference, manifest statistics and
pruning, and use as a view or MV source.

A `files` *provider* - map a bucket, one folder per table, as a workspace -
remains possible later, as a thin binding that lists folders and projects one
of these per folder. It needs this; this does not need it.

## 3. Concepts

* **Source** - where the bytes are. Either a glob or a list of file URIs, all
  under the SCOPE of one secret.
* **Mode** - `glob` or `files`.
  * `glob`: the file set is whatever the glob matches *at query time*. New
    files are read by the next query without anyone doing anything.
  * `files`: the file set is exactly the list given. Nothing else is ever
    read, whatever else lands beside them.
* **Secret** - a `gcs_service_account` or `aws_access_key` secret in the same
  workspace. It is the only credential any read of this table uses (S6.1).
* **Location** - as for every dataset, a prefix the CATALOG owns
  (`gs://{catalog-bucket}/{ws}/{coll}/{name}`). It holds the statistics
  manifest and nothing else. It is NEVER the source (S6.2).
* **Statistics manifest** - the ordinary manifest format, one row per file,
  keyed by `(file_path, generation)`. In `files` mode it is the authoritative
  file list; in `glob` mode it is a cache (S4).

## 4. Data model

### 4.1 The `external` block on the dataset document

```yaml
external:
  mode: "glob" | "files"
  source: "gs://acme-lake/events/**/*.parquet"   # glob mode only
  secret: "lake_reader"                           # name, workspace-local
  format: "parquet"
  manifest-list: "gs://{catalog-bucket}/.../stats-<uuid>.parquet"
  refreshed-at-ms: int | None
  refreshed-by: str | None
  schema-policy: "frozen"                         # S5
```

A new document field and a matching `DatasetMetadata.external: ExternalSource
| None`, read back in the loader beside `external_catalog`. It is deliberately
NOT the `external-catalog` stub marker: a stub is a projection something else
refreshes and nothing commits to, and every reader that branches on
`external_catalog` assumes a binding exists behind it.

The schema is stored the native way (the `schemas` subcollection and
`current-schema-id`), not inline as stubs do, because this catalog owns its
evolution (S5).

There are no snapshots. Nothing commits rows, so there is no history for a
snapshot to record; `current_snapshot_id` is `None`, as for a stub.

### 4.2 The statistics manifest

Same schema as a snapshot manifest, plus one column: `generation` (GCS object
generation, or S3 ETag). Written under the dataset's own location. Each
refresh writes a new file and swaps the pointer on the document; the previous
file is deleted after the swap, as an owned path.

Explicit file lists are held here and not on the document: a few thousand URIs
is enough to put a document near Firestore's 1 MiB limit.

### 4.3 Cleaners must know about it

`DatasetDeepClean` and orphan detection compute the referenced set from
SNAPSHOT manifests (`deep_clean.py:317`). An external table has none, so its
statistics manifest - which sits under its own location - would read as an
orphan and be quarantined. Both must add `external.manifest-list` to the
referenced set. For a `files` table that manifest IS the table definition, so
losing it loses the table.

## 5. Schema

Inferred at CREATE from the files (the engine already does this), then frozen.
Reading a file against the frozen schema:

* a column the file lacks reads as null;
* a column only the file has is ignored;
* a type the engine cannot widen to the frozen type is an error that names the
  file and the column.

`ALTER TABLE t REFRESH SCHEMA` re-infers and writes a new schema version. It is
explicit because a new file with a renamed column should not silently change
what every dashboard on the table sees.

## 6. Invariants

### 6.1 Every read goes through the table's secret, never ambient credentials

`is_admissible_path` (`ownership.py`) exists because a manifest is a
capability: the engine reads every path it names with its own credentials, so
an entry naming someone else's bucket is a confused deputy. External tables
are the deliberate exception to "paths under location only", and the price of
the exception is that no external path is EVER opened with ambient
credentials. The engine resolves the secret and reads with it, or does not
read.

`scope_admits` is checked at READ time for every file, not only at CREATE. The
secret's scope can be narrowed after the table is made, and a glob's
expansion is not known at CREATE anyway.

### 6.2 The catalog never deletes a source file

`is_own_path` permits deleting anything under `location`. That is safe here
only because `location` is a catalog-owned prefix and is never set to the
source. On top of that, every physical deleter checks `metadata.external` and
deletes nothing that is not the statistics manifest: `SnapshotExpiration`,
`DatasetDeepClean`, orphan quarantine, compaction, `drop_dataset`,
`drop_workspace`, and `rename_dataset`'s copy pass (which must not copy, or
remap, source paths - it moves only the statistics manifest).

### 6.3 Creating a table delegates the secret

Reading an external table does not require permission to use its secret -
that is the point of the table. So CREATE is a grant: whoever creates it lets
everyone who can read the table read whatever the source covers. CREATE
therefore requires the creator to be allowed to use the secret, and the audit
record for CREATE names the secret.

### 6.4 Dropping the secret

`DROP SECRET` refuses while external tables name it, listing them, unless
`FORCE`. A forced drop leaves the tables in place; reads fail with "secret
lake_reader no longer exists" rather than anything vaguer.

## 7. Reading

### 7.1 `files` mode

Plan from the statistics manifest exactly as for a native snapshot. Before a
file is read, its current generation is compared with the manifest's. On a
mismatch the file has been overwritten in place: the read proceeds, but that
file's statistics are not used for pruning, and the table is marked as needing
a refresh. Pruning on stale min/max is the one way this can return wrong
answers silently, so it is the one thing that must not happen.

A missing file is an error naming it. In `files` mode the user said exactly
which files, so a vanished one is not a smaller table.

### 7.2 `glob` mode

One LIST per query, through the same glob expander `READ_*` uses (so the two
cannot disagree about what `**` means). Then, against the manifest by
`(path, generation)`:

* **known and unchanged** - planned with its statistics, prunable;
* **new, or generation changed** - read unpruned, and queued for statistics;
* **in the manifest but not listed** - dropped silently. In glob mode the
  glob is the definition, and a deleted file is a smaller table.

A query is correct with no refresh ever having run. Refresh only makes it
faster.

LIST cost scales with file count (GCS returns 1,000 objects a page): a
100,000-file glob is 100 calls before any data is read. S13 asks whether a
staleness window is wanted.

### 7.3 Secret audit volume

`use_secret` writes one audit record per call, with the path. Calling it per
file would write thousands of records per query against a large glob. The
engine resolves the secret ONCE per query with `path` set to the source glob
(or the table's scope root in `files` mode), then checks each file with
`SecretMaterial.admits` in-process.

## 8. Refresh

`ALTER TABLE t REFRESH` (and a scheduled equivalent): list the source (glob
mode) or stat the files (files mode), read footers for any file whose
`(path, generation)` is not in the manifest, write a new manifest, swap the
pointer, stamp `refreshed-at-ms`. Footers only - no data pages are read - so
cost tracks the number of new files, not their size.

Refresh also refreshes `statistics` (`row-count`, `total-file-size-bytes`) on
the document, so planning and the Studio have totals without a LIST.

## 9. What does not apply

| Feature | External table | Why |
|---|---|---|
| INSERT / UPDATE / DELETE / MERGE | Refused | Not our bytes to write. |
| Snapshots, time travel, tags, rollback | Refused | No history: the source changes without telling us. |
| Compaction, expiration | Skipped | Nothing of theirs to rewrite or retire. |
| CLONE as upstream | Refused | A fork borrows an upstream's files with the ENGINE's credentials; these must only ever be read with the secret. Same guard as `external_catalog` (`opteryx_catalog.py:6597`). |
| CLONE as target | Refused | |
| MV source, commit-triggered | Refused | No commits, so no trigger can fire. |
| MV source, schedule-triggered | Allowed | |
| Views | Allowed | |
| Vector indexes | Refused, v1 | They are keyed by file path and ordinal and assume immutable files. |

Provenance: a read records the dataset with `snapshot_id = None` and a
fingerprint of the `(path, generation)` set it read, so "what did this MV
refresh read" is still answerable.

## 10. SQL surface

```sql
CREATE EXTERNAL TABLE ws.coll.events
  FROM 'gs://acme-lake/events/**/*.parquet'
  WITH (SECRET = lake_reader);

CREATE EXTERNAL TABLE ws.coll.ref_codes
  FROM FILES ('gs://acme-lake/ref/codes-2026.parquet',
              'gs://acme-lake/ref/codes-2025.parquet')
  WITH (SECRET = lake_reader);

ALTER TABLE ws.coll.ref_codes ADD FILES ('gs://acme-lake/ref/codes-2024.parquet');
ALTER TABLE ws.coll.ref_codes DROP FILES ('gs://acme-lake/ref/codes-2025.parquet');
ALTER TABLE ws.coll.events REFRESH;
ALTER TABLE ws.coll.events REFRESH SCHEMA;
DROP TABLE ws.coll.events;   -- metadata and statistics only; the source is untouched
```

`ADD FILES` / `DROP FILES` are `files` mode only, and validate every path
against the secret's scope before writing the new manifest. `DROP TABLE`'s
message says the source files were not deleted, because somebody will expect
them to be.

## 11. Surfaces

* `information_schema.tables` gains a `table_type` of `EXTERNAL`, and a
  `information_schema.external_tables` view carrying mode, source, secret
  name, format and `refreshed-at-ms`. Never the secret's material.
* OData `$metadata` lists them like any dataset; statistics come from the
  document and the manifest as for a native one.
* Studio shows the source and the last refresh time beside a refresh control,
  as it does for a bound workspace's listing.

## 12. Billing

Storage is theirs, so nothing is billed for it; the statistics manifest is
negligible. Reads are billed as reads. Cross-cloud reads (S3 from GCP) put
egress on the customer's cloud bill, not ours, and the docs should say so
plainly, because it is the cost they will not see coming.

## 13. Rollout

1. **opteryx-catalog**: the `external` block and its loader; `create_external_
   dataset` / `add_external_files` / `drop_external_files` /
   `refresh_external_dataset`; the S6.2 guard in every deleter; the S4.3
   referenced-set fix in deep clean and orphan detection; the CLONE and
   DROP SECRET rules. Deployable alone: nothing reaches it.
2. **opteryx-core**: the SQL in S10, the connector read path (S7) through
   the secret, the refusals in S9, `information_schema`.
3. **jobs.opteryx**: scheduled refresh.
4. Studio and OData.

## 14. Open questions

1. **Hive partitions.** Should `dt=2026-10-01/` path segments become columns?
   The CLI may already do this; matching it is the natural default, but it is
   more schema to freeze. Proposal: off in v1, `WITH (HIVE_PARTITIONING = TRUE)`
   later.
2. **Formats beyond parquet.** The engine reads CSV and JSONL, but they have no
   footers, so refresh cannot gather statistics without reading the whole
   file. Proposal: parquet only in v1.
3. **A staleness window for glob listing.** `WITH (LIST_CACHE_SECONDS = 300)`
   would cut LIST cost for very large globs at the price of "always discovers
   new files". Not in v1 unless the first large customer needs it.
4. **Refresh as a trigger.** A refresh that sees new files could fire the same
   signal a commit does, which would let commit-style MV triggers work on
   external tables. Attractive, but it makes refresh frequency an MV latency
   knob.
5. **A source spanning several secrets** (two buckets, two service accounts).
   Proposal: no. One table, one secret; a view over two tables covers it.
