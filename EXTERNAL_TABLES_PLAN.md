# External File Tables — Cross-Repo Implementation Plan

Implements `EXTERNAL_TABLES_DESIGN.md` (section references below are to it). Read that
first - this file sequences the work and records what the code survey settled; it does
not restate the rationale.

Repos touched: **opteryx-catalog**, **worker.opteryx**, **opteryx-core**, **xb500.opteryx**
(maintenance sweeps), **control.opteryx** and **odata.opteryx** (surfaces). jobs.opteryx
needs no code if scheduled refresh rides the existing schedule triggers (Phase 4) - check
that rather than assume it, per the lesson in WORKSPACE_CATALOG_RESOLUTION_PLAN.md: when
this says "the place that does X", look for a second one.

## Settled by the code survey (not in the design doc)

1. **The catalog never reads source bytes.** It holds no customer credentials and its
   `iops` read with the catalog's own identity, which is exactly the ambient read S6.1
   forbids. Footers are read in the engine through the credentialed filesystem that
   `credentials =>` already built (`opteryx-core/opteryx/connectors/io_systems/
   credentialed_filesystem.py`, `object_listing.py`). The catalog's API takes finished
   manifest entries and validates every path against the secret's SCOPE before writing.
   This also answers where refresh runs (S8): in the engine, as a statement.
2. **The secret resolver needs a table-bound path.** `worker.opteryx/app/
   secret_resolver.py::_may_use` admits a caller only with `ALTER` on the whole workspace
   (jobs secrets.md S9.1). A reader of an external table must not need that (S6.3). The
   new path takes a DATASET identifier, never a secret name: the secret comes from the
   dataset document, and the gate is READ on the dataset, which the binder has already
   checked. A query can never name the secret this path resolves.
3. **`save_dataset_metadata` rewrites the dataset document whole** with `set()`
   (`opteryx_catalog.py:8822`). The `external` block must be carried there the way `fork`
   is (conditional key, absent for a native dataset), or the first metadata save
   (a `COMMENT ON`, say) un-externals the table.
4. **The manifest pointer is swapped by compare-and-swap, not through
   `save_dataset_metadata`.** Two concurrent refreshes must not both win: a transaction
   that updates `external.manifest-list` only if it still holds the value the refresh
   started from, the same shape as `_set_if_pointer_unmoved`. The loser's manifest is an
   owned path and is deleted.
5. **The secret's dependents are a registry, like forks.** `DROP SECRET` cannot
   discover referencing tables cheaply (it would be a collection-group query on a nested
   field). `$secrets/{name}/dependents/{collection.dataset}`, written on create, removed
   on drop, moved on rename - the `forks/` registry pattern (`register_fork` /
   `deregister_fork`).
6. **Maintenance runs in xb500.opteryx** (`app/operations/trigger_expiration.py` and
   siblings). The catalog-side guards make it safe whatever xb500 does; the xb500 change
   is only to stop it spending a lease and a log line on a dataset it cannot touch.

## Dependency graph

```
Phase 1 (catalog) ──┬──> Phase 2 (worker resolver) ──> Phase 3b (engine read path)
                    ├──> Phase 3a (engine DDL + refresh)  ──> Phase 3b
                    ├──> Phase 4 (xb500 skip)
                    └──> Phase 5 (control / odata / Studio)    [after 3b]
```

Phase 1 is deployable alone: nothing calls it. Phase 2 is deployable alone: nothing
calls the new resolver entry. 3a and 3b ship together - a CREATE with no read path is a
table nobody can query.

---

## Phase 1 — opteryx-catalog

### 1.1 Data model
- `catalog/metadata.py`: `ExternalSource` dataclass (`mode`, `source`, `files_count`,
  `secret`, `format`, `manifest_list`, `refreshed_at_ms`, `refreshed_by`,
  `schema_policy`) with `to_dict` / `from_dict` in the `Fork` style;
  `DatasetMetadata.external: ExternalSource | None = None`.
- Loader (`opteryx_catalog.py` near 1661, beside `external_catalog`): read the block.
- `save_dataset_metadata` (8822): carry `external` conditionally, as `fork`.
- `resource_types.py`: no new member; an external table is a `DATASET`.

### 1.2 Ownership and the read-only rule
- `catalog/ownership.py`: `may_delete(metadata, path)` - `is_own_path` for a native
  dataset; for an external one, true only for a path under `location` (its manifests).
  Every physical deleter calls this instead of `is_own_path` directly:
  `SnapshotExpiration._delete_file`, `DatasetDeepClean`, `orphan_quarantine`,
  `drop_dataset` (1851), `drop_workspace` (2437).
- `rename_dataset` (2062): an external table moves its manifest and rewrites nothing in
  it - `_remap` already returns foreign paths unchanged, but assert it rather than rely on
  it, and move the secret's dependents entry.
- `clone_dataset` (6657): extend the `external_catalog` guard at 6597 to `external`,
  both as upstream and as target.
- `catalog/dataset.py`: one `_refuse_if_external(operation)` at the top of every public
  write - `append`, `add_files`, `truncate_and_add_files`, the streaming writer, MoR
  deletes, merge, rollback, tag create, vector index create. New exception
  `ExternalTableReadOnly` in `exceptions.py`, naming the operation and saying the source
  is the customer's to change.

### 1.3 Cleaners see the manifest
- `deep_clean.py` (317, 522) and `orphan_quarantine.py`: add `external.manifest_list`
  to the referenced set. Without this the first deep clean quarantines a `files`-mode
  table's definition (S4.3).
- `integrity.py` fsck: an external dataset whose `manifest-list` is missing is an
  integrity error, not "no snapshots yet".

### 1.4 Lifecycle API
- `create_external_dataset(identifier, *, mode, source=None, entries=(), secret, schema,
  author)`. Checks: secret exists in this workspace; type matches the scheme;
  `scope_admits` for the glob's literal prefix (glob mode) or every entry (files mode);
  no name collision. Writes the dataset document, the schema, the manifest under
  `location`, and the dependents entry. Audit `create_external_dataset` naming the
  secret (S6.3).
- `replace_external_manifest(identifier, entries, *, expected_manifest_list, author,
  schema=None)`: the one write path for refresh, `ADD FILES`, `DROP FILES`, and
  `REFRESH SCHEMA` (when `schema` is given). Validates every entry against the scope,
  writes the new manifest, CAS-swaps the pointer (settled 4), deletes the old manifest.
- `ExternalSource` stats on the document: `statistics.row-count`,
  `statistics.total-file-size-bytes`, written by the same transaction.
- Manifest entries gain a `generation` column (`catalog/manifest.py`,
  `manifest_arrow.py`). Absent on native manifests; readers treat absent as unknown.

### 1.5 Secrets
- `secrets/store.py`: `register_secret_dependent` / `deregister_secret_dependent`;
  `drop_secret(..., force=False)` refuses with `SecretInUse` listing the dependents;
  `force=True` drops anyway and leaves the dependents registry for the error message to
  read (S6.4).
- `use_external_table_secret(identifier, *, used_by, query_id)`: loads the dataset,
  takes `external.secret`, calls `use_secret` once with `path` set to the source root and
  `purpose="external-table"` (S7.3). The worker's table-bound path (Phase 2) calls this;
  nothing takes a secret name from the caller.

### 1.6 Provenance
- `provenance.py`: a read of an external table records `read_source_key(dataset, None)`
  plus a `files-fingerprint` (hash of the sorted `(path, generation)` set actually read).

### 1.7 Tests (new files)
`test_external_dataset_lifecycle.py` (create, both modes, scope refusals, collision,
rename, drop leaves source untouched), `test_external_never_deletes.py` (every deleter
over a manifest naming foreign paths: none deleted; the manifest itself is),
`test_external_read_only.py` (each refused write), `test_external_manifest_cas.py`
(concurrent refresh: one wins, loser's manifest gone), `test_external_deep_clean.py`
(manifest not quarantined), `test_secret_dependents.py`, and a round-trip case in
`test_dataset_metadata.py` proving a plain `save_dataset_metadata` keeps the block.

Run per the test-environment note: `PYTHONPATH=$PWD python3 -m pytest tests/ -q -rs`;
baseline is the 21 `croniter` failures; check skips, not just failures.

---

## Phase 2 — worker.opteryx: the table-bound resolver

- `app/secret_resolver.py`: `resolve_table_secret(execution_context, identifier)`.
  Gate: READ on the dataset (re-checked here, not trusted from the binder). Calls
  `use_external_table_secret`. Returns the same `ObjectStoreCredential` the
  `credentials =>` path does, so core's credentialed filesystem is reused unchanged.
- Register it beside the existing resolver in `app/worker.py`.
- Tests: a caller with READ on the table and no ALTER on the workspace resolves; a
  caller without READ is refused before the secret is touched; the audit record names
  the table, not just the secret.
- **Security review before merge.** This is the one place a non-owner reaches a
  customer credential. The review question: can any input to this function choose
  which secret is resolved, other than a dataset the caller can already read?

---

## Phase 3 — opteryx-core

### 3a DDL and refresh
- `utils/query_parser.py` pre_parse intercepts, the `RESYNC`/`DETACH` precedent:
  `CREATE EXTERNAL TABLE ... FROM '<glob>' | FROM FILES (...) WITH (SECRET = name)`,
  `ALTER TABLE ... ADD FILES | DROP FILES | REFRESH | REFRESH SCHEMA`,
  `DROP SECRET ... FORCE`.
- Binder (`planner/binder/relation.py`): CREATE requires CREATE in the collection AND the
  workspace's manage-secrets right (`ALTER`, as jobs secrets.md S15 settled), because
  CREATE delegates the secret (S6.3). ADD/DROP FILES the same. REFRESH requires ALTER on
  the table only - it widens nothing the creator did not already grant.
- Refresh executor: list (glob mode) or stat (files mode) through the credentialed
  filesystem; read footers for new `(path, generation)` pairs only; build entries with
  the same stats code native writes use; `replace_external_manifest`. Schema inference at
  CREATE uses the same footer reader.
- `OpteryxConnector` (`connectors/opteryx_connector.py`): `create_external_relation`,
  `alter_external_relation`, `refresh_relation`, alongside `clone_relation`.
- `parse_error.py` keyword list: `EXTERNAL`, `FILES`, `REFRESH` (the `LOAD`/`SAMPLE`
  miss from the forks build).

### 3b Read path
- `OpteryxConnector`: when the loaded metadata has `external`, resolve the credential
  through the table-bound resolver and build the scan on the credentialed filesystem.
  **No fallback**: no resolver registered, or a refusal, is an error.
- Glob mode (S7.2): one listing through `object_listing.py`, diff against the manifest
  by `(path, generation)`; known files prunable, new or changed files unpruned, absent
  files dropped. Every listed file through `credential.admits` before it is planned.
- Files mode (S7.1): generation check per file; mismatch reads the file without its
  stats; missing file is an error naming it.
- Frozen-schema reconciliation (S5): null-fill, ignore extras, typed error naming file
  and column.
- `physical_planner/__init__.py:678`: the existing "every external table would return
  empty" guard is about bound workspaces; check it does not swallow this kind.
- `information_schema.tables.table_type = 'EXTERNAL'`; new
  `information_schema.external_tables`.
- Refusals at bind time for everything S9 lists, so the user sees the SQL error rather
  than the catalog backstop.

### 3c Tests
`tests/unit/planner/test_external_table_statements.py` (parse, bind, gates), and storage
tests against a fake credentialed filesystem: new-file discovery, deleted file, overwritten
file not pruned, scope refusal on a listed file, no ambient read (a filesystem that
raises if constructed without a credential).

---

## Phase 4 — scheduled refresh and maintenance
- Scheduled refresh: a schedule trigger whose statement is `ALTER TABLE t REFRESH`.
  Verify `schedules.py` and dispatch accept an ALTER statement; if they only run
  SELECT-shaped work, this phase grows a jobs.opteryx change.
- xb500.opteryx: skip `external` datasets in the expiration, deep-clean and compaction
  sweeps.

## Phase 5 — surfaces
- control.opteryx: dataset detail returns the `external` block minus nothing secret
  (the secret NAME is fine); refresh endpoint.
- odata.opteryx: BOTH `$metadata` implementations (service-wide and per-dataset - the
  resolution plan's lesson), statistics from the document.
- Studio: source, mode, last refresh, refresh control.
- docs.opteryx: the syntax, and the cross-cloud egress note (S12).

---

## Live verification (staging, before release)

Two buckets: A, readable by the secret and NOT by the worker's service account; B,
readable by the worker's service account and NOT by the secret.

1. Glob table over A reads. This is the proof that the secret is used: ambient
   credentials cannot read A.
2. A table whose glob would reach B is refused at CREATE (scope) - and with the scope
   widened to cover B's bucket name, refused at read (the store says no). It must never
   succeed, which would mean ambient credentials were used.
3. Drop a file into A; the next query sees it with no refresh.
4. Overwrite a file in A in place with different values; a query filtering on the old
   min/max still returns the new rows.
5. DROP TABLE; every object in A is still there.
6. Deep clean and expiration over the workspace; every object in A is still there, and
   the table still reads.
7. A user with READ on the table and no workspace ALTER can query it; the same user
   cannot use `credentials => 'ws.secret'` directly.

## Recommended execution order

Phase 1 (1.1-1.3 first: the guards are useful even before anything creates an external
table) → Phase 2 with its security review → 3a + 3b together → live verification →
Phase 4 → Phase 5.

## Out of scope (settled in the design doc)

Writes of any kind; snapshots and time travel; forking either way; commit-triggered MVs;
hive partitioning, non-parquet formats and a listing cache (S14 open questions, not v1);
a source spanning several secrets.
