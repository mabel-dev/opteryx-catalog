#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Rewrite legacy CATEGORY type names in persisted Firestore schemas to the
canonical exact type names the catalog writes today.

Background. Schemas used to persist the dispatch CATEGORY name rather than the
exact type - ``INTEGER`` for every integer width and for IPV4, ``FLOAT`` for
both float widths - which is lossy in the direction that silently WIDENS (see
``_core_type_to_stored`` in ``opteryx_catalog/opteryx_catalog.py``). The write
path was fixed to store the exact name; this script brings the ALREADY-PERSISTED
documents into line.

This is meaning-preserving, not a reinterpretation. Every rename below is a
spelling that opteryx-core's ``_SQL_NAME_ALIASES`` (``opteryx/types/
logical_type.py``) ALREADY resolves to the same target, so a reader resolves
these documents identically before and after. What changes is that the stored
name stops being ambiguous.

    NOTE the widths are NOT being recovered, because they are not recoverable
    from Firestore - the old name genuinely did not record them. A column that
    was physically UINT32, INT8 or IPV4 was persisted as ``INTEGER`` and
    becomes ``INT64`` here, which is exactly what a reader already resolves it
    to today. Recovering true widths means re-deriving schemas from the parquet
    footers in GCS; that is a separate job, and this migration neither helps nor
    hinders it.

Scope. Only ``{workspace}/{collection}/datasets/{dataset}/schemas/{id}`` docs,
and within them only ``columns[].type`` and ``columns[].element-type``. View
documents carry no column types. Every other field is left byte-identical -
the write uses ``update()`` on the ``columns`` field alone, never ``set()``.

Safety. Dry-run by default; ``--apply`` writes. Every document that will change
is dumped to a backup JSON first, and ``--restore <file>`` puts it back. The
rename table is idempotent (no target is itself a key), so re-running is a
no-op.

Usage:

    python scripts/migrate_schema_type_names.py                      # dry run, all workspaces
    python scripts/migrate_schema_type_names.py --workspace opteryx  # dry run, one workspace
    python scripts/migrate_schema_type_names.py --apply
    python scripts/migrate_schema_type_names.py --restore backup-....json

Requires ``GOOGLE_APPLICATION_CREDENTIALS`` plus ``GCP_PROJECT_ID`` and
``FIRESTORE_DATABASE`` (or the matching ``--project``/``--database`` flags).
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import sys
import time

from google.cloud import firestore

# Legacy stored spelling -> canonical name to store instead.
#
# Sourced from opteryx-core's `_SQL_NAME_ALIASES`; `--verify-mapping` re-checks
# every entry against that table when opteryx-core is importable, so this cannot
# drift silently. Names observed live are marked; the rest are aliases the same
# table accepts, included so the sweep is complete rather than only covering
# what today's snapshot happens to contain.
_RENAMES = {
    "INTEGER": "INT64",  # observed
    "INT": "INT64",
    "BIGINT": "INT64",
    "TINYINT": "INT8",
    "SMALLINT": "INT16",
    "DOUBLE": "FLOAT64",  # observed
    "FLOAT": "FLOAT64",  # observed -- FLOAT means double, not FLOAT32; see below
    "REAL": "FLOAT32",
    "STRING": "VARCHAR",
    "TEXT": "VARCHAR",
    "BYTES": "VARBINARY",
    "BLOB": "VARBINARY",  # observed
    "STRUCT": "NVARCHAR",  # observed (as an element-type)
    "JSONB": "NVARCHAR",  # observed
    # BOOL is canonical in opteryx-core; BOOLEAN only still reads back via the
    # alias table. `_DRAKEN_STORED_NAME` was changed to emit BOOL in the same
    # commit as this script, so the rewrite sticks instead of being undone by
    # the next schema write.
    "BOOLEAN": "BOOL",  # observed
}

# Deliberately NOT renamed:
#
# TIMESTAMP/
# DATE/TIME - bare spellings are canonical-with-default (bare TIMESTAMP means
#            microseconds) and are what the write path still emits. Rewriting to
#            TIMESTAMP[us] is churn, not a fix.
# ARRAY/
# DECIMAL   - deliberately do NOT parse standalone: their parameters live in the
#            separate element-type / precision / scale fields, and the reader has
#            a parameter-aware branch for them.


def _verify_mapping() -> int:
    """Cross-check every rename against opteryx-core's own parser."""
    try:
        from opteryx.types.logical_type import try_parse_column_type
    except ImportError as exc:
        print(f"cannot verify: opteryx-core not importable ({exc})", file=sys.stderr)
        return 2

    failures = 0
    for legacy, canonical in _RENAMES.items():
        before = try_parse_column_type(legacy)
        after = try_parse_column_type(canonical)
        ok = before is not None and str(before) == str(after)
        if not ok:
            failures += 1
        print(f"  {'ok ' if ok else 'FAIL'}  {legacy:9} -> {canonical:9} ({before} vs {after})")
    print("mapping verified" if not failures else f"{failures} mapping FAILURES")
    return 1 if failures else 0


def _rewrite_columns(columns: list, renames: dict) -> tuple[list, list]:
    """Return ``(new_columns, changes)``. ``changes`` is a list of
    ``(column_name, field, old, new)`` describing what moved."""
    new_columns = []
    changes = []
    for col in columns:
        col = dict(col)
        for field in ("type", "element-type"):
            old = col.get(field)
            if not isinstance(old, str):
                continue
            new = renames.get(old.upper())
            if new is not None and new != old:
                col[field] = new
                changes.append((col.get("name"), field, old, new))
        new_columns.append(col)
    return new_columns, changes


def _iter_schema_docs(client, workspace: str | None):
    """Yield ``(doc_ref, data)`` for every schema document in scope."""
    workspaces = [client.collection(workspace)] if workspace else list(client.collections())
    for ws in workspaces:
        for coll_doc in ws.list_documents():
            for ds_ref in coll_doc.collection("datasets").list_documents():
                for snap in ds_ref.collection("schemas").stream():
                    yield snap.reference, (snap.to_dict() or {})


def migrate(client, *, workspace, apply, renames, backup_path) -> int:
    planned = []  # (doc_ref, original_data, new_columns, changes)
    scanned = 0
    tally = collections.Counter()

    for doc_ref, data in _iter_schema_docs(client, workspace):
        scanned += 1
        columns = data.get("columns") or []
        new_columns, changes = _rewrite_columns(columns, renames)
        if changes:
            planned.append((doc_ref, data, new_columns, changes))
            for _, field, old, new in changes:
                tally[f"{old} -> {new}" + (" (element-type)" if field != "type" else "")] += 1

    print(f"scanned {scanned} schema documents; {len(planned)} need changes\n")
    if not planned:
        print("nothing to do -- catalogs already use canonical type names")
        return 0

    for doc_ref, _, _, changes in planned:
        print(f"  {doc_ref.path}")
        for name, field, old, new in changes:
            suffix = "" if field == "type" else f"  [{field}]"
            print(f"      {name}: {old} -> {new}{suffix}")

    print("\nrename totals:")
    for label, count in tally.most_common():
        print(f"  {count:5}  {label}")

    if not apply:
        print("\nDRY RUN -- nothing written. Re-run with --apply to commit.")
        return 0

    with open(backup_path, "w") as handle:
        json.dump(
            [{"path": ref.path, "document": data} for ref, data, _, _ in planned],
            handle,
            indent=2,
            default=str,
        )
    print(f"\nbacked up {len(planned)} documents to {backup_path}")

    written = 0
    for doc_ref, _, new_columns, _ in planned:
        # update(), not set() -- a schema doc also carries timestamp-ms, author
        # and sequence-number, and set() would erase anything not restated.
        doc_ref.update({"columns": new_columns})
        written += 1
    print(f"updated {written} schema documents")
    return 0


def restore(client, backup_path: str) -> int:
    with open(backup_path) as handle:
        entries = json.load(handle)
    for entry in entries:
        client.document(entry["path"]).update({"columns": entry["document"]["columns"]})
    print(f"restored columns on {len(entries)} documents from {backup_path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project", default=os.environ.get("GCP_PROJECT_ID"))
    parser.add_argument("--database", default=os.environ.get("FIRESTORE_DATABASE"))
    parser.add_argument("--workspace", help="limit to one workspace (default: all)")
    parser.add_argument("--apply", action="store_true", help="write changes")
    parser.add_argument("--restore", metavar="BACKUP_JSON", help="undo a prior --apply")
    parser.add_argument(
        "--verify-mapping",
        action="store_true",
        help="check the rename table against opteryx-core and exit",
    )
    parser.add_argument("--backup-dir", default=".")
    args = parser.parse_args()

    if args.verify_mapping:
        return _verify_mapping()

    if not args.project or not args.database:
        parser.error("--project/--database (or GCP_PROJECT_ID/FIRESTORE_DATABASE) required")

    client = firestore.Client(project=args.project, database=args.database)

    if args.restore:
        return restore(client, args.restore)

    backup_path = os.path.join(args.backup_dir, f"schema-type-backup-{int(time.time())}.json")
    return migrate(
        client,
        workspace=args.workspace,
        apply=args.apply,
        renames=_RENAMES,
        backup_path=backup_path,
    )


if __name__ == "__main__":
    raise SystemExit(main())
