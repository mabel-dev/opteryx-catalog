#!/usr/bin/env python3
"""Move vector index definitions from each dataset's `indexes` subcollection onto the
dataset document's `vector-indexes` map (ruled 2026-10-04, catalog/vector_indexes.py).

Usage:
    python scripts/migrate_vector_index_definitions.py                  # dry run
    python scripts/migrate_vector_index_definitions.py --apply          # copy onto the documents
    python scripts/migrate_vector_index_definitions.py --apply --retire # ...then delete the old docs

WHY. The catalog now reads and writes definitions ONLY on the dataset document, so a
query plans an index search from the dataset it already loaded. A definition still in the
old subcollection is invisible: its table is searched exactly (the safe direction - no
answer changes) until this has run.

TWO PHASES:
  1. `--apply` copies each subcollection definition into the document's map, in a
     transaction, unless the map already holds that name. Idempotent.
  2. `--retire` (with `--apply`) deletes a subcollection document only when the map holds
     the SAME definition (same index id).

REFUSED, AND REPORTED: a name the map already holds under a DIFFERENT index id - an index
was re-created under the new code while the old definition lingered. The map's is the
live one (its id keys the manifest's index files); the old document is left for a person.
"""

import argparse
import sys

DATASETS_SUBCOLLECTION = "datasets"
LEGACY_SUBCOLLECTION = "indexes"
FIELD = "vector-indexes"


def _qualified(dataset_ref) -> str:
    """`workspace.collection.dataset` from `{workspace}/{collection}/datasets/{dataset}`."""
    parts = dataset_ref.path.split("/")
    return f"{parts[0]}.{parts[1]}.{parts[3]}"


def collect(client, workspaces=None) -> dict:
    """{dataset ref path: (dataset ref, [(legacy doc ref, definition)])}."""
    found: dict = {}
    for doc in client.collection_group(LEGACY_SUBCOLLECTION).stream():
        dataset_ref = doc.reference.parent.parent
        if dataset_ref is None or dataset_ref.parent.id != DATASETS_SUBCOLLECTION:
            continue  # an `indexes` subcollection under something that is not a dataset
        if workspaces and _qualified(dataset_ref).split(".", 1)[0] not in workspaces:
            continue
        found.setdefault(dataset_ref.path, (dataset_ref, []))[1].append(
            (doc.reference, doc.to_dict() or {})
        )
    return found


def migrate(client, dataset_ref, legacy, apply: bool) -> tuple:
    """Copy `legacy` definitions onto the document. Returns (copied, already, conflicts)."""
    from google.cloud import firestore

    outcome = {"copied": [], "already": [], "conflicts": []}

    @firestore.transactional
    def _copy(transaction):
        for key in outcome:
            outcome[key] = []
        doc = dataset_ref.get(transaction=transaction)
        if not doc.exists:
            outcome["conflicts"] = [(d.get("name"), "the dataset document is gone") for _, d in legacy]
            return
        definitions = dict((doc.to_dict() or {}).get(FIELD) or {})
        for _ref, definition in legacy:
            name = definition.get("name")
            held = definitions.get(name)
            if held is None:
                definitions[name] = definition
                outcome["copied"].append(name)
            elif held.get("index-id") == definition.get("index-id"):
                outcome["already"].append(name)
            else:
                outcome["conflicts"].append(
                    (name, f"the document holds index id {held.get('index-id')}, the old "
                           f"subcollection {definition.get('index-id')}")
                )
        if apply and outcome["copied"]:
            transaction.update(dataset_ref, {FIELD: definitions})

    _copy(client.transaction())
    return outcome["copied"], outcome["already"], outcome["conflicts"]


def retire(dataset_ref, legacy) -> list:
    """Delete each old document whose definition the map now holds (same id)."""
    held = (dataset_ref.get().to_dict() or {}).get(FIELD) or {}
    retired = []
    for ref, definition in legacy:
        name = definition.get("name")
        if held.get(name, {}).get("index-id") == definition.get("index-id"):
            ref.delete()
            retired.append(name)
    return retired


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workspaces", nargs="*", help="limit to these workspaces (default: all)")
    parser.add_argument("--apply", action="store_true", help="write (default: dry run)")
    parser.add_argument("--retire", action="store_true",
                        help="with --apply: delete the migrated subcollection documents")
    parser.add_argument("--project", default="mabeldev")
    parser.add_argument("--database", default="catalogs")
    args = parser.parse_args()
    if args.retire and not args.apply:
        parser.error("--retire writes, so it needs --apply")

    from google.cloud import firestore

    client = firestore.Client(project=args.project, database=args.database)
    found = collect(client, set(args.workspaces) or None)
    copied = conflicts = retired = 0
    for dataset_ref, legacy in found.values():
        name = _qualified(dataset_ref)
        done, already, problems = migrate(client, dataset_ref, legacy, args.apply)
        for index in done:
            print(f"[{'COPIED' if args.apply else 'WOULD COPY'}] {name}: {index}")
        for index in already:
            print(f"[CURRENT] {name}: {index}")
        for index, problem in problems:
            print(f"[SKIPPED] {name}: {index}: {problem}")
        copied += len(done)
        conflicts += len(problems)
        if args.retire:
            for index in retire(dataset_ref, legacy):
                print(f"[RETIRED] {name}: {index}")
                retired += 1
    print(f"{len(found)} dataset(s); {copied} definition(s) "
          f"{'copied' if args.apply else 'to copy'}, {conflicts} skipped, {retired} retired")
    return 1 if conflicts else 0


if __name__ == "__main__":
    sys.exit(main())
