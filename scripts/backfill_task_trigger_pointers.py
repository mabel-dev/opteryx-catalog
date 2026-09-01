#!/usr/bin/env python3
"""Plant the `trigger` back-pointer on task documents that predate it.

Usage:
    python scripts/backfill_task_trigger_pointers.py                 # dry run
    python scripts/backfill_task_trigger_pointers.py --apply
    python scripts/backfill_task_trigger_pointers.py <workspace> ... [--apply]

WHY THIS EXISTS. `create_trigger` enforces the one-trigger rule by reading a
back-pointer on the TASK document (`trigger: {source, name}`), written in the
same transaction as the trigger itself. Triggers created before that field
existed have no pointer, so the guard reads absence and lets a second trigger
through - the rule is strict in code and silent on every task already wired.
This writes the pointer those tasks would have had.

DERIVED, NEVER DECLARED. The pointer is a cache of a fact Firestore already
stores the other way round: a trigger lives under the dataset that fires it.
This reads that fact and writes it back, so a run changes no wiring - the same
triggers fire the same tasks before and after. That is what makes it safe to
re-run: it is idempotent, and a pointer that already agrees is left alone.

WHAT IT REFUSES TO TOUCH:

- A task with TWO OR MORE triggers. Which one is real is a question about
  intent that no record answers, and writing either would silently bless it as
  the survivor and lock out the other. Reported and skipped; run
  `find_multiply_triggered_tasks.py` and unwire by hand first.
- A task whose pointer already names a DIFFERENT trigger. Same reason, plus:
  the pointer is the guard, so overwriting one is exactly the write that lets a
  second trigger through.
- A task document that does not exist. `drop_task` leaves triggers behind, so
  this is the ordinary dangling pair the orphan sweep reports; there is no
  document to carry a pointer and inventing one would invent a task.

Dry run by DEFAULT. `--apply` is the only thing that writes. Exits 1 if any
task was skipped for one of the reasons above (whether or not it wrote), 0 when
everything was either planted or already correct.

Companion to `scripts/find_multiply_triggered_tasks.py`, which is the verifier
this trusts: run that first, and expect it clean.
"""

import argparse
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from opteryx_catalog.opteryx_catalog import TASK_TRIGGER_KIND  # noqa: E402
from opteryx_catalog.opteryx_catalog import TASKS_SUBCOLLECTION  # noqa: E402
from opteryx_catalog.opteryx_catalog import TRIGGERS_SUBCOLLECTION  # noqa: E402


def _source_of(reference) -> str:
    """The qualified dataset a trigger document hangs under.

    Path is `{workspace}/{collection}/datasets/{dataset}/triggers/{name}`.
    """
    parts = reference.path.split("/")
    return f"{parts[0]}.{parts[1]}.{parts[3]}"


def _task_doc_ref(client, target_task: str):
    """The task document a trigger's `target-task` names.

    Split left-anchored with maxsplit, the same way `_split_qualified` does it:
    workspace and collection names may not contain dots, so a task name that
    does stays whole.
    """
    parts = target_task.split(".", 2)
    if len(parts) != 3:
        return None
    workspace, collection, task_name = parts
    return (
        client.collection(workspace)
        .document(collection)
        .collection(TASKS_SUBCOLLECTION)
        .document(task_name)
    )


def collect(client, workspaces=None) -> dict[str, list[dict]]:
    """Every task trigger in the catalog, grouped by the task it fires.

    One collection-group query, the same read `find_multiply_triggered_tasks`
    makes and for the same reason: triggers live under their sources, so this
    question has no home to be asked from.
    """
    by_task: dict[str, list[dict]] = defaultdict(list)
    for doc in client.collection_group(TRIGGERS_SUBCOLLECTION).stream():
        data = doc.to_dict() or {}
        if data.get("kind") != TASK_TRIGGER_KIND:
            continue
        target = data.get("target-task")
        if not target:
            continue
        source = _source_of(doc.reference)
        if workspaces and source.split(".", 1)[0] not in workspaces:
            continue
        by_task[target].append({"name": doc.id, "source": source, "reference": doc.reference})
    return by_task


def plan(client, by_task: dict[str, list[dict]]) -> list[dict]:
    """What each task needs, decided from the records as they stand.

    Reads only. The write in `--apply` re-reads inside a transaction and makes
    the same decision there, so a plan that went stale between the two is
    refused rather than applied against changed records.
    """
    actions = []
    for target, triggers in sorted(by_task.items()):
        if len(triggers) > 1:
            wired = ", ".join(
                f"{t['name']} ON {t['source']}"
                for t in sorted(triggers, key=lambda t: (t["source"], t["name"]))
            )
            actions.append(
                {"action": "skip", "task": target, "reason": f"{len(triggers)} triggers: {wired}"}
            )
            continue

        (trigger,) = triggers
        pointer = {"source": trigger["source"], "name": trigger["name"]}
        task_ref = _task_doc_ref(client, target)
        if task_ref is None:
            actions.append(
                {"action": "skip", "task": target, "reason": "target is not a qualified task name"}
            )
            continue

        doc = task_ref.get()
        if not doc.exists:
            actions.append(
                {
                    "action": "skip",
                    "task": target,
                    "reason": f"no task document; {trigger['name']} ON {trigger['source']} dangles",
                }
            )
            continue

        held = (doc.to_dict() or {}).get("trigger") or {}
        if not held.get("name"):
            actions.append({"action": "plant", "task": target, "pointer": pointer, "ref": task_ref})
        elif (held.get("name"), held.get("source")) == (pointer["name"], pointer["source"]):
            actions.append({"action": "current", "task": target, "pointer": pointer})
        else:
            actions.append(
                {
                    "action": "skip",
                    "task": target,
                    "reason": (
                        f"pointer names {held.get('name')} ON {held.get('source')}, "
                        f"but {trigger['name']} ON {trigger['source']} is what fires it"
                    ),
                }
            )
    return actions


def apply(client, action: dict, trigger_reference) -> str:
    """Write one pointer, conditional on nothing having moved.

    A transaction rather than a bare update because the decision was made from
    a read taken earlier: the trigger may have been dropped since (its pointer
    would then be a phantom locking the task out of ever taking another), or
    another writer may have planted one. Both are re-checked here, against the
    same records, inside the write.
    """
    from google.cloud import firestore

    task_ref = action["ref"]
    pointer = action["pointer"]

    @firestore.transactional
    def _plant(transaction) -> str:
        # Every read first: a Firestore transaction refuses a read that follows
        # a write in the same transaction.
        task_doc = task_ref.get(transaction=transaction)
        trigger_doc = trigger_reference.get(transaction=transaction)

        if not trigger_doc.exists:
            return "the trigger was dropped since the plan was made"
        if not task_doc.exists:
            return "the task was dropped since the plan was made"
        held = (task_doc.to_dict() or {}).get("trigger") or {}
        if held.get("name"):
            if (held.get("name"), held.get("source")) == (pointer["name"], pointer["source"]):
                return None
            return f"a pointer to {held.get('name')} ON {held.get('source')} appeared since"

        transaction.update(task_ref, {"trigger": dict(pointer)})
        return None

    return _plant(client.transaction())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "workspaces", nargs="*", help="limit the backfill to these workspaces (default: all)"
    )
    parser.add_argument(
        "--apply", action="store_true", help="write the pointers (default: dry run)"
    )
    parser.add_argument("--project", default="mabeldev")
    parser.add_argument("--database", default="catalogs")
    args = parser.parse_args()

    from google.cloud import firestore

    client = firestore.Client(project=args.project, database=args.database)

    by_task = collect(client, set(args.workspaces) or None)
    actions = plan(client, by_task)

    planted = skipped = current = 0
    for action in actions:
        task = action["task"]
        if action["action"] == "current":
            current += 1
            print(f"[ok]      {task}: already points at {action['pointer']['name']}")
        elif action["action"] == "skip":
            skipped += 1
            print(f"[SKIPPED] {task}: {action['reason']}")
        elif not args.apply:
            planted += 1
            pointer = action["pointer"]
            print(f"[would]   {task}: -> {pointer['name']} ON {pointer['source']}")
        else:
            (trigger,) = by_task[task]
            refused = apply(client, action, trigger["reference"])
            if refused:
                skipped += 1
                print(f"[SKIPPED] {task}: {refused}")
            else:
                planted += 1
                pointer = action["pointer"]
                print(f"[planted] {task}: -> {pointer['name']} ON {pointer['source']}")

    verb = "planted" if args.apply else "to plant"
    print(f"{planted} {verb}, {current} already correct, {skipped} skipped")
    if not args.apply and planted:
        print("dry run - nothing was written; re-run with --apply")
    return 1 if skipped else 0


if __name__ == "__main__":
    raise SystemExit(main())
