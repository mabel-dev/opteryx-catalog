#!/usr/bin/env python3
"""Sweep for tasks fired by more than one trigger.

Usage:
    python scripts/find_multiply_triggered_tasks.py
    python scripts/find_multiply_triggered_tasks.py <workspace> [<workspace> ...]
    python scripts/find_multiply_triggered_tasks.py --json

THE MIGRATION FOR THE ONE-TRIGGER RULE. `create_trigger` now refuses a second
trigger for a task, but it can only refuse NEW ones - anything already wired
that way predates the rule and stays wired until someone unwires it. This is
what finds them, and it must be run before the rule is relied on.

Every finding it prints is a LIVE BUG, not a configuration to grandfather. A
task's unattended window is the committing snapshot and its parent, bound at
fire time from whichever dataset fired it (`trigger_firing._fire_task`), and
snapshot ids from two datasets are not comparable. So a task with two triggers
is receiving two incomparable version sequences through the same two parameter
names - producing plausible wrong rows with no error if its statement is
windowed, and corrupting `last-window-to` (now a guard, not a breadcrumb) into
an interleave of two sequences either way.

It REPORTS AND DELETES NOTHING. Which trigger is the real one is a question
about intent - fan-in is spelled as two tasks, each windowed on its own source,
or as a materialized view if the work was a rewrite all along - and neither
answer is derivable from the records. Read-only, prints findings, exits 1 if any
were found (0 when clean), so it can run on a schedule and fail loudly.

Materialized views are untouched by the rule and by this sweep: a refresh is a
wholesale re-derivation that consumes no window, so a view legitimately keeps
one trigger per source. Only `kind == "task"` triggers are counted.

See `TASK_WINDOWING_DESIGN.md` for the reasoning, and
`scripts/find_catalog_orphans.py` for the sweep this follows.
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from opteryx_catalog.opteryx_catalog import TASK_TRIGGER_KIND  # noqa: E402
from opteryx_catalog.opteryx_catalog import TRIGGERS_SUBCOLLECTION  # noqa: E402


def _source_of(reference) -> str:
    """The qualified dataset a trigger document hangs under.

    A trigger's path is `{workspace}/{collection}/datasets/{dataset}/triggers/
    {name}`, so the source is read off the path rather than stored in the
    document - the same fact the collection-group query is exploiting.
    """
    parts = reference.path.split("/")
    return f"{parts[0]}.{parts[1]}.{parts[3]}"


def sweep(client, workspaces=None) -> list[dict]:
    """Every task with two or more triggers, as findings.

    ONE collection-group query over the `triggers` subcollections, which is the
    only way to ask this question at all: triggers live under the datasets that
    fire them, so "which triggers point at task t" is a reverse lookup with no
    home. `RELATIONSHIPS_SUBCOLLECTION` uses the same pattern for the same
    reason (`find_relationships_to`).

    Unfiltered on the server and grouped here: the kind and the target are
    hyphenated field names, which a Firestore query can only reach through
    backtick-quoted paths and a collection-group index that does not exist for
    this one-off. The whole triggers population is small - triggers shipped
    2026-08 - so one stream costs less than provisioning an index for a sweep.
    """
    by_task: dict[str, list[dict]] = defaultdict(list)
    for doc in client.collection_group(TRIGGERS_SUBCOLLECTION).stream():
        data = doc.to_dict() or {}
        if data.get("kind") != TASK_TRIGGER_KIND:
            continue
        target = data.get("target-task")
        if not target:
            # A task trigger with no target is broken in a different way; the
            # orphan sweep is what reports that one.
            continue
        source = _source_of(doc.reference)
        if workspaces and source.split(".", 1)[0] not in workspaces:
            continue
        by_task[target].append({"name": doc.id, "source": source})

    findings = []
    for target, triggers in sorted(by_task.items()):
        if len(triggers) < 2:
            continue
        findings.append(
            {
                "kind": "task-multiply-triggered",
                "task": target,
                "triggers": sorted(triggers, key=lambda t: (t["source"], t["name"])),
                "detail": (
                    f"{len(triggers)} triggers fire {target}; a task has one trigger - "
                    "its window is that source's version sequence"
                ),
            }
        )
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "workspaces", nargs="*", help="limit findings to these workspaces (default: all)"
    )
    parser.add_argument("--json", action="store_true", help="emit findings as JSON lines")
    parser.add_argument("--project", default="mabeldev")
    parser.add_argument("--database", default="catalogs")
    args = parser.parse_args()

    from google.cloud import firestore

    client = firestore.Client(project=args.project, database=args.database)

    findings = sweep(client, set(args.workspaces) or None)

    if args.json:
        for finding in findings:
            print(json.dumps(finding))
    else:
        for finding in findings:
            print(f"[{finding['kind']}] {finding['task']}: {finding['detail']}")
            for trigger in finding["triggers"]:
                print(f"    {trigger['name']} ON {trigger['source']}")
        scope = ", ".join(args.workspaces) if args.workspaces else "all workspaces"
        print(f"{len(findings)} multiply-triggered task(s) across {scope}")

    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
