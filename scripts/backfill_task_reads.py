#!/usr/bin/env python3
"""Derive `reads` for task documents from the statement each task now runs.

Usage:
    python scripts/backfill_task_reads.py --all                     # dry run
    python scripts/backfill_task_reads.py --all --apply
    python scripts/backfill_task_reads.py <workspace> ... [--apply]

WHY THIS EXISTS. `create_task` records `reads` - the catalog relations a task's
statement reads, qualified - beside `writes`, and `find_inbound_edges` carries
it on every `writes` row so "what feeds the thing that writes this" is one
query (PROVENANCE_DESIGN.md S2.3). The engine derives it at registration, so a
task registered before the field existed has no `reads` key, and one registered
through a catalog that received `reads=None` has `[]`. Both read as "this task
reads nothing", which is the wrong answer for almost every task in the catalog.
This asks the question that registration never asked.

DERIVED, NEVER DECLARED. The value is read off the task's CURRENT statement
with the same AST walk `plan_create_task` uses - `_extract_tables_from_ast`
minus `extract_write_targets` - so a run cannot disagree with what a fresh
`CREATE OR REPLACE TASK` of the same text would record. That is also why the
parser is borrowed from the engine rather than reimplemented: two derivations
would drift, and the drift would show up as a task whose `reads` depends on
which of them last ran. The catalog must not depend on the engine at import
time, so the import happens inside the function that needs it, where the
script runs and both packages are installed.

WHAT IT DOES TO EACH NAME, matching `visit_create_task` and `_qualify`:

- A write target is not a read. The table a MERGE, UPDATE, DELETE or INSERT
  lands on appears in the AST walk like any other relation, and is removed.
- `$planets` and anything under `information_schema` are not catalog objects,
  so they are dropped, as the binder skips them.
- A one-part name is not a catalog relation either, and is dropped.
- A two-part name is relative to the task's OWN workspace - the first segment
  of its document path - and is qualified with it. A three-or-more-part name
  already carries a workspace and is kept as written.

WHAT IT REFUSES TO TOUCH: a statement that does not parse. Nothing can be
derived from it, and writing `[]` would turn "unknown" into a confident
"reads nothing". It is reported and left alone; the fix is to redefine the
task. A task whose statement changed between the plan and the write is also
left alone - a redefinition through a current engine has already recorded the
right answer, and this must not overwrite it with one derived from the old
text.

Dry run by DEFAULT: every task is printed with the list it holds and the list
its statement gives, and whether they differ. `--apply` is the only thing that
writes, and it writes only `reads` - a single-field update, never the whole
document - and only where the stored value is missing or differs. Exits 0
either way; the summary line is the report.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from opteryx_catalog.opteryx_catalog import TASKS_SUBCOLLECTION

# `{workspace}/{collection}/tasks/{task}` - the one shape a task document has.
_TASK_PATH_LENGTH = 4

# Where `create_task` versions the statement: the task document carries only a
# `statement-id`, and the text lives under it.
STATEMENT_SUBCOLLECTION = "statement"


def _parser():
    """The engine's parser and the two AST walks, imported when first needed.

    Inside a function, not at module top, because this script is the ONE place
    the catalog repository touches the engine: `opteryx_catalog` must import
    without `opteryx` installed, and a top-level import here would make the
    scripts directory the exception to that.
    """
    try:
        from opteryx.third_party import sqloxide
        from opteryx.utils.query_parser import _extract_tables_from_ast
        from opteryx.utils.query_parser import extract_write_targets
    except ImportError as err:
        raise RuntimeError(
            "this backfill derives `reads` with the engine's own parser and needs "
            "`opteryx` (opteryx-core) importable alongside `opteryx_catalog`; "
            f"run it where both are installed ({err})"
        ) from err
    return sqloxide.parse_sql, _extract_tables_from_ast, extract_write_targets


def derive_reads(sql: str, workspace: str) -> list[str]:
    """What a task statement reads, as `create_task` would store it.

    Raises whatever the parser raises for a statement it cannot read; the
    caller decides that this means "skip", and it must not be turned into an
    empty list here, where that would be indistinguishable from `SELECT 1`.
    """
    parse_sql, extract_tables, extract_write_targets = _parser()

    parsed = parse_sql(sql, _dialect="opteryx")
    if len(parsed) != 1:
        # `plan_create_task` refuses this at registration, so a stored task
        # never has it; if one does, it did not come through the engine and
        # its reads are not something this can vouch for.
        raise ValueError(f"a task runs ONE statement; this one has {len(parsed)}")
    ast = parsed[0]

    targets = set(extract_write_targets(ast))
    reads: set[str] = set()
    for name in extract_tables(ast):
        if name in targets:
            continue
        parts = name.split(".")
        if len(parts) < 2 or name.startswith("$") or "information_schema" in parts:
            continue
        reads.add(name if len(parts) >= 3 else f"{workspace}.{name}")
    return sorted(reads)


def _statement_of(reference, data: dict) -> str | None:
    """The SQL a task document currently points at, or None if it has none."""
    statement_id = data.get("statement-id")
    if not statement_id:
        return None
    statement = (
        reference.collection(STATEMENT_SUBCOLLECTION).document(str(statement_id)).get().to_dict()
        or {}
    )
    return statement.get("sql")


def collect(client, workspaces=None) -> list[dict]:
    """Every task document in the catalog, or in the named workspaces.

    One collection-group query, the read `find_inbound_edges` makes: tasks live
    under their collections in every workspace, and neither the document nor
    its fields say which, so the workspace is read off the path.
    """
    tasks = []
    for doc in client.collection_group(TASKS_SUBCOLLECTION).stream():
        parts = doc.reference.path.split("/")
        if len(parts) != _TASK_PATH_LENGTH or parts[2] != TASKS_SUBCOLLECTION:
            continue
        workspace = parts[0]
        if workspaces and workspace not in workspaces:
            continue
        tasks.append(
            {
                "task": f"{workspace}.{parts[1]}.{parts[3]}",
                "workspace": workspace,
                "reference": doc.reference,
                "data": doc.to_dict() or {},
            }
        )
    tasks.sort(key=lambda task: task["task"])
    return tasks


def plan(tasks: list[dict]) -> list[dict]:
    """What each task holds and what its statement gives, decided from reads only."""
    actions = []
    for task in tasks:
        data = task["data"]
        # Absent and `[]` are different facts about the record (S2.4): one was
        # never asked, the other answered "nothing". Both are replaced when the
        # derivation says otherwise, and an absent key is written even when the
        # derivation agrees it is empty, so that "never asked" stops existing.
        stored = data.get("reads") if "reads" in data else None
        action = {
            "task": task["task"],
            "reference": task["reference"],
            "statement_id": data.get("statement-id"),
            "stored": stored,
        }

        sql = _statement_of(task["reference"], data)
        if not sql:
            action.update(action="no-statement", reason="task document points at no statement")
            actions.append(action)
            continue

        try:
            derived = derive_reads(sql, task["workspace"])
        except Exception as err:
            # Anything the parser raises - a syntax error, an unsupported form,
            # a statement count - means the same thing here: nothing can be
            # derived, so nothing is written. The message is the operator's.
            action.update(action="unparseable", reason=str(err).splitlines()[0])
            actions.append(action)
            continue

        action["derived"] = derived
        if stored is not None and list(stored) == derived:
            action["action"] = "current"
        else:
            action["action"] = "update"
        actions.append(action)
    return actions


def apply(action: dict) -> str | None:
    """Write one task's `reads`, unless its statement moved under the plan.

    A single-field `update`, never a `set`: the task document carries the
    trigger back-pointer, firing state and the version guard, and a `set` would
    erase every one of them (PROVENANCE_DESIGN.md S4.1). Re-read first because
    the decision was made from an earlier read: a task redefined since has had
    its `reads` recorded by the engine from the new text, and that answer, not
    this one, is the right one.
    """
    reference = action["reference"]
    current = reference.get()
    if not current.exists:
        return "the task was dropped since the plan was made"
    if (current.to_dict() or {}).get("statement-id") != action["statement_id"]:
        return "the statement was redefined since the plan was made"
    reference.update({"reads": list(action["derived"])})
    return None


def run(client, workspaces, *, apply_changes: bool, out=print) -> dict:
    """Plan, print, and (with `apply_changes`) write; returns the counts."""
    actions = plan(collect(client, workspaces))

    counts = {"seen": len(actions), "unchanged": 0, "updated": 0, "unparseable": 0, "skipped": 0}
    for action in actions:
        task = action["task"]
        stored = "<absent>" if action["stored"] is None else repr(list(action["stored"]))
        if action["action"] == "current":
            counts["unchanged"] += 1
            out(f"[ok]      {task}: stored {stored}, derived {action['derived']!r}, same")
        elif action["action"] == "unparseable":
            counts["unparseable"] += 1
            out(f"[SKIPPED] {task}: statement does not parse: {action['reason']}")
        elif action["action"] == "no-statement":
            counts["skipped"] += 1
            out(f"[SKIPPED] {task}: {action['reason']}")
        elif not apply_changes:
            counts["updated"] += 1
            out(f"[would]   {task}: stored {stored}, derived {action['derived']!r}, differ")
        else:
            refused = apply(action)
            if refused:
                counts["skipped"] += 1
                out(f"[SKIPPED] {task}: {refused}")
            else:
                counts["updated"] += 1
                out(f"[updated] {task}: stored {stored}, derived {action['derived']!r}")

    verb = "updated" if apply_changes else "to update"
    out(
        f"{counts['seen']} task(s) seen, {counts['unchanged']} unchanged, "
        f"{counts['updated']} {verb}, {counts['unparseable']} unparseable, "
        f"{counts['skipped']} skipped"
    )
    if not apply_changes and counts["updated"]:
        out("dry run - nothing was written; re-run with --apply")
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workspaces", nargs="*", help="limit the backfill to these workspaces")
    parser.add_argument("--all", action="store_true", help="walk every workspace")
    parser.add_argument("--apply", action="store_true", help="write `reads` (default: dry run)")
    parser.add_argument("--project", default="mabeldev")
    parser.add_argument("--database", default="catalogs")
    args = parser.parse_args()

    if not args.workspaces and not args.all:
        parser.error("name at least one workspace, or pass --all")

    from google.cloud import firestore

    client = firestore.Client(project=args.project, database=args.database)

    run(client, set(args.workspaces) or None, apply_changes=args.apply)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
