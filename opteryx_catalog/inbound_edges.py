"""What, anywhere in the catalog, puts work into one dataset.

THE QUESTION THIS ANSWERS, and why it needs its own module. Every other read of
the trigger graph starts from the SOURCE: "this dataset was committed, what does
that fire" - one dataset's `triggers` subcollection, one read, inside one
workspace. Reading the graph backwards has no such home. A trigger lives on the
dataset whose commits fire it, so a chain that crosses a workspace boundary is
recorded entirely in the SOURCE's workspace, and from the target's side there is
nothing to read at all: the dataset sits there looking like something nobody
writes. `information_schema.triggers` enumerates one workspace and cannot say
otherwise (see WORKFLOWS-DESIGN.md 2.6 in web.opteryx, where this gap was
named).

TWO RECORDS CAN PUT AN EDGE INTO A DATASET, and both are asked for here. Missing
the second is the easy mistake, and it is the one that prompted this: a
materialized view is refreshed by a TRIGGER that names it, but a plain table is
written by a TASK, whose statement's targets are declared on the task record and
whose triggers name the task, not the table. Asking only about triggers answers
"nothing upstream" for exactly the case people notice.

  1. `triggers` where `target-view` or `target-task` is the target - another
     workspace's trigger pointing straight at it.
  2. `tasks` where `writes` array-contains the target - a task whose statement
     lands rows in it. The trigger that fires THAT task is a separate hop and is
     not followed here: this returns the edge into the dataset, and the caller
     that wants the hop above it can ask again with the task as the target.

     TWO SPELLINGS, because `writes` is what the authoring planner derived from
     the statement's AST and a statement may name its target either way. A bare
     `collection.name` entry can only ever mean the task's OWN workspace - that
     is what an unqualified name means everywhere - so a row matched on it is
     kept only when the task sits in the target's workspace, and dropped
     otherwise rather than reported as an edge that resolves somewhere else.

COLLECTION GROUP QUERIES, which is what makes this affordable - one indexed
query each, rather than the walk over every collection and dataset that reading
a whole workspace's triggers costs. Workspaces are sibling root collections and
their subcollections share names, so a collection group spans the database; the
filter is on the TARGET, which is fully qualified, so it is the query itself
that confines the answer rather than the position of a document in the tree.
Both filters are single-field and carried by the automatic index, provided
collection group scope has not been removed by a field exemption.

NOTHING HERE IS AUTHORIZED. These rows are the whole catalog's answer, not the
caller's: a source in a workspace the caller cannot see comes back like any
other. Every caller owes a read check on `source` before showing it to anyone -
odata.opteryx's `$inbound-edges` route is this module's only caller and makes
that check per row, eliding what the caller may not know about rather than
dropping the row, because the EXISTENCE of an upstream is not the secret. Its
name is.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

TRIGGERS_SUBCOLLECTION = "triggers"
TASKS_SUBCOLLECTION = "tasks"

# What kind of thing the edge is, as reported on each row. The first two are the
# trigger's own `kind`, passed through rather than re-derived. `writes` is not a
# trigger kind at all: it is a task's declared output, which is an edge in the
# graph and nothing in the trigger vocabulary describes.
WRITES_EDGE = "writes"

# `{workspace}/{collection}/datasets/{dataset}/triggers/{name}` and
# `{workspace}/{collection}/tasks/{task}/triggers/{name}` - the two places a
# trigger document can sit, and `{workspace}/{collection}/tasks/{task}` for a
# task. Paths are parsed rather than trusted to a field because the workspace is
# not written on either record; where a record DOES carry the answer (`holder`
# on a trigger) the two are cross-checked, and a disagreement is dropped rather
# than reported as an edge that may point somewhere it does not.
_TRIGGER_PATH_LENGTH = 6
_TASK_PATH_LENGTH = 4


def _path_parts(doc) -> list[str]:
    path = getattr(getattr(doc, "reference", None), "path", None) or ""
    return path.split("/") if path else []


def _trigger_row(doc, target: str) -> dict | None:
    """One trigger document as an inbound edge, or None if it is not one."""
    parts = _path_parts(doc)
    if len(parts) != _TRIGGER_PATH_LENGTH or parts[4] != TRIGGERS_SUBCOLLECTION:
        logger.warning("trigger at an unexpected path, not reported inbound: %s", parts)
        return None
    data = doc.to_dict() or {}
    workspace, collection, holder_kind_segment, holder_name = (
        parts[0],
        parts[1],
        parts[2],
        parts[3],
    )
    source = f"{workspace}.{collection}.{holder_name}"
    held = data.get("holder")
    if held and held != source:
        # The record and the tree disagree about what this trigger hangs off.
        # Neither is safe to report: the row's whole purpose is to name the
        # thing upstream, and this row cannot say which name that is.
        logger.warning(
            "trigger %s records holder %r but sits under %r; not reported inbound",
            parts[5],
            held,
            source,
        )
        return None
    return {
        "target": target,
        "source": source,
        "workspace": workspace,
        # A trigger on a TASK is fired by that task's clock or signal; one on a
        # dataset is fired by its commits. The caller draws them differently.
        "source_kind": "task" if holder_kind_segment == TASKS_SUBCOLLECTION else "dataset",
        "kind": data.get("kind"),
        "trigger": data.get("name") or parts[5],
        "runs_as": data.get("runs-as"),
        "last_fired_at_ms": data.get("last-fired-at-ms"),
        "last_fired_status": data.get("last-fired-status"),
        "suspended_at_ms": data.get("suspended-at-ms"),
    }


def _task_row(doc, target: str) -> dict | None:
    """One task document as a `writes` edge into the target."""
    parts = _path_parts(doc)
    if len(parts) != _TASK_PATH_LENGTH or parts[2] != TASKS_SUBCOLLECTION:
        logger.warning("task at an unexpected path, not reported inbound: %s", parts)
        return None
    data = doc.to_dict() or {}
    workspace, collection, task_name = parts[0], parts[1], parts[3]
    return {
        "target": target,
        "source": f"{workspace}.{collection}.{task_name}",
        "workspace": workspace,
        "source_kind": "task",
        "kind": WRITES_EDGE,
        # A `writes` edge is the task's own declaration, not a trigger: there is
        # no trigger name to give, and no firing of its own to report. What
        # fired the task is a hop further up, on the task as a target.
        "trigger": None,
        "runs_as": None,
        "last_fired_at_ms": None,
        "last_fired_status": None,
        "suspended_at_ms": None,
    }


def find_inbound_edges(client, target: str) -> list[dict]:
    """Every trigger or task, in ANY workspace, whose work lands on `target`.

    `target` is a fully-qualified `workspace.collection.name`; a shorter name
    cannot be matched, because what is stored on both records is qualified and a
    bare `collection.dataset` means a different thing in every workspace.

    Rows are returned unauthorized and in a stable order (source, then trigger),
    so a caller can diff two answers without sorting them itself.
    """
    from google.cloud.firestore_v1 import FieldFilter

    if not target or len(str(target).split(".")) < 3:
        raise ValueError(
            f"target must be a fully-qualified workspace.collection.name, got {target!r}"
        )
    target = str(target)

    rows: list[dict] = []

    # Two `where`s rather than one OR: a trigger has exactly one target and the
    # two fields are alternative spellings of it (a view's refresh, a task's
    # execution), so the queries are disjoint and their union needs no dedupe.
    for field in ("target-view", "target-task"):
        query = client.collection_group(TRIGGERS_SUBCOLLECTION).where(
            filter=FieldFilter(field, "==", target)
        )
        for doc in query.stream():
            row = _trigger_row(doc, target)
            if row is not None:
                rows.append(row)

    workspace, relative = target.split(".", 1)
    for written, same_workspace_only in ((target, False), (relative, True)):
        tasks = client.collection_group(TASKS_SUBCOLLECTION).where(
            filter=FieldFilter("writes", "array_contains", written)
        )
        for doc in tasks.stream():
            row = _task_row(doc, target)
            if row is None:
                continue
            # An unqualified `writes` entry names something in the task's own
            # workspace. Matching one from elsewhere would report an edge into a
            # dataset of the same relative name in a different tenant.
            if same_workspace_only and row["workspace"] != workspace:
                continue
            # A task declaring both spellings of the same target is one edge.
            # Compared against the `writes` rows only: a trigger HELD BY that
            # same task is a different edge that happens to share a source.
            if any(
                existing["kind"] == WRITES_EDGE and existing["source"] == row["source"]
                for existing in rows
            ):
                continue
            rows.append(row)

    rows.sort(key=lambda row: (row["source"], row["trigger"] or ""))
    return rows
