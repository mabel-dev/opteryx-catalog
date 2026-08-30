"""Find catalog debris Firestore itself will never surface.

Firestore does not cascade deletes: removing a document leaves its
subcollections fully addressable, reachable only by `list_documents()`, and
invisible to every `stream()` in the catalog. A crash or race in any
delete path therefore leaves GHOSTS - paths like `datasets/create` that hold
no document but still carry a `triggers` subcollection - and nothing in
normal operation ever reads them again. The one found in production (a
day-one refresh trigger surviving under a long-dropped dataset) sat
undetected for three weeks precisely because no read path can see a ghost.

This module is the detection side of that problem: a read-only sweep of one
workspace that reports every inconsistency the write paths promise not to
create. Run it from `scripts/find_catalog_orphans.py`, on a schedule or
after an incident. It never repairs anything - a finding names a path and a
reason, and deleting is a decision for whoever reads the report.

Finding kinds:

- ``ghost-dataset``: a dataset path with no document, kept addressable by
  leftover subcollection documents (the production incident's shape).
- ``dangling-trigger``: a trigger on a live dataset whose target view or
  task no longer exists - it will fire, fail, and alert forever.
- ``missing-source-trigger``: a materialized view with a source that
  carries no refresh trigger pointing back at it - the MV silently never
  refreshes from that source.
"""

from __future__ import annotations

from typing import Any

from .opteryx_catalog import MATERIALIZED_VIEW_TYPE
from .opteryx_catalog import MV_REFRESH_TRIGGER_KIND
from .opteryx_catalog import TASKS_SUBCOLLECTION
from .opteryx_catalog import TRIGGERS_SUBCOLLECTION

__all__ = ["audit_workspace"]


def _split_target(name: str, default_workspace: str) -> tuple[str, str, str] | None:
    """(workspace, collection, leaf) for a trigger target, however qualified.

    Trigger records written today carry the full three-part form, but records
    from before `_qualify` was the rule are two-part and relative - the live
    orphan's was - so both must resolve. Left-anchored split, matching
    `_split_qualified`: only the leaf may contain dots.
    """
    parts = name.split(".", 2)
    if len(parts) == 2:
        return default_workspace, parts[0], parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    return None


def _finding(kind: str, path: str, detail: str) -> dict[str, str]:
    return {"kind": kind, "path": path, "detail": detail}


def _dataset_exists(client, workspace: str, collection: str, dataset: str) -> bool:
    return (
        client.collection(workspace)
        .document(collection)
        .collection("datasets")
        .document(dataset)
        .get()
        .exists
    )


def _task_exists(client, workspace: str, collection: str, task: str) -> bool:
    return (
        client.collection(workspace)
        .document(collection)
        .collection(TASKS_SUBCOLLECTION)
        .document(task)
        .get()
        .exists
    )


def _leftover_subcollections(dataset_ref) -> list[str]:
    """Names of the subcollections keeping a ghost dataset path addressable."""
    names = []
    for sub in dataset_ref.collections():
        if any(True for _ in sub.list_documents()):
            names.append(sub.id)
    return sorted(names)


def _check_trigger(
    client, workspace: str, dataset_path: str, trigger: dict[str, Any]
) -> dict[str, str] | None:
    """A finding for a trigger whose target is gone, or None if it resolves."""
    name = trigger.get("name") or "?"
    target_view = trigger.get("target-view")
    target_task = trigger.get("target-task")
    target, exists_check = (
        (target_view, _dataset_exists) if target_view else (target_task, _task_exists)
    )
    if not target:
        return _finding(
            "dangling-trigger",
            f"{dataset_path}/triggers/{name}",
            "trigger has neither target-view nor target-task",
        )
    parts = _split_target(target, workspace)
    if parts is None or not exists_check(client, *parts):
        return _finding(
            "dangling-trigger",
            f"{dataset_path}/triggers/{name}",
            f"target {target} does not exist",
        )
    return None


def _check_mv_sources(
    client, workspace: str, collection_id: str, dataset_id: str, data: dict[str, Any]
) -> list[dict[str, str]]:
    """Findings for MV sources missing their refresh trigger.

    Matched by TARGET, not by trigger name: `_mv_trigger_name` has changed
    shape before (the digest suffix is newer than the oldest live triggers),
    and a name-based check would flag every trigger written under the old
    scheme. What actually matters is that SOME refresh trigger on the source
    points back at this view.
    """
    findings = []
    mv_qualified = f"{workspace}.{collection_id}.{dataset_id}"
    for source in data.get("source-tables") or []:
        parts = _split_target(source, workspace)
        if parts is None:
            continue
        src_ws, src_coll, src_name = parts
        source_ref = (
            client.collection(src_ws)
            .document(src_coll)
            .collection("datasets")
            .document(src_name)
        )
        if not source_ref.get().exists:
            findings.append(
                _finding(
                    "missing-source-trigger",
                    f"{workspace}/{collection_id}/datasets/{dataset_id}",
                    f"source {source} does not exist",
                )
            )
            continue
        pointed_back = False
        for doc in source_ref.collection(TRIGGERS_SUBCOLLECTION).stream():
            trigger = doc.to_dict() or {}
            if trigger.get("kind") != MV_REFRESH_TRIGGER_KIND:
                continue
            target = _split_target(trigger.get("target-view") or "", src_ws)
            if target and ".".join(target) == mv_qualified:
                pointed_back = True
                break
        if not pointed_back:
            findings.append(
                _finding(
                    "missing-source-trigger",
                    f"{workspace}/{collection_id}/datasets/{dataset_id}",
                    f"source {source} has no refresh trigger targeting {mv_qualified}",
                )
            )
    return findings


def audit_workspace(client, workspace: str) -> list[dict[str, str]]:
    """Every integrity finding in one workspace, as `{kind, path, detail}` rows.

    Read-only. Walks with `list_documents()` throughout, because that is the
    ONLY Firestore read that returns a ghost - `stream()` skips documents that
    do not exist, which is exactly what makes ghosts invisible everywhere else.
    """
    findings: list[dict[str, str]] = []
    for collection_ref in client.collection(workspace).list_documents():
        if collection_ref.id.startswith("$"):
            continue
        for dataset_ref in collection_ref.collection("datasets").list_documents():
            dataset_path = f"{workspace}/{collection_ref.id}/datasets/{dataset_ref.id}"
            snapshot = dataset_ref.get()
            if not snapshot.exists:
                leftovers = _leftover_subcollections(dataset_ref)
                findings.append(
                    _finding(
                        "ghost-dataset",
                        dataset_path,
                        "no document; kept addressable by subcollection(s): "
                        + (", ".join(leftovers) or "unknown"),
                    )
                )
                continue
            data = snapshot.to_dict() or {}
            for doc in dataset_ref.collection(TRIGGERS_SUBCOLLECTION).stream():
                finding = _check_trigger(client, workspace, dataset_path, doc.to_dict() or {})
                if finding:
                    findings.append(finding)
            if data.get("dataset-type") == MATERIALIZED_VIEW_TYPE:
                findings.extend(
                    _check_mv_sources(client, workspace, collection_ref.id, dataset_ref.id, data)
                )
    return findings
