"""Provenance: what a commit read, and what a dataset is built from.

Two records, two lifetimes (PROVENANCE_DESIGN.md):

- The RECEIPT, `read-sources` on a snapshot document: one entry per (catalog
  relation, snapshot) the statement that produced the commit read. Written
  once, never edited.
- The SOURCE LIST, `sources` on the dataset document: the distinct names of
  every dataset whose data is in the current content, most recent first,
  capped. Maintained by the commit path from the receipts - an append adds,
  a rewrite replaces, a truncate clears - and recomputable from them after a
  rollback.

Everything that decides the shape of either record is here, so the nine
snapshot construction sites in `dataset.py` and the rollback path in
`opteryx_catalog.py` share one rule set rather than nine readings of it.

`None` IS A BUG. A writer that does not report its receipt is a writer that
was not updated (or an engine older than this catalog). The commit still lands
- a write must not fail after its files are on disk - but the omission is
alerted, audited, and reported by the integrity sweep. `[]` is the honest
receipt for a statement that read no catalog relation: an upload, a literal
INSERT, a compaction.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..exceptions import ReceiptMissing
from .metadata import MAX_READ_SOURCES
from .metadata import MAX_SOURCES

# How the snapshot a read resolved to was chosen. Exactly the branches of the
# engine connector's `_resolve_snapshot`. `current` and `previous` name a
# moving pointer, so a commit whose receipt carries either would read
# something else if it ran again.
RESOLVED_BY = frozenset({"current", "version", "previous", "tag", "date"})

# What a commit does to the dataset's standing source list.
APPEND = "append"  # union the receipt's datasets in, most recent first
REWRITE = "rewrite"  # the receipt's datasets ARE the list now
CLEAR = "clear"  # nothing of the old content remains; the list is empty
UNCHANGED = "unchanged"  # maintenance: the content is the same content

# Operation types, as stamped on snapshots, that replace or empty the content.
# Everything else that is user-created is an append; compaction, statistics
# refresh and expiration are maintenance and touch nothing here.
REWRITE_OPERATIONS = frozenset({"overwrite", "truncate-and-add-files"})
CLEAR_OPERATIONS = frozenset({"truncate"})
MAINTENANCE_OPERATIONS = frozenset({"compact", "statistics-refresh", "expire"})
# Operations that may leave the content empty, which clears the list: nothing
# of the old sources survives a delete that removed every row.
ROW_REMOVING_OPERATIONS = frozenset({"delete", "merge", "update", "delete-files"})

# When receipts became required. A snapshot committed at or after this instant
# with no `read-sources` is a `missing-receipt` integrity finding; one before
# it is pre-feature history, which is permanently unrecorded and not a fault.
# 2026-09-08T00:00:00Z - the day the catalog began requiring receipts.
RECEIPTS_REQUIRED_SINCE_MS = 1788825600000

# Bound on the rollback recompute walk. A chain of appends since the last
# rewrite can be arbitrarily long; past this many hops the list is reported
# incomplete rather than the walk reading the whole history.
MAX_SOURCES_WALK = 1024


def normalize_read_sources(entries: Any) -> tuple[list[dict], bool]:
    """The canonical receipt: validated, deduplicated on (dataset, snapshot-id),
    sorted by that pair, capped at MAX_READ_SOURCES.

    Returns `(entries, truncated)`. A relation read twice at one version is ONE
    entry; a relation read at two versions is two - collapsing those would drop
    the version somebody re-deriving the result needs. Entries carry exactly
    three keys so Firestore's `array_contains`, which matches a map element
    only on exact equality, can find one.
    """
    if entries is None:
        raise TypeError("read_sources must be a list; None means 'not reported'")
    seen: dict[tuple[str, int | None], dict] = {}
    for raw in entries:
        if isinstance(raw, dict):
            dataset = raw.get("dataset")
            snapshot_id = raw.get("snapshot-id", raw.get("snapshot_id"))
            resolved_by = raw.get("resolved-by", raw.get("resolved_by")) or "current"
        else:
            # (dataset, snapshot_id[, resolved_by]) tuples are accepted from
            # library callers; the stored shape is always the dict.
            dataset, snapshot_id, *rest = raw
            resolved_by = rest[0] if rest else "current"
        if not isinstance(dataset, str) or len(dataset.split(".")) < 3:
            raise ValueError(
                f"read source {dataset!r} is not a fully-qualified workspace.collection.dataset"
            )
        if snapshot_id is not None:
            snapshot_id = int(snapshot_id)
        if resolved_by not in RESOLVED_BY:
            raise ValueError(
                f"resolved-by {resolved_by!r} for {dataset} is not one of {sorted(RESOLVED_BY)}"
            )
        key = (dataset, snapshot_id)
        if key not in seen:
            seen[key] = {
                "dataset": dataset,
                "snapshot-id": snapshot_id,
                "resolved-by": resolved_by,
            }
    ordered = [seen[k] for k in sorted(seen, key=lambda k: (k[0], -1 if k[1] is None else k[1]))]
    truncated = len(ordered) > MAX_READ_SOURCES
    return ordered[:MAX_READ_SOURCES], truncated


# The keys a receipt is queryable by live with the document shape in
# metadata.py, beside the writer that stores them; re-exported here so the rule
# set is importable from one place.
from .metadata import read_source_key
from .metadata import read_source_keys


def effect_for_operation(operation_type: str | None, live_records: int | None) -> str:
    """What a commit of this kind does to the source list.

    `live_records` is the content's row count AFTER the commit; a row-removing
    operation that leaves none clears the list. The catalog cannot see a
    DELETE's predicate, so "bare DELETE" is defined by its outcome.
    """
    if operation_type in MAINTENANCE_OPERATIONS:
        return UNCHANGED
    if operation_type in REWRITE_OPERATIONS:
        return REWRITE
    if operation_type in CLEAR_OPERATIONS:
        return CLEAR
    if operation_type in ROW_REMOVING_OPERATIONS and live_records == 0:
        return CLEAR
    return APPEND


def is_self(dataset: str, own_identifier: str, workspace: str | None) -> bool:
    """Whether a receipt entry names the dataset it sits on. Y is never in its
    own source list, though it is in its own receipt when the statement read it
    (a MERGE matches against the target)."""
    if workspace:
        return dataset == f"{workspace}.{own_identifier}"
    # No workspace to hand (a library dataset with no catalog attached): the
    # relative name is the most that can be compared.
    return dataset.split(".", 1)[-1] == own_identifier


def merge_sources(
    receipt_datasets: list[str], previous: list[str], effect: str
) -> tuple[list[str], bool]:
    """Apply one commit's effect to a source list.

    Returns `(sources, dropped)`: `dropped` is True when the MAX_SOURCES cap
    removed a name, which the caller records as the list being incomplete.
    Most recent first, so the cap drops the oldest.
    """
    if effect == UNCHANGED:
        return list(previous), False
    if effect == CLEAR:
        return [], False
    fresh: list[str] = []
    for name in receipt_datasets:
        if name not in fresh:
            fresh.append(name)
    if effect == REWRITE:
        merged = fresh
    else:
        merged = fresh + [name for name in previous if name not in fresh]
    return merged[:MAX_SOURCES], len(merged) > MAX_SOURCES


def _live_records(summary: dict | None) -> int | None:
    summary = summary or {}
    total = summary.get("total-records")
    if total is None:
        return None
    return int(total) - int(summary.get("total-deleted-records") or 0)


def recompute_sources(
    head_snapshot_id: int | None,
    fetch: Callable[[int], dict | None],
    own_identifier: str,
    workspace: str | None,
) -> tuple[list[str], bool]:
    """The source list as the receipts along a chain say it should be.

    Walks from `head_snapshot_id` back through `parent-snapshot-id`, newest
    first, until a rewrite or a clear (which start a list afresh), the chain's
    beginning, or MAX_SOURCES_WALK hops. `fetch` returns one snapshot DOCUMENT
    (the stored dict) or None. Used after a rollback, when the stored list
    describes content that is no longer current; the chain behind a snapshot
    is immutable, so this needs no transaction.

    Returns `(sources, complete)`. Incomplete when any snapshot in the walk has
    no receipt (pre-feature history, or a writer that did not report), when the
    cap dropped a name, or when the walk was cut short.
    """
    sources: list[str] = []
    complete = True
    snapshot_id = head_snapshot_id
    hops = 0
    while snapshot_id is not None:
        if hops >= MAX_SOURCES_WALK:
            complete = False
            break
        hops += 1
        doc = fetch(int(snapshot_id))
        if doc is None:
            complete = False
            break
        operation = doc.get("operation-type")
        effect = effect_for_operation(operation, _live_records(doc.get("summary")))
        if effect == CLEAR:
            break
        if effect == UNCHANGED:
            snapshot_id = doc.get("parent-snapshot-id")
            continue
        receipt = doc.get("read-sources")
        if receipt is None:
            complete = False
            if effect == REWRITE:
                break
        else:
            if doc.get("read-sources-truncated"):
                complete = False
            names = [
                entry["dataset"]
                for entry in receipt
                if not is_self(entry["dataset"], own_identifier, workspace)
            ]
            # Walking newest-first, everything already collected is more recent
            # than this commit's names, so this commit's go AFTER them.
            for name in names:
                if name not in sources:
                    sources.append(name)
            if len(sources) > MAX_SOURCES:
                complete = False
                sources = sources[:MAX_SOURCES]
            if effect == REWRITE:
                break
        snapshot_id = doc.get("parent-snapshot-id")
    return sources, complete


__all__ = [
    "APPEND",
    "CLEAR",
    "RESOLVED_BY",
    "REWRITE",
    "UNCHANGED",
    "ReceiptMissing",
    "effect_for_operation",
    "is_self",
    "merge_sources",
    "normalize_read_sources",
    "read_source_key",
    "read_source_keys",
    "recompute_sources",
]
