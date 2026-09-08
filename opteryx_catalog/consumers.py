"""Who read a dataset - the OUTGOING half of provenance, from the receipts.

`inbound_edges.py` answers "what puts work INTO this dataset" from the plan:
triggers and task declarations. This answers the question nothing in the plan
can: "which commits, anywhere, actually READ this dataset" - and, with a
version, "which commits read THIS version of it". A trigger says what a commit
fires, not what read it, and a hand-run statement fires nothing at all; the
receipt on the consuming snapshot (PROVENANCE_DESIGN.md S2.1) is the only
record.

ONE COLLECTION-GROUP QUERY, the same shape `inbound_edges` uses over `tasks`:
every snapshot document carries `read-source-keys`, an array holding the bare
name of each dataset it read and `dataset@snapshot-id` for each version, so
either question is one `array_contains` over the `snapshots` collection group.
The index for it is NOT automatic: Firestore's automatic single-field indexes
are collection-scoped, so a collection-group query needs one declared with
collection-group scope (README.md lists it). Without it this raises
FAILED_PRECONDITION rather than returning nothing, which is the right way
round - a missing index must not read as "nothing consumes this".

NOTHING HERE IS AUTHORIZED, exactly as for `inbound_edges`: the rows are the
whole catalog's answer. Every caller owes a read check on `dataset` (the
consumer) before showing it, eliding the NAME rather than dropping the row -
that something downstream exists is not the secret, its name is.

Receipts cannot be backfilled, so a commit from before receipts existed is not
a consumer here even if it did read the dataset. The plan (`inbound_edges`) is
the answer for those.
"""

from __future__ import annotations

import logging

from .catalog.metadata import PRODUCED_BY_KEY
from .catalog.metadata import READ_SOURCE_KEYS_KEY
from .catalog.metadata import READ_SOURCES_KEY
from .catalog.metadata import read_source_key
from .catalog.metadata import snapshot_is_tombstoned

logger = logging.getLogger(__name__)

SNAPSHOTS_SUBCOLLECTION = "snapshots"

# `{workspace}/{collection}/datasets/{dataset}/snapshots/{snapshot-id}` - the
# workspace is not written on a snapshot document, so it is read off the path,
# as `inbound_edges` reads it off a trigger's.
_SNAPSHOT_PATH_LENGTH = 6


def _consumer_row(doc, source: str, snapshot_id: int | None) -> dict | None:
    parts = (getattr(getattr(doc, "reference", None), "path", None) or "").split("/")
    if len(parts) != _SNAPSHOT_PATH_LENGTH or parts[2] != "datasets" or parts[4] != SNAPSHOTS_SUBCOLLECTION:
        logger.warning("snapshot at an unexpected path, not reported as a consumer: %s", parts)
        return None
    data = doc.to_dict() or {}
    if snapshot_is_tombstoned(data):
        # Expired: the data that read the source is gone, and the receipt is a
        # record of history, not of a live consumer.
        return None
    workspace, collection, dataset = parts[0], parts[1], parts[3]
    # The entries for the source, so the caller sees WHICH version was read
    # even when it asked about the dataset as a whole. More than one when a
    # statement read two versions of it.
    matched = [
        entry
        for entry in data.get(READ_SOURCES_KEY) or []
        if entry.get("dataset") == source
        and (snapshot_id is None or entry.get("snapshot-id") == snapshot_id)
    ]
    return {
        "source": source,
        "dataset": f"{workspace}.{collection}.{dataset}",
        "workspace": workspace,
        "snapshot_id": data.get("snapshot-id"),
        "committed_at_ms": data.get("timestamp-ms"),
        "produced_by": data.get(PRODUCED_BY_KEY),
        "source_snapshot_ids": [entry.get("snapshot-id") for entry in matched],
        "resolved_by": [entry.get("resolved-by") for entry in matched],
    }


def find_consumers(client, source: str, snapshot_id: int | None = None) -> list[dict]:
    """Every live snapshot, in ANY workspace, whose receipt names `source`.

    `source` is a fully-qualified `workspace.collection.dataset`; with
    `snapshot_id` the answer narrows to the commits that read that version.
    Rows come back unauthorized, in a stable order (consumer, then newest
    first), so two answers can be compared without sorting.
    """
    from google.cloud.firestore_v1 import FieldFilter

    if not source or len(str(source).split(".")) < 3:
        raise ValueError(
            f"source must be a fully-qualified workspace.collection.dataset, got {source!r}"
        )
    source = str(source)
    key = read_source_key(source, snapshot_id)
    wanted = None if snapshot_id is None else int(snapshot_id)

    rows: list[dict] = []
    # Backticked: a Firestore field path is parsed, and an unquoted segment
    # must match `[a-zA-Z_][a-zA-Z_0-9]*`, so the hyphenated stored key is
    # refused with INVALID_ARGUMENT before any index is consulted. The stored
    # name and the queried path are deliberately the same constant, quoted
    # here rather than stored differently - the document's key is what it is.
    query = client.collection_group(SNAPSHOTS_SUBCOLLECTION).where(
        filter=FieldFilter(f"`{READ_SOURCE_KEYS_KEY}`", "array_contains", key)
    )
    for doc in query.stream():
        row = _consumer_row(doc, source, wanted)
        if row is not None:
            rows.append(row)

    rows.sort(key=lambda row: (row["dataset"], -(row["snapshot_id"] or 0)))
    return rows


__all__ = ["find_consumers"]
