"""Who is downstream of a dataset - impact analysis, and the forensic version.

THE QUESTION THIS ANSWERS: "if I change this dataset, who is affected?" A
dataset's own records only ever look UPWARDS - `sources` names what it was
built from, `read-sources` names what each of its commits read - so the owner
of a table has no way to see who is standing on it. That is the question you
ask before a schema change, a backfill, or a deletion, and it is the one the
catalog could not answer at all.

TWO QUESTIONS, NOT ONE, and they differ in a way that matters:

  `find_impacted` - who is built from this dataset RIGHT NOW, from the
  standing `sources` list on each dataset document. This is the impact
  answer: it is about current content, so a consumer that has since been
  rebuilt from something else is correctly absent.

  `find_readers` - whose commits have READ this dataset, from the
  `read-source-keys` index on each snapshot, optionally narrowed to one
  version of it. This is the forensic answer: it includes a consumer that
  read it last month and has moved on, and it can say WHICH version was read.

ANSWERS CROSS WORKSPACE BOUNDARIES, DELIBERATELY (decision 2026-09-09). A
consumer in a workspace the caller cannot read is named like any other. The
alternative was reporting it as "something you cannot see", and an impact
answer that will not say who is affected does not answer the question it was
asked - it just moves the work to a human who has to go and find out anyway.
Provenance is a citation: knowing that a dataset exists and is built from
yours is not the same as being able to read it, which still needs a grant.
This is the opposite of `inbound_edges`' old contract, and the reversal is
deliberate rather than an oversight.

WHY THERE IS A SEPARATE KEY ARRAY TO QUERY. Firestore cannot filter on a
field INSIDE an array element: a subfield path matches nothing (verified on
the emulator, 2026-09-09), and `array_contains` compares the WHOLE element,
so finding a reader would mean supplying `resolved-by` too and guessing among
its five values. `read-source-keys` exists for exactly this - it is the
queryable projection of `read-sources`, holding the bare dataset name (any
version) and `dataset@snapshot-id` (one version) as flat scalars, which is
the only shape Firestore can match. See `metadata.read_source_keys`.

CAPPED AT 64 CONSUMERS, and it says when the cap bit. Sixty-four is the same
bound the standing `sources` list carries, and it is far past the point where
a list is what anybody wanted: a dataset with more consumers than that needs
a different answer - a rollup, a graph, a report - not this one paginated.
Growing past it is a redesign, deliberately, rather than a cursor bolted on.
"""

from __future__ import annotations

import logging

from .catalog.metadata import MAX_SOURCES
from .catalog.metadata import PRODUCED_BY_KEY
from .catalog.metadata import READ_SOURCE_KEYS_KEY
from .catalog.metadata import READ_SOURCES_KEY
from .catalog.metadata import read_source_key
from .catalog.metadata import snapshot_is_tombstoned

logger = logging.getLogger(__name__)

DATASETS_SUBCOLLECTION = "datasets"
SNAPSHOTS_SUBCOLLECTION = "snapshots"
SOURCES_KEY = "sources"
SOURCES_COMPLETE_KEY = "sources-complete"

# `{workspace}/{collection}/datasets/{dataset}` and one level deeper for a
# snapshot. The workspace is not written on either document, so it is read off
# the path - the same reason and the same method the integrity sweep uses.
_DATASET_PATH_LENGTH = 4
_SNAPSHOT_PATH_LENGTH = 6


def _require_qualified(dataset: str) -> str:
    if not dataset or len(str(dataset).split(".")) < 3:
        raise ValueError(
            f"dataset must be a fully-qualified workspace.collection.dataset, got {dataset!r}"
        )
    return str(dataset)


def _path_parts(doc) -> list[str]:
    path = getattr(getattr(doc, "reference", None), "path", None) or ""
    return path.split("/") if path else []


def find_impacted(client, dataset: str, limit: int = MAX_SOURCES) -> dict:
    """Every dataset whose CURRENT content was built from `dataset`.

    One indexed collection-group query over `datasets`, matching the standing
    `sources` list that the commit path maintains. Returns

        {"dataset", "consumers": [...], "truncated": bool}

    with one row per consumer - `{dataset, workspace, sources_complete}` -
    ordered by name so two answers can be compared without sorting. Rows are
    NOT authorized: see the module docstring for why they are named in full.

    `sources_complete` is the consumer's own flag, carried through because it
    qualifies THIS answer: a consumer whose list is incomplete may be built
    from things it cannot name, and a consumer missing from this result may
    be missing because its list was capped rather than because it does not
    read you.
    """
    from google.cloud.firestore_v1 import FieldFilter

    dataset = _require_qualified(dataset)

    consumers: list[dict] = []
    truncated = False
    query = client.collection_group(DATASETS_SUBCOLLECTION).where(
        # Backticked because a Firestore field path is parsed; `sources` is a
        # bare identifier and needs none, but the habit is what keeps the
        # hyphenated ones right. See tests/test_firestore_field_paths.py.
        filter=FieldFilter(SOURCES_KEY, "array_contains", dataset)
    )
    for doc in query.stream():
        parts = _path_parts(doc)
        if len(parts) != _DATASET_PATH_LENGTH or parts[2] != DATASETS_SUBCOLLECTION:
            logger.warning("dataset at an unexpected path, not reported: %s", parts)
            continue
        data = doc.to_dict() or {}
        if len(consumers) >= limit:
            truncated = True
            break
        consumers.append(
            {
                "dataset": f"{parts[0]}.{parts[1]}.{parts[3]}",
                "workspace": parts[0],
                "sources_complete": bool(data.get(SOURCES_COMPLETE_KEY, False)),
            }
        )

    consumers.sort(key=lambda row: row["dataset"])
    return {"dataset": dataset, "consumers": consumers, "truncated": truncated}


def find_readers(
    client, dataset: str, snapshot_id: int | None = None, limit: int = MAX_SOURCES
) -> dict:
    """Every dataset whose commits have READ `dataset`, folded one row each.

    With `snapshot_id`, only the commits that read THAT version. One indexed
    collection-group query over `snapshots`, matching `read-source-keys`.

    Folded to one row per consuming dataset rather than one per commit,
    because a table refreshed hourly would otherwise bury every other
    consumer under thousands of its own rows, and "who is using my data" is a
    question about datasets. Each row carries how many of its commits read
    this, the most recent of them, and the versions they read:

        {dataset, workspace, commits, latest_snapshot_id, latest_committed_at_ms,
         produced_by, versions_read}

    Expired snapshots are skipped - the data that read it is gone, and this
    is a question about who is standing on you now, not a complete history of
    everything that ever did.
    """
    from google.cloud.firestore_v1 import FieldFilter

    dataset = _require_qualified(dataset)
    wanted = None if snapshot_id is None else int(snapshot_id)
    key = read_source_key(dataset, wanted)

    folded: dict[str, dict] = {}
    truncated = False
    query = client.collection_group(SNAPSHOTS_SUBCOLLECTION).where(
        filter=FieldFilter(f"`{READ_SOURCE_KEYS_KEY}`", "array_contains", key)
    )
    for doc in query.stream():
        parts = _path_parts(doc)
        if (
            len(parts) != _SNAPSHOT_PATH_LENGTH
            or parts[2] != DATASETS_SUBCOLLECTION
            or parts[4] != SNAPSHOTS_SUBCOLLECTION
        ):
            logger.warning("snapshot at an unexpected path, not reported: %s", parts)
            continue
        data = doc.to_dict() or {}
        if snapshot_is_tombstoned(data):
            continue

        consumer = f"{parts[0]}.{parts[1]}.{parts[3]}"
        if consumer not in folded:
            if len(folded) >= limit:
                truncated = True
                continue
            folded[consumer] = {
                "dataset": consumer,
                "workspace": parts[0],
                "commits": 0,
                "latest_snapshot_id": None,
                "latest_committed_at_ms": None,
                "produced_by": None,
                "versions_read": [],
            }
        row = folded[consumer]
        row["commits"] += 1

        committed = data.get("timestamp-ms")
        if row["latest_committed_at_ms"] is None or (
            committed is not None and committed > row["latest_committed_at_ms"]
        ):
            row["latest_committed_at_ms"] = committed
            row["latest_snapshot_id"] = data.get("snapshot-id")
            # The producer of the LATEST commit, not of every one: a task can
            # be repointed, and the current answer is the useful one.
            row["produced_by"] = data.get(PRODUCED_BY_KEY)

        for entry in data.get(READ_SOURCES_KEY) or []:
            if entry.get("dataset") != dataset:
                continue
            version = entry.get("snapshot-id")
            if wanted is not None and version != wanted:
                continue
            if version not in row["versions_read"]:
                row["versions_read"].append(version)

    consumers = sorted(folded.values(), key=lambda row: row["dataset"])
    for row in consumers:
        row["versions_read"].sort(key=lambda v: -1 if v is None else v)
    return {
        "dataset": dataset,
        "snapshot_id": wanted,
        "consumers": consumers,
        "truncated": truncated,
    }


__all__ = ["find_impacted", "find_readers"]
