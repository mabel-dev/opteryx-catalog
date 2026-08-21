"""Stub dataset projections: making a bound workspace's tables *listable*.

A workspace bound to an external catalog (see `binding.py`) has a shell
`$properties` document and no dataset documents at all - its tables live in
someone else's Iceberg catalog. The engine reaches them fine, and permissions
never enumerate anything, but LISTING is a Firestore collection-group query
over dataset docs (`odata.opteryx`'s service document), so those tables are
invisible to every catalogue-browsing surface.

This module closes that gap the way WORKSPACE_CATALOG_RESOLUTION.md section 5
settled it: project the external catalog's listing into Firestore as *stub*
dataset documents carrying nothing but

    workspace / collection / name / external-catalog: true

- no snapshots, no schemas, no sort orders, no location. The `external-catalog`
marker is what keeps everything snapshot-hungry (compaction, the dropped
sweep, `DatasetInfo` sort-order resolution) from mistaking a stub for a real
dataset, and it is also this module's delete guard: reconciliation only ever
removes documents that carry it, so a stub projection can never eat a real
dataset document that happens to share a name.

Staleness is a listing-only concern - queries and permissions consult the
external catalog, never these stubs - and refreshing is COST-IMPACTING, since
it re-lists every namespace in a customer's catalog. So this is never
scheduled and never auto-triggered: it runs when a person asks for it,
through control.opteryx's `POST /v1/workspaces/{name}/catalog/sync`. Code that
notices a stale-looking listing may RECOMMEND a refresh; it must not call one.

Producing the listing is the caller's job - this module knows nothing about
Iceberg, credentials or namespaces. It takes `[(collection, name), ...]` and
reconciles.

The freshness stamp lives on the binding block, not here as a separate
document: `catalog.listing-synced-at-ms` and `catalog.listing-count` are
written by the same call that reconciles, and surfaced by control.opteryx's
`GET /v1/workspaces/{name}/catalog`. Without them a UI cannot say when the
list was last refreshed, and a refresh button with no "last refreshed" beside
it is a button people press repeatedly to find out whether they needed to -
which is exactly the cost this design is trying not to incur.
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from typing import NamedTuple

from opteryx_catalog.exceptions import InvalidCatalogBinding
from opteryx_catalog.exceptions import WorkspaceNotFound

PROPERTIES_DOC = "$properties"
DATASETS_SUBCOLLECTION = "datasets"

# The field that marks a document as a projection rather than a dataset this
# catalog owns. Everything in this module keys off it.
STUB_MARKER = "external-catalog"


class StubSyncResult(NamedTuple):
    """What one reconciliation did.

    `added`/`removed` are the pair the plan specifies; `total` and
    `synced_at_ms` come along because they are what was WRITTEN to the binding
    block, and the endpoint reports the same numbers the document now holds
    rather than recomputing them and risking a quiet disagreement.
    """

    added: int
    removed: int
    total: int
    synced_at_ms: int


def _properties_ref(firestore_client, workspace: str):
    return firestore_client.collection(workspace).document(PROPERTIES_DOC)


def _normalize(listing: Iterable[tuple]) -> set:
    """`[(collection, name), ...]` -> a clean set, or a loud error.

    Both segments become Firestore document ids, so an empty or slash-bearing
    one is rejected here rather than at the write, where the failure would be
    a Firestore error naming neither the workspace nor the offending table.
    A dot in `name` is fine and expected - a nested external namespace
    `a.b.table` maps onto collection `a`, dataset `b.table`, matching the
    left-anchored split the rest of the catalog uses for qualified names. A
    dot in `collection` is not, for that same reason.
    """
    normalized = set()
    for entry in listing:
        try:
            collection, name = entry
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"stub listing entries must be (collection, name) pairs, got {entry!r}"
            ) from exc
        collection = str(collection).strip()
        name = str(name).strip()
        for segment, label in ((collection, "collection"), (name, "name")):
            if not segment or "/" in segment or segment.startswith("$"):
                raise ValueError(
                    f"stub listing carries an unusable {label} {segment!r} for entry {entry!r}"
                )
        if "." in collection:
            raise ValueError(
                f"stub listing collection {collection!r} contains a dot - qualified names "
                "are split left-anchored, so a dotted collection would be ambiguous"
            )
        normalized.add((collection, name))
    return normalized


def _existing_datasets(firestore_client, workspace: str) -> dict:
    """Every dataset document in the workspace -> is it a stub?

    Reads the real ones too, deliberately: they are what stops an `add` from
    overwriting a dataset this catalog owns, and they must NOT be deleted when
    the external catalog stops listing a name.
    """
    found = {}
    workspace_ref = firestore_client.collection(workspace)
    for collection_doc in workspace_ref.list_documents():
        if collection_doc.id.startswith("$"):
            continue
        for snapshot in collection_doc.collection(DATASETS_SUBCOLLECTION).stream():
            data = snapshot.to_dict() or {}
            found[(collection_doc.id, snapshot.id)] = bool(data.get(STUB_MARKER))
    return found


def sync_stub_datasets(
    firestore_client,
    workspace: str,
    listing: Iterable[tuple],
) -> StubSyncResult:
    """Reconcile `workspace`'s stub documents to `listing`; stamp the binding.

    `listing` is `[(collection, name), ...]` as read from the external
    catalog. Returns what changed.

    Reconciliation, in one pass:

    - listed and absent            -> a stub is written
    - listed and already a stub    -> left alone (no write, no churn)
    - listed and a REAL dataset    -> left alone; a document this catalog owns
                                      is never overwritten by a projection
    - a stub no longer listed      -> deleted
    - a real dataset not listed    -> left alone

    Raises `WorkspaceNotFound` if the workspace has no `$properties` document
    and `InvalidCatalogBinding` if it has one with no `catalog` block: a stub
    projection is only meaningful for a workspace whose datasets live
    somewhere else, and writing stubs into a native workspace would put
    unqueryable names into its listing.

    Plain writes rather than a batch, matching the rest of this library, and
    the steady state is zero writes - only the difference costs anything.
    """
    desired = _normalize(listing)

    reference = _properties_ref(firestore_client, workspace)
    snapshot = reference.get()
    if not snapshot.exists:
        raise WorkspaceNotFound(f"Workspace does not exist: {workspace}")
    if not (snapshot.to_dict() or {}).get("catalog"):
        raise InvalidCatalogBinding(
            f"workspace {workspace!r} has no catalog binding - its datasets are already "
            "listed from this catalog, so there is nothing to project"
        )

    existing = _existing_datasets(firestore_client, workspace)
    workspace_ref = firestore_client.collection(workspace)

    added = 0
    for collection, name in sorted(desired - set(existing)):
        workspace_ref.document(collection).collection(DATASETS_SUBCOLLECTION).document(name).set(
            {
                "workspace": workspace,
                "collection": collection,
                "name": name,
                STUB_MARKER: True,
            }
        )
        added += 1

    removed = 0
    for key in sorted(set(existing) - desired):
        if not existing[key]:
            continue  # a real dataset document - not ours to remove
        collection, name = key
        workspace_ref.document(collection).collection(DATASETS_SUBCOLLECTION).document(
            name
        ).delete()
        removed += 1

    synced_at_ms = int(time.time() * 1000)
    total = len(desired)
    # Targeted field paths rather than rewriting the whole `catalog` map: the
    # stamp must not clobber a binding write that landed between the read
    # above and this update. The names are kebab-case, so they are escaped
    # through FieldPath rather than spelled as a dotted string.
    from google.cloud.firestore_v1.field_path import FieldPath

    reference.update(
        {
            FieldPath("catalog", "listing-synced-at-ms").to_api_repr(): synced_at_ms,
            FieldPath("catalog", "listing-count").to_api_repr(): total,
        }
    )

    return StubSyncResult(added=added, removed=removed, total=total, synced_at_ms=synced_at_ms)
