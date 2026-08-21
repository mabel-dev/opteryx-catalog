"""Stub dataset projections: making a bound workspace's tables *listable*.

A workspace bound to an external catalog (see `binding.py`) has a shell
`$properties` document and no dataset documents at all - its tables live in
someone else's Iceberg catalog. The engine reaches them fine, and permissions
never enumerate anything, but LISTING is a Firestore collection-group query
over dataset docs (`odata.opteryx`'s service document), so those tables are
invisible to every catalogue-browsing surface.

This module closes that gap the way WORKSPACE_CATALOG_RESOLUTION.md section 5
settled it: project the external catalog's listing into Firestore as *stub*
dataset documents. Each carries

    workspace / collection / name / external-catalog: true

plus, when the caller collected them, an inline `schema`, a `statistics`
block, and the structural facts a foreign catalog's own metadata already
knows: `timestamp-ms` (last modified), `sort-orders` and `partition-columns`.
The last two are spelled the way odata already reads them, so an external
table reports its last-modified time and its `ordered`/`orderBy` with no
special case anywhere and no extra read. The `external-catalog` marker is what keeps everything snapshot-hungry
(compaction, the dropped sweep, `DatasetInfo` sort-order resolution) from
mistaking a stub for a real dataset, and it is also this module's delete
guard: reconciliation only ever removes documents that carry it, so a stub
projection can never eat a real dataset document that happens to share a name.

**Names were the original scope; the rest was added deliberately, and the
line is drawn at what the catalog gives away.** A name-only projection costs
one `list_tables` per namespace. Everything above costs one table load per
TABLE - and a load hands back a metadata document that already contains the
schema, the row/file/size/delete counts, the last-updated time, the sort order
and the partition spec. Taking those is free.

What is deliberately NOT taken is per-column min/max, null counts and column
sizes. Those live in the manifests, a further read per table that scales with
the table's file COUNT rather than its size, and nothing in the product reads
them. A cost with no reader is not worth charging a customer's catalog for on
every refresh.

Schema earned its place because odata's `$metadata` emits no EntityType for a
dataset with no resolvable columns: a name-only stub appeared in the service
document and vanished from `$metadata` - visible to a browser, invisible to
Excel and Power BI. The counts earned theirs because a listing with no row
counts cannot answer the first question anyone asks of a table they have just
found.

Everything beyond the four identity fields is OPTIONAL and independently
degradable. A caller that could not load one table still projects its name;
a caller that could not read one table's manifests still projects its schema.
A stub with no schema is exactly the original name-only stub, and still
correct - just invisible to `$metadata`, as it always was.

Nothing here is authoritative. The external catalog remains the source of
truth for reads: a query resolves the live schema through the engine's own
connector and never consults these documents. What is stored here is a
point-in-time projection for LISTING, and its age is the binding block's
`listing-synced-at-ms` - which is why the per-document statistics carry no
timestamp of their own. One freshness stamp, one meaning.

Staleness is a listing-only concern - queries and permissions consult the
external catalog, never these stubs - and refreshing is COST-IMPACTING, since
it re-lists every namespace in a customer's catalog. So this is never
scheduled and never auto-triggered: it runs when a person asks for it,
through control.opteryx's `POST /v1/workspaces/{name}/catalog/sync`. Code that
notices a stale-looking listing may RECOMMEND a refresh; it must not call one.

Producing the listing is the caller's job - this module knows nothing about
Iceberg, credentials or namespaces. It takes `[(collection, name), ...]` or
`[(collection, name, detail), ...]` and reconciles.

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

# The two optional blocks. `schema` is spelled INLINE rather than as a
# `schemas` subcollection with a `current-schema-id` pointer, which is how the
# native path stores it, for two reasons: a stub has no schema HISTORY to keep
# (the external catalog owns evolution, and a projection of one version is all
# there is), and odata's `$metadata` reads an inline schema straight off the
# document it already fetched while a schema id costs it a second read per
# dataset. The stored column spelling is the native one either way, so
# `_stored_type_display` renders a stub's column exactly as it renders a real
# one.
SCHEMA_FIELD = "schema"
STATISTICS_FIELD = "statistics"

# What a caller's `detail` may carry, and the document field each lands in:
# python_key -> stored kebab-case field. Explicit rather than a passthrough of
# whatever the caller hands over, because these documents are read by services
# that key off exact field names - odata's `$metadata` reads `timestamp-ms` for
# last-modified and its service document reads `sort-orders` for `ordered` /
# `orderBy`. A typo in a caller would otherwise be a field silently ignored by
# every reader, which is indistinguishable from "the catalog did not say".
PROJECTED_FIELDS = {
    "schema": SCHEMA_FIELD,
    "statistics": STATISTICS_FIELD,
    "timestamp_ms": "timestamp-ms",
    "sort_orders": "sort-orders",
    "partition_columns": "partition-columns",
}

# A detail key meaning "I listed this table but could not look inside it this
# time - keep whatever detail is already stored." It exists because the
# alternative is worse in the case that actually happens: one table failing to
# load during an otherwise good refresh would erase a schema that is almost
# certainly still correct, and a dataset with no columns is dropped from
# odata's `$metadata` entirely. A schema we could not RE-read is stale at
# worst; a schema we deleted is a table that vanished from Excel because of a
# transient error. Retention is explicit rather than the default for absent
# detail, so a name-only projection stays exactly what it always was.
RETAIN_DETAIL = "retain-detail"


class StubSyncResult(NamedTuple):
    """What one reconciliation did.

    `added`/`removed` are the pair the plan specifies; `total` and
    `synced_at_ms` come along because they are what was WRITTEN to the binding
    block, and the endpoint reports the same numbers the document now holds
    rather than recomputing them and risking a quiet disagreement.

    `updated` counts stubs that were already listed but whose projected
    content changed - a column added externally, a row count moved. It exists
    because carrying schema and statistics means a re-sync of an unchanged
    NAME list is no longer necessarily a no-op, and a caller that reported
    only added/removed would describe such a run as "nothing happened" while
    it rewrote half the workspace.
    """

    added: int
    removed: int
    updated: int
    total: int
    synced_at_ms: int


def _properties_ref(firestore_client, workspace: str):
    return firestore_client.collection(workspace).document(PROPERTIES_DOC)


def _normalize(listing: Iterable[tuple]) -> dict:
    """`[(collection, name[, detail]), ...]` -> `{(collection, name): detail}`.

    Both segments become Firestore document ids, so an empty or slash-bearing
    one is rejected here rather than at the write, where the failure would be
    a Firestore error naming neither the workspace nor the offending table.
    A dot in `name` is fine and expected - a nested external namespace
    `a.b.table` maps onto collection `a`, dataset `b.table`, matching the
    left-anchored split the rest of the catalog uses for qualified names. A
    dot in `collection` is not, for that same reason.

    `detail` is optional and may carry `schema` and/or `statistics`; a
    two-element entry is the original name-only projection and stays exactly
    that. A later duplicate of the same (collection, name) wins, so a caller
    that lists a table twice - once with detail, once without - does not get a
    result that depends on set iteration order.
    """
    normalized: dict = {}
    for entry in listing:
        if isinstance(entry, (str, bytes)) or not isinstance(entry, (tuple, list)):
            # ValueError, not TypeError, on purpose: every rejection this
            # function makes is "your listing is malformed", one thing the
            # caller handles one way (control.opteryx answers 409), and
            # splitting it by whether the defect is shape or type would make
            # that caller catch both to say the same sentence.
            raise ValueError(  # noqa: TRY004
                f"stub listing entries must be (collection, name[, detail]) tuples, got {entry!r}"
            )
        if len(entry) == 2:
            collection, name = entry
            detail = None
        elif len(entry) == 3:
            collection, name, detail = entry
        else:
            raise ValueError(
                f"stub listing entries must be (collection, name[, detail]) tuples, got {entry!r}"
            )
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
        if detail is not None and not isinstance(detail, dict):
            raise ValueError(
                f"stub listing detail for {collection}.{name} must be a dict, got "
                f"{type(detail).__name__}"
            )
        normalized[(collection, name)] = detail or {}
    return normalized


def firestore_safe(value):
    """One projected statistic, in a spelling Firestore will actually store.

    Everything here is derived from a foreign catalog's own metadata, and a
    catalog is free to hand back types Firestore will not store - `Decimal`,
    `date`, `UUID`, an enum, a transform object. Left alone they fail the
    write, which would lose an entire table's projection over one stray value.
    This is a boundary guard rather than a hot path: the fields projected today
    are numbers, strings and lists.

    `Decimal` becomes a STRING rather than a float, deliberately: these are
    bounds on a decimal column, and rendering `0.1` as 0.1000000000000000055
    would make the stored bound disagree with the data it describes. A reader
    wanting arithmetic can parse it back exactly; a reader wanting a float
    could not have recovered the digits.

    `datetime` passes through untouched (Firestore stores it natively), while
    a bare `date` - which Firestore does not accept - becomes its ISO form.
    Anything else unrecognised is rendered with `str()` rather than dropped:
    "we could not type this bound" is more useful to a reader than a silently
    absent one.
    """
    import datetime
    import decimal
    import uuid

    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return value
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (list, tuple, set)):
        return [firestore_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): firestore_safe(item) for key, item in value.items()}
    return str(value)


def _stub_document(
    workspace: str, collection: str, name: str, detail: dict, stored: dict | None = None
) -> dict:
    """The document one listed table projects to.

    The four identity fields are always present; `schema` and `statistics`
    appear only when the caller collected them. An absent block is written as
    an ABSENT KEY rather than an explicit null - "we did not collect this" and
    "this table has no columns" are different statements, and a null would
    make every reader distinguish them by hand.

    With `RETAIN_DETAIL` set, the blocks are carried over from `stored`
    instead: the caller listed the table but could not look inside it, and
    what is already on file is better than nothing. Retaining from a document
    that has nothing to retain is simply a name-only stub.
    """
    document = {
        "workspace": workspace,
        "collection": collection,
        "name": name,
        STUB_MARKER: True,
    }
    if detail.get(RETAIN_DETAIL):
        for field in PROJECTED_FIELDS.values():
            carried = (stored or {}).get(field)
            if carried is not None:
                document[field] = carried
        return document

    unknown = sorted(set(detail) - set(PROJECTED_FIELDS) - {RETAIN_DETAIL})
    if unknown:
        raise ValueError(
            f"stub detail for {collection}.{name} carries unknown key(s) {unknown} - "
            f"projected fields are {sorted(PROJECTED_FIELDS)}"
        )

    for key, field in PROJECTED_FIELDS.items():
        value = detail.get(key)
        if value:
            document[field] = firestore_safe(value)
    return document


def _existing_datasets(firestore_client, workspace: str) -> dict:
    """Every dataset document in the workspace -> `(is_stub, stored_document)`.

    Reads the real ones too, deliberately: they are what stops an `add` from
    overwriting a dataset this catalog owns, and they must NOT be deleted when
    the external catalog stops listing a name. The stored document comes back
    with them so an unchanged stub can be recognised WITHOUT writing it - now
    that a stub carries schema and statistics, "already listed" is no longer
    the same question as "already correct", and blind re-writing would turn
    every refresh into one write per table forever.
    """
    found = {}
    workspace_ref = firestore_client.collection(workspace)
    for collection_doc in workspace_ref.list_documents():
        if collection_doc.id.startswith("$"):
            continue
        for snapshot in collection_doc.collection(DATASETS_SUBCOLLECTION).stream():
            data = snapshot.to_dict() or {}
            found[(collection_doc.id, snapshot.id)] = (bool(data.get(STUB_MARKER)), data)
    return found


def sync_stub_datasets(
    firestore_client,
    workspace: str,
    listing: Iterable[tuple],
) -> StubSyncResult:
    """Reconcile `workspace`'s stub documents to `listing`; stamp the binding.

    `listing` is `[(collection, name), ...]` as read from the external
    catalog, or `[(collection, name, detail), ...]` where `detail` may carry
    `schema` (a list of stored-spelling column dicts) and `statistics`. The
    two forms mix freely in one call, which is what lets a caller project a
    table it could list but could not load. Returns what changed.

    Reconciliation, in one pass:

    - listed and absent              -> a stub is written
    - listed, a stub, unchanged      -> left alone (no write, no churn)
    - listed, a stub, now different  -> rewritten, counted as `updated`
    - listed and a REAL dataset      -> left alone; a document this catalog
                                        owns is never overwritten by a
                                        projection
    - a stub no longer listed        -> deleted
    - a real dataset not listed      -> left alone

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
    updated = 0
    for key in sorted(desired):
        collection, name = key
        is_stub, stored = existing.get(key, (False, None))
        if key in existing and not is_stub:
            continue  # a real dataset document - never overwritten
        document = _stub_document(workspace, collection, name, desired[key], stored)
        if key in existing:
            if stored == document:
                continue  # already exactly this - no write, no churn
            updated += 1
        else:
            added += 1
        workspace_ref.document(collection).collection(DATASETS_SUBCOLLECTION).document(name).set(
            document
        )

    removed = 0
    for key in sorted(set(existing) - set(desired)):
        if not existing[key][0]:
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

    return StubSyncResult(
        added=added,
        removed=removed,
        updated=updated,
        total=total,
        synced_at_ms=synced_at_ms,
    )
