"""Stub dataset projection: reconciling a bound workspace's listing.

Covers the Phase 6 primitive from WORKSPACE_CATALOG_RESOLUTION_PLAN.md: what
gets added, what gets removed, what is deliberately left alone (real dataset
documents, in both directions), the freshness stamp written onto the binding
block, and the two refusals - an unknown workspace and an unbound one.
"""

from __future__ import annotations

import pytest

from opteryx_catalog.binding import read_catalog_binding
from opteryx_catalog.exceptions import InvalidCatalogBinding
from opteryx_catalog.exceptions import WorkspaceNotFound
from opteryx_catalog.stub_projection import STUB_MARKER
from opteryx_catalog.stub_projection import sync_stub_datasets


# ---------------------------------------------------------------------------
# A Firestore double with just the shape this module uses: a workspace
# collection of documents, each with a `datasets` subcollection, plus
# field-path updates (the stamp uses escaped `catalog.`x`` paths).
# ---------------------------------------------------------------------------


class _Snapshot:
    def __init__(self, doc_id, data):
        self.id = doc_id
        self.exists = data is not None
        self._data = data

    def to_dict(self):
        return dict(self._data) if self._data is not None else None


def _set_path(data: dict, path: str, value) -> None:
    # "catalog.`listing-count`" -> data["catalog"]["listing-count"]
    parts = [part.strip("`") for part in path.split(".")]
    node = data
    for part in parts[:-1]:
        node = node.setdefault(part, {})
    node[parts[-1]] = value


class _Doc:
    def __init__(self, doc_id):
        self.id = doc_id
        self._data = None
        self._subcollections = {}

    def get(self):
        return _Snapshot(self.id, self._data)

    def set(self, data):
        self._data = dict(data)

    def update(self, fields):
        if self._data is None:
            raise KeyError("update on missing document")
        for key, value in fields.items():
            _set_path(self._data, key, value)

    def delete(self):
        self._data = None

    def collection(self, name):
        return self._subcollections.setdefault(name, _Collection())

    @property
    def _exists_or_has_children(self):
        return self._data is not None or any(
            coll._docs for coll in self._subcollections.values()
        )


class _Collection:
    def __init__(self):
        self._docs = {}

    def document(self, doc_id):
        return self._docs.setdefault(doc_id, _Doc(doc_id))

    def list_documents(self):
        # Firestore returns "missing" parents that still have subcollections,
        # which is exactly what a stub-only collection looks like.
        return [doc for doc in self._docs.values() if doc._exists_or_has_children]

    def stream(self):
        return [_Snapshot(doc.id, doc._data) for doc in self._docs.values() if doc._data is not None]


class _FakeFirestore:
    def __init__(self):
        self._collections = {}

    def collection(self, name):
        return self._collections.setdefault(name, _Collection())


WS = "tarchia"


@pytest.fixture
def db():
    client = _FakeFirestore()
    client.collection(WS).document("$properties").set(
        {
            "timestamp-ms": 1,
            "catalog": {"kind": "iceberg", "config": {}, "version": 7},
        }
    )
    return client


def _stubs(db, workspace=WS):
    found = {}
    for collection_doc in db.collection(workspace).list_documents():
        if collection_doc.id.startswith("$"):
            continue
        for snapshot in collection_doc.collection("datasets").stream():
            found[(collection_doc.id, snapshot.id)] = snapshot.to_dict()
    return found


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------


def test_first_sync_writes_stubs(db):
    result = sync_stub_datasets(db, WS, [("interop", "people"), ("interop", "orders")])
    assert (result.added, result.removed, result.total) == (2, 0, 2)

    stubs = _stubs(db)
    assert set(stubs) == {("interop", "people"), ("interop", "orders")}
    assert stubs[("interop", "people")] == {
        "workspace": WS,
        "collection": "interop",
        "name": "people",
        STUB_MARKER: True,
    }


def test_resync_of_the_same_listing_is_a_no_op(db):
    listing = [("interop", "people"), ("interop", "orders")]
    sync_stub_datasets(db, WS, listing)
    result = sync_stub_datasets(db, WS, listing)
    assert (result.added, result.removed, result.total) == (0, 0, 2)


def test_added_and_removed_are_reported_and_applied(db):
    sync_stub_datasets(db, WS, [("interop", "people"), ("interop", "orders")])
    result = sync_stub_datasets(db, WS, [("interop", "people"), ("sales", "invoices")])
    assert (result.added, result.removed, result.total) == (1, 1, 2)
    assert set(_stubs(db)) == {("interop", "people"), ("sales", "invoices")}


def test_duplicate_entries_count_once(db):
    result = sync_stub_datasets(db, WS, [("interop", "people"), ("interop", "people")])
    assert (result.added, result.total) == (1, 1)


def test_nested_external_namespace_splits_left_anchored(db):
    # `a.b.people` in the external catalog is collection `a`, dataset `b.people`
    result = sync_stub_datasets(db, WS, [("a", "b.people")])
    assert result.added == 1
    assert ("a", "b.people") in _stubs(db)


# ---------------------------------------------------------------------------
# A real dataset document in a bound workspace is impossible, and refused
#
# An externally-bound workspace cannot domicile datasets of this catalog's
# own: every relation-scoped write against one is routed at its bound
# metastore, which refuses. So one found here means that routing has been
# breached upstream. This used to be skipped silently in both directions,
# which let such a document sit in a bound workspace with nothing saying so;
# the projection's next act is to overwrite or delete it, and doing either to
# a real dataset loses data.
# ---------------------------------------------------------------------------


def _seed_real_dataset(db, collection, name):
    db.collection(WS).document(collection).collection("datasets").document(name).set(
        {"workspace": WS, "collection": collection, "name": name, "location": "gs://real"}
    )


def test_a_real_dataset_in_a_bound_workspace_is_refused(db):
    _seed_real_dataset(db, "interop", "people")
    with pytest.raises(InvalidCatalogBinding):
        sync_stub_datasets(db, WS, [("interop", "people")])


def test_the_refusal_fires_even_when_the_name_is_not_listed(db):
    # Being absent from the listing does not make it legitimate - and this is
    # the direction the old code was quietest about.
    _seed_real_dataset(db, "interop", "people")
    with pytest.raises(InvalidCatalogBinding):
        sync_stub_datasets(db, WS, [("interop", "orders")])


def test_the_refusal_names_the_offending_dataset(db):
    _seed_real_dataset(db, "interop", "people")
    with pytest.raises(InvalidCatalogBinding) as raised:
        sync_stub_datasets(db, WS, [("interop", "people")])
    assert "interop.people" in str(raised.value)


def test_nothing_is_written_when_the_refusal_fires(db):
    # The refusal must come BEFORE any write, or a partial projection is left
    # behind next to the dataset it refused to touch.
    _seed_real_dataset(db, "interop", "people")
    with pytest.raises(InvalidCatalogBinding):
        sync_stub_datasets(db, WS, [("interop", "orders")])
    # "orders" was listed and would have been written had the refusal come any
    # later; only the seeded document remains.
    assert set(_stubs(db)) == {("interop", "people")}


# ---------------------------------------------------------------------------
# The freshness stamp
# ---------------------------------------------------------------------------


def test_stamp_lands_on_the_binding_block_and_reads_back(db):
    result = sync_stub_datasets(db, WS, [("interop", "people")])
    binding = read_catalog_binding(db, WS)
    assert binding.listing_count == 1
    assert binding.listing_synced_at_ms == result.synced_at_ms
    # and the binding itself is untouched
    assert binding.kind == "iceberg"
    assert binding.version == 7


def test_stamp_is_rewritten_on_every_sync(db):
    first = sync_stub_datasets(db, WS, [("interop", "people")])
    second = sync_stub_datasets(db, WS, [])
    assert second.synced_at_ms >= first.synced_at_ms
    binding = read_catalog_binding(db, WS)
    assert binding.listing_count == 0


def test_binding_read_reports_never_synced_before_a_sync(db):
    binding = read_catalog_binding(db, WS)
    assert binding.listing_synced_at_ms is None
    assert binding.listing_count is None


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


def test_unknown_workspace_raises(db):
    with pytest.raises(WorkspaceNotFound):
        sync_stub_datasets(db, "nowhere", [("interop", "people")])


def test_unbound_workspace_raises(db):
    db.collection("native_ws").document("$properties").set({"timestamp-ms": 1})
    with pytest.raises(InvalidCatalogBinding):
        sync_stub_datasets(db, "native_ws", [("interop", "people")])


@pytest.mark.parametrize(
    "entry",
    [("", "people"), ("interop", ""), ("interop", "a/b"), ("a.b", "people"), ("$dropped", "x")],
)
def test_unusable_listing_entries_are_rejected_before_any_write(db, entry):
    with pytest.raises(ValueError):
        sync_stub_datasets(db, WS, [entry])
    assert _stubs(db) == {}


def test_malformed_listing_entry_is_rejected(db):
    with pytest.raises(ValueError):
        sync_stub_datasets(db, WS, ["interop.people"])


# ---------------------------------------------------------------------------
# Schema and statistics
#
# Names alone left a stub in odata's service document but out of its
# $metadata, which emits no EntityType for a dataset with no resolvable
# columns. These are optional and independently degradable: a table the caller
# could not load still projects its name, and a table whose manifests it could
# not read still projects its schema.
# ---------------------------------------------------------------------------


COLUMNS = [
    {"id": 1, "name": "user_id", "type": "BIGINT", "nullable": False},
    {"id": 2, "name": "country", "type": "VARCHAR", "nullable": True},
]
STATS = {
    "row-count": 1200,
    "data-file-count": 3,
    "columns": {"country": {"min": "AU", "max": "ZW", "null-count": 0}},
}


def test_schema_and_statistics_are_stored_on_the_stub(db):
    sync_stub_datasets(
        db, WS, [("interop", "people", {"schema": COLUMNS, "statistics": STATS})]
    )
    stored = _stubs(db)[("interop", "people")]
    assert stored["schema"] == COLUMNS
    assert stored["statistics"] == STATS
    # the identity fields are untouched by the addition
    assert stored["name"] == "people"
    assert stored[STUB_MARKER] is True


def test_a_name_only_entry_still_projects_name_only(db):
    sync_stub_datasets(db, WS, [("interop", "people")])
    stored = _stubs(db)[("interop", "people")]
    # absent keys, not explicit nulls: "not collected" and "no columns" are
    # different statements and a null would conflate them
    assert "schema" not in stored
    assert "statistics" not in stored


def test_detailed_and_name_only_entries_mix_in_one_listing(db):
    result = sync_stub_datasets(
        db,
        WS,
        [("interop", "people", {"schema": COLUMNS}), ("interop", "orders")],
    )
    assert result.added == 2
    stubs = _stubs(db)
    assert stubs[("interop", "people")]["schema"] == COLUMNS
    assert "schema" not in stubs[("interop", "orders")]


def test_an_unchanged_detailed_stub_is_not_rewritten(db):
    listing = [("interop", "people", {"schema": COLUMNS, "statistics": STATS})]
    sync_stub_datasets(db, WS, listing)
    result = sync_stub_datasets(db, WS, listing)
    # the steady state stays zero writes even now that a stub carries content
    assert (result.added, result.removed, result.updated) == (0, 0, 0)


def test_a_changed_schema_counts_as_updated(db):
    sync_stub_datasets(db, WS, [("interop", "people", {"schema": COLUMNS})])
    evolved = COLUMNS + [{"id": 3, "name": "signup_ts", "type": "TIMESTAMP", "nullable": True}]
    result = sync_stub_datasets(db, WS, [("interop", "people", {"schema": evolved})])
    assert (result.added, result.removed, result.updated) == (0, 0, 1)
    assert _stubs(db)[("interop", "people")]["schema"] == evolved


def test_a_changed_row_count_counts_as_updated(db):
    sync_stub_datasets(db, WS, [("interop", "people", {"statistics": STATS})])
    moved = dict(STATS, **{"row-count": 1300})
    result = sync_stub_datasets(db, WS, [("interop", "people", {"statistics": moved})])
    assert result.updated == 1
    assert _stubs(db)[("interop", "people")]["statistics"]["row-count"] == 1300


def test_losing_detail_rewrites_the_stub_back_to_name_only(db):
    # the caller could load the table last time and could not this time; the
    # name survives, and the stale schema does not linger as if still true
    sync_stub_datasets(db, WS, [("interop", "people", {"schema": COLUMNS})])
    result = sync_stub_datasets(db, WS, [("interop", "people")])
    assert result.updated == 1
    assert "schema" not in _stubs(db)[("interop", "people")]


def test_detail_never_revives_a_removed_stub(db):
    sync_stub_datasets(db, WS, [("interop", "people", {"schema": COLUMNS})])
    result = sync_stub_datasets(db, WS, [])
    assert (result.removed, result.total) == (1, 0)
    assert _stubs(db) == {}


def test_detail_over_a_real_dataset_is_refused_too(db):
    # Carrying schema/statistics does not make the write acceptable - the
    # refusal is about what is already stored, not about what is being written.
    _seed_real_dataset(db, "interop", "people")
    with pytest.raises(InvalidCatalogBinding):
        sync_stub_datasets(
            db, WS, [("interop", "people", {"schema": COLUMNS, "statistics": STATS})]
        )


def test_a_duplicate_entry_takes_the_last_detail(db):
    # deterministic rather than dependent on iteration order
    result = sync_stub_datasets(
        db,
        WS,
        [("interop", "people"), ("interop", "people", {"schema": COLUMNS})],
    )
    assert (result.added, result.total) == (1, 1)
    assert _stubs(db)[("interop", "people")]["schema"] == COLUMNS


def test_malformed_detail_is_rejected(db):
    with pytest.raises(ValueError):
        sync_stub_datasets(db, WS, [("interop", "people", ["not", "a", "dict"])])
    assert _stubs(db) == {}


def test_over_long_entries_are_rejected(db):
    with pytest.raises(ValueError):
        sync_stub_datasets(db, WS, [("interop", "people", {}, "extra")])


# -- values Firestore will actually accept ----------------------------------


def test_bounds_are_normalised_to_storable_values(db):
    import datetime
    import decimal
    import uuid

    identifier = uuid.UUID("12345678-1234-5678-1234-567812345678")
    stats = {
        "columns": {
            "price": {"min": decimal.Decimal("0.10"), "max": decimal.Decimal("99.99")},
            "born": {"min": datetime.date(1999, 12, 31)},
            "seen": {"min": datetime.datetime(2026, 8, 21, 12, 0, 0)},
            "ref": {"min": identifier},
        }
    }
    sync_stub_datasets(db, WS, [("interop", "people", {"statistics": stats})])
    columns = _stubs(db)[("interop", "people")]["statistics"]["columns"]

    # a decimal bound stays exact - rendering 0.10 as a float would make the
    # stored bound disagree with the column it describes
    assert columns["price"]["min"] == "0.10"
    assert columns["born"]["min"] == "1999-12-31"
    # datetime is a Firestore-native value and survives as one
    assert columns["seen"]["min"] == datetime.datetime(2026, 8, 21, 12, 0, 0)
    assert columns["ref"]["min"] == str(identifier)


def test_an_unrecognised_bound_is_rendered_not_dropped():
    from opteryx_catalog.stub_projection import firestore_safe

    class Opaque:
        def __str__(self):
            return "opaque-bound"

    assert firestore_safe(Opaque()) == "opaque-bound"
    assert firestore_safe(None) is None
    assert firestore_safe(True) is True


# -- "I listed it but could not look inside it this time" --------------------


def test_retain_keeps_stored_detail_when_the_caller_could_not_collect(db):
    from opteryx_catalog.stub_projection import RETAIN_DETAIL

    sync_stub_datasets(
        db, WS, [("interop", "people", {"schema": COLUMNS, "statistics": STATS})]
    )
    result = sync_stub_datasets(db, WS, [("interop", "people", {RETAIN_DETAIL: True})])
    # nothing changed, so nothing was written - and crucially the schema did
    # not evaporate because one table failed to load during a good refresh
    assert (result.added, result.removed, result.updated) == (0, 0, 0)
    stored = _stubs(db)[("interop", "people")]
    assert stored["schema"] == COLUMNS
    assert stored["statistics"] == STATS


def test_retain_on_a_table_with_nothing_stored_is_just_a_name(db):
    from opteryx_catalog.stub_projection import RETAIN_DETAIL

    result = sync_stub_datasets(db, WS, [("interop", "people", {RETAIN_DETAIL: True})])
    assert result.added == 1
    assert "schema" not in _stubs(db)[("interop", "people")]


def test_retain_still_removes_a_table_that_left_the_listing(db):
    from opteryx_catalog.stub_projection import RETAIN_DETAIL

    sync_stub_datasets(db, WS, [("interop", "people", {"schema": COLUMNS})])
    result = sync_stub_datasets(db, WS, [("interop", "orders", {RETAIN_DETAIL: True})])
    assert (result.added, result.removed) == (1, 1)
    assert set(_stubs(db)) == {("interop", "orders")}


# -- the structural facts a catalog's own metadata already knows -------------


SORT_ORDERS = [{"order-id": 1, "fields": [{"source-id": 2, "name": "country", "direction": "desc"}]}]


def test_structural_facts_land_in_the_fields_odata_reads(db):
    sync_stub_datasets(
        db,
        WS,
        [
            (
                "interop",
                "people",
                {
                    "schema": COLUMNS,
                    "timestamp_ms": 1_755_000_000_000,
                    "sort_orders": SORT_ORDERS,
                    "partition_columns": ["country"],
                },
            )
        ],
    )
    stored = _stubs(db)[("interop", "people")]
    # kebab-case, because that is what the readers key off: odata's $metadata
    # reads `timestamp-ms`, its service document reads `sort-orders`
    assert stored["timestamp-ms"] == 1_755_000_000_000
    assert stored["sort-orders"] == SORT_ORDERS
    assert stored["partition-columns"] == ["country"]


def test_a_typo_in_detail_is_refused_rather_than_ignored(db):
    # a silently-dropped field is indistinguishable from "the catalog did not
    # say", which is the failure this rejection exists to prevent
    with pytest.raises(ValueError, match="unknown key"):
        sync_stub_datasets(db, WS, [("interop", "people", {"sort_order": SORT_ORDERS})])
    assert _stubs(db) == {}


def test_structural_facts_are_retained_when_a_table_will_not_open(db):
    from opteryx_catalog.stub_projection import RETAIN_DETAIL

    sync_stub_datasets(
        db,
        WS,
        [("interop", "people", {"sort_orders": SORT_ORDERS, "timestamp_ms": 1_755_000_000_000})],
    )
    result = sync_stub_datasets(db, WS, [("interop", "people", {RETAIN_DETAIL: True})])
    assert result.updated == 0
    stored = _stubs(db)[("interop", "people")]
    assert stored["sort-orders"] == SORT_ORDERS
    assert stored["timestamp-ms"] == 1_755_000_000_000


def test_a_table_that_lost_its_sort_order_loses_the_field(db):
    sync_stub_datasets(db, WS, [("interop", "people", {"sort_orders": SORT_ORDERS})])
    result = sync_stub_datasets(db, WS, [("interop", "people", {"schema": COLUMNS})])
    assert result.updated == 1
    assert "sort-orders" not in _stubs(db)[("interop", "people")]
