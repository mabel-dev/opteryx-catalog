"""Relationships declared on a dataset, and never enforced.

`ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... NOT ENFORCED` records that
two columns hold corresponding values. Nothing validates it, now or later: a
write that breaks the relationship succeeds and no query plan consults it.

Stored as a subcollection under the dataset the constraint is declared ON, the
same shape as triggers. That is what answers the dominant read -- "what relates
to THIS dataset" -- as a keyed read rather than a scan, and it is why a dropped
dataset takes its relationships with it for free.
"""

from __future__ import annotations

import re

import pytest

from opteryx_catalog.exceptions import ConstraintNotFound
from opteryx_catalog.exceptions import DatasetNotFound
from opteryx_catalog.opteryx_catalog import RELATIONSHIPS_SUBCOLLECTION
from opteryx_catalog.opteryx_catalog import OpteryxCatalog


class _Doc:
    def __init__(self, id_, data, exists):
        self.id = id_
        self.exists = exists
        self._data = dict(data)

    def to_dict(self):
        return dict(self._data)


class _DocRef:
    def __init__(self, id_, registry):
        self.id = id_
        self._registry = registry
        self._data = {}
        self._exists = False
        self._subcollections = {}

    def get(self):
        return _Doc(self.id, self._data, self._exists)

    def set(self, data, merge=False):
        self._data = {**self._data, **data} if merge else dict(data)
        self._exists = True

    def delete(self):
        self._data = {}
        self._exists = False

    def collection(self, name):
        if name not in self._subcollections:
            self._subcollections[name] = _Collection(name, self._registry)
        return self._subcollections[name]


class _Collection:
    """Registers itself by name so a collection group query can find it -
    which is the whole point of the reverse lookup and cannot be tested
    against a fake that only models parent-child reads."""

    def __init__(self, name, registry):
        self._name = name
        self._registry = registry
        self._docs = {}
        registry.setdefault(name, []).append(self)

    def document(self, doc_id):
        if doc_id not in self._docs:
            self._docs[doc_id] = _DocRef(doc_id, self._registry)
        return self._docs[doc_id]

    def stream(self):
        return [ref.get() for ref in self._docs.values() if ref._exists]


# An unquoted Firestore property path must match this; anything else has to be
# backtick-quoted. Every field this catalog stores is hyphenated, so a query
# against one is rejected unless it is quoted -- which real Firestore enforces
# and a fake that just does dict lookups does not. Reproduced here because it
# was found in production, by a write, after the tests were green.
_UNQUOTED_PROPERTY_PATH = re.compile(r"^[a-zA-Z_][a-zA-Z_0-9]*$")


class _Query:
    """A collection group query: every document in every collection of that
    name, narrowed by equality filters."""

    def __init__(self, docs, filters=()):
        self._docs = docs
        self._filters = list(filters)

    def where(self, filter=None):
        path = filter.field_path
        if path.startswith("`"):
            if not path.endswith("`") or len(path) < 3:
                raise ValueError(f"Invalid quoted property path {path!r}")
        elif not _UNQUOTED_PROPERTY_PATH.match(path):
            raise ValueError(
                f'Invalid property path "{path}". Unquoted property paths must match '
                "regex ([a-zA-Z_][a-zA-Z_0-9]*)"
            )
        return _Query(self._docs, self._filters + [filter])

    def stream(self):
        out = []
        for doc in self._docs:
            data = doc.to_dict()
            if all(data.get(f.field_path.strip("`")) == f.value for f in self._filters):
                out.append(doc)
        return out


class _FirestoreClient:
    def __init__(self, registry):
        self._registry = registry
        self._collections = {}

    def collection(self, name):
        if name not in self._collections:
            self._collections[name] = _Collection(name, self._registry)
        return self._collections[name]

    def collection_group(self, name):
        docs = []
        for collection in self._registry.get(name, []):
            docs.extend(collection.stream())
        return _Query(docs)


def _catalog(workspace="ws"):
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = workspace
    catalog.io = None
    catalog._snapshot_cache = {}
    catalog._schema_cache = {}
    registry = {}
    catalog._registry = registry
    catalog.firestore_client = _FirestoreClient(registry)
    catalog._catalog_ref = catalog.firestore_client.collection(workspace)
    catalog._datasets_collection = lambda coll: catalog._catalog_ref.document(coll).collection(
        "datasets"
    )
    catalog._dataset_doc_ref = lambda c, n: catalog._datasets_collection(c).document(n)
    catalog._snapshots_collection = lambda c, n: catalog._dataset_doc_ref(c, n).collection(
        "snapshots"
    )
    return catalog


def _add_dataset(catalog, identifier):
    coll, name = identifier.split(".", 1)
    ref = catalog._dataset_doc_ref(coll, name)
    ref.set(
        {
            "name": name,
            "collection": coll,
            "workspace": catalog.workspace,
            "location": f"gs://bucket/{catalog.workspace}/{coll}/{name}",
        }
    )
    return ref


def _seeded():
    catalog = _catalog()
    _add_dataset(catalog, "helpdesk.tickets")
    _add_dataset(catalog, "crm.customers")
    return catalog


def _declare(catalog, **overrides):
    kwargs = {
        "dataset_identifier": "helpdesk.tickets",
        "constraint_name": "tickets_customer_fk",
        "column": "customer_ref",
        "references_dataset": "crm.customers",
        "references_column": "id",
        "cardinality": "many_to_one",
        "author": "olive",
    }
    kwargs.update(overrides)
    return catalog.declare_relationship(**kwargs)


def test_a_declaration_is_stored_under_the_dataset_it_is_declared_on():
    catalog = _seeded()
    _declare(catalog)

    rows = catalog.list_relationships("helpdesk.tickets")
    assert len(rows) == 1
    row = rows[0]
    assert row["name"] == "tickets_customer_fk"
    assert row["kind"] == "maps"
    assert row["column"] == "customer_ref"
    assert row["origin"] == "asserted"
    assert row["status"] == "active"
    assert row["asserted-by"] == "olive"
    # The far end is stored SPLIT, never as a dotted string.
    assert row["references-workspace"] == "ws"
    assert row["references-collection"] == "crm"
    assert row["references-dataset"] == "customers"
    assert row["references-column"] == "id"


def test_the_constraint_name_is_the_document_id():
    """Uniqueness is Firestore's, not ours -- no read-then-write race to lose."""
    catalog = _seeded()
    _declare(catalog)

    subcollection = catalog._dataset_doc_ref("helpdesk", "tickets").collection(
        RELATIONSHIPS_SUBCOLLECTION
    )
    assert [doc.id for doc in subcollection.stream()] == ["tickets_customer_fk"]


def test_a_duplicate_constraint_name_is_refused():
    catalog = _seeded()
    _declare(catalog)

    with pytest.raises(ValueError, match="already exists"):
        _declare(catalog)


def test_both_ends_must_exist():
    catalog = _seeded()

    with pytest.raises(DatasetNotFound):
        _declare(catalog, dataset_identifier="helpdesk.missing")

    with pytest.raises(DatasetNotFound):
        _declare(catalog, references_dataset="crm.missing")


def test_an_author_is_required():
    catalog = _seeded()

    with pytest.raises(ValueError, match="author"):
        _declare(catalog, author=None)


def test_only_the_near_side_is_listed():
    """`list_relationships` answers "what does this dataset point at", not
    "what points at it" -- there is no mirrored row to make the reverse a
    keyed read, by design."""
    catalog = _seeded()
    _declare(catalog)

    assert catalog.list_relationships("crm.customers") == []


def test_dropping_removes_it_by_name():
    catalog = _seeded()
    _declare(catalog)

    assert catalog.drop_relationship("helpdesk.tickets", "tickets_customer_fk", author="olive")
    assert catalog.list_relationships("helpdesk.tickets") == []


def test_dropping_something_absent_needs_missing_ok():
    """A drop that silently matched nothing would let a typo read as success."""
    catalog = _seeded()

    with pytest.raises(ConstraintNotFound):
        catalog.drop_relationship("helpdesk.tickets", "no_such_fk", author="olive")

    assert not catalog.drop_relationship(
        "helpdesk.tickets", "no_such_fk", author="olive", missing_ok=True
    )


def test_a_dropped_dataset_takes_its_relationships_with_it():
    """Firestore does not cascade, so this has to be explicit or the documents
    survive their parent, addressable and unreachable."""
    catalog = _seeded()
    _declare(catalog)

    doc_ref = catalog._dataset_doc_ref("helpdesk", "tickets")
    catalog._delete_subcollection(doc_ref.collection(RELATIONSHIPS_SUBCOLLECTION))

    assert catalog.list_relationships("helpdesk.tickets") == []


# --- the reverse lookup ------------------------------------------------------


def test_the_near_end_is_stored_on_the_row():
    """Denormalised from the path so a collection group result can say what it
    is attached to without walking four parents back up."""
    catalog = _seeded()
    _declare(catalog)

    row = catalog.list_relationships("helpdesk.tickets")[0]
    assert row["workspace"] == "ws"
    assert row["collection"] == "helpdesk"
    assert row["dataset"] == "tickets"


def test_what_points_at_a_dataset_is_a_collection_group_query():
    catalog = _seeded()
    _add_dataset(catalog, "billing.invoices")
    _declare(catalog)
    _declare(
        catalog,
        dataset_identifier="billing.invoices",
        constraint_name="invoice_customer_fk",
        column="customer",
    )

    inbound = catalog.find_relationships_to("crm.customers")

    assert {(row["collection"], row["dataset"], row["name"]) for row in inbound} == {
        ("helpdesk", "tickets", "tickets_customer_fk"),
        ("billing", "invoices", "invoice_customer_fk"),
    }


def test_the_reverse_lookup_finds_nothing_for_an_unreferenced_dataset():
    catalog = _seeded()
    _declare(catalog)

    assert catalog.find_relationships_to("helpdesk.tickets") == []


def test_the_reverse_lookup_does_not_cross_workspaces():
    """A collection group query spans every workspace in the database, so the
    workspace filter is the only thing keeping another tenant's relationships
    out of this answer."""
    catalog = _seeded()
    _declare(catalog)

    # A second workspace sharing the same Firestore, with a dataset of the same
    # collection and name being referenced.
    other = _catalog(workspace="other")
    other.firestore_client = catalog.firestore_client
    other._registry = catalog._registry
    other._catalog_ref = catalog.firestore_client.collection("other")
    other._datasets_collection = lambda coll: other._catalog_ref.document(coll).collection(
        "datasets"
    )
    other._dataset_doc_ref = lambda c, n: other._datasets_collection(c).document(n)
    _add_dataset(other, "helpdesk.tickets")
    _add_dataset(other, "crm.customers")
    _declare(other)

    ours = catalog.find_relationships_to("crm.customers")
    assert [row["workspace"] for row in ours] == ["ws"]

    theirs = other.find_relationships_to("crm.customers")
    assert [row["workspace"] for row in theirs] == ["other"]
