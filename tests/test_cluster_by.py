from __future__ import annotations

import json

from opteryx_catalog.exceptions import DatasetNotFound
from opteryx_catalog.opteryx_catalog import OpteryxCatalog


class _Doc:
    """A stand-in for a Firestore document snapshot."""

    def __init__(self, data=None, exists=True):
        self.exists = exists
        self._data = data or {}

    def to_dict(self):
        return self._data


class _Collection:
    """A stand-in for a Firestore CollectionReference, keyed by document id."""

    def __init__(self, docs=None):
        self._docs = docs or {}

    def document(self, doc_id):
        return self._docs.get(doc_id, _DocRef(exists=False))


class _DocRef:
    """A stand-in for a Firestore DocumentReference. Only `update` (not `set`)
    is used by ``update_dataset_sort_order``, so that's the only write op that
    records what was written."""

    def __init__(self, data=None, exists=True, subcollections=None):
        self._doc = _Doc(data, exists)
        self._subcollections = subcollections or {}
        self.updated = None

    def get(self):
        return self._doc

    def update(self, data):
        self.updated = data
        # Mirror Firestore's merge-update semantics on our stand-in so a
        # second update() call in the same test sees prior fields too.
        self._doc._data = {**self._doc._data, **data}

    def collection(self, name):
        return self._subcollections.get(name, _Collection())


def _catalog_with_dataset(columns=("id", "name", "region"), sort_orders=None):
    """A catalog whose only dataset is coll.tbl, with a two/three-column schema."""
    schema_columns = [{"id": i + 1, "name": c} for i, c in enumerate(columns)]
    schemas = _Collection(docs={"sc1": _DocRef(data={"columns": schema_columns})})

    dataset_data = {"current-schema-id": "sc1"}
    if sort_orders is not None:
        dataset_data["sort-orders"] = sort_orders
    dataset_ref = _DocRef(data=dataset_data, subcollections={"schemas": schemas})

    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog._dataset_doc_ref = lambda c, n: dataset_ref
    return catalog, dataset_ref


def _emitted(capsys):
    out = capsys.readouterr().out
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def test_writes_iceberg_style_sort_order(capsys):
    catalog, dataset_ref = _catalog_with_dataset()

    catalog.update_dataset_sort_order("coll.tbl", ["name"], author="alice")

    assert dataset_ref.updated["sort-orders"] == [
        {"order-id": 1, "fields": [{"name": "name", "direction": "asc"}]}
    ]


def test_multi_column_preserves_order(capsys):
    catalog, dataset_ref = _catalog_with_dataset()

    catalog.update_dataset_sort_order("coll.tbl", ["region", "name"], author="alice")

    fields = dataset_ref.updated["sort-orders"][0]["fields"]
    assert [f["name"] for f in fields] == ["region", "name"]


def test_replaces_not_appends_previous_sort_order(capsys):
    previous = [{"order-id": 1, "fields": [{"name": "id", "direction": "asc"}]}]
    catalog, dataset_ref = _catalog_with_dataset(sort_orders=previous)

    catalog.update_dataset_sort_order("coll.tbl", ["name"], author="alice")

    assert dataset_ref.updated["sort-orders"] == [
        {"order-id": 1, "fields": [{"name": "name", "direction": "asc"}]}
    ]


def test_emits_audit_record(capsys):
    catalog, _dataset_ref = _catalog_with_dataset()

    catalog.update_dataset_sort_order("coll.tbl", ["name"], author="alice")

    record = _emitted(capsys)[0]
    assert record["action"] == "update_sort_order"
    assert record["resource_type"] == "dataset"
    assert record["collection"] == "coll"
    assert record["resource"] == "tbl"
    assert record["author"] == "alice"
    assert record["detail"]["columns"] == ["name"]


def test_unattributed_change_is_visibly_unattributed(capsys):
    catalog, _dataset_ref = _catalog_with_dataset()

    catalog.update_dataset_sort_order("coll.tbl", ["name"])

    assert _emitted(capsys)[0]["author"] is None


def test_empty_columns_rejected():
    catalog, dataset_ref = _catalog_with_dataset()

    try:
        catalog.update_dataset_sort_order("coll.tbl", [])
        assert False, "expected ValueError"
    except ValueError:
        pass

    assert dataset_ref.updated is None


def test_unknown_column_rejected():
    catalog, dataset_ref = _catalog_with_dataset(columns=("id", "name"))

    try:
        catalog.update_dataset_sort_order("coll.tbl", ["not_a_column"])
        assert False, "expected ValueError"
    except ValueError:
        pass

    assert dataset_ref.updated is None


def test_missing_dataset_raises_dataset_not_found():
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog._dataset_doc_ref = lambda c, n: _DocRef(exists=False)

    try:
        catalog.update_dataset_sort_order("coll.tbl", ["name"])
        assert False, "expected DatasetNotFound"
    except DatasetNotFound:
        pass
