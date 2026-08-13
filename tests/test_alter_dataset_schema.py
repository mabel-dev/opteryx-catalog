"""`alter_dataset_schema` - the catalog half of ALTER TABLE ... ADD/DROP/RENAME/
ALTER COLUMN.

The property under test throughout is FIELD-ID STABILITY. Manifest statistics
are keyed by field id, not by name or position, so a surviving column that comes
out the other side with a different id has silently acquired another column's
min/max. Names and order are the visible part; the ids are the part that makes
the statistics keep meaning what they say.

Firestore is stood in for by the same minimal doubles `test_cluster_by.py` uses -
`update_dataset_sort_order` and this method touch the same two documents.
"""

from __future__ import annotations

import json

import pytest

from opteryx_catalog.exceptions import DatasetNotFound
from opteryx_catalog.opteryx_catalog import OpteryxCatalog


class _Doc:
    def __init__(self, data=None, exists=True):
        self.exists = exists
        self._data = data or {}

    def to_dict(self):
        return self._data


class _Collection:
    """Records documents written to it, so a newly written schema can be read
    back the way the dataset document's `current-schema-id` would find it."""

    def __init__(self, docs=None):
        self._docs = docs or {}
        self.written = {}

    def document(self, doc_id):
        if doc_id not in self._docs:
            self._docs[doc_id] = _DocRef(exists=False)
        return self._docs[doc_id]

    def stream(self):
        return [ref.get() for ref in self._docs.values() if ref.get().exists]


class _DocRef:
    def __init__(self, data=None, exists=True, subcollections=None):
        self._doc = _Doc(data, exists)
        self._subcollections = subcollections or {}
        self.updated = None

    def get(self):
        return self._doc

    def set(self, data):
        self._doc = _Doc(data, exists=True)

    def update(self, data):
        self.updated = data
        self._doc._data = {**self._doc._data, **data}

    def collection(self, name):
        if name not in self._subcollections:
            self._subcollections[name] = _Collection()
        return self._subcollections[name]


_INITIAL = [
    {"id": 1, "name": "id", "type": "INTEGER", "element-type": None,
     "scale": None, "precision": None, "expectation-policies": [], "annotations": []},
    {"id": 2, "name": "name", "type": "VARCHAR", "element-type": None,
     "scale": None, "precision": None, "expectation-policies": [], "annotations": []},
    {"id": 3, "name": "region", "type": "VARCHAR", "element-type": None,
     "scale": None, "precision": None, "expectation-policies": [], "annotations": []},
]


def _catalog(columns=None, next_field_id=4):
    columns = [dict(c) for c in (columns if columns is not None else _INITIAL)]
    schemas = _Collection(docs={"sc1": _DocRef(data={"columns": columns})})
    dataset_data = {"current-schema-id": "sc1"}
    if next_field_id is not None:
        dataset_data["next-field-id"] = next_field_id
    dataset_ref = _DocRef(data=dataset_data, subcollections={"schemas": schemas})

    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog._dataset_doc_ref = lambda c, n: dataset_ref
    return catalog, dataset_ref, schemas


def _new_schema(dataset_ref, schemas, sid):
    assert dataset_ref.updated["current-schema-id"] == sid
    return schemas.document(sid).get().to_dict()["columns"]


def _emitted(capsys):
    out = capsys.readouterr().out
    return [json.loads(line) for line in out.splitlines() if line.strip()]


# --- dropping ----------------------------------------------------------------


def test_drop_removes_the_column_and_leaves_the_others_ids_alone(capsys):
    catalog, dataset_ref, schemas = _catalog()

    sid = catalog.alter_dataset_schema("coll.tbl", drop=["name"], author="alice")

    columns = _new_schema(dataset_ref, schemas, sid)
    assert [c["name"] for c in columns] == ["id", "region"]
    # region keeps id 3 - it does NOT slide down to 2 to fill the gap. Manifest
    # statistics are keyed by id, so renumbering would hand region the stats
    # that belong to the dropped column.
    assert [c["id"] for c in columns] == [1, 3]


def test_dropping_every_column_is_refused(capsys):
    catalog, _dataset_ref, _schemas = _catalog()

    with pytest.raises(ValueError, match="no relation"):
        catalog.alter_dataset_schema(
            "coll.tbl", drop=["id", "name", "region"], author="alice"
        )


# --- renaming ----------------------------------------------------------------


def test_rename_changes_the_name_and_nothing_else(capsys):
    catalog, dataset_ref, schemas = _catalog()

    sid = catalog.alter_dataset_schema(
        "coll.tbl", rename={"region": "territory"}, author="alice"
    )

    columns = _new_schema(dataset_ref, schemas, sid)
    renamed = [c for c in columns if c["name"] == "territory"][0]
    assert renamed["id"] == 3, "a rename must not re-identify the column"
    assert renamed["type"] == "VARCHAR"
    assert [c["name"] for c in columns] == ["id", "name", "territory"]


def test_rename_onto_an_existing_name_is_refused(capsys):
    catalog, _dataset_ref, _schemas = _catalog()

    with pytest.raises(ValueError, match="two columns called 'name'"):
        catalog.alter_dataset_schema("coll.tbl", rename={"region": "name"}, author="alice")


def test_rename_onto_a_name_being_dropped_is_allowed(capsys):
    """The collision check is against what SURVIVES, not against what the schema
    happened to contain on the way in."""
    catalog, dataset_ref, schemas = _catalog()

    sid = catalog.alter_dataset_schema(
        "coll.tbl", drop=["name"], rename={"region": "name"}, author="alice"
    )

    columns = _new_schema(dataset_ref, schemas, sid)
    assert [(c["name"], c["id"]) for c in columns] == [("id", 1), ("name", 3)]


# --- adding ------------------------------------------------------------------


def test_add_appends_with_a_fresh_field_id(capsys):
    catalog, dataset_ref, schemas = _catalog()

    sid = catalog.alter_dataset_schema(
        "coll.tbl", add=[{"name": "added", "type": "INTEGER"}], author="alice"
    )

    columns = _new_schema(dataset_ref, schemas, sid)
    assert [c["name"] for c in columns] == ["id", "name", "region", "added"]
    assert columns[-1]["id"] == 4
    assert dataset_ref.updated["next-field-id"] == 5


def test_added_columns_never_reuse_a_dropped_columns_id(capsys):
    """A reused id would attach the dropped column's manifest statistics to the
    new column - stats for values that were never in it."""
    catalog, dataset_ref, schemas = _catalog()

    sid = catalog.alter_dataset_schema(
        "coll.tbl", drop=["name"], add=[{"name": "fresh", "type": "VARCHAR"}], author="alice"
    )

    columns = _new_schema(dataset_ref, schemas, sid)
    assert [(c["name"], c["id"]) for c in columns] == [("id", 1), ("region", 3), ("fresh", 4)]


def test_add_fills_in_the_stored_column_shape(capsys):
    catalog, dataset_ref, schemas = _catalog()

    sid = catalog.alter_dataset_schema(
        "coll.tbl",
        add=[{"name": "amount", "type": "DECIMAL", "precision": 10, "scale": 2}],
        author="alice",
    )

    added = _new_schema(dataset_ref, schemas, sid)[-1]
    assert added["type"] == "DECIMAL"
    assert (added["precision"], added["scale"]) == (10, 2)
    assert added["element-type"] is None
    assert added["expectation-policies"] == [] and added["annotations"] == []


def test_add_of_an_existing_name_is_refused(capsys):
    catalog, _dataset_ref, _schemas = _catalog()

    with pytest.raises(ValueError, match="already has a column called 'name'"):
        catalog.alter_dataset_schema(
            "coll.tbl", add=[{"name": "name", "type": "VARCHAR"}], author="alice"
        )


def test_next_field_id_is_derived_when_the_dataset_predates_it(capsys):
    """Datasets written before `next-field-id` existed still have ids on their
    columns; the next one has to clear the highest of them, not restart at 1."""
    catalog, dataset_ref, schemas = _catalog(next_field_id=None)

    sid = catalog.alter_dataset_schema(
        "coll.tbl", add=[{"name": "added", "type": "INTEGER"}], author="alice"
    )

    assert _new_schema(dataset_ref, schemas, sid)[-1]["id"] == 4


# --- retyping ----------------------------------------------------------------


def test_retype_rewrites_the_type_in_place(capsys):
    catalog, dataset_ref, schemas = _catalog()

    sid = catalog.alter_dataset_schema(
        "coll.tbl", retype={"id": {"type": "INT64"}}, author="alice"
    )

    columns = _new_schema(dataset_ref, schemas, sid)
    assert columns[0]["name"] == "id" and columns[0]["type"] == "INT64"
    assert columns[0]["id"] == 1, "a retype must not re-identify the column"
    assert [c["name"] for c in columns] == ["id", "name", "region"]


def test_retype_carries_precision_and_scale(capsys):
    catalog, dataset_ref, schemas = _catalog()

    sid = catalog.alter_dataset_schema(
        "coll.tbl",
        retype={"id": {"type": "DECIMAL", "precision": 38, "scale": 18}},
        author="alice",
    )

    column = _new_schema(dataset_ref, schemas, sid)[0]
    assert (column["precision"], column["scale"]) == (38, 18)


# --- composition, refusals, audit --------------------------------------------


def test_all_four_operations_compose_in_one_call(capsys):
    catalog, dataset_ref, schemas = _catalog()

    sid = catalog.alter_dataset_schema(
        "coll.tbl",
        drop=["name"],
        rename={"region": "territory"},
        retype={"id": {"type": "INT64"}},
        add=[{"name": "added", "type": "VARCHAR"}],
        author="alice",
    )

    columns = _new_schema(dataset_ref, schemas, sid)
    assert [(c["name"], c["id"]) for c in columns] == [
        ("id", 1),
        ("territory", 3),
        ("added", 4),
    ]
    assert columns[0]["type"] == "INT64"


def test_unknown_columns_are_refused_before_anything_is_written(capsys):
    """A schema document written for a partially-applied edit is not a state to
    reach - the dataset would point at a shape nobody asked for."""
    catalog, dataset_ref, schemas = _catalog()

    with pytest.raises(ValueError, match="no column named 'nope'"):
        catalog.alter_dataset_schema(
            "coll.tbl", drop=["name"], rename={"nope": "other"}, author="alice"
        )

    assert dataset_ref.updated is None, "the dataset was updated despite the refusal"
    assert schemas.document("sc1").get().to_dict()["columns"] == _INITIAL


def test_a_call_with_no_changes_is_refused(capsys):
    catalog, _dataset_ref, _schemas = _catalog()

    with pytest.raises(ValueError, match="no changes"):
        catalog.alter_dataset_schema("coll.tbl", author="alice")


def test_a_missing_dataset_is_reported_as_such(capsys):
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog._dataset_doc_ref = lambda c, n: _DocRef(exists=False)

    with pytest.raises(DatasetNotFound):
        catalog.alter_dataset_schema("coll.tbl", drop=["name"], author="alice")


def test_the_previous_schema_document_is_left_intact(capsys):
    """Snapshots taken before this edit still resolve their own schema id, so
    the document they point at must not be edited in place."""
    catalog, dataset_ref, schemas = _catalog()

    catalog.alter_dataset_schema("coll.tbl", drop=["name"], author="alice")

    assert schemas.document("sc1").get().to_dict()["columns"] == _INITIAL


def test_emits_an_audit_record(capsys):
    catalog, _dataset_ref, _schemas = _catalog()

    catalog.alter_dataset_schema(
        "coll.tbl",
        drop=["name"],
        rename={"region": "territory"},
        add=[{"name": "added", "type": "VARCHAR"}],
        retype={"id": {"type": "INT64"}},
        author="alice",
    )

    record = [r for r in _emitted(capsys) if r["action"] == "alter_schema"][0]
    assert record["resource_type"] == "dataset"
    assert (record["collection"], record["resource"]) == ("coll", "tbl")
    assert record["author"] == "alice"
    assert record["detail"]["dropped"] == ["name"]
    assert record["detail"]["added"] == ["added"]
    assert record["detail"]["renamed"] == {"region": "territory"}
    assert record["detail"]["retyped"] == ["id"]


def test_an_unrecognised_stored_field_rides_through_untouched(capsys):
    """Column dicts are copied, not rebuilt, so a field this method does not
    know about survives a rename instead of being silently dropped."""
    columns = [dict(c) for c in _INITIAL]
    columns[2]["expectation-policies"] = [{"kind": "not-null"}]
    columns[2]["some-future-field"] = "keep me"
    catalog, dataset_ref, schemas = _catalog(columns=columns)

    sid = catalog.alter_dataset_schema(
        "coll.tbl", rename={"region": "territory"}, author="alice"
    )

    renamed = [c for c in _new_schema(dataset_ref, schemas, sid) if c["id"] == 3][0]
    assert renamed["expectation-policies"] == [{"kind": "not-null"}]
    assert renamed["some-future-field"] == "keep me"


def test_writing_a_schema_requires_an_author(capsys):
    catalog, _dataset_ref, _schemas = _catalog()

    with pytest.raises(ValueError, match="author"):
        catalog.alter_dataset_schema("coll.tbl", drop=["name"], author=None)
