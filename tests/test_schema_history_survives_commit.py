"""Committing a dataset must never delete a schema document.

`save_dataset_metadata` used to reconcile the `schemas` subcollection against
`metadata.schemas`, deleting anything not in it. That set is not the complete
one: the default `load_dataset(load_history=False)` - which every write path
uses - loads the CURRENT schema only. So any commit deleted every older schema
document.

Every snapshot records the schema id it was written under, so a deleted schema
document makes an `AS OF` query against an older snapshot resolve a schema that
no longer exists - a time-travel failure caused by an INSERT that had nothing to
do with schemas. It went unnoticed because nothing created a second schema
document until column DDL (ALTER TABLE ... ADD/DROP/RENAME COLUMN) did.

The snapshot upsert in the same method already had this rule and says so; these
tests hold the schema half to it.
"""

from __future__ import annotations

from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.opteryx_catalog import OpteryxCatalog


class _Doc:
    def __init__(self, data=None, exists=True):
        self.exists = exists
        self._data = data or {}

    def to_dict(self):
        return self._data


class _DocRef:
    def __init__(self, doc_id, data=None, exists=True):
        self.id = doc_id
        self._doc = _Doc(data, exists)
        self.deleted = False
        self._subcollections = {}

    def get(self):
        return self._doc

    def set(self, data):
        self._doc = _Doc(data, exists=True)

    def update(self, data):
        self._doc._data = {**self._doc._data, **data}

    def delete(self):
        self.deleted = True

    def collection(self, name):
        if name not in self._subcollections:
            self._subcollections[name] = _Collection()
        return self._subcollections[name]


class _Collection:
    def __init__(self, docs=None):
        self._docs = docs or {}

    def document(self, doc_id):
        if doc_id not in self._docs:
            self._docs[doc_id] = _DocRef(doc_id, exists=False)
        return self._docs[doc_id]

    def stream(self):
        return [r.get() for r in self._docs.values() if r.get().exists]

    def refs(self):
        return self._docs


def _schema_entry(sid, seq, columns):
    return {
        "schema_id": sid,
        "columns": columns,
        "timestamp-ms": seq * 1000,
        "author": "alice",
        "sequence-number": seq,
    }


def _catalog_with_schema_history():
    """A dataset carrying three schema generations, as a table that has been
    altered twice would."""
    dataset_ref = _DocRef("tbl", data={})
    schemas = dataset_ref.collection("schemas")
    for sid, seq in (("sc1", 1), ("sc2", 2), ("sc3", 3)):
        schemas.document(sid).set({"columns": [], "sequence-number": seq})

    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog._dataset_doc_ref = lambda c, n: dataset_ref
    catalog._snapshots_collection = lambda c, n: _Collection()
    return catalog, dataset_ref, schemas


def _metadata(schemas):
    meta = DatasetMetadata(
        dataset_identifier="coll.tbl", location="mem://ws/tbl", schema=None, properties={}
    )
    meta.schemas = schemas
    meta.current_schema_id = schemas[-1]["schema_id"] if schemas else None
    return meta


def test_a_commit_carrying_only_the_current_schema_deletes_nothing():
    """The exact shape a default-loaded dataset commits in: metadata.schemas
    holds one entry, the subcollection holds three."""
    catalog, _dataset_ref, schemas = _catalog_with_schema_history()

    catalog.save_dataset_metadata("coll.tbl", _metadata([_schema_entry("sc3", 3, [])]))

    assert not any(ref.deleted for ref in schemas.refs().values())
    assert set(schemas.refs()) == {"sc1", "sc2", "sc3"}


def test_a_commit_carrying_no_schemas_at_all_deletes_nothing():
    """A dataset loaded without resolving its schema still commits; that must
    not be read as `there are no schemas`."""
    catalog, _dataset_ref, schemas = _catalog_with_schema_history()

    catalog.save_dataset_metadata("coll.tbl", _metadata([]))

    assert not any(ref.deleted for ref in schemas.refs().values())
    assert set(schemas.refs()) == {"sc1", "sc2", "sc3"}


def test_a_schema_written_outside_the_metadata_survives_the_next_commit():
    """`alter_dataset_schema` writes its document straight to Firestore and
    repoints current-schema-id; the commit that follows must not delete it. This
    is the exact sequence ALTER TABLE ... ADD COLUMN runs."""
    catalog, dataset_ref, schemas = _catalog_with_schema_history()
    sid = catalog._write_schema_columns("coll", "tbl", [{"id": 1, "name": "c"}], "alice")
    dataset_ref.update({"current-schema-id": sid})

    # The dataset in hand was loaded before that write, so its metadata does not
    # know about the new schema.
    catalog.save_dataset_metadata("coll.tbl", _metadata([_schema_entry("sc3", 3, [])]))

    assert schemas.document(sid).get().exists, "the new schema was deleted by the commit"
    assert schemas.document(sid).get().to_dict()["columns"] == [{"id": 1, "name": "c"}]


def test_schemas_present_in_the_metadata_are_still_written():
    """Not deleting must not become not writing - a new or edited schema entry
    carried in the metadata still has to be persisted."""
    catalog, _dataset_ref, schemas = _catalog_with_schema_history()
    columns = [{"id": 1, "name": "id"}, {"id": 2, "name": "added"}]

    catalog.save_dataset_metadata("coll.tbl", _metadata([_schema_entry("sc4", 4, columns)]))

    written = schemas.document("sc4").get()
    assert written.exists
    assert written.to_dict()["columns"] == columns
    assert written.to_dict()["sequence-number"] == 4
