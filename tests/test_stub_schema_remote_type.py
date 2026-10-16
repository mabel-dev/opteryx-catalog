"""A projected relation's stored `remote-type` survives the load.

The workspace catalog refresh stores each column of an externally-bound relation
(PostgreSQL) with `remote-type` - the column's type OID on the source server.
The engine's PostgreSQL reader builds its schema from this record and chooses
the wire decoder by that OID; it refuses a column without one, because several
server types bind to one engine type and guessing would decode the wrong thing.

0.4.181 began building the stub's inline schema into a `RelationSchema` where it
enters (`_build_dataset`). That conversion kept the generic column fields and
dropped `remote-type`, so every PostgreSQL relation lost the one field its
reader cannot do without (2026-10-15, every PostgreSQL SELECT in production).

Tested against the REAL `OpteryxCatalog._build_dataset` - a fake would just
restate the assumption.
"""

from opteryx_catalog.catalog.dataset import RelationSchema
from opteryx_catalog.catalog.dataset import relation_schema_from_stored
from opteryx_catalog.stub_projection import STUB_MARKER


def _catalog_shell():
    """An OpteryxCatalog without __init__ (which would talk to Firestore),
    carrying just enough state for the non-history `_build_dataset` of a stub -
    which names no snapshot and no schema id, so reads nothing further."""
    from opteryx_catalog import OpteryxCatalog

    class _Coll:
        def document(self, _name):  # pragma: no cover - a stub names no id to fetch
            raise AssertionError("a stub has no snapshot or schema document to read")

    class _DocRef:
        def collection(self, _name):
            return _Coll()

    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "health"
    catalog.gcs_bucket = "bucket"
    catalog.io = None
    catalog._snapshot_cache = {}
    catalog._schema_cache = {}
    catalog._dataset_doc_ref = lambda c, d: _DocRef()
    return catalog


def _stub_columns():
    # As control.opteryx's refresh writes them (workspace_catalog.py).
    return [
        {"id": 1, "name": "probe_id", "type": "VARCHAR", "nullable": False, "remote-type": 25},
        {"id": 2, "name": "observed_at", "type": "TIMESTAMP", "nullable": False,
         "remote-type": 1184},
        {"id": 3, "name": "latency_ms", "type": "DECIMAL", "nullable": True,
         "precision": 10, "scale": 3, "remote-type": 1700},
    ]


class _StubDoc:
    def to_dict(self):
        return {
            "location": "gs://bucket/health/probe/results",
            "schema": _stub_columns(),
            STUB_MARKER: True,
        }


def test_a_stub_load_carries_each_columns_remote_type():
    dataset = _catalog_shell()._build_dataset(
        "health.probe.results", "probe", "results", _StubDoc()
    )

    schema = dataset.metadata.schema
    assert isinstance(schema, RelationSchema)
    assert [c.remote_type for c in schema.columns] == [25, 1184, 1700]
    # The fields the reader rebuilds a NUMERIC's typmod from travel with it.
    assert (schema.columns[2].precision, schema.columns[2].scale) == (10, 3)


def test_a_column_stored_without_remote_type_reads_as_none():
    """An entry written before `remote-type` was recorded, and every native
    column. None, not a default OID - the reader refuses it and names the
    refresh, which is the only honest answer."""
    schema = relation_schema_from_stored(
        "health.probe.results", [{"id": 1, "name": "probe_id", "type": "VARCHAR"}]
    )

    assert schema.columns[0].remote_type is None
