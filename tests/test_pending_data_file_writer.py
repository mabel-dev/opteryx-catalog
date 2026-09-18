"""A data file streamed for a dataset that does not exist yet.

`CREATE TABLE ... AS SELECT` writes every data file before the dataset is
created, so that a statement dying mid-write leaves no catalog document behind.
The writer therefore cannot be opened from a loaded dataset - there is none -
and asking for one answered `DatasetNotFound` naming the table being created.

`open_pending_data_file_writer` opens it from the schema the dataset is ABOUT
to be created with instead. What is pinned here is that the two derivations it
does agree EXACTLY with what `create_dataset` will go on to persist:

1. the file lands under the location `create_dataset` assigns that identifier;
2. its statistics are keyed by the field-ids `create_dataset` allocates.

(2) is the one that fails silently if it ever drifts: a manifest entry keyed by
the wrong ids describes its columns under ids the finished dataset gives to
OTHER columns, so pruning answers from another column's bounds and rows go
missing with no error anywhere.
"""

import io
import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

import pytest
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel

from opteryx_catalog.exceptions import DatasetAlreadyExists
from opteryx_catalog.opteryx_catalog import OpteryxCatalog

SCHEMA = {
    "columns": [
        {"name": "project", "type": "VARCHAR"},
        {"name": "kind", "type": "VARCHAR"},
        {"name": "size", "type": "INTEGER"},
    ]
}


class _MemIO:
    def __init__(self):
        self.mapping: dict = {}

    def new_output(self, path: str):
        mem = self

        class Out:
            def __init__(self):
                self._buf = io.BytesIO()

            def write(self, data: bytes):
                self._buf.write(data)

            def close(self):
                mem.mapping[path] = self._buf.getvalue()

            def abort(self):
                pass

            def create(self):
                return self

        return Out()


class _Doc:
    def __init__(self, exists: bool):
        self.exists = exists


class _Ref:
    def __init__(self, exists: bool):
        self._exists = exists

    def get(self):
        return _Doc(self._exists)


class _FakeCatalog:
    """Enough of OpteryxCatalog for the pending writer, with the real methods.

    The methods under test are taken unbound from OpteryxCatalog, so this tests
    the catalog's own derivations and not a re-implementation of them.
    """

    def __init__(self, exists: bool = False):
        self.io = _MemIO()
        self.gcs_bucket = "bkt"
        self.workspace = "opteryx"
        self._exists = exists

    def _dataset_doc_ref(self, collection, dataset_name):
        return _Ref(self._exists)

    _schema_to_columns = OpteryxCatalog._schema_to_columns
    _dataset_location = OpteryxCatalog._dataset_location
    _initial_field_ids = OpteryxCatalog._initial_field_ids
    open_pending_data_file_writer = OpteryxCatalog.open_pending_data_file_writer


def _morsel():
    m = Morsel()
    m.append_vector("project", vector_from_sequence(["a", "b", "c"], dtype="VARCHAR"))
    m.append_vector("kind", vector_from_sequence(["x", "y", "z"], dtype="VARCHAR"))
    m.append_vector("size", vector_from_sequence([5, 1, 9], dtype="INTEGER"))
    return m


def test_pending_file_lands_where_create_dataset_will_put_it():
    catalog = _FakeCatalog()
    writer = catalog.open_pending_data_file_writer("ops.zz_probe", SCHEMA)
    writer.write_row_group(_morsel())
    entry = writer.close()

    # The formula create_dataset uses for this identifier, and nothing else.
    assert entry.file_path.startswith("gs://bkt/opteryx/ops/zz_probe/data/")
    assert entry.file_path in catalog.io.mapping
    assert entry.record_count == 3


def test_pending_stats_are_keyed_by_the_ids_the_create_will_allocate():
    catalog = _FakeCatalog()
    writer = catalog.open_pending_data_file_writer("ops.zz_probe", SCHEMA)
    writer.write_row_group(_morsel())
    entry = writer.close().to_dict()

    # What create_dataset persists for this schema: 1..N in column order.
    field_ids = catalog._initial_field_ids(SCHEMA)
    assert field_ids == [1, 2, 3]
    persisted = {
        column["name"]: column["id"]
        for column in catalog._schema_to_columns(SCHEMA, field_ids=field_ids)
    }

    # `field_ids` is parallel to every other per-column list, in the order the
    # morsel's columns were written - which for a CTAS is the target schema's
    # order. A None in any slot would mean the writer could not identify that
    # column at all.
    assert entry["field_ids"] == [persisted["project"], persisted["kind"], persisted["size"]]
    assert entry["field_ids"] == [1, 2, 3]

    size_slot = entry["field_ids"].index(persisted["size"])
    assert entry["min_values"][size_slot] == 1
    assert entry["max_values"][size_slot] == 9


def test_pending_writer_refuses_a_name_that_is_taken():
    catalog = _FakeCatalog(exists=True)
    with pytest.raises(DatasetAlreadyExists):
        catalog.open_pending_data_file_writer("ops.zz_probe", SCHEMA)


def test_pending_writer_touches_no_catalog_document():
    catalog = _FakeCatalog()
    writer = catalog.open_pending_data_file_writer("ops.zz_probe", SCHEMA)
    writer.write_row_group(_morsel())
    writer.close()
    # Only the data file exists; registering it is the caller's next commit.
    assert list(catalog.io.mapping) == [
        path for path in catalog.io.mapping if "/data/" in path
    ]
    assert len(catalog.io.mapping) == 1


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
