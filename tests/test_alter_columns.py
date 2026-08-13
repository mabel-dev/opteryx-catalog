"""`SimpleDataset.alter_columns` - the file half of ALTER TABLE ... ADD/DROP/
RENAME/ALTER COLUMN.

`test_alter_dataset_schema.py` covers the schema document. This covers what
happens to the DATA: every current file is rewritten to the new shape and
committed as a new snapshot, and - the property the whole design exists for -
the columns that are not changing are copied byte-for-byte rather than decoded
and re-encoded.

Real parquet files are used throughout, written and patched by rugo, with an
in-memory FileIO standing in for GCS. A test that patched a fabricated byte
string would prove nothing about the operation that matters.
"""

from __future__ import annotations

import pytest

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.metadata import DatasetMetadata

# The rewrite is rugo's `patch_columns`; without it the capability genuinely is
# not present, and `alter_columns` says so rather than degrading. Skipping is
# the honest report for that environment - these tests would be asserting the
# behaviour of a dependency that is not installed.
pytest.importorskip("rugo.parquet")
if not hasattr(__import__("rugo.parquet", fromlist=["parquet"]), "patch_columns"):
    pytest.skip(
        "this rugo has no parquet.patch_columns - column DDL needs a newer rugo",
        allow_module_level=True,
    )


# --- in-memory storage --------------------------------------------------------


class _Input:
    def __init__(self, store, path):
        self._store = store
        self._path = path

    def open(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return self._store[self._path]


class _Output:
    def __init__(self, store, path):
        self._store = store
        self._path = path

    def create(self):
        return self

    def write(self, data):
        self._store[self._path] = data

    def close(self):
        pass


class _MemoryIO:
    def __init__(self, store):
        self.store = store

    def new_input(self, path):
        if path not in self.store:
            raise FileNotFoundError(path)
        return _Input(self.store, path)

    def new_output(self, path):
        return _Output(self.store, path)


class _FakeCatalog:
    """Records the schema call and stands in for the manifest/snapshot writes."""

    workspace = "ws"

    def __init__(self):
        self.altered = None
        self.manifests = []

    def alter_dataset_schema(self, identifier, add=None, drop=None, rename=None,
                             retype=None, author=None):
        self.altered = {
            "identifier": identifier, "add": add, "drop": drop,
            "rename": rename, "retype": retype, "author": author,
        }
        return "schema-2"

    def write_parquet_manifest(self, snapshot_id, entries, dataset_location):
        self.manifests.append(entries)
        return f"{dataset_location}/metadata/manifest-{snapshot_id}.parquet"

    def save_snapshot(self, identifier, snapshot):
        pass

    def save_dataset_metadata(self, identifier, metadata):
        pass


# --- fixtures -----------------------------------------------------------------


def _write_source(rows=200):
    """A real multi-column parquet file, written by rugo's own writer."""
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    morsel = Morsel.from_vectors(
        ["id", "small", "label"],
        [
            vector_from_sequence(list(range(rows)), dtype="INT64"),
            vector_from_sequence([(i % 600) - 300 for i in range(rows)], dtype="INT16"),
            vector_from_sequence([f"r{i % 7}" for i in range(rows)], dtype="VARCHAR"),
        ],
    )
    return write_parquet(morsel, compression="zstd")


def _donor(name, value, sql_type):
    """A one-column, one-row donor, exactly as the query engine builds one."""
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    morsel = Morsel.from_vectors(
        [name], [vector_from_sequence([value], dtype=sql_type)]
    )
    return write_parquet(
        morsel, compression="none", bloom_filters=False, dictionary=False
    )


def _pages(raw):
    """The encoded pages of a parquet file, and nothing else."""
    assert raw[:4] == b"PAR1" and raw[-4:] == b"PAR1"
    footer_len = int.from_bytes(raw[-8:-4], "little")
    return raw[4 : len(raw) - 8 - footer_len]


def _read(raw):
    from rugo.parquet import read_parquet

    out = {}
    with read_parquet(raw) as reader:
        for morsel in reader:
            for name, values in morsel.to_arrow().to_pydict().items():
                out.setdefault(name, []).extend(values)
    return out


@pytest.fixture
def dataset():
    """A dataset with two real data files, over in-memory storage."""
    store = {}
    catalog = _FakeCatalog()
    location = "mem://ws/coll/tbl"
    paths = []
    for i in range(2):
        path = f"{location}/data/seed-{i}.parquet"
        store[path] = _write_source()
        paths.append(path)

    meta = DatasetMetadata(
        dataset_identifier="coll.tbl", location=location, schema=None, properties={}
    )
    ds = SimpleDataset(identifier="coll.tbl", _metadata=meta)
    ds.catalog = catalog
    ds.io = _MemoryIO(store)
    # The manifest lookup is what alter_columns reads the current file list
    # from; the surrounding snapshot machinery is exercised by its own tests.
    ds._parent_manifest_entries = lambda snap: [{"file_path": p} for p in paths]
    ds.snapshot = lambda *a, **k: type("S", (), {"manifest_list": "m", "summary": {}})()
    committed = {}
    ds.truncate_and_add_files = lambda files, author=None, commit_message=None: committed.update(
        files=files, author=author
    )
    return ds, store, catalog, paths, committed


# --- the load-bearing property ------------------------------------------------


def test_rename_does_not_touch_a_single_data_byte(dataset):
    """A rename implemented by decode-and-rewrite would return equal VALUES and
    different BYTES, and would pass every value test in this file."""
    ds, store, _catalog, paths, committed = dataset
    before = _pages(store[paths[0]])

    ds.alter_columns(rename={"label": "name"}, author="alice")

    patched = store[committed["files"][0]]
    assert _pages(patched) == before
    assert set(_read(patched)) == {"id", "small", "name"}


def test_drop_does_not_carry_the_dropped_columns_pages(dataset):
    """Dropping the LAST column leaves the earlier chunks exactly where they
    were, so the new page region is a byte-for-byte PREFIX of the old."""
    ds, store, _catalog, paths, committed = dataset
    before = _pages(store[paths[0]])

    ds.alter_columns(drop=["label"], author="alice")

    after = _pages(store[committed["files"][0]])
    assert len(after) < len(before)
    assert before.startswith(after)


def test_add_backfills_and_leaves_existing_pages_alone(dataset):
    ds, store, _catalog, paths, committed = dataset
    before = _pages(store[paths[0]])

    ds.alter_columns(
        add=[{"name": "flag", "type": "BOOL", "donor": _donor("flag", True, "BOOL")}],
        author="alice",
    )

    patched = store[committed["files"][0]]
    assert _pages(patched).startswith(before), "existing pages were re-encoded"
    values = _read(patched)
    assert set(values["flag"]) == {True}
    assert len(values["flag"]) == 200


def test_add_without_a_value_backfills_null(dataset):
    ds, store, _catalog, _paths, committed = dataset

    ds.alter_columns(
        add=[{"name": "note", "type": "VARCHAR",
              "donor": _donor("note", None, "VARCHAR")}],
        author="alice",
    )

    values = _read(store[committed["files"][0]])
    assert values["note"] == [None] * 200


def test_retype_preserves_the_values(dataset):
    ds, store, _catalog, paths, committed = dataset
    before = _read(store[paths[0]])

    ds.alter_columns(
        retype={"small": {"type": "INT64", "donor": _donor("small", 1, "INT64")}},
        author="alice",
    )

    after = _read(store[committed["files"][0]])
    assert after["small"] == before["small"]
    assert after["id"] == before["id"] and after["label"] == before["label"]


# --- every file, new paths, correct sequencing --------------------------------


def test_every_current_file_is_rewritten(dataset):
    """A single-file implementation passes the tests above and fails here."""
    ds, _store, _catalog, paths, committed = dataset

    ds.alter_columns(drop=["label"], author="alice")

    assert len(committed["files"]) == len(paths) == 2


def test_the_source_files_are_left_byte_for_byte_alone(dataset):
    """Older snapshots still point at them. Mutating one in place would make a
    snapshot start answering with a shape it was never written under."""
    ds, store, _catalog, paths, committed = dataset
    frozen = {p: store[p] for p in paths}

    ds.alter_columns(drop=["label"], rename={"id": "key"}, author="alice")

    for path, contents in frozen.items():
        assert store[path] == contents
    assert not set(committed["files"]) & set(paths), "a patched file reused a source path"


def test_the_schema_is_recorded_with_the_donors_stripped(dataset):
    """Donors describe the files; the catalog stores columns. A donor reaching
    the schema document would put a parquet file in Firestore."""
    ds, _store, catalog, _paths, _committed = dataset

    ds.alter_columns(
        drop=["label"],
        rename={"id": "key"},
        add=[{"name": "flag", "type": "BOOL", "donor": _donor("flag", True, "BOOL")}],
        author="alice",
    )

    assert catalog.altered["drop"] == ["label"]
    assert catalog.altered["rename"] == {"id": "key"}
    assert catalog.altered["add"] == [{"name": "flag", "type": "BOOL"}]
    assert catalog.altered["author"] == "alice"


def test_the_schema_lands_before_the_snapshot(dataset):
    """Manifest entries are keyed by field id, resolved against the CURRENT
    schema - so the schema has to be the new one by the time files are added."""
    ds, _store, catalog, _paths, _committed = dataset
    order = []
    real_alter = catalog.alter_dataset_schema
    ds.truncate_and_add_files = lambda files, author=None, commit_message=None: order.append(
        "snapshot"
    )

    def _record(*args, **kwargs):
        order.append("schema")
        return real_alter(*args, **kwargs)

    catalog.alter_dataset_schema = _record

    ds.alter_columns(drop=["label"], author="alice")

    assert order == ["schema", "snapshot"]
    assert ds.metadata.current_schema_id == "schema-2"


def test_all_four_operations_compose(dataset):
    ds, store, _catalog, paths, committed = dataset
    before = _read(store[paths[0]])

    ds.alter_columns(
        drop=["label"],
        rename={"id": "key"},
        retype={"small": {"type": "INT64", "donor": _donor("small", 1, "INT64")}},
        add=[{"name": "flag", "type": "BOOL", "donor": _donor("flag", True, "BOOL")}],
        author="alice",
    )

    values = _read(store[committed["files"][0]])
    assert set(values) == {"key", "small", "flag"}
    assert values["key"] == before["id"]
    assert values["small"] == before["small"]
    assert set(values["flag"]) == {True}


# --- refusals -----------------------------------------------------------------


def test_an_author_is_required(dataset):
    ds, _store, _catalog, _paths, _committed = dataset

    with pytest.raises(ValueError, match="author"):
        ds.alter_columns(drop=["label"], author=None)


def test_a_call_with_no_changes_is_refused(dataset):
    ds, _store, _catalog, _paths, _committed = dataset

    with pytest.raises(ValueError, match="no changes"):
        ds.alter_columns(author="alice")


def test_an_add_without_a_donor_is_refused(dataset):
    """The donor is the only thing that says what type the new column is and
    what goes in it - there is nothing sensible to assume."""
    ds, _store, _catalog, _paths, _committed = dataset

    with pytest.raises(ValueError, match="no donor"):
        ds.alter_columns(add=[{"name": "flag", "type": "BOOL"}], author="alice")


def test_a_retype_without_a_donor_is_refused(dataset):
    ds, _store, _catalog, _paths, _committed = dataset

    with pytest.raises(ValueError, match="no donor"):
        ds.alter_columns(retype={"small": {"type": "INT64"}}, author="alice")


def test_an_unreadable_file_stops_the_whole_operation(dataset):
    """Committing a schema whose files were never rewritten would leave the
    dataset describing a shape its data does not have."""
    ds, store, catalog, paths, _committed = dataset
    del store[paths[1]]

    from opteryx_catalog.exceptions import AddFilesReadError

    with pytest.raises(AddFilesReadError, match="Refusing"):
        ds.alter_columns(drop=["label"], author="alice")

    assert catalog.altered is None, "the schema was changed despite the failure"


def test_a_bad_column_name_is_refused_before_any_file_is_rewritten(dataset):
    """The catalog validates the change too, but only after the files are
    written. Catching it here means a typo costs nothing and reports the COLUMN,
    not a parquet-level message, and leaves no rewritten files behind."""
    ds, store, catalog, paths, committed = dataset
    ds.schema = lambda *a, **k: type(
        "S", (), {"columns": [type("C", (), {"name": n})() for n in ("id", "small", "label")]}
    )()
    before = dict(store)

    with pytest.raises(ValueError, match="no column named 'nope'"):
        ds.alter_columns(drop=["nope"], author="alice")

    assert store == before, "files were rewritten despite the refusal"
    assert catalog.altered is None
    assert not committed


def test_a_colliding_rename_is_refused_before_any_file_is_rewritten(dataset):
    ds, store, catalog, _paths, _committed = dataset
    ds.schema = lambda *a, **k: type(
        "S", (), {"columns": [type("C", (), {"name": n})() for n in ("id", "small", "label")]}
    )()
    before = dict(store)

    with pytest.raises(ValueError, match="two columns called 'small'"):
        ds.alter_columns(rename={"label": "small"}, author="alice")

    assert store == before
    assert catalog.altered is None
