"""Provenance through the commit paths (PROVENANCE_DESIGN.md S2, S3, S4.1).

Every snapshot construction site in `dataset.py` stamps the receipt and applies
it to the dataset's standing `sources` list through ONE helper; these tests
drive the public commit methods and read the result back the way the catalog
will - through `_snapshot_to_document`, into a store with `.set()` semantics,
and out again through `_snapshot_from_dict`.

THE SURVIVAL TEST IS THE ONE THAT MATTERS. `save_snapshot` and
`save_dataset_metadata` both `.set()` the same snapshot document, and the
second re-serializes whatever `metadata.snapshots` holds. A field the reader
does not carry is written by the first commit and erased by the second. That
already happened once, to `operation-type` and `parent-snapshot-id`, for the
catalog's whole history.
"""

import io
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import pytest
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo.parquet import write_parquet

from opteryx_catalog.catalog import dataset as dataset_module
from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.exceptions import ReceiptMissing
from opteryx_catalog.opteryx_catalog import OpteryxCatalog
from opteryx_catalog.opteryx_catalog import _snapshot_to_document

WORKSPACE = "ws"
IDENTIFIER = "col.y"
SELF = f"{WORKSPACE}.{IDENTIFIER}"
A = "ws.ops.a"
B = "ws.ops.b"
C = "other.ops.c"
LOCATION = "mem://ws/y"


class _MemInput:
    def __init__(self, data: bytes):
        self._data = data

    def open(self):
        return io.BytesIO(self._data)


class _MemIO:
    def __init__(self, mapping: dict):
        self._mapping = mapping

    def new_input(self, path: str):
        return _MemInput(self._mapping[path])

    def new_output(self, path: str):
        class Out:
            def __init__(self, mapping, path):
                self._buf = io.BytesIO()
                self._mapping = mapping
                self._path = path

            def write(self, data: bytes):
                self._buf.write(data)

            def close(self):
                self._mapping[self._path] = self._buf.getvalue()

            def create(self):
                return self

        return Out(self._mapping, path)


class _DocumentStore:
    """The two real writers' behaviour, and nothing else: each `.set()`s the
    WHOLE document for a snapshot id, through the one serializer, exactly as
    `save_snapshot` and `save_dataset_metadata`'s upsert loop do."""

    def __init__(self, io):
        self.io = io
        self.workspace = WORKSPACE
        self.snapshot_docs: dict[int, dict] = {}
        self.dataset_doc: dict = {}

    write_parquet_manifest = OpteryxCatalog.write_parquet_manifest

    def save_snapshot(self, identifier, snapshot):
        self.snapshot_docs[snapshot.snapshot_id] = _snapshot_to_document(snapshot)

    def save_dataset_metadata(self, identifier, metadata, **kwargs):
        self.dataset_doc = {
            "sources": list(metadata.sources),
            "sources-complete": metadata.sources_complete,
            "current-snapshot-id": metadata.current_snapshot_id,
        }
        for snap in metadata.snapshots:
            self.snapshot_docs[snap.snapshot_id] = _snapshot_to_document(snap)


def _morsel(values):
    m = Morsel()
    m.append_vector("a", vector_from_sequence(values, dtype="INTEGER"))
    return m


def _stage(storage, path, values):
    storage[path] = write_parquet(_morsel(values), compression="zstd")
    return path


def _dataset():
    clear_parsed_manifest_cache()
    storage: dict[str, bytes] = {}
    mem_io = _MemIO(storage)
    meta = DatasetMetadata(dataset_identifier=IDENTIFIER, location=LOCATION, schema=None, properties={})
    ds = SimpleDataset(identifier=IDENTIFIER, _metadata=meta)
    ds.io = mem_io
    ds.catalog = _DocumentStore(mem_io)
    return ds, storage


def _seeded(values=(1, 2, 3), read_sources=()):
    """A dataset with one committed file, its receipt as given."""
    ds, storage = _dataset()
    ds.add_files([_stage(storage, "f1.parquet", list(values))], author="seed", read_sources=list(read_sources))
    return ds, storage


def _reader():
    # `_snapshot_from_dict` reads no instance state.
    return object.__new__(OpteryxCatalog)


@pytest.fixture
def alerts(monkeypatch):
    reported = []
    monkeypatch.setattr(dataset_module, "_alert", lambda exc, **kw: reported.append((exc, kw)))
    return reported


@pytest.fixture
def audits(monkeypatch):
    recorded = []
    monkeypatch.setattr(dataset_module, "emit_audit", lambda action, **kw: recorded.append((action, kw)))
    return recorded


# ── the receipt lands, and survives the second writer ────────────────────────


def test_a_receipt_is_written_on_the_snapshot_document():
    ds, _ = _seeded(read_sources=[(A, 10, "version"), (B, 7)])
    doc = ds.catalog.snapshot_docs[ds.metadata.current_snapshot_id]

    assert doc["read-sources"] == [
        {"dataset": A, "snapshot-id": 10, "resolved-by": "version"},
        {"dataset": B, "snapshot-id": 7, "resolved-by": "current"},
    ]
    assert doc["read-source-keys"] == [A, B, f"{A}@10", f"{B}@7"]
    assert "read-sources-truncated" not in doc
    assert "produced-by" not in doc


def test_the_first_commits_receipt_survives_the_second_commit():
    """Both writers `.set()` the same document; the second one re-serializes the
    first snapshot from what the reader built. Mandatory (S4.1)."""
    ds, storage = _seeded(read_sources=[(A, 10, "version")])
    first = ds.metadata.current_snapshot_id

    ds.add_files([_stage(storage, "f2.parquet", [4])], author="t", read_sources=[(B, 7)], produced_by="task:ws.ops.t")

    assert ds.catalog.snapshot_docs[first]["read-sources"] == [
        {"dataset": A, "snapshot-id": 10, "resolved-by": "version"}
    ]
    second = ds.catalog.snapshot_docs[ds.metadata.current_snapshot_id]
    assert second["read-sources"][0]["dataset"] == B
    assert second["produced-by"] == "task:ws.ops.t"


def test_a_stored_receipt_round_trips_through_the_reader_unchanged():
    ds, _ = _seeded(read_sources=[(A, 10, "tag"), (A, None)])
    doc = ds.catalog.snapshot_docs[ds.metadata.current_snapshot_id]

    restored = _reader()._snapshot_from_dict(doc)

    assert restored.read_sources == doc["read-sources"]
    assert restored.read_sources_truncated is False
    assert _snapshot_to_document(restored) == doc


def test_an_unreported_receipt_is_absent_on_the_document_not_null(alerts, audits):
    ds, _ = _dataset()
    ds.add_files([_stage(ds.io._mapping, "f1.parquet", [1])], author="t")
    doc = ds.catalog.snapshot_docs[ds.metadata.current_snapshot_id]

    assert "read-sources" not in doc
    assert "read-source-keys" not in doc
    assert _reader()._snapshot_from_dict(doc).read_sources is None


def test_a_snapshot_fetched_by_id_carries_its_receipt():
    """`SimpleDataset.snapshot(id)` has its own reader; it must not drop the
    receipt the loader keeps."""
    from opteryx_catalog.catalog.metadata import provenance_fields_from_document

    doc = {"read-sources": [{"dataset": A, "snapshot-id": 1, "resolved-by": "current"}], "produced-by": "view:ws.ops.v"}
    fields = provenance_fields_from_document(doc)
    assert fields == {"read_sources": doc["read-sources"], "read_sources_truncated": False, "produced_by": "view:ws.ops.v"}


# ── `None` is a bug ──────────────────────────────────────────────────────────


def test_a_commit_without_a_receipt_lands_but_is_alerted_and_audited(alerts, audits):
    ds, _ = _dataset()
    ds.add_files([_stage(ds.io._mapping, "f1.parquet", [1])], author="t")

    assert ds.metadata.current_snapshot_id is not None, "the write must not be refused"
    assert len(alerts) == 1
    exc, kwargs = alerts[0]
    assert isinstance(exc, ReceiptMissing)
    assert kwargs["fingerprint"][0] == "missing-receipt"
    assert [a for a, _ in audits if a == "missing_receipt"]


def test_a_commit_without_a_receipt_leaves_the_list_but_marks_it_incomplete(alerts, audits):
    ds, storage = _seeded(read_sources=[(A, 1)])
    assert ds.metadata.sources_complete is True

    ds.add_files([_stage(storage, "f2.parquet", [4])], author="t")

    assert ds.metadata.sources == [A]
    assert ds.metadata.sources_complete is False


def test_a_rewrite_without_a_receipt_empties_the_list_because_the_old_one_is_certainly_wrong(alerts, audits):
    ds, storage = _seeded(read_sources=[(A, 1)])

    ds.truncate_and_add_files([_stage(storage, "f2.parquet", [4])], author="t")

    assert ds.metadata.sources == []
    assert ds.metadata.sources_complete is False


def test_maintenance_never_needs_a_receipt(alerts, audits):
    ds, storage = _seeded(read_sources=[(A, 1)])
    retired = "f1.parquet"

    ds.compaction_commit([_stage(storage, "f1c.parquet", [1, 2, 3])], [retired], author="t")

    assert alerts == []
    doc = ds.catalog.snapshot_docs[ds.metadata.current_snapshot_id]
    assert doc["read-sources"] == []
    assert ds.metadata.sources == [A]
    assert ds.metadata.sources_complete is True


# ── what each commit does to `sources` ───────────────────────────────────────


def test_an_empty_receipt_is_recorded_and_adds_nothing():
    ds, _ = _seeded(read_sources=[])
    assert ds.catalog.snapshot_docs[ds.metadata.current_snapshot_id]["read-sources"] == []
    assert ds.metadata.sources == []
    assert ds.metadata.sources_complete is True


def test_an_append_adds_to_the_list_most_recent_first():
    ds, storage = _seeded(read_sources=[(A, 1)])
    ds.add_files([_stage(storage, "f2.parquet", [4])], author="t", read_sources=[(B, 1), (C, 2)])

    # This commit's names first (in receipt order, which is sorted), then the old.
    assert ds.metadata.sources == [C, B, A]
    assert ds.catalog.dataset_doc["sources"] == [C, B, A]
    assert ds.catalog.dataset_doc["sources-complete"] is True


def test_an_append_keeps_the_list_distinct():
    ds, storage = _seeded(read_sources=[(A, 1), (B, 1)])
    ds.add_files([_stage(storage, "f2.parquet", [4])], author="t", read_sources=[(B, 9)])

    assert ds.metadata.sources == [B, A]


def test_a_rewrite_replaces_the_list():
    ds, storage = _seeded(read_sources=[(A, 1)])
    ds.truncate_and_add_files([_stage(storage, "f2.parquet", [4])], author="t", read_sources=[(C, 2)])

    assert ds.metadata.sources == [C]
    assert ds.metadata.sources_complete is True


def test_a_rewrite_makes_an_incomplete_list_complete_again(alerts, audits):
    ds, storage = _seeded(read_sources=[(A, 1)])
    ds.add_files([_stage(storage, "f2.parquet", [4])], author="t")  # no receipt
    assert ds.metadata.sources_complete is False

    ds.truncate_and_add_files([_stage(storage, "f3.parquet", [5])], author="t", read_sources=[(C, 2)])

    assert ds.metadata.sources == [C]
    assert ds.metadata.sources_complete is True


def test_a_truncate_clears_the_list():
    ds, _ = _seeded(read_sources=[(A, 1)])
    ds.truncate(author="t")

    assert ds.metadata.sources == []
    assert ds.metadata.sources_complete is True
    assert ds.catalog.snapshot_docs[ds.metadata.current_snapshot_id]["read-sources"] == []


def test_a_delete_that_leaves_rows_is_an_append():
    ds, _ = _seeded(values=[1, 2, 3], read_sources=[(A, 1)])
    ds.delete_rows({"f1.parquet": [0]}, author="t", read_sources=[(SELF, 1)])

    assert ds.metadata.sources == [A]


def test_a_delete_of_every_row_clears_the_list():
    ds, _ = _seeded(values=[1, 2, 3], read_sources=[(A, 1)])
    ds.delete_rows({"f1.parquet": [0, 1, 2]}, author="t", read_sources=[(SELF, 1)])

    assert ds.metadata.sources == []
    assert ds.metadata.sources_complete is True


def test_a_merge_records_the_target_in_the_receipt_but_not_in_the_list():
    ds, storage = _seeded(values=[1, 2, 3], read_sources=[(A, 1)])
    head = ds.metadata.current_snapshot_id

    ds.merge_commit(
        [_stage(storage, "f2.parquet", [9])],
        {"f1.parquet": [0]},
        author="t",
        read_sources=[(SELF, head), (B, 4)],
        produced_by="task:ws.ops.merger",
    )

    doc = ds.catalog.snapshot_docs[ds.metadata.current_snapshot_id]
    assert {e["dataset"] for e in doc["read-sources"]} == {SELF, B}
    assert doc["produced-by"] == "task:ws.ops.merger"
    assert ds.metadata.sources == [B, A]


def test_the_list_is_capped_at_sixty_four_and_marked_incomplete():
    from opteryx_catalog.catalog.metadata import MAX_SOURCES

    ds, storage = _seeded(read_sources=[(f"ws.ops.t{i:03d}", 1) for i in range(MAX_SOURCES)])
    assert len(ds.metadata.sources) == MAX_SOURCES
    assert ds.metadata.sources_complete is True

    ds.add_files([_stage(storage, "f2.parquet", [4])], author="t", read_sources=[(B, 1)])

    assert len(ds.metadata.sources) == MAX_SOURCES
    assert ds.metadata.sources[0] == B
    assert ds.metadata.sources_complete is False


def test_a_truncated_receipt_is_flagged_on_the_snapshot_and_the_list():
    from opteryx_catalog.catalog.metadata import MAX_READ_SOURCES

    many = [(f"ws.ops.t{i:04d}", 1) for i in range(MAX_READ_SOURCES + 1)]
    ds, _ = _seeded(read_sources=many)

    doc = ds.catalog.snapshot_docs[ds.metadata.current_snapshot_id]
    assert doc["read-sources-truncated"] is True
    assert len(doc["read-sources"]) == MAX_READ_SOURCES
    assert ds.metadata.sources_complete is False


def test_a_malformed_receipt_refuses_the_commit_before_anything_is_written():
    """Only a caller that walked the plan may assert a receipt, and one that
    asserts nonsense has not."""
    ds, storage = _dataset()
    with pytest.raises(ValueError):
        ds.add_files([_stage(storage, "f1.parquet", [1])], author="t", read_sources=[("a", 1)])
    assert ds.metadata.current_snapshot_id is None
