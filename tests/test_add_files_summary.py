import io
import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

import pytest
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo.parquet import write_parquet

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.exceptions import AddFilesReadError
from opteryx_catalog.opteryx_catalog import OpteryxCatalog


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


class _FakeCatalog:
    def __init__(self, io):
        self.io = io

    # Reuse the real implementation so this fixture can't drift from it.
    write_parquet_manifest = OpteryxCatalog.write_parquet_manifest

    def save_snapshot(self, identifier, snapshot):
        pass

    def save_dataset_metadata(self, identifier, metadata, **kwargs):
        pass


def _make_parquet_file(mapping: dict, path: str, rows: list) -> bytes:
    m = Morsel()
    m.append_vector("a", vector_from_sequence([r[0] for r in rows], dtype="INTEGER"))
    m.append_vector("b", vector_from_sequence([r[1] for r in rows], dtype="INTEGER"))
    data = write_parquet(m, compression="zstd")
    mapping[path] = data
    return data


def _make_dataset(identifier: str, mem_io: _MemIO) -> SimpleDataset:
    meta = DatasetMetadata(
        dataset_identifier=identifier, location="mem://", schema=None, properties={}
    )
    ds = SimpleDataset(identifier=identifier, _metadata=meta)
    ds.io = mem_io
    ds.catalog = _FakeCatalog(mem_io)
    return ds


def test_add_files_accumulates_files_size():
    mapping = {}
    mem_io = _MemIO(mapping)
    f1 = "mem://data/f1.parquet"
    f2 = "mem://data/f2.parquet"
    d1 = _make_parquet_file(mapping, f1, [(1, 10), (2, 20)])
    d2 = _make_parquet_file(mapping, f2, [(3, 30), (4, 40), (5, 50)])

    ds = _make_dataset("tests_temp.add_files_summary", mem_io)

    ds.add_files([f1, f2], author="tester")

    snap = ds.snapshot()
    expected_size = len(d1) + len(d2)

    assert snap.summary["added-files-size"] == expected_size
    assert snap.summary["added-files-size"] > 0
    assert snap.summary["total-files-size"] == expected_size


def test_truncate_and_add_files_accumulates_files_size():
    mapping = {}
    mem_io = _MemIO(mapping)
    f1 = "mem://data/g1.parquet"
    f2 = "mem://data/g2.parquet"
    d1 = _make_parquet_file(mapping, f1, [(1, 10), (2, 20)])
    d2 = _make_parquet_file(mapping, f2, [(3, 30), (4, 40), (5, 50)])

    ds = _make_dataset("tests_temp.truncate_add_files_summary", mem_io)

    ds.truncate_and_add_files([f1, f2], author="tester")

    snap = ds.snapshot()
    expected_size = len(d1) + len(d2)

    assert snap.summary["added-files-size"] == expected_size
    assert snap.summary["added-files-size"] > 0
    assert snap.summary["total-files-size"] == expected_size


def test_add_files_refuses_a_file_it_cannot_read():
    """An unreadable file fails the commit instead of registering zero rows.

    `add_files` used to substitute a placeholder entry recording record_count=0
    and file_size_in_bytes=0, so the commit landed with a summary that
    undercounted by the whole file and nothing reported it.
    """
    mapping = {}
    mem_io = _MemIO(mapping)
    present = "mem://data/present.parquet"
    _make_parquet_file(mapping, present, [(1, 10), (2, 20)])
    missing = "mem://data/not-there.parquet"

    ds = _make_dataset("tests_temp.add_files_unreadable", mem_io)

    with pytest.raises(AddFilesReadError) as caught:
        ds.add_files([present, missing], author="tester")

    assert missing in str(caught.value)
    # Nothing was committed: the dataset is exactly as it was.
    assert ds.snapshot() is None


def test_truncate_and_add_files_refuses_a_file_it_cannot_read():
    mapping = {}
    mem_io = _MemIO(mapping)
    missing = "mem://data/also-not-there.parquet"

    ds = _make_dataset("tests_temp.truncate_add_files_unreadable", mem_io)

    with pytest.raises(AddFilesReadError):
        ds.truncate_and_add_files([missing], author="tester")

    assert ds.snapshot() is None


def test_add_files_accepts_a_genuinely_empty_object():
    """An empty object is a real state and still registers, with zero rows."""
    mapping = {}
    mem_io = _MemIO(mapping)
    empty = "mem://data/empty.parquet"
    mapping[empty] = b""

    ds = _make_dataset("tests_temp.add_files_empty", mem_io)
    ds.add_files([empty], author="tester")

    snap = ds.snapshot()
    assert snap is not None
    assert snap.summary["total-data-files"] == 1
    assert snap.summary["total-records"] == 0
