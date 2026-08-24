"""Source cache serves candidate files from local disk, not the heap.

Streaming compaction re-reads each candidate file once per window it
contributes to. The cache exists so that amplification lands on local
(ephemeral) disk rather than the network, and - since ``read_parquet`` accepts a
filename - without re-materialising the whole compressed file on the heap for
every window.
"""

import io as _io
import os
import sys
import tempfile

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from opteryx_catalog.catalog.compaction import _SourceFileCache  # noqa: E402


class _MemInput:
    def __init__(self, data: bytes):
        self._data = data

    def open(self):
        return _io.BytesIO(self._data)


class _CountingIO:
    """Counts network reads so the cache's at-most-once promise is testable."""

    def __init__(self, mapping: dict):
        self._mapping = mapping
        self.reads = 0

    def new_input(self, path: str):
        self.reads += 1
        return _MemInput(self._mapping[path])


def _parquet_bytes(values):
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    morsel = Morsel()
    morsel.append_vector("id", vector_from_sequence(values, dtype="INTEGER"))
    return write_parquet(morsel)


def test_source_returns_a_local_path_and_fetches_once():
    data = _parquet_bytes([1, 2, 3])
    io = _CountingIO({"gs://bucket/a.parquet": data})

    with tempfile.TemporaryDirectory() as tmpdir:
        cache = _SourceFileCache(io, tmpdir)

        first = cache.source("gs://bucket/a.parquet")
        # A filename, not bytes: this is what lets rugo stream row groups
        # instead of the caller holding the whole compressed file.
        assert isinstance(first, str)
        assert os.path.dirname(first) == tmpdir
        assert os.path.getsize(first) == len(data)

        # Every later window is served locally - one network read, whatever the
        # window count.
        for _ in range(5):
            assert cache.source("gs://bucket/a.parquet") == first
        assert io.reads == 1


def test_local_path_is_readable_by_rugo():
    from rugo.parquet import read_parquet

    data = _parquet_bytes([7, 8, 9])
    io = _CountingIO({"gs://bucket/a.parquet": data})

    with tempfile.TemporaryDirectory() as tmpdir:
        src = _SourceFileCache(io, tmpdir).source("gs://bucket/a.parquet")
        with read_parquet(src) as reader:
            rows = sum(rg.num_rows for rg in reader)
    assert rows == 3


def test_falls_back_to_bytes_when_the_disk_budget_is_exhausted():
    data = _parquet_bytes([1, 2, 3])
    io = _CountingIO({"gs://bucket/a.parquet": data})

    with tempfile.TemporaryDirectory() as tmpdir:
        # Budget below one file: caching is impossible, but compaction must
        # still proceed rather than fail on a full disk.
        cache = _SourceFileCache(io, tmpdir, disk_budget=1)
        served = cache.source("gs://bucket/a.parquet")

    assert isinstance(served, bytes)
    assert served == data


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
