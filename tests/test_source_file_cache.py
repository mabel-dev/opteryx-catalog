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

from opteryx_catalog.catalog.compaction import _SourceFileCache


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
        cache = _SourceFileCache(io, tmpdir, spill=True)

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
        src = _SourceFileCache(io, tmpdir, spill=True).source("gs://bucket/a.parquet")
        with read_parquet(src) as reader:
            rows = sum(rg.num_rows for rg in reader)
    assert rows == 3


def test_falls_back_to_bytes_when_the_disk_budget_is_exhausted():
    data = _parquet_bytes([1, 2, 3])
    io = _CountingIO({"gs://bucket/a.parquet": data})

    with tempfile.TemporaryDirectory() as tmpdir:
        # Budget below one file: caching is impossible, but compaction must
        # still proceed rather than fail on a full disk.
        cache = _SourceFileCache(io, tmpdir, disk_budget=1, spill=True)
        served = cache.source("gs://bucket/a.parquet")

    assert isinstance(served, bytes)
    assert served == data


def test_spilling_off_keeps_candidates_on_the_heap():
    """The switch is what makes the disk/heap trade explicit: with it off nothing
    is written locally, because a temp directory that is really a tmpfs would be
    charging the memory limit for a "spill"."""
    data = _parquet_bytes([1, 2, 3])
    io = _CountingIO({"gs://bucket/a.parquet": data})

    with tempfile.TemporaryDirectory() as tmpdir:
        cache = _SourceFileCache(io, tmpdir, spill=False)
        served = cache.source("gs://bucket/a.parquet")

        assert isinstance(served, bytes)
        assert os.listdir(tmpdir) == []
        # Still cached - the at-most-once promise holds either way.
        assert cache.source("gs://bucket/a.parquet") is served
        assert io.reads == 1


def test_no_tmpdir_means_no_spilling_whatever_the_setting():
    data = _parquet_bytes([1, 2, 3])
    io = _CountingIO({"gs://bucket/a.parquet": data})

    cache = _SourceFileCache(io, None, spill=True)
    assert isinstance(cache.source("gs://bucket/a.parquet"), bytes)


def test_disk_budget_is_a_share_of_the_device_not_its_size():
    """The ephemeral disk belongs to the container, not to one compaction: two
    uvicorn workers can be spilling at once. Sizing the cache at the device size
    is the same mistake CONTAINER_RAM_MB warns about, made against disk."""
    import importlib

    from opteryx_catalog.catalog import compaction

    def _reload(**env):
        old = {k: os.environ.get(k) for k in env}
        os.environ.update({k: v for k, v in env.items() if v is not None})
        try:
            return importlib.reload(compaction).SOURCE_CACHE_DISK_BYTES
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
            importlib.reload(compaction)

    # 10 GB device, two concurrent compactions, 20% headroom.
    assert _reload(OPTERYX_COMPACTION_DISK_MB="10240", OPTERYX_COMPACTION_DISK_SHARES="2") == (
        4096 * 1024 * 1024
    )
    # An explicit share wins outright - including zero, which means "never cache
    # on disk" and must not read as "unset" and fall back to the derived value.
    assert _reload(OPTERYX_COMPACTION_SOURCE_CACHE_DISK_MB="0") == 0


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
