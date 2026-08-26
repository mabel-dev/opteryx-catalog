"""IO-layer memory and transfer behaviour.

Three properties, each guarding a cost that used to be silent:

1. `new_input` transfers nothing - the fetch happens at open(). Constructing
   an input used to download the whole object, so code that never read it (or
   only wanted existence) paid a full transfer.
2. The read cache is bounded in BYTES, not just entries: 32 uncapped whole
   objects was ~15 GB of RSS at data-file sizes.
3. `exists()` on the fileio adapter is a HEAD, never a download.
"""

import pytest

from opteryx_catalog.iops import gcs
from opteryx_catalog.iops.base import _ByteBudgetLRU


class _Response:
    def __init__(self, status_code, content=b"", headers=None):
        self.status_code = status_code
        self.content = content
        self.text = content.decode(errors="replace")
        self.headers = headers or {}


class _Session:
    def __init__(self, *responses):
        self._responses = list(responses)
        self.get_calls = 0
        self.head_calls = 0

    def get(self, url, headers=None, timeout=None):
        self.get_calls += 1
        reply = self._responses.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply

    def head(self, url, headers=None, timeout=None):
        self.head_calls += 1
        return self._responses.pop(0)


def test_new_input_transfers_nothing_until_open():
    session = _Session(_Response(200, b"payload"))

    handle = gcs._GcsInputFile("gs://bucket/object", session, lambda: "token", None)
    assert session.get_calls == 0  # construction is free

    with handle.open() as f:
        assert f.read() == b"payload"
    assert session.get_calls == 1

    # A second open serves the already-fetched bytes.
    with handle.open() as f:
        assert f.read() == b"payload"
    assert session.get_calls == 1


def test_read_cache_declines_objects_above_its_per_object_cap():
    cache = _ByteBudgetLRU(max_entries=32, max_bytes=100, max_object_bytes=10)
    session = _Session(_Response(200, b"x" * 50))

    handle = gcs._GcsInputFile("gs://bucket/big", session, lambda: "token", cache)
    with handle.open() as f:
        assert len(f.read()) == 50
    # Served, but never admitted: one oversized object must not evict the
    # small metadata reads the cache exists for.
    assert len(cache) == 0
    assert cache.held_bytes == 0


def test_read_cache_evicts_to_stay_inside_its_byte_budget():
    cache = _ByteBudgetLRU(max_entries=32, max_bytes=100, max_object_bytes=60)

    for name, size in (("a", 40), ("b", 40), ("c", 40)):
        session = _Session(_Response(200, b"x" * size))
        with gcs._GcsInputFile(f"gs://bucket/{name}", session, lambda: "token", cache).open():
            pass

    assert cache.held_bytes <= 100
    assert "gs://bucket/a" not in cache  # oldest evicted
    assert "gs://bucket/c" in cache


def test_adapter_exists_uses_head_not_a_download(monkeypatch):
    from opteryx_catalog.iops.fileio import GcsFileIO as AdapterIO

    class _Impl:
        def __init__(self):
            self.head_calls = 0
            self.input_calls = 0

        def exists(self, location):
            self.head_calls += 1
            return True

        def new_input(self, location):  # pragma: no cover - must not be called
            self.input_calls += 1
            raise AssertionError("exists() must not construct an input")

    adapter = AdapterIO.__new__(AdapterIO)
    adapter._impl = _Impl()

    assert adapter.exists("gs://bucket/object") is True
    assert adapter._impl.head_calls == 1
    assert adapter._impl.input_calls == 0


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
