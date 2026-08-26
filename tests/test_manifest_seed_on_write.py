"""A commit seeds the parsed-manifest cache with the manifest it just wrote,
so the NEXT commit's parent read is a cache hit rather than a re-download and
re-parse of a file this process created moments ago."""

import io as _io
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache
from opteryx_catalog.catalog.manifest import get_parsed_manifest
from opteryx_catalog.catalog.manifest import seed_parsed_manifest


class _CountingIO:
    def __init__(self, mapping):
        self._mapping = mapping
        self.reads = 0

    def new_input(self, path):
        self.reads += 1
        outer = self

        class _Input:
            def open(self):
                return _io.BytesIO(outer._mapping[path])

        return _Input()


def _manifest_bytes():
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    m = Morsel()
    m.append_vector("file_path", vector_from_sequence(["f1.parquet"], dtype="VARCHAR"))
    m.append_vector("record_count", vector_from_sequence([10], dtype="INTEGER"))
    m.append_vector("file_size_in_bytes", vector_from_sequence([100], dtype="INTEGER"))
    return write_parquet(m)


def test_seeded_manifest_is_served_without_a_storage_read():
    clear_parsed_manifest_cache()
    data = _manifest_bytes()
    path = "gs://bucket/metadata/manifest-1-abc.parquet"

    seed_parsed_manifest(path, data)

    io = _CountingIO({path: data})
    rows = get_parsed_manifest(io, path)
    assert io.reads == 0
    assert rows[0]["file_path"] == "f1.parquet"
    assert rows[0]["record_count"] == 10


def test_seeding_replaces_a_stale_entry_at_the_same_path():
    clear_parsed_manifest_cache()
    path = "gs://bucket/metadata/manifest-1-abc.parquet"
    io = _CountingIO({path: _manifest_bytes()})

    # Prime the cache with a first read, then overwrite the path.
    get_parsed_manifest(io, path)

    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    m = Morsel()
    m.append_vector("file_path", vector_from_sequence(["f2.parquet"], dtype="VARCHAR"))
    newer = write_parquet(m)

    seed_parsed_manifest(path, newer)
    rows = get_parsed_manifest(io, path)
    assert rows[0]["file_path"] == "f2.parquet"


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
