"""add_files appends to its parent manifest instead of re-encoding it.

Re-encoding decoded every parent entry's statistics on every commit - over 1 GB
for a 28 MB manifest, which OOMed the upload service on 2026-11-04. The parent's
row groups are now copied; only its file paths and summary columns are read.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from rugo.parquet import patch_columns
from rugo.rugo_native import read_rowgroup_stats

from opteryx_catalog.catalog import manifest as manifest_module
from opteryx_catalog.catalog import manifest_arrow
from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache
from opteryx_catalog.catalog.manifest import read_manifest_rows
from test_add_files_summary import _make_dataset
from test_add_files_summary import _make_parquet_file
from test_add_files_summary import _MemIO


def _manifest(mapping, ds):
    return mapping[ds.snapshot().manifest_list]


def _seeded(name, files=3):
    mapping = {}
    ds = _make_dataset(f"tests_temp.{name}", _MemIO(mapping))
    paths = []
    for i in range(files):
        path = f"mem://data/{name}-{i}.parquet"
        _make_parquet_file(mapping, path, [(i, i * 10), (i + 1, i * 10 + 1)])
        paths.append(path)
    return mapping, ds, paths


def test_a_second_commit_appends_a_row_group_and_decodes_no_parent_statistics(monkeypatch):
    mapping, ds, paths = _seeded("append_rg")
    ds.add_files(paths[:2], author="tester")

    def _no_full_decode(*args, **kwargs):
        raise AssertionError("add_files decoded the parent manifest's statistics")

    # No cache to hide a decode behind, and both names a full decode goes through.
    clear_parsed_manifest_cache()
    monkeypatch.setattr(manifest_module, "read_manifest_columns", _no_full_decode)
    monkeypatch.setattr(manifest_arrow, "read_manifest_columns", _no_full_decode)
    # Seeding the cache with the manifest just written is a decode of the NEW
    # manifest (tests/test_manifest_seed_on_write.py), not of the parent.
    monkeypatch.setattr(manifest_module, "seed_parsed_manifest", lambda path, data: None)
    ds.add_files(paths[2:], author="tester")
    monkeypatch.undo()

    data = _manifest(mapping, ds)
    assert [rg["num_rows"] for rg in read_rowgroup_stats(data)] == [2, 1]
    assert [r["file_path"] for r in read_manifest_rows(data)] == paths
    summary = ds.snapshot().summary
    assert summary["total-data-files"] == 3
    assert summary["total-records"] == 6
    assert summary["added-data-files"] == 1


def test_a_batch_with_empty_statistics_appends_to_one_with_full_statistics():
    # The encoder pins every ARRAY column's leaf and depth, so empty lists are not
    # written one level shallower than full ones and the schemas still agree.
    mapping, ds, paths = _seeded("append_empty_stats", files=1)
    ds.add_files(paths, author="tester")
    bare = "mem://data/append_empty_stats-bare.parquet"
    ds.add_files(entries=[{"file_path": bare, "record_count": 0, "file_size_in_bytes": 0}], author="tester")
    data = _manifest(mapping, ds)
    assert [rg["num_rows"] for rg in read_rowgroup_stats(data)] == [1, 1]
    assert [r["file_path"] for r in read_manifest_rows(data)] == paths + [bare]


def test_re_adding_known_files_keeps_the_manifest_and_its_totals():
    mapping, ds, paths = _seeded("append_none")
    ds.add_files(paths, author="tester")
    before = _manifest(mapping, ds)
    ds.add_files(paths, author="tester")
    assert _manifest(mapping, ds) == before
    assert ds.snapshot().summary["total-data-files"] == 3
    assert ds.snapshot().summary["added-data-files"] == 0


def test_an_older_encoders_manifest_is_re_encoded_once_then_appended_to():
    mapping, ds, paths = _seeded("append_legacy")
    ds.add_files(paths[:1], author="tester")
    # Stand in for an older encoder: the parent manifest lacks the vidx columns.
    mapping[ds.snapshot().manifest_list] = patch_columns(
        _manifest(mapping, ds),
        drop=["vidx_ids", "vidx_paths", "vidx_bytes", "vidx_footer_bytes", "vidx_logical_bytes"],
    )

    ds.add_files(paths[1:2], author="tester")
    upgraded = _manifest(mapping, ds)
    assert len(read_rowgroup_stats(upgraded)) == 1  # re-encoded whole, in the current schema
    assert [r["file_path"] for r in read_manifest_rows(upgraded)] == paths[:2]

    ds.add_files(paths[2:], author="tester")
    appended = _manifest(mapping, ds)
    assert [rg["num_rows"] for rg in read_rowgroup_stats(appended)] == [2, 1]
    assert [r["file_path"] for r in read_manifest_rows(appended)] == paths
    assert ds.snapshot().summary["total-data-files"] == 3


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
