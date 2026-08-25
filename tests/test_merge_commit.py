"""`merge_commit`: appends and row-deletes landing in ONE snapshot.

MERGE replaces a row by marking its old ordinal deleted and appending the
replacement. A reader that saw either half alone would see the row twice or
not at all, so the two must share a snapshot — which is the only thing this
primitive exists to provide. The halves themselves are `add_files` and
`delete_rows`, both covered by test_mor_deletes.py.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
# The seed/inspect fixtures live with the delete tests they were written for;
# importing them keeps one definition of "a dataset with these data files"
# rather than a second copy that could drift from the commit path it exercises.
sys.path.insert(0, os.path.dirname(__file__))

import pytest
from rugo.parquet import write_parquet

from opteryx_catalog.catalog.deletes import DELETE_FILE_PATH_KEY
from opteryx_catalog.catalog.deletes import DELETED_RECORD_COUNT_KEY
from opteryx_catalog.catalog.manifest import read_manifest_rows

from test_mor_deletes import _current_entries
from test_mor_deletes import _make_morsel
from test_mor_deletes import _seed_dataset


def _stage(ds, storage, path, values):
    """Write a data file into storage without registering it, as the engine's
    write_morsel does before the sink commits."""
    storage[path] = write_parquet(_make_morsel(values), compression="zstd")
    return path


def _entry(entries, path):
    return next(e for e in entries if e.get("file_path") == path)


def _paths(entries):
    return {e.get("file_path") for e in entries}


# ---------------------------------------------------------------------------
# The atomicity the primitive exists for
# ---------------------------------------------------------------------------


def test_add_and_delete_land_in_one_snapshot():
    ds, storage = _seed_dataset({"f1.parquet": [1, 2, 3, 4]})
    before = ds.metadata.current_snapshot_id

    new = _stage(ds, storage, "f2.parquet", [99])
    snap = ds.merge_commit([new], {"f1.parquet": [1]}, author="tester")

    # ONE new snapshot, carrying both halves.
    assert ds.metadata.current_snapshot_id != before
    assert snap.parent_snapshot_id == before
    assert snap.operation_type == "merge"
    assert snap.summary["added-data-files"] == 1
    assert snap.summary["added-records"] == 1
    assert snap.summary["deleted-records"] == 1

    entries = _current_entries(ds)
    assert _paths(entries) == {"f1.parquet", "f2.parquet"}
    assert _entry(entries, "f1.parquet")[DELETED_RECORD_COUNT_KEY] == 1
    # The appended file carries no delete debt — nothing could have marked a
    # row of a file that did not exist until this commit.
    assert _entry(entries, "f2.parquet")[DELETED_RECORD_COUNT_KEY] == 0
    assert _entry(entries, "f2.parquet")[DELETE_FILE_PATH_KEY] is None


def test_only_one_snapshot_is_written():
    ds, storage = _seed_dataset({"f1.parquet": [1, 2, 3]})
    n_before = len(ds.metadata.snapshots)
    ds.merge_commit(
        [_stage(ds, storage, "f2.parquet", [7])], {"f1.parquet": [0]}, author="tester"
    )
    assert len(ds.metadata.snapshots) == n_before + 1


# ---------------------------------------------------------------------------
# Degenerate but legitimate shapes
# ---------------------------------------------------------------------------


def test_insert_only_merge_names_no_positions():
    ds, storage = _seed_dataset({"f1.parquet": [1, 2]})
    snap = ds.merge_commit([_stage(ds, storage, "f2.parquet", [3])], {}, author="tester")
    assert snap.summary["added-data-files"] == 1
    assert snap.summary["deleted-records"] == 0
    assert _entry(_current_entries(ds), "f1.parquet")[DELETED_RECORD_COUNT_KEY] == 0


def test_delete_only_merge_names_no_files():
    ds, _ = _seed_dataset({"f1.parquet": [1, 2, 3]})
    snap = ds.merge_commit([], {"f1.parquet": [2]}, author="tester")
    assert snap.summary["added-data-files"] == 0
    assert snap.summary["deleted-records"] == 1


def test_redundant_deletes_do_not_fail_a_merge_with_real_work():
    """delete_rows refuses an all-redundant delete; merge_commit must not —
    the appends are real work even when every named position is already gone."""
    ds, storage = _seed_dataset({"f1.parquet": [1, 2, 3]})
    ds.merge_commit([], {"f1.parquet": [0]}, author="tester")

    snap = ds.merge_commit(
        [_stage(ds, storage, "f2.parquet", [4])], {"f1.parquet": [0]}, author="tester"
    )
    assert snap.summary["added-data-files"] == 1
    assert snap.summary["deleted-records"] == 0
    assert _entry(_current_entries(ds), "f1.parquet")[DELETED_RECORD_COUNT_KEY] == 1


def test_complete_noop_is_refused():
    ds, _ = _seed_dataset({"f1.parquet": [1, 2, 3]})
    ds.merge_commit([], {"f1.parquet": [0]}, author="tester")
    with pytest.raises(ValueError, match="nothing to commit"):
        ds.merge_commit([], {"f1.parquet": [0]}, author="tester")


def test_empty_merge_is_refused():
    ds, _ = _seed_dataset({"f1.parquet": [1]})
    with pytest.raises(ValueError, match="files to add, positions to delete, or both"):
        ds.merge_commit([], {}, author="tester")


def test_author_is_required():
    ds, _ = _seed_dataset({"f1.parquet": [1]})
    with pytest.raises(ValueError, match="author must be provided"):
        ds.merge_commit([], {"f1.parquet": [0]})


# ---------------------------------------------------------------------------
# Fail-closed behaviour inherited from delete_rows
# ---------------------------------------------------------------------------


def test_all_deleted_file_leaves_the_manifest():
    ds, storage = _seed_dataset({"f1.parquet": [1, 2], "f2.parquet": [3]})
    snap = ds.merge_commit(
        [_stage(ds, storage, "f3.parquet", [9])], {"f1.parquet": [0, 1]}, author="tester"
    )
    entries = _current_entries(ds)
    assert "f1.parquet" not in _paths(entries)
    assert _paths(entries) == {"f2.parquet", "f3.parquet"}
    assert snap.summary["deleted-data-files"] == 1


def test_position_out_of_range_aborts_the_whole_commit():
    ds, storage = _seed_dataset({"f1.parquet": [1, 2]})
    before = ds.metadata.current_snapshot_id
    with pytest.raises(ValueError, match="out of range"):
        ds.merge_commit(
            [_stage(ds, storage, "f2.parquet", [5])], {"f1.parquet": [7]}, author="tester"
        )
    # No snapshot, and the staged file stays unregistered.
    assert ds.metadata.current_snapshot_id == before
    assert "f2.parquet" not in _paths(_current_entries(ds))


def test_unknown_file_aborts_the_whole_commit():
    ds, storage = _seed_dataset({"f1.parquet": [1, 2]})
    before = ds.metadata.current_snapshot_id
    with pytest.raises(ValueError, match="not in the current manifest"):
        ds.merge_commit(
            [_stage(ds, storage, "f2.parquet", [5])], {"gone.parquet": [0]}, author="tester"
        )
    assert ds.metadata.current_snapshot_id == before


def test_unreadable_new_file_aborts_before_any_delete_is_recorded():
    ds, _ = _seed_dataset({"f1.parquet": [1, 2, 3]})
    before = ds.metadata.current_snapshot_id
    with pytest.raises(Exception):
        ds.merge_commit(["missing.parquet"], {"f1.parquet": [0]}, author="tester")
    assert ds.metadata.current_snapshot_id == before
    assert _entry(_current_entries(ds), "f1.parquet")[DELETED_RECORD_COUNT_KEY] == 0


# ---------------------------------------------------------------------------
# Delete state accumulates across merges, as a feed would drive it
# ---------------------------------------------------------------------------


def test_successive_merges_accumulate_delete_debt():
    ds, storage = _seed_dataset({"f1.parquet": [1, 2, 3, 4, 5]})

    ds.merge_commit([_stage(ds, storage, "a.parquet", [10])], {"f1.parquet": [0]}, author="t")
    ds.merge_commit([_stage(ds, storage, "b.parquet", [20])], {"f1.parquet": [2]}, author="t")

    entries = _current_entries(ds)
    assert _entry(entries, "f1.parquet")[DELETED_RECORD_COUNT_KEY] == 2
    assert _paths(entries) == {"f1.parquet", "a.parquet", "b.parquet"}
    # Untouched appended files keep their own (absent) delete state.
    assert _entry(entries, "a.parquet")[DELETED_RECORD_COUNT_KEY] == 0


def test_a_later_merge_can_delete_rows_of_an_earlier_merges_file():
    ds, storage = _seed_dataset({"f1.parquet": [1, 2]})
    ds.merge_commit([_stage(ds, storage, "a.parquet", [10, 11])], {}, author="t")
    ds.merge_commit([], {"a.parquet": [1]}, author="t")

    entries = _current_entries(ds)
    assert _entry(entries, "a.parquet")[DELETED_RECORD_COUNT_KEY] == 1
    assert _entry(entries, "f1.parquet")[DELETED_RECORD_COUNT_KEY] == 0


def test_manifest_is_readable_and_totals_reflect_live_rows():
    ds, storage = _seed_dataset({"f1.parquet": [1, 2, 3, 4]})
    snap = ds.merge_commit(
        [_stage(ds, storage, "f2.parquet", [5, 6])], {"f1.parquet": [0, 3]}, author="t"
    )
    with ds.io.new_input(snap.manifest_list).open() as f:
        rows = read_manifest_rows(f.read())
    assert sum(int(r.get("record_count") or 0) for r in rows) == 6  # physical
    assert snap.summary["total-deleted-records"] == 2  # live = 6 - 2
