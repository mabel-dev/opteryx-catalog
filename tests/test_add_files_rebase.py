"""`add_files` rebases over a race it can commute with, and only that.

A commit that loses the pointer race (`SnapshotRaceError`) re-reads the dataset and rebuilds
on the new head when everything that landed in between is an append or changes no rows -
compaction, statistics refresh, index build/drop (architect, 2026-11-03). Anything else, a
moved schema, or a third lost race in a row, keeps the error. The rebuild reuses the entries
it already built: the files are never read twice.
"""

import copy
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import pytest
from test_add_files_summary import _FakeCatalog
from test_add_files_summary import _make_parquet_file
from test_add_files_summary import _MemIO

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot
from opteryx_catalog.exceptions import SnapshotRaceError

IDENTIFIER = "tests_temp.add_files_rebase"


class _RacingCatalog(_FakeCatalog):
    """Holds the committed head; `interlopers` land a commit just before the next save."""

    def __init__(self, io):
        super().__init__(io)
        self.stored = None
        self.snapshots = {}
        self.interlopers = []

    def save_snapshot(self, identifier, snapshot):
        self.snapshots[snapshot.snapshot_id] = snapshot

    def save_dataset_metadata(self, identifier, metadata, expected_current_snapshot_id=None):
        if self.interlopers:
            self.interlopers.pop(0)(self)
        head = self.stored.current_snapshot_id if self.stored else None
        if expected_current_snapshot_id != head:
            raise SnapshotRaceError(f"moved: built on {expected_current_snapshot_id}, head {head}")
        self.stored = copy.deepcopy(metadata)

    def load_dataset(self, identifier):
        meta = copy.deepcopy(self.stored)
        meta.snapshots = list(self.snapshots.values())
        ds = SimpleDataset(identifier=identifier, _metadata=meta)
        ds.io, ds.catalog = self.io, self
        return ds


def _land(operation):
    """Another writer's commit of `operation` on the current head, carrying its manifest."""

    def interloper(catalog):
        head = catalog.snapshots[catalog.stored.current_snapshot_id]
        snap = Snapshot(
            snapshot_id=head.snapshot_id + 1,
            timestamp_ms=head.snapshot_id + 1,
            author="other",
            sequence_number=head.sequence_number + 1,
            operation_type=operation,
            parent_snapshot_id=head.snapshot_id,
            manifest_list=head.manifest_list,
            schema_id=head.schema_id,
            summary=dict(head.summary),
        )
        catalog.snapshots[snap.snapshot_id] = snap
        catalog.stored.snapshots.append(snap)
        catalog.stored.current_snapshot_id = snap.snapshot_id

    return interloper


def _dataset_with_one_file():
    mapping = {}
    mem_io = _MemIO(mapping)
    _make_parquet_file(mapping, "mem://data/f1.parquet", [(1, 10), (2, 20)])
    _make_parquet_file(mapping, "mem://data/f2.parquet", [(3, 30), (4, 40), (5, 50)])
    catalog = _RacingCatalog(mem_io)
    ds = SimpleDataset(
        identifier=IDENTIFIER,
        _metadata=DatasetMetadata(dataset_identifier=IDENTIFIER, location="mem://", schema=None, properties={}),
    )
    ds.io, ds.catalog = mem_io, catalog
    ds.add_files(["mem://data/f1.parquet"], author="tester")
    return ds, catalog, mapping


@pytest.mark.parametrize(
    "operation", ["append", "add-files", "compact", "statistics-refresh", "index-build", "index-drop"]
)
def test_an_append_rebases_over_a_commit_that_adds_or_changes_no_rows(operation):
    ds, catalog, mapping = _dataset_with_one_file()
    # The interloper also takes the file away: a rebase that read it again would fail.
    catalog.interlopers = [lambda c: (_land(operation)(c), mapping.pop("mem://data/f2.parquet"))]

    ds.add_files(["mem://data/f2.parquet"], author="tester")

    head = catalog.snapshots[catalog.stored.current_snapshot_id]
    assert head.operation_type == "add-files"
    assert catalog.snapshots[head.parent_snapshot_id].operation_type == operation
    assert head.summary["total-records"] == 5
    assert head.summary["total-data-files"] == 2


@pytest.mark.parametrize(
    "operation", ["overwrite", "truncate", "truncate-and-add-files", "delete", "merge", "something-new"]
)
def test_an_append_keeps_the_race_over_anything_that_may_remove_rows(operation):
    ds, catalog, _ = _dataset_with_one_file()
    catalog.interlopers = [_land(operation)]

    with pytest.raises(SnapshotRaceError):
        ds.add_files(["mem://data/f2.parquet"], author="tester")

    assert catalog.snapshots[catalog.stored.current_snapshot_id].operation_type == operation


def test_a_moved_schema_keeps_the_race():
    ds, catalog, _ = _dataset_with_one_file()

    def evolve(c):
        _land("compact")(c)
        c.stored.current_schema_id = "a-newer-schema"

    catalog.interlopers = [evolve]

    with pytest.raises(SnapshotRaceError):
        ds.add_files(["mem://data/f2.parquet"], author="tester")


def test_a_third_lost_race_in_a_row_is_raised():
    ds, catalog, _ = _dataset_with_one_file()
    catalog.interlopers = [_land("compact"), _land("compact"), _land("compact")]

    with pytest.raises(SnapshotRaceError):
        ds.add_files(["mem://data/f2.parquet"], author="tester")


def test_two_lost_races_then_a_commit():
    ds, catalog, _ = _dataset_with_one_file()
    catalog.interlopers = [_land("compact"), _land("add-files")]

    ds.add_files(["mem://data/f2.parquet"], author="tester")

    assert catalog.snapshots[catalog.stored.current_snapshot_id].summary["total-records"] == 5
