"""`current` and `previous` shown as tags in a snapshot listing.

Neither is stored. A stored tag is immutable and pins its snapshot from expiry,
and both of these words must MOVE as the dataset moves - writing them down would
freeze them where they were written and pin two snapshots nobody asked to keep.

The one that carries the weight is `previous`: it is the previous VERSION OF THE
DATA, not the previous snapshot. Compaction rewrites files without changing a
row, so the snapshot behind the head is routinely the same data the unqualified
read already returns.
"""

from __future__ import annotations

import pytest

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot
from opteryx_catalog.exceptions import SnapshotMissingError
from opteryx_catalog.opteryx_catalog import RESERVED_TAG_NAMES
from opteryx_catalog.opteryx_catalog import VIRTUAL_TAG_NAMES
from opteryx_catalog.opteryx_catalog import OpteryxCatalog


def _snap(sid, user_created, parent=None, op=None):
    return Snapshot(
        snapshot_id=sid,
        timestamp_ms=sid,
        sequence_number=sid,
        user_created=user_created,
        operation_type=op,
        parent_snapshot_id=parent,
        manifest_list=f"manifest-{sid}.parquet",
    )


def _dataset(history, head_id):
    meta = DatasetMetadata(
        dataset_identifier="ops.test", location="mem://", schema=None, properties={}
    )
    meta.snapshots = list(history)
    meta.current_snapshot_id = head_id
    return SimpleDataset(identifier="ops.test", _metadata=meta)


class _Catalog(OpteryxCatalog):
    """Only the two things `virtual_tags` and `list_tags_for_display` reach for."""

    def __init__(self, dataset, stored_tags=None):
        self._dataset = dataset
        self._stored_tags = list(stored_tags or [])

    def load_dataset(self, identifier, load_history=False):
        return self._dataset

    def list_tags(self, dataset_identifier):
        return sorted(self._stored_tags, key=lambda tag: tag["name"])


def _by_name(rows):
    return {row["name"]: row for row in rows}


# --- what the words resolve to -------------------------------------------


def test_current_is_the_head_and_previous_is_the_version_behind_it():
    history = [_snap(1, True), _snap(2, True, parent=1)]
    catalog = _Catalog(_dataset(history, head_id=2))

    rows = _by_name(catalog.virtual_tags("ops.test"))

    assert rows["current"]["snapshot-id"] == 2
    assert rows["previous"]["snapshot-id"] == 1


def test_previous_skips_a_compaction_because_it_is_not_a_new_version():
    """The reason this feature exists.

    Snapshot 3 compacted snapshot 2's files. It holds the same rows the head
    holds, so answering `previous` with it would return the data the unqualified
    read already returns - indistinguishable from a working time-travel read.
    """
    history = [
        _snap(1, True),
        _snap(2, True, parent=1),
        _snap(3, False, parent=2, op="compact"),
    ]
    catalog = _Catalog(_dataset(history, head_id=3))

    rows = _by_name(catalog.virtual_tags("ops.test"))

    assert rows["current"]["snapshot-id"] == 3, "current is the head, compaction or not"
    assert rows["previous"]["snapshot-id"] == 1, "2 is the current version; 1 precedes it"


def test_previous_skips_a_run_of_maintenance_commits():
    history = [
        _snap(1, True),
        _snap(2, True, parent=1),
        _snap(3, False, parent=2, op="compact"),
        _snap(4, False, parent=3, op="statistics-refresh"),
    ]
    catalog = _Catalog(_dataset(history, head_id=4))

    assert _by_name(catalog.virtual_tags("ops.test"))["previous"]["snapshot-id"] == 1


def test_a_dataset_with_one_version_has_no_previous():
    """Absent, not null: there is no row in the listing to attach it to."""
    catalog = _Catalog(_dataset([_snap(1, True)], head_id=1))

    rows = _by_name(catalog.virtual_tags("ops.test"))

    assert rows["current"]["snapshot-id"] == 1
    assert "previous" not in rows


def test_an_expired_previous_version_omits_the_row_rather_than_failing():
    """`previous_user_snapshot` raises when the version behind has expired.

    It is unreadable and unlisted, so there is nothing to label - but the
    listing itself must still come back.
    """
    history = [_snap(2, True, parent=1)]
    dataset = _dataset(history, head_id=2)

    def _gone():
        raise SnapshotMissingError("expired")

    dataset.previous_user_snapshot = _gone
    rows = _by_name(_Catalog(dataset).virtual_tags("ops.test"))

    assert rows["current"]["snapshot-id"] == 2
    assert "previous" not in rows


def test_no_head_yields_no_virtual_tags():
    assert _Catalog(_dataset([], head_id=None)).virtual_tags("ops.test") == []


# --- shape ---------------------------------------------------------------


def test_a_virtual_row_is_marked_virtual_and_names_no_creator():
    rows = _by_name(_Catalog(_dataset([_snap(1, True)], head_id=1)).virtual_tags("ops.test"))

    row = rows["current"]
    assert row["virtual"] is True
    assert row["created-by"] is None, "nobody created it; the key must not merely be absent"
    assert row["created-at-ms"] is None
    assert isinstance(row["snapshot-id"], int)


# --- the listing ---------------------------------------------------------


def test_display_listing_leads_with_virtual_tags_then_real_ones_by_name():
    history = [_snap(1, True), _snap(2, True, parent=1)]
    stored = [
        {"name": "release_2", "snapshot-id": 2},
        {"name": "audited", "snapshot-id": 1},
    ]
    catalog = _Catalog(_dataset(history, head_id=2), stored_tags=stored)

    names = [row["name"] for row in catalog.list_tags_for_display("ops.test")]

    assert names == ["current", "previous", "audited", "release_2"]


def test_stored_tags_are_never_marked_virtual():
    catalog = _Catalog(
        _dataset([_snap(1, True)], head_id=1),
        stored_tags=[{"name": "audited", "snapshot-id": 1}],
    )

    rows = _by_name(catalog.list_tags_for_display("ops.test"))

    assert rows["current"]["virtual"] is True
    assert rows["audited"].get("virtual") is not True


def test_virtual_tags_stay_out_of_list_tags():
    """`list_tags` decides what expiration may not delete. A virtual tag is not a pin.

    If these leaked into it, expiration would treat `current` and `previous` as
    retention pins - and a listing helper would be silently steering what gets
    kept.
    """
    catalog = _Catalog(
        _dataset([_snap(1, True), _snap(2, True, parent=1)], head_id=2),
        stored_tags=[{"name": "audited", "snapshot-id": 1}],
    )

    assert [row["name"] for row in catalog.list_tags("ops.test")] == ["audited"]


# --- the words cannot collide with a real tag ----------------------------


@pytest.mark.parametrize("name", VIRTUAL_TAG_NAMES)
def test_every_virtual_name_is_reserved(name):
    """A stored tag by one of these names would shadow the computed row."""
    assert name in RESERVED_TAG_NAMES
    with pytest.raises(ValueError):
        OpteryxCatalog.normalize_tag_name(name)
