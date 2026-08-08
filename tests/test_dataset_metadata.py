from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot


def _dataset_with_snapshots(*snapshots: Snapshot) -> SimpleDataset:
    meta = DatasetMetadata(
        dataset_identifier="tests_temp.test",
        location="gs://bucket/ws/tests_temp/test",
        schema=None,
        properties={},
        snapshots=list(snapshots),
        current_snapshot_id=snapshots[-1].snapshot_id if snapshots else None,
    )
    return SimpleDataset(identifier="tests_temp.test", _metadata=meta)


def test_dataset_metadata_and_simpledataset():
    meta = DatasetMetadata(
        dataset_identifier="tests_temp.test",
        location="gs://bucket/ws/tests_temp/test",
        schema=None,
        properties={},
    )
    ds = SimpleDataset(identifier="tests_temp.test", _metadata=meta)
    assert ds.metadata.dataset_identifier == "tests_temp.test"
    assert ds.snapshot() is None
    assert list(ds.snapshots()) == []


def test_sequence_number_requires_history():
    """Test that _next_sequence_number works with empty snapshots."""
    meta = DatasetMetadata(
        dataset_identifier="tests_temp.test",
        location="gs://bucket/ws/tests_temp/test",
        schema=None,
        properties={},
    )
    ds = SimpleDataset(identifier="tests_temp.test", _metadata=meta)

    # Should return 1 when no snapshots are loaded (first snapshot)
    assert ds._next_sequence_number() == 1


def test_sequence_number_follows_current_snapshot():
    ds = _dataset_with_snapshots(Snapshot(snapshot_id=100, timestamp_ms=100, sequence_number=7))
    assert ds._next_sequence_number() == 8


def test_sequence_number_accepts_numeric_strings():
    """Older writers stored the counter as a string; it still has to count."""
    ds = _dataset_with_snapshots(Snapshot(snapshot_id=100, timestamp_ms=100, sequence_number="7"))
    assert ds._next_sequence_number() == 8


def test_sequence_number_never_restarts_behind_existing_snapshots():
    """A missing or unusable number on the current snapshot must not reset to 1.

    `sequence_number` is the primary sort key for `select_last_user_snapshot`
    and for expiration, so a fresh snapshot numbered 1 sitting behind snapshots
    numbered 40+ silently changes which commit those read as the latest. The
    previous code caught the failure and returned 1; it now falls back to the
    highest number actually on record.
    """
    for unusable in (None, "not-a-number", object()):
        ds = _dataset_with_snapshots(
            Snapshot(snapshot_id=100, timestamp_ms=100, sequence_number=41),
            Snapshot(snapshot_id=200, timestamp_ms=200, sequence_number=42),
            Snapshot(snapshot_id=300, timestamp_ms=300, sequence_number=unusable),
        )
        assert ds._next_sequence_number() == 43, f"reset the counter for {unusable!r}"


def test_sequence_number_starts_at_one_when_nothing_is_numbered():
    """A dataset written entirely before the field existed legitimately starts at 1."""
    ds = _dataset_with_snapshots(
        Snapshot(snapshot_id=100, timestamp_ms=100, sequence_number=None),
        Snapshot(snapshot_id=200, timestamp_ms=200, sequence_number=None),
    )
    assert ds._next_sequence_number() == 1
