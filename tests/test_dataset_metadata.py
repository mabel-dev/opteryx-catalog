from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.dataset import SimpleDataset


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
