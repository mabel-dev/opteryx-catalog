import copy

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.metadata import DatasetMetadata, Snapshot


class _FakeCatalog:
    """Minimal duck-typed catalog that mimics the persisted-document split:

    `save_snapshot` persists individual snapshot documents, while
    `save_dataset_metadata` persists the dataset document (including the
    `current-snapshot-id` pointer) -- matching OpteryxCatalog's real
    behavior. `load_dataset` returns a fresh SimpleDataset built only from
    whatever was last passed to `save_dataset_metadata`, simulating a
    real reload from storage.
    """

    def __init__(self):
        self._persisted_metadata = {}

    def save_snapshot(self, identifier, snapshot):
        pass

    def save_dataset_metadata(self, identifier, metadata):
        self._persisted_metadata[identifier] = copy.deepcopy(metadata)

    def load_dataset(self, identifier):
        meta = copy.deepcopy(self._persisted_metadata[identifier])
        ds = SimpleDataset(identifier=identifier, _metadata=meta)
        ds.catalog = self
        return ds


def _make_seeded_dataset(catalog: _FakeCatalog, identifier: str) -> SimpleDataset:
    """Build a dataset with one pre-existing, non-empty snapshot and
    persist it via the fake catalog so `load_dataset` has something to
    return before truncate() runs.
    """
    meta = DatasetMetadata(
        dataset_identifier=identifier,
        location="mem://ws/test",
        schema=None,
        properties={},
    )
    seed_snap = Snapshot(
        snapshot_id=1,
        timestamp_ms=1,
        author="seed",
        sequence_number=1,
        user_created=True,
        operation_type="append",
        summary={"total-data-files": 3, "total-records": 100},
    )
    meta.snapshots.append(seed_snap)
    meta.current_snapshot_id = 1

    ds = SimpleDataset(identifier=identifier, _metadata=meta)
    ds.catalog = catalog
    catalog.save_dataset_metadata(identifier, meta)
    return ds


def test_truncate_default_does_not_commit_pointer():
    catalog = _FakeCatalog()
    identifier = "tests_temp.truncate_default"
    ds = _make_seeded_dataset(catalog, identifier)

    ds.truncate(author="tester")

    # In-memory state reflects the truncate immediately.
    assert ds.metadata.current_snapshot_id != 1

    # But a fresh load still sees the pre-truncate snapshot, since the
    # dataset document pointer was never persisted.
    reloaded = catalog.load_dataset(identifier)
    assert reloaded.metadata.current_snapshot_id == 1
    assert reloaded.snapshot().summary["total-data-files"] == 3
    assert reloaded.snapshot().summary["total-records"] == 100


def test_truncate_commit_truncation_persists_pointer():
    catalog = _FakeCatalog()
    identifier = "tests_temp.truncate_commit"
    ds = _make_seeded_dataset(catalog, identifier)

    ds.truncate(author="tester", commit_truncation=True)
    new_snapshot_id = ds.metadata.current_snapshot_id
    assert new_snapshot_id != 1

    # A fresh load now reflects the truncation: zero files/records.
    reloaded = catalog.load_dataset(identifier)
    assert reloaded.metadata.current_snapshot_id == new_snapshot_id
    assert reloaded.snapshot().summary["total-data-files"] == 0
    assert reloaded.snapshot().summary["total-records"] == 0
