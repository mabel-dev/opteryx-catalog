"""The six places the catalog now reports a platform inconsistency.

Each asserts two things: that the alert fires with the right severity and a
per-dataset identity, and that the control flow is unchanged - these sites were
promoted from log lines, not turned into new failure modes. The one exception is
the compaction entry-recovery path, which was leaking its output files.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog import alerts
from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.deep_clean import DatasetDeepClean
from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot
from opteryx_catalog.exceptions import ManifestProtectionError
from opteryx_catalog.exceptions import QuarantineUnavailable


@pytest.fixture
def sink():
    alerts.reset()
    collector = alerts.ListSink()
    alerts.configure(component="catalog-test", sink=collector)
    yield collector
    alerts.reset()


class _DeletingIO:
    """Records deletes so a test can prove an aborted pass cleaned up."""

    def __init__(self):
        self.deleted = []

    def delete(self, path):
        self.deleted.append(path)


class _StubDataset:
    def __init__(self, identifier="landing.scan_metadata"):
        self.identifier = identifier
        self.io = _DeletingIO()
        self.metadata = DatasetMetadata(
            dataset_identifier=identifier, location=f"mem://{identifier}"
        )


# The compaction row-count invariant used to alert from `DatasetCompactor` and
# was covered here. Compaction moved to the engine and the invariant moved with
# the commit: `SimpleDataset.compaction_commit` RAISES `CompactionInvariantError`
# rather than alerting and returning, so the caller can remove the files it
# wrote. It is covered by
# `test_mor_deletes.test_compaction_commit_row_count_invariant_uses_live_rows`.


class _SweepCatalog:
    """Two datasets; the named one cannot have its protected set established."""

    workspace = "ws"

    def __init__(self, broken):
        self.io = None
        self.broken = broken

    def list_datasets(self, collection):
        return ["good", "broken"]


def test_expiration_sweep_alerts_once_and_keeps_going(sink):
    catalog = _SweepCatalog("coll.broken")
    expiration = SnapshotExpiration(catalog, author="test")

    def expire_dataset(identifier, *, dry_run):
        if identifier == catalog.broken:
            raise ManifestProtectionError(f"cannot read manifest of {identifier}")

    expiration.expire_dataset = expire_dataset
    results = expiration.expire_collection("coll", dry_run=False)

    assert results["datasets_processed"] == 2  # the sweep continued
    assert results["datasets_skipped_unprotectable"] == ["coll.broken"]
    assert sink.count == 1
    assert sink.alerts[0].severity == "CRITICAL"
    assert sink.alerts[0].context["dataset"] == "coll.broken"
    assert sink.alerts[0].context["sweep"] == "expiration"


def test_deep_clean_sweep_alerts_once_and_keeps_going(sink):
    catalog = _SweepCatalog("coll.broken")
    cleaner = DatasetDeepClean(catalog)

    def clean_dataset(identifier, *, dry_run):
        if identifier == catalog.broken:
            raise ManifestProtectionError(f"cannot read manifest of {identifier}")

    cleaner.clean_dataset = clean_dataset
    results = cleaner.clean_collection("coll", dry_run=False)

    assert results["datasets_processed"] == 2
    assert results["datasets_skipped_unprotectable"] == ["coll.broken"]
    assert sink.count == 1
    assert sink.alerts[0].context["sweep"] == "deep-clean"


# --------------------------------------------------------------------------
# 4. quarantine unavailable
# --------------------------------------------------------------------------


def test_unavailable_quarantine_alerts_and_deletes_nothing(sink):
    from opteryx_catalog.catalog.orphan_quarantine import OrphanQuarantine

    quarantine = OrphanQuarantine(catalog=None)

    def review(identifier, candidates, persist=True, now_ms=None):
        raise QuarantineUnavailable("firestore down")

    quarantine.review = review

    to_delete, fields = quarantine.review_for_deletion(
        "landing.scan_metadata", {"a.parquet", "b.parquet"}, dry_run=False
    )

    assert to_delete == set()  # stalling is still the answer
    assert fields["quarantine_available"] is False
    assert sink.count == 1
    assert sink.alerts[0].severity == "ERROR"
    assert sink.alerts[0].context["dataset"] == "landing.scan_metadata"
    assert sink.alerts[0].context["candidates"] == 2


def test_available_quarantine_is_silent(sink):
    from opteryx_catalog.catalog.orphan_quarantine import OrphanQuarantine
    from opteryx_catalog.catalog.orphan_quarantine import QuarantineDecision

    quarantine = OrphanQuarantine(catalog=None)
    quarantine.review = lambda identifier, candidates, persist=True, now_ms=None: (
        QuarantineDecision(to_delete=set(), held={}, newly_quarantined=set(), released=set())
    )

    quarantine.review_for_deletion("landing.scan_metadata", set(), dry_run=False)
    assert sink.count == 0


# --------------------------------------------------------------------------
# 5. summary vs manifest disagreement
# --------------------------------------------------------------------------


def test_summary_disagreement_alerts_but_does_not_stop_the_commit(sink):
    """Non-fatal by design: the new totals are derived from the manifest, so
    proceeding repairs the counters. Refusing would strand the dataset."""
    meta = DatasetMetadata(dataset_identifier="landing.scan_metadata", location="mem://x")
    dataset = SimpleDataset(identifier="landing.scan_metadata", _metadata=meta, io=None)
    parent = Snapshot(
        snapshot_id=1785906332806,
        timestamp_ms=1785906332806,
        manifest_list="mem://x/metadata/manifest-1785906332806.parquet",
        summary={"total-data-files": 4, "total-records": 746},
    )

    # One entry against a summary claiming four - the shape of the incident.
    dataset._warn_if_summary_disagrees(parent, [{"file_path": "a.parquet", "record_count": 2}])

    assert sink.count == 1
    alert = sink.alerts[0]
    assert alert.severity == "WARNING"
    assert alert.context["recorded_total_data_files"] == 4
    assert alert.context["actual_manifest_entries"] == 1


def test_agreeing_summary_is_silent(sink):
    meta = DatasetMetadata(dataset_identifier="landing.scan_metadata", location="mem://x")
    dataset = SimpleDataset(identifier="landing.scan_metadata", _metadata=meta, io=None)
    parent = Snapshot(
        snapshot_id=1,
        timestamp_ms=1,
        manifest_list="mem://x/metadata/manifest-1.parquet",
        summary={"total-data-files": 1},
    )

    dataset._warn_if_summary_disagrees(parent, [{"file_path": "a.parquet"}])
    assert sink.count == 0


# --------------------------------------------------------------------------
# 6. dangling current-snapshot-id
# --------------------------------------------------------------------------


def test_dangling_snapshot_pointer_alerts_and_still_loads(sink):
    """A dataset naming a snapshot that does not exist loads as EMPTY today.

    That is the metastore-side analogue of the manifest 404 that caused the
    incident, and nothing detected it. Reported, not raised - making it fatal
    changes what load_dataset returns and needs its own change.
    """
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    catalog = OpteryxCatalog.__new__(OpteryxCatalog)
    catalog.workspace = "ichnos"
    catalog.gcs_bucket = "opteryx_data"
    catalog.io = None
    catalog._snapshot_cache = {}
    catalog._schema_cache = {}
    catalog.firestore_client = None

    class _Coll:
        def document(self, name):
            class _Ref:
                path = f"snapshots/{name}"

            return _Ref()

    class _DocRef:
        def collection(self, name):
            return _Coll()

    catalog._snapshots_collection = lambda collection, dataset_name: _Coll()
    catalog._dataset_doc_ref = lambda collection, dataset_name: _DocRef()

    class _Doc:
        def to_dict(self):
            return {
                "location": "gs://opteryx_data/ichnos/landing/scan_metadata",
                "current-snapshot-id": 1785906332806,
            }

    dataset = catalog._build_dataset(
        "landing.scan_metadata", "landing", "scan_metadata", _Doc(), load_history=False
    )

    # Unchanged behaviour: it still returns, still empty.
    assert dataset.metadata.snapshots == []

    assert sink.count == 1
    alert = sink.alerts[0]
    assert alert.severity == "CRITICAL"
    assert alert.context["current_snapshot_id"] == 1785906332806
    assert alert.context["dataset"] == "landing.scan_metadata"


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
