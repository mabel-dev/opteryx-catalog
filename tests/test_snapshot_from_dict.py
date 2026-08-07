from opteryx_catalog.opteryx_catalog import OpteryxCatalog


def _make_catalog():
    # _snapshot_from_dict doesn't touch any instance state, so we can
    # bypass __init__ (which requires a live Firestore client) entirely.
    return object.__new__(OpteryxCatalog)


def test_snapshot_from_dict_preserves_commit_message():
    catalog = _make_catalog()
    sd = {
        "snapshot-id": "abc123",
        "timestamp-ms": 1234567890,
        "author": "alice",
        "sequence-number": 1,
        "user-created": True,
        "manifest": "manifest-abc123.parquet",
        "schema-id": "0",
        "summary": {},
        "operation-type": "append",
        "parent-snapshot-id": None,
        "commit-message": "add Q3 sales data",
    }

    snapshot = catalog._snapshot_from_dict(sd)

    assert snapshot.commit_message == "add Q3 sales data"


def test_snapshot_from_dict_defaults_missing_commit_message_to_none():
    catalog = _make_catalog()
    sd = {"snapshot-id": "abc123"}

    snapshot = catalog._snapshot_from_dict(sd)

    assert snapshot.commit_message is None


# ── snapshot documents round-trip through ONE serializer ────────────────────
#
# `save_snapshot` and `save_dataset_metadata` both `.set()` the SAME snapshot
# document. `.set()` REPLACES rather than merges, so any field one writer
# omitted was destroyed by the other. They carried different field sets, and
# `save_dataset_metadata` runs last in every write path, so `operation-type`
# and `parent-snapshot-id` were written by `save_snapshot` and then wiped on
# every single commit — every snapshot read back as `operation_type=None`
# with no parent link. `_snapshot_to_document` is now the single serializer
# both use; these tests keep it in step with the reader.

from opteryx_catalog.catalog.metadata import Snapshot
from opteryx_catalog.opteryx_catalog import _snapshot_to_document


def _full_snapshot():
    return Snapshot(
        snapshot_id=999,
        timestamp_ms=1234567890,
        author="alice",
        user_created=False,
        sequence_number=7,
        manifest_list="manifest-999.parquet",
        operation_type="statistics-refresh",
        parent_snapshot_id=998,
        schema_id="s1",
        commit_message="refresh stats",
        summary={"total-records": 42},
    )


def test_snapshot_document_round_trips_every_field_the_reader_asks_for():
    catalog = _make_catalog()
    restored = catalog._snapshot_from_dict(_snapshot_to_document(_full_snapshot()))

    original = _full_snapshot()
    for field in (
        "snapshot_id",
        "timestamp_ms",
        "author",
        "user_created",
        "sequence_number",
        "manifest_list",
        "operation_type",
        "parent_snapshot_id",
        "schema_id",
        "commit_message",
    ):
        assert getattr(restored, field) == getattr(original, field), field


def test_snapshot_document_carries_provenance_fields():
    # The two specific fields that were being erased on every commit.
    doc = _snapshot_to_document(_full_snapshot())
    assert doc["operation-type"] == "statistics-refresh"
    assert doc["parent-snapshot-id"] == 998
    # ...and the one that would have been erased in the reverse ordering.
    assert doc["user-created"] is False


def test_snapshot_document_fills_missing_summary_keys():
    doc = _snapshot_to_document(_full_snapshot())
    assert doc["summary"]["total-records"] == 42
    assert doc["summary"]["added-data-files"] == 0  # defaulted, not dropped


def test_snapshot_document_does_not_mutate_the_snapshots_summary():
    snap = _full_snapshot()
    _snapshot_to_document(snap)
    assert snap.summary == {"total-records": 42}, "summary defaulted in place"


def test_both_writers_emit_identical_documents():
    # The invariant that makes the clobbering impossible: whatever
    # save_snapshot writes, save_dataset_metadata's upsert must write too.
    # Both now call _snapshot_to_document, so this is a guard against either
    # one growing its own inline dict again.
    import inspect

    from opteryx_catalog import opteryx_catalog as mod

    save_snapshot_src = inspect.getsource(mod.OpteryxCatalog.save_snapshot)
    save_metadata_src = inspect.getsource(mod.OpteryxCatalog.save_dataset_metadata)
    assert "_snapshot_to_document" in save_snapshot_src
    assert "_snapshot_to_document" in save_metadata_src
    # Neither may hand-roll a "snapshot-id" dict of its own any more.
    assert '"snapshot-id":' not in save_snapshot_src
    assert '"snapshot-id":' not in save_metadata_src
