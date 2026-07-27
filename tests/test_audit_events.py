from __future__ import annotations

import json
from datetime import datetime

from unittest.mock import patch

from opteryx_catalog.audit import AUDIT_SEVERITY
from opteryx_catalog.audit import audit_enabled
from opteryx_catalog.audit import emit_audit


def _emitted(capsys):
    """Parse every audit line written to stdout."""
    out = capsys.readouterr().out
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def test_emits_single_line_json(capsys):
    """Cloud Run only makes a log entry from a complete single line."""
    emit_audit(
        "drop_dataset",
        resource_type="dataset",
        workspace="opteryx",
        collection="coll",
        resource="tbl",
        author="alice",
    )
    out = capsys.readouterr().out
    assert out.count("\n") == 1
    json.loads(out)  # parses as one object


def test_severity_is_the_pipeline_discriminator(capsys):
    """`severity: AUDIT` is what the downstream ingestion filters on."""
    emit_audit("drop_dataset", resource_type="dataset", workspace="ws", resource="t")
    assert _emitted(capsys)[0]["severity"] == AUDIT_SEVERITY


def test_severity_is_not_a_real_cloud_logging_level():
    """If AUDIT ever became a valid severity, GCP would strip it from the
    payload and the downstream filter would silently match nothing."""
    cloud_logging_severities = {
        "DEFAULT", "DEBUG", "INFO", "NOTICE",
        "WARNING", "ERROR", "CRITICAL", "ALERT", "EMERGENCY",
    }
    assert AUDIT_SEVERITY not in cloud_logging_severities


def test_records_who_and_what(capsys):
    emit_audit(
        "create_dataset",
        resource_type="dataset",
        workspace="opteryx",
        collection="coll",
        resource="tbl",
        author="alice",
        location="gs://bucket/ws/coll/tbl",
    )
    record = _emitted(capsys)[0]

    assert record["action"] == "create_dataset"
    assert record["resource_type"] == "dataset"
    assert record["workspace"] == "opteryx"
    assert record["collection"] == "coll"
    assert record["resource"] == "tbl"
    assert record["identifier"] == "coll.tbl"
    assert record["author"] == "alice"
    assert record["detail"]["location"] == "gs://bucket/ws/coll/tbl"
    # an ISO-8601 instant, not epoch ticks
    assert datetime.fromisoformat(record["event_time"]).tzinfo is not None


def test_unattributed_change_is_visibly_unattributed(capsys):
    """A missing author is recorded as null, never substituted."""
    emit_audit("drop_dataset", resource_type="dataset", workspace="ws", resource="t")
    record = _emitted(capsys)[0]
    assert record["author"] is None
    assert "unknown" in record["message"]


def test_avoids_cloud_logging_reserved_timestamp_keys(capsys):
    """`time`/`timestamp` are promoted onto the entry and removed from the
    payload; our own stamp must survive in json_payload."""
    emit_audit("append", resource_type="dataset", workspace="ws", resource="t")
    record = _emitted(capsys)[0]
    assert "timestamp" not in record
    assert "time" not in record
    assert "event_time" in record


def test_non_serialisable_detail_does_not_break_the_caller(capsys):
    """An audit record must never turn a completed write into an exception."""
    class Odd:
        def __repr__(self):
            return "<odd>"

    emit_audit("append", resource_type="dataset", workspace="ws", resource="t", odd=Odd())
    assert _emitted(capsys)[0]["detail"]["odd"] == "<odd>"


def test_newlines_in_values_stay_on_one_line(capsys):
    emit_audit(
        "create_view",
        resource_type="view",
        workspace="ws",
        resource="v",
        statement="SELECT 1\nFROM t",
    )
    out = capsys.readouterr().out
    assert out.count("\n") == 1
    assert json.loads(out)["detail"]["statement"] == "SELECT 1\nFROM t"


def test_can_be_disabled_by_env(capsys, monkeypatch):
    monkeypatch.setenv("OPTERYX_CATALOG_AUDIT", "0")
    assert audit_enabled() is False
    emit_audit("drop_dataset", resource_type="dataset", workspace="ws", resource="t")
    assert capsys.readouterr().out == ""


def test_enabled_by_default(monkeypatch):
    """Auditing is not opt-in."""
    monkeypatch.delenv("OPTERYX_CATALOG_AUDIT", raising=False)
    assert audit_enabled() is True


def test_catalog_drop_dataset_emits_audit(capsys):
    """The catalog's mutations actually emit, not just the helper in isolation."""
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    class _Snap:
        exists = True

        def to_dict(self):
            return {"location": "gs://bucket/opteryx/coll/tbl"}

    class _Ref:
        def get(self):
            return _Snap()

        def set(self, data):
            pass

        def delete(self):
            pass

        def collection(self, name):
            return _Coll()

    class _Coll:
        def document(self, doc_id):
            return _Ref()

        def stream(self):
            return []

    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "opteryx"
    catalog._dataset_doc_ref = lambda c, n: _Ref()
    catalog._snapshots_collection = lambda c, n: _Coll()
    catalog._tombstones_collection = lambda: _Coll()

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.drop_dataset("coll.tbl", author="alice")

    records = [r for r in _emitted(capsys) if r.get("action") == "drop_dataset"]
    assert len(records) == 1
    assert records[0]["author"] == "alice"
    assert records[0]["identifier"] == "coll.tbl"
    assert records[0]["workspace"] == "opteryx"


def test_write_audit_record_stamps_severity(capsys):
    """A caller cannot emit an audit record the pipeline would drop."""
    from opteryx_catalog.audit import write_audit_record

    write_audit_record({"job": "housekeep:update_dependencies", "severity": "INFO"})
    record = _emitted(capsys)[0]
    assert record["severity"] == AUDIT_SEVERITY
    assert record["job"] == "housekeep:update_dependencies"


def test_write_audit_record_relocates_reserved_timestamp(capsys):
    """`timestamp` is consumed by Cloud Logging; the value must still reach the
    table. The HTTP middleware payload really does carry this key."""
    from opteryx_catalog.audit import write_audit_record

    write_audit_record({"path": "/x", "timestamp": 1785162355, "duration_ms": 12})
    record = _emitted(capsys)[0]

    assert "timestamp" not in record
    assert record["audit_timestamp"] == 1785162355
    assert record["duration_ms"] == 12


def test_write_audit_record_relocates_other_reserved_keys(capsys):
    from opteryx_catalog.audit import write_audit_record

    write_audit_record(
        {
            "time": "now",
            "httpRequest": {"m": "GET"},
            "trace": "t1",
            "logging.googleapis.com/labels": {"a": "b"},
            "kept": 1,
        }
    )
    record = _emitted(capsys)[0]

    assert record["audit_time"] == "now"
    assert record["audit_httpRequest"] == {"m": "GET"}
    assert record["audit_trace"] == "t1"
    assert record["audit_logging.googleapis.com/labels"] == {"a": "b"}
    assert record["kept"] == 1


def test_write_audit_record_adds_iso_event_time(capsys):
    from opteryx_catalog.audit import write_audit_record

    write_audit_record({"job": "x"})
    event_time = _emitted(capsys)[0]["event_time"]

    # a parseable, timezone-aware ISO-8601 instant - not epoch ticks
    assert isinstance(event_time, str)
    assert datetime.fromisoformat(event_time).tzinfo is not None


def test_event_time_is_never_epoch_ticks(capsys):
    """The whole point of the rename: a reader should not have to know the unit."""
    emit_audit("drop_dataset", resource_type="dataset", workspace="ws", resource="t")
    event_time = _emitted(capsys)[0]["event_time"]

    assert not isinstance(event_time, (int, float))
    assert event_time.startswith("2")  # "2026-..." not "1785166..."


def test_write_audit_record_respects_disable(capsys, monkeypatch):
    from opteryx_catalog.audit import write_audit_record

    monkeypatch.setenv("OPTERYX_CATALOG_AUDIT", "0")
    write_audit_record({"job": "x"})
    assert capsys.readouterr().out == ""
