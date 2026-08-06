"""The stdout sink - the delivery guarantee.

The record format is a contract with the log pipeline, not a local preference,
so these assert the same things `test_audit_events.py` does, inverted where the
two must differ.
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog import alerts
from opteryx_catalog import audit
from opteryx_catalog.exceptions import ManifestProtectionError
from opteryx_catalog.exceptions import SummaryInconsistencyError


def _emitted(capsys):
    out = capsys.readouterr().out
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def _setup(**overrides):
    alerts.reset()
    alerts.configure(component="expiration", environment="production", **overrides)


def test_one_parseable_line_per_alert(capsys):
    _setup()
    alerts.report(ManifestProtectionError("unreadable"), blocking=True)

    records = _emitted(capsys)
    assert len(records) == 1
    assert records[0]["event"] == "catalog.alert"
    assert records[0]["component"] == "expiration"


def test_severity_is_a_real_level_and_not_the_audit_discriminator(capsys):
    """The test that stops alerts disappearing from every error view.

    The downstream transforms partition on `severity == "AUDIT"`: one selects it
    into ops.audit_log, the other applies the inverted predicate into
    ops.stdout_logs. An alert stamped AUDIT lands in the audit table and is
    invisible where anyone would look for a failure.
    """
    _setup()
    alerts.report(ManifestProtectionError("unreadable"), blocking=True)

    record = _emitted(capsys)[0]
    assert record["severity"] != audit.AUDIT_SEVERITY
    assert record["severity"] in ("WARNING", "ERROR", "CRITICAL")
    assert record["severity"] == "CRITICAL"


def test_severity_comes_from_the_exception_class(capsys):
    _setup()
    alerts.report(SummaryInconsistencyError("totals disagree"), blocking=True)
    assert _emitted(capsys)[0]["severity"] == "WARNING"


def test_explicit_severity_beats_the_class_default(capsys):
    _setup()
    alerts.report(SummaryInconsistencyError("totals disagree"), severity="ERROR", blocking=True)
    assert _emitted(capsys)[0]["severity"] == "ERROR"


def test_uses_event_time_not_a_reserved_key(capsys):
    """`timestamp`/`time` get lifted onto the LogEntry and deleted from the payload."""
    _setup()
    alerts.report(ManifestProtectionError("unreadable"), blocking=True)

    record = _emitted(capsys)[0]
    assert audit.EVENT_TIME_KEY in record
    assert "timestamp" not in record
    assert "time" not in record


def test_reserved_keys_in_context_are_relocated_not_dropped(capsys):
    _setup()
    alerts.report(
        ManifestProtectionError("unreadable"),
        context={"dataset": "landing.http"},
        blocking=True,
    )
    record = _emitted(capsys)[0]
    assert record["detail"]["dataset"] == "landing.http"


def test_fingerprint_is_in_the_record(capsys):
    """So a downstream filer dedupes on the identity computed here."""
    _setup()
    alerts.report(
        ManifestProtectionError("unreadable"),
        fingerprint=("gc-unprotectable", "landing.http"),
        blocking=True,
    )
    record = _emitted(capsys)[0]
    assert record["fingerprint"]
    assert len(record["fingerprint"]) == 16


def test_cooloff_suppresses_the_second_line(capsys):
    """Cooloff runs before any sink, so stdout is deduplicated too."""
    _setup()
    for _ in range(3):
        alerts.report(
            ManifestProtectionError("unreadable"),
            fingerprint=("gc-unprotectable", "landing.http"),
            blocking=True,
        )
    assert len(_emitted(capsys)) == 1


def test_different_datasets_are_different_alerts(capsys):
    """The per-dataset fingerprint rule - one ticket per affected dataset."""
    _setup()
    for dataset in ("landing.http", "landing.ssh", "landing.versions"):
        alerts.report(
            ManifestProtectionError("unreadable"),
            fingerprint=("gc-unprotectable", dataset),
            blocking=True,
        )
    records = _emitted(capsys)
    assert len(records) == 3
    assert len({r["fingerprint"] for r in records}) == 3


def test_disabled_emits_nothing(capsys):
    _setup(enabled=False)
    alerts.report(ManifestProtectionError("unreadable"), blocking=True)
    assert _emitted(capsys) == []


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
