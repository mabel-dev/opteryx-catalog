"""Per-sink severity thresholds.

The routing rule that lets one channel carry everything and another carry only
what is worth interrupting someone for.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog import alerts
from opteryx_catalog.alerts.sinks import Alert
from opteryx_catalog.alerts.sinks import accepts
from opteryx_catalog.exceptions import AlertSeverity
from opteryx_catalog.exceptions import ManifestProtectionError
from opteryx_catalog.exceptions import ManifestRefreshError
from opteryx_catalog.exceptions import SummaryInconsistencyError


class _Threshold:
    """A sink that only takes alerts at or above `min_severity`."""

    synchronous = True

    def __init__(self, min_severity):
        self.min_severity = min_severity
        self.alerts = []

    def deliver(self, alert):
        self.alerts.append(alert)

    @property
    def count(self):
        return len(self.alerts)


def _alert(severity):
    return Alert(fingerprint="f", severity=severity, title="t", body="b")


def test_no_threshold_takes_everything():
    """Stdout must stay complete - it is the searchable history."""
    sink = alerts.ListSink()
    for severity in (AlertSeverity.WARNING, AlertSeverity.ERROR, AlertSeverity.CRITICAL):
        assert accepts(sink, _alert(severity))


def test_threshold_filters_below_and_admits_at_or_above():
    sink = _Threshold(AlertSeverity.ERROR)
    assert not accepts(sink, _alert(AlertSeverity.WARNING))
    assert accepts(sink, _alert(AlertSeverity.ERROR))
    assert accepts(sink, _alert(AlertSeverity.CRITICAL))


def test_unknown_severity_fails_open():
    """A typo in a severity should be noisy, never silently dropped."""
    sink = _Threshold(AlertSeverity.CRITICAL)
    assert accepts(sink, _alert("WHOOPS"))


def test_dispatch_routes_by_threshold():
    """The whole point: one channel complete, another only for the worst."""
    everything = alerts.ListSink()
    critical_only = _Threshold(AlertSeverity.CRITICAL)

    alerts.reset()
    alerts.configure(component="expiration", sink=[everything, critical_only])

    alerts.report(SummaryInconsistencyError("totals disagree"), blocking=True)  # WARNING
    alerts.report(ManifestRefreshError("stats failed"), blocking=True)  # ERROR
    alerts.report(ManifestProtectionError("unreadable"), blocking=True)  # CRITICAL

    assert everything.count == 3
    assert critical_only.count == 1
    assert critical_only.alerts[0].severity == AlertSeverity.CRITICAL
    alerts.reset()


def test_a_filtered_alert_is_not_re_delivered_next_time():
    """A sink declining on severity is handled, not failed.

    Treating it as a failure would make the dispatcher forget the fingerprint
    and deliver again on the next occurrence - turning the quiet channel into
    the noisiest thing in the system.
    """
    from opteryx_catalog.alerts import _dispatch

    critical_only = _Threshold(AlertSeverity.CRITICAL)
    alerts.reset()
    alerts.configure(component="expiration", sink=critical_only)

    for _ in range(5):
        alerts.report(
            SummaryInconsistencyError("totals disagree"),
            fingerprint=("summary-disagreement", "landing.http"),
            blocking=True,
        )

    assert critical_only.count == 0
    # One claim, held - not forgotten and re-claimed five times.
    assert len(_dispatch._seen) == 1
    alerts.reset()


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
