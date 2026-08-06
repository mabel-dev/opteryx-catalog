"""Where a delivered alert goes.

`StdoutSink` is the default and the guarantee: it writes synchronously, so the
record exists before the process can die. `GitHubSink` (in `github.py`) is an
addition, not a replacement - see its module docstring for what it does not
promise.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from dataclasses import field

from ..audit import EVENT_TIME_KEY
from ..audit import _now_iso
from ..audit import _relocate_reserved
from ..audit import _write
from ..exceptions import AlertSeverity

logger = logging.getLogger(__name__)

# Ranking for a sink's `min_severity` threshold. Not on AlertSeverity itself:
# that class is the vocabulary, this is a routing policy over it, and
# `exceptions.py` stays free of anything the alerts layer needs.
SEVERITY_ORDER = {
    AlertSeverity.WARNING: 10,
    AlertSeverity.ERROR: 20,
    AlertSeverity.CRITICAL: 30,
}


def accepts(sink, alert) -> bool:
    """Whether `sink` wants this alert, given its `min_severity` threshold.

    A sink with no threshold takes everything, which keeps the stdout record
    complete - it is the searchable history, and filtering it would lose the
    WARNING that turns out to have been the first sign of an incident.
    Thresholds are for the channels that interrupt people.

    An unrecognised severity ranks ABOVE every threshold rather than below it.
    Fail open: a typo in a severity should produce a noisy alert, never a
    silently dropped one.
    """
    threshold = getattr(sink, "min_severity", None)
    if not threshold:
        return True
    return SEVERITY_ORDER.get(alert.severity, 99) >= SEVERITY_ORDER.get(threshold, 0)


@dataclass(frozen=True)
class Alert:
    """One delivered alert. Built once, handed to every configured sink."""

    fingerprint: str
    severity: str
    title: str
    body: str
    labels: tuple = ()
    context: dict = field(default_factory=dict)
    component: str = "unknown"
    environment: str = "unknown"
    exc_type: str = ""
    origin: str = ""


class StdoutSink:
    """One GCP-structured JSON line per alert, on stdout.

    Shares `audit.py`'s `_write` and `_relocate_reserved` because the format is a
    contract with the log pipeline, not a local preference.

    It must NOT go through `write_audit_record`: that function overwrites
    `severity` with the AUDIT discriminator (`audit.py`, "the discriminator is
    not the caller's to set"), and the downstream transforms partition on that
    exact string. An alert stamped AUDIT lands in `ops.audit_log` and vanishes
    from every error view. Emitting a real Cloud Logging severity is what routes
    these to `ops.stdout_logs`, with no pipeline change needed.

    The fingerprint is in the record on purpose, so anything downstream that
    files tickets can deduplicate on the identity computed here rather than
    trying to re-derive it from the text.
    """

    # Delivered inline. A local write is cheap, and queueing it behind a network
    # call would mean losing the record to an OOM kill - the failure this exists
    # to report.
    synchronous = True

    def deliver(self, alert: Alert) -> None:
        record = _relocate_reserved(
            {
                "severity": alert.severity,
                "event": "catalog.alert",
                "fingerprint": alert.fingerprint,
                "title": alert.title,
                "component": alert.component,
                "environment": alert.environment,
                "exception_type": alert.exc_type,
                "origin": alert.origin,
                "labels": list(alert.labels),
                "message": alert.title,
                "detail": alert.context,
            }
        )
        record.setdefault(EVENT_TIME_KEY, _now_iso())
        _write(record)


class ListSink:
    """Collects alerts in memory. For tests.

    `count` exists so a success-path test can assert `sink.count == 0` without
    parsing anything - proving a happy path is silent is as important as proving
    a broken one alerts.
    """

    synchronous = True

    def __init__(self) -> None:
        self.alerts: list = []

    def deliver(self, alert: Alert) -> None:
        self.alerts.append(alert)

    @property
    def count(self) -> int:
        return len(self.alerts)

    def fingerprints(self) -> list:
        return [a.fingerprint for a in self.alerts]

    def clear(self) -> None:
        self.alerts.clear()
