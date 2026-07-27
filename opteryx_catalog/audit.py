"""
GCP-compatible audit event emission.

Every mutation of a dataset, view or collection passes through this catalog, so
this is the one place that can record who changed what. Reads are not covered
here - those come from the query log.

Wire format: one JSON object per line on stdout. Cloud Run turns a stdout line
that parses as JSON into a Cloud Logging entry's `jsonPayload`, and the
downstream ingestion (`xb500.opteryx` `transform_audit_logs`) selects entries
where `jsonPayload.severity == "AUDIT"`, materialising them into
`ops.audit_log`. `transform_stdout_logs` applies the same predicate inverted, so
audit events land in exactly one of the two tables.

⚠ "AUDIT" is deliberately NOT one of Cloud Logging's severities (DEBUG, INFO,
NOTICE, WARNING, ERROR, ...). An unrecognised value is left in the payload
rather than promoted onto the entry, which is precisely what lets the
downstream filter see it. "Correcting" this to a real severity would strip the
field into entry metadata and silently empty the audit table.

Emission goes straight to stdout rather than through `logging`. The format is a
contract with the log pipeline, not a host preference: a caller that configured
its own formatter (or none) would otherwise emit text the pipeline cannot
match - which is exactly how this capability was lost before.
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any
from typing import Optional

# Discriminator the ingestion pipeline matches on. See the module docstring
# before changing it - both sides of the pipeline key off this exact string.
AUDIT_SEVERITY = "AUDIT"

_AUDIT_ENV = "OPTERYX_CATALOG_AUDIT"

# Payload keys Cloud Logging consumes: it lifts these onto the LogEntry and
# REMOVES them from jsonPayload, so a caller's value would vanish from the
# audit table. They are relocated under an `audit_` prefix rather than dropped.
_GCP_RESERVED_KEYS = frozenset(
    {"time", "timestamp", "httpRequest", "trace", "spanId", "traceSampled"}
)
_GCP_RESERVED_PREFIX = "logging.googleapis.com/"


def _relocate_reserved(payload: dict) -> dict:
    """Move Cloud-Logging-reserved keys aside so their values reach the table."""
    relocated = {}
    for key, value in payload.items():
        if key in _GCP_RESERVED_KEYS or key.startswith(_GCP_RESERVED_PREFIX):
            relocated[f"audit_{key}"] = value
        else:
            relocated[key] = value
    return relocated


def write_audit_record(payload: dict) -> None:
    """Emit an arbitrary audit payload as one GCP-structured stdout line.

    Stamps the `severity` discriminator the ingestion pipeline matches on, so a
    caller cannot accidentally emit an audit record the pipeline drops. Use
    `emit_audit()` for catalog mutations; this is the general entry point for
    application-level audit events (HTTP requests, housekeeping jobs).
    """
    if not audit_enabled():
        return

    record = _relocate_reserved(payload)
    # Ours wins: the discriminator is not the caller's to set.
    record["severity"] = AUDIT_SEVERITY
    record.setdefault("timestamp_ms", int(time.time() * 1000))
    _write(record)


def audit_enabled() -> bool:
    """Whether audit emission is on. Defaults to ON - auditing is not opt-in.

    Set OPTERYX_CATALOG_AUDIT=0 to silence it (local scripts, noisy test runs).
    """
    return os.environ.get(_AUDIT_ENV, "1").strip().lower() not in ("0", "false", "no", "off")


def emit_audit(
    action: str,
    *,
    resource_type: str,
    workspace: Optional[str],
    resource: Optional[str] = None,
    collection: Optional[str] = None,
    author: Optional[str] = None,
    **detail: Any,
) -> None:
    """Record one catalog mutation.

    Args:
        action: what happened, e.g. "create_dataset", "drop_view", "append"
        resource_type: "dataset" | "view" | "collection"
        workspace: catalog workspace the resource belongs to
        resource: the dataset/view/collection name
        collection: parent collection, where the resource is not itself one
        author: the identity that made the change - None when unauthenticated,
            never substituted, so an unattributed change is visibly unattributed
        **detail: action-specific extras (location, snapshot_id, record_count...)
    """
    identifier = ".".join(part for part in (collection, resource) if part)
    qualified = ".".join(part for part in (workspace, identifier) if part)

    record = {
        "severity": AUDIT_SEVERITY,
        "event": "catalog.mutation",
        "action": action,
        "resource_type": resource_type,
        "workspace": workspace,
        "collection": collection,
        "resource": resource,
        "identifier": identifier or None,
        "author": author,
        # Not "timestamp"/"time": Cloud Logging promotes those onto the entry and
        # removes them from the payload, taking the field out of json_payload.
        "timestamp_ms": int(time.time() * 1000),
        "message": f"{action} {qualified} by {author or 'unknown'}",
    }
    if detail:
        record["detail"] = detail

    write_audit_record(record)


def _write(record: dict) -> None:
    """Write one complete JSON line and flush.

    `default=str` keeps a stray non-serialisable value (a datetime, a Decimal)
    from turning an audit record into an exception in the caller's write path.
    Newlines inside values are escaped by json.dumps, so the record stays on a
    single physical line - Cloud Run only makes an entry from a complete line.
    """
    sys.stdout.write(json.dumps(record, default=str, separators=(",", ":")) + "\n")
    sys.stdout.flush()
