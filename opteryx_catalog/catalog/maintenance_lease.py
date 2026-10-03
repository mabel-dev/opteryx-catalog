"""
The per-dataset maintenance lease (opteryx-core docs/VECTOR_INDEX_DESIGN.md §5.7, D-14).

Compaction and index builds never run at the same time on a dataset. The commit
compare-and-set already stops either from overwriting the other, but only at the END: an
index build that loses spends minutes of embedding for nothing, and a compaction that
retires a file mid-build forces the build to start over. The lease stops the overlap up
front; correctness still rests on the CAS.

One document per dataset, `maintenance/lease`, claimed in one Firestore transaction (read,
check free or expired, set) - the `claim_trigger_fire` pattern. The holder renews while it
works and releases after its commit. An expired lease (a crashed holder) can be claimed;
that holder's uncommitted files are orphans for deep clean.

Taken by every compaction, `REFRESH INDEX`, and the build a `sync` CREATE INDEX runs. NOT
by INSERT/CTAS/MERGE/DELETE/UPDATE, including a sync index's build inside them: they index
only their own new files, which no compaction can have selected yet.

`claim-id` identifies one claim, so a holder that renews or releases late can never act on
a later claim - even one by the same holder.
"""

from __future__ import annotations

from dataclasses import dataclass

# Lives beside the orphan quarantine in the dataset's existing `maintenance`
# subcollection, which drop and rename already clean up.
from .orphan_quarantine import MAINTENANCE_SUBCOLLECTION  # noqa: F401 - re-exported

LEASE_DOCUMENT = "lease"
LEASE_OPERATIONS = frozenset({"compaction", "index-build"})
MAX_LEASE_SECONDS = 3600


@dataclass(frozen=True)
class MaintenanceLease:
    """A granted claim. Hand it back to `renew_maintenance_lease` / `release_maintenance_lease`."""

    dataset: str
    claim_id: str
    holder: str
    operation: str
    claimed_at_ms: int
    expires_at_ms: int

    def to_document(self) -> dict:
        return {
            "claim-id": self.claim_id,
            "holder": self.holder,
            "operation": self.operation,
            "claimed-at-ms": self.claimed_at_ms,
            "expires-at-ms": self.expires_at_ms,
        }


def validate_lease_request(holder: str, operation: str, ttl_seconds: int) -> None:
    """Refuse, never coerce."""
    if type(holder) is not str or not holder:
        raise ValueError("A maintenance lease needs a holder (who is doing the work).")
    if operation not in LEASE_OPERATIONS:
        raise ValueError(
            f"Unknown maintenance operation '{operation}'; supported: {sorted(LEASE_OPERATIONS)}."
        )
    if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= MAX_LEASE_SECONDS:
        raise ValueError(
            f"ttl_seconds must be an integer between 1 and {MAX_LEASE_SECONDS}; renew to hold longer."
        )


def describe_holder(document: dict) -> str:
    """The holder as a refusal message names it."""
    return (
        f"{document.get('operation')} by {document.get('holder')} "
        f"(claimed at {document.get('claimed-at-ms')}, expires at {document.get('expires-at-ms')})"
    )
