"""
Two-strike quarantine for orphaned data files.

Expiration decides a data file is orphaned by diffing what the retained
snapshots reference against what is physically in storage. Both sides of that
diff can be wrong for reasons that have nothing to do with the file: a manifest
read that fails transiently shrinks the referenced set, and a listing taken
mid-write shows a file whose snapshot has not committed yet. Acting on a single
observation turns either of those into permanent data loss.

So no data file is deleted on the strength of one observation. The first time a
file is seen as orphaned it is recorded here and left alone; it is deleted only
once a later run independently reaches the same conclusion, at least
`min_age_ms` after the first sighting. Because expiration's two detectors
disagree about which files they can even see - the delta method loses sight of a
file as soon as its snapshot document is deleted, leaving full reconciliation to
find it again from storage - the second sighting is usually made by a different
code path than the first.

A file that does *not* reappear is dropped from the record entirely. That is the
part that makes this more than a delay: if exonerated entries lingered, every
file ever flagged would carry a permanently armed second strike, and the next
transient failure - months later - would delete it on the spot. Forgetting is
what keeps the guarantee ("two independent sightings, a day apart") true at
every point in time rather than just the first time.

The record is one Firestore document per dataset, holding one entry per
quarantined file. Datasets here run to hundreds of files, not millions, and only
the suspect subset is ever recorded, so a single document has ample headroom;
`MAX_QUARANTINE_ENTRIES` exists to fail loudly rather than silently truncate if
that assumption is ever wrong.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from dataclasses import field
from typing import Dict
from typing import Optional
from typing import Set
from typing import Tuple

from ..alerts import report as _alert
from ..exceptions import QuarantineUnavailable

logger = logging.getLogger(__name__)

# Minimum time between the first and second sighting of an orphaned file. A
# file seen twice inside this window stays quarantined - two runs are not two
# days, and nothing stops expiration being run twice in an hour.
ORPHAN_QUARANTINE_MIN_AGE_MS = 24 * 60 * 60 * 1000  # 1 day

# Subcollection under the dataset document holding maintenance-only state.
# Kept out of the dataset document itself so `load_dataset` - which reads that
# document on every single call - does not pay for it.
MAINTENANCE_SUBCOLLECTION = "maintenance"
QUARANTINE_DOC = "orphan-quarantine"

# Refuse to write a quarantine document larger than this rather than truncate
# it. Truncation would silently arm or disarm an arbitrary subset of files.
MAX_QUARANTINE_ENTRIES = 10_000


# Re-exported: the definition moved to `opteryx_catalog.exceptions` so the
# hierarchy has one home, and it gained `Alertable` and `CatalogError` bases
# there. Importing it from here still works, which is how every existing caller
# and test refers to it.
__all__ = ["MAX_QUARANTINE_ENTRIES", "OrphanQuarantine", "QuarantineDecision", "QuarantineUnavailable"]


@dataclass
class QuarantineDecision:
    """What a single review concluded about this run's orphan candidates."""

    # Candidates whose second sighting has landed and aged: safe to delete.
    to_delete: Set[str] = field(default_factory=set)
    # Candidates still serving their quarantine, mapped to first-sighting time.
    held: Dict[str, int] = field(default_factory=dict)
    # Candidates recorded for the first time by this run.
    newly_quarantined: Set[str] = field(default_factory=set)
    # Previously quarantined files this run did NOT flag, so they are forgotten.
    released: Set[str] = field(default_factory=set)


def review_candidates(
    previous: Dict[str, int],
    candidates: Set[str],
    now_ms: int,
    min_age_ms: int = ORPHAN_QUARANTINE_MIN_AGE_MS,
) -> QuarantineDecision:
    """Decide which candidates are deletable, given the previous record.

    Pure: does no IO, so the promotion rule can be tested directly.

    Args:
        previous: quarantined path -> first-sighting timestamp (ms)
        candidates: paths this run believes are orphaned
        now_ms: current wall-clock time in ms
        min_age_ms: minimum gap between first and second sighting

    Returns:
        A `QuarantineDecision`. Deleted files are absent from `held` - once the
        file is gone there is nothing left to quarantine.
    """
    decision = QuarantineDecision()

    for path in candidates:
        first_seen = previous.get(path)
        if first_seen is None:
            # First sighting: record and leave alone.
            decision.held[path] = now_ms
            decision.newly_quarantined.add(path)
        elif now_ms - first_seen >= min_age_ms:
            # Second sighting, far enough after the first.
            decision.to_delete.add(path)
        else:
            # Seen again, but too soon to count. Keep the ORIGINAL first-seen
            # time - refreshing it here would let a frequently-run job push the
            # deadline back forever and never delete anything.
            decision.held[path] = first_seen

    decision.released = set(previous) - set(candidates)
    return decision


class OrphanQuarantine:
    """Reads and writes the per-dataset record of suspected orphaned files."""

    def __init__(self, catalog, min_age_ms: int = ORPHAN_QUARANTINE_MIN_AGE_MS):
        """
        Args:
            catalog: OpteryxCatalog instance
            min_age_ms: minimum gap between an orphan's first and second
                sighting before it may be deleted.
        """
        self.catalog = catalog
        self.min_age_ms = min_age_ms

    def _doc_ref(self, identifier: str):
        """Firestore document holding this dataset's quarantine record."""
        # pylint: disable=protected-access
        doc_ref_factory = getattr(self.catalog, "_dataset_doc_ref", None)
        if doc_ref_factory is None:
            raise QuarantineUnavailable("catalog cannot address dataset documents")

        collection, dataset_name = identifier.split(".", 1)
        return (
            doc_ref_factory(collection, dataset_name)
            .collection(MAINTENANCE_SUBCOLLECTION)
            .document(QUARANTINE_DOC)
        )

    def load(self, identifier: str) -> Dict[str, int]:
        """Read the record. Returns path -> first-sighting timestamp (ms).

        A document that has never been written is an empty record, which is a
        real state and not an error - every file is simply on its first strike.
        Raises `QuarantineUnavailable` if the record exists but cannot be read.
        """
        try:
            doc = self._doc_ref(identifier).get()
        except QuarantineUnavailable:
            raise
        except Exception as exc:  # noqa: BLE001 - any backend failure is fatal here
            raise QuarantineUnavailable(f"could not read quarantine record: {exc}") from exc

        if not getattr(doc, "exists", False):
            return {}

        data = doc.to_dict() or {}
        entries = {}
        for entry in data.get("entries") or []:
            path = entry.get("path")
            first_seen = entry.get("first-seen-ms")
            if isinstance(path, str) and isinstance(first_seen, int):
                entries[path] = first_seen
            else:
                # A malformed entry cannot prove a first sighting, so dropping it
                # sends that file back to strike one - the safe direction.
                logger.warning("Discarding malformed quarantine entry for %s: %r", identifier, entry)
        return entries

    def save(self, identifier: str, entries: Dict[str, int]) -> None:
        """Replace the record with `entries`.

        A full replace, not a merge: files absent from `entries` are meant to be
        forgotten (see the module docstring on exoneration), and a merge would
        keep them armed forever.
        """
        if len(entries) > MAX_QUARANTINE_ENTRIES:
            raise QuarantineUnavailable(
                f"{len(entries)} quarantine entries exceeds the {MAX_QUARANTINE_ENTRIES} "
                "single-document limit; refusing to truncate"
            )

        payload = {
            "entries": [
                {"path": path, "first-seen-ms": first_seen}
                for path, first_seen in sorted(entries.items())
            ],
            "updated-at-ms": int(time.time() * 1000),
        }

        try:
            self._doc_ref(identifier).set(payload)
        except QuarantineUnavailable:
            raise
        except Exception as exc:  # noqa: BLE001 - any backend failure is fatal here
            raise QuarantineUnavailable(f"could not write quarantine record: {exc}") from exc

    def review(
        self,
        identifier: str,
        candidates: Set[str],
        persist: bool = True,
        now_ms: Optional[int] = None,
    ) -> QuarantineDecision:
        """Review this run's candidates against the stored record.

        Args:
            identifier: dataset identifier (collection.name)
            candidates: paths this run believes are orphaned
            persist: write the updated record back. False for dry runs, which
                must not advance any file towards deletion.
            now_ms: override for the current time, for tests

        Returns:
            A `QuarantineDecision`. Raises `QuarantineUnavailable` if the record
            could not be read or written - the caller must then delete nothing.
        """
        previous = self.load(identifier)
        decision = review_candidates(
            previous,
            candidates,
            now_ms if now_ms is not None else int(time.time() * 1000),
            self.min_age_ms,
        )

        if persist and decision.held != previous:
            # Written before anything is deleted. If the delete then fails, the
            # file is simply re-flagged next run and deleted then; the reverse
            # order risks deleting a file whose record still marks it as held.
            #
            # The write is skipped only when the record is already exactly
            # right - NOT when there are no candidates. A run that finds nothing
            # orphaned must still clear the previous record, or every file ever
            # flagged stays armed for a second strike forever.
            self.save(identifier, decision.held)

        return decision

    def review_for_deletion(
        self, identifier: str, candidates: Set[str], dry_run: bool
    ) -> Tuple[Set[str], Dict[str, object]]:
        """`review`, wrapped for callers that are about to delete.

        Turns an unavailable record into "delete nothing" rather than an
        exception, and returns summary fields so a caller can report what it
        held back. Shared by expiration and deep clean so the two agree on both
        the decision and how it is reported.

        Call this ONCE per run with the complete candidate set. Files absent
        from `candidates` are exonerated (see the module docstring), so a
        partial call silently clears sightings the caller did not make - and a
        call made when the caller could not observe the dataset at all is worse
        still, because "I saw nothing" and "I could not look" are opposite
        statements about the same empty set.

        Args:
            identifier: Dataset identifier (collection.name)
            candidates: Every file this run judges orphaned
            dry_run: When True the record is left untouched

        Returns:
            (files to delete now, summary fields)
        """
        try:
            decision = self.review(identifier, candidates, persist=not dry_run)
        except QuarantineUnavailable as exc:
            # Deleting without the record would delete on first sighting, which
            # is the behaviour the record exists to prevent. Reclamation stalls
            # until it is readable; the files are still there when it is.
            logger.error(
                "Orphan quarantine unavailable for %s, skipping file deletion: %s",
                identifier,
                exc,
            )
            # Stalling is the safe answer, but it is also invisible: a record
            # that stays unreadable means storage grows forever while every
            # sweep reports success. The exception is raised and absorbed right
            # here, so this is the only place that can surface it.
            _alert(
                exc,
                fingerprint=("quarantine-unavailable", identifier),
                context={"dataset": identifier, "candidates": len(candidates)},
            )
            return set(), {
                "quarantine_available": False,
                "orphans_quarantined": 0,
                "orphans_newly_quarantined": 0,
                "orphans_released": 0,
                "quarantined_files": [],
            }

        if decision.newly_quarantined or decision.released:
            logger.info(
                "Quarantine for %s: %d newly held, %d released, %d cleared for deletion",
                identifier,
                len(decision.newly_quarantined),
                len(decision.released),
                len(decision.to_delete),
            )

        return decision.to_delete, {
            "quarantine_available": True,
            "orphans_quarantined": len(decision.held),
            "orphans_newly_quarantined": len(decision.newly_quarantined),
            "orphans_released": len(decision.released),
            "quarantined_files": sorted(decision.held),
        }

