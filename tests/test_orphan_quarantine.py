"""Tests for the two-strike quarantine on orphaned data files.

The rule under test: a data file is deleted only when two separate expiration
runs, at least a day apart, independently conclude it is orphaned. A candidate
that fails to reappear is forgotten entirely rather than left holding a second
strike - see `orphan_quarantine` for why that distinction is the whole point.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog.catalog.orphan_quarantine import MAX_QUARANTINE_ENTRIES
from opteryx_catalog.catalog.orphan_quarantine import ORPHAN_QUARANTINE_MIN_AGE_MS
from opteryx_catalog.catalog.orphan_quarantine import OrphanQuarantine
from opteryx_catalog.catalog.orphan_quarantine import QuarantineUnavailable
from opteryx_catalog.catalog.orphan_quarantine import review_candidates

DAY_MS = 24 * 60 * 60 * 1000


# --- the promotion rule, in isolation --------------------------------------


def test_first_sighting_is_recorded_not_deleted():
    decision = review_candidates({}, {"a.parquet"}, now_ms=1_000_000, min_age_ms=DAY_MS)

    assert decision.to_delete == set()
    assert decision.held == {"a.parquet": 1_000_000}
    assert decision.newly_quarantined == {"a.parquet"}


def test_second_sighting_a_day_later_is_deleted():
    previous = {"a.parquet": 1_000_000}

    decision = review_candidates(
        previous, {"a.parquet"}, now_ms=1_000_000 + DAY_MS, min_age_ms=DAY_MS
    )

    assert decision.to_delete == {"a.parquet"}
    # Nothing left to quarantine once the file is gone.
    assert decision.held == {}


def test_second_sighting_too_soon_keeps_waiting():
    """Two runs are not two days - the gap is what the guarantee is made of."""
    previous = {"a.parquet": 1_000_000}

    decision = review_candidates(
        previous, {"a.parquet"}, now_ms=1_000_000 + 60_000, min_age_ms=DAY_MS
    )

    assert decision.to_delete == set()
    assert decision.held == {"a.parquet": 1_000_000}
    assert decision.newly_quarantined == set()


def test_repeated_sightings_do_not_push_the_deadline_back():
    """The first-seen time is never refreshed.

    If each sighting reset the clock, a job running more often than the
    quarantine period would re-arm the file every run and never delete it.
    """
    entries = {"a.parquet": 0}

    for now in range(60_000, DAY_MS, 60_000):
        decision = review_candidates(entries, {"a.parquet"}, now_ms=now, min_age_ms=DAY_MS)
        assert decision.to_delete == set()
        entries = decision.held
        assert entries == {"a.parquet": 0}

    final = review_candidates(entries, {"a.parquet"}, now_ms=DAY_MS, min_age_ms=DAY_MS)
    assert final.to_delete == {"a.parquet"}


def test_candidate_that_does_not_reappear_is_forgotten():
    """Exoneration, not just delay.

    A file flagged by a transient failure and not flagged again must leave the
    record entirely. If it stayed, it would carry a permanently armed second
    strike and the next transient failure - however far in the future - would
    delete it on the spot.
    """
    previous = {"live.parquet": 1_000_000}

    decision = review_candidates(previous, set(), now_ms=1_000_000 + DAY_MS, min_age_ms=DAY_MS)

    assert decision.to_delete == set()
    assert decision.held == {}
    assert decision.released == {"live.parquet"}


def test_exonerated_file_starts_from_zero_when_flagged_again():
    """The end-to-end version of the case above: flag, clear, flag, no delete."""
    first = review_candidates({}, {"live.parquet"}, now_ms=0, min_age_ms=DAY_MS)
    cleared = review_candidates(first.held, set(), now_ms=DAY_MS, min_age_ms=DAY_MS)

    # A year later, another transient failure flags the same live file.
    later = 400 * DAY_MS
    reflagged = review_candidates(cleared.held, {"live.parquet"}, now_ms=later, min_age_ms=DAY_MS)

    assert reflagged.to_delete == set()
    assert reflagged.held == {"live.parquet": later}


def test_clock_skew_backwards_does_not_delete():
    """A first-seen stamp in the future must not satisfy the age test."""
    previous = {"a.parquet": 5_000_000}

    decision = review_candidates(previous, {"a.parquet"}, now_ms=1_000_000, min_age_ms=DAY_MS)

    assert decision.to_delete == set()
    assert decision.held == {"a.parquet": 5_000_000}


def test_mixed_candidates_are_partitioned_independently():
    previous = {"old.parquet": 0, "recent.parquet": 1_000_000, "gone.parquet": 0}
    now = DAY_MS + 1

    decision = review_candidates(
        previous, {"old.parquet", "recent.parquet", "new.parquet"}, now_ms=now, min_age_ms=DAY_MS
    )

    assert decision.to_delete == {"old.parquet"}
    assert decision.held == {"recent.parquet": 1_000_000, "new.parquet": now}
    assert decision.released == {"gone.parquet"}


# --- the Firestore-backed store --------------------------------------------


class _FakeDoc:
    def __init__(self, store, key):
        self._store = store
        self._key = key

    def get(self):
        payload = self._store.get(self._key)

        class _Snapshot:
            exists = payload is not None

            def to_dict(self_inner):
                return payload

        return _Snapshot()

    def set(self, payload):
        self._store[self._key] = payload


class _FakeCollection:
    def __init__(self, store, prefix):
        self._store = store
        self._prefix = prefix

    def document(self, name):
        return _FakeDoc(self._store, f"{self._prefix}/{name}")


class _FakeDatasetDoc:
    def __init__(self, store, prefix):
        self._store = store
        self._prefix = prefix

    def collection(self, name):
        return _FakeCollection(self._store, f"{self._prefix}/{name}")


class _FakeCatalog:
    def __init__(self):
        self.store = {}

    def _dataset_doc_ref(self, collection, dataset_name):
        return _FakeDatasetDoc(self.store, f"{collection}/{dataset_name}")


def test_round_trip_through_the_store():
    quarantine = OrphanQuarantine(_FakeCatalog())

    quarantine.save("github.events", {"gs://b/a.parquet": 123})

    assert quarantine.load("github.events") == {"gs://b/a.parquet": 123}


def test_absent_record_is_empty_not_an_error():
    assert OrphanQuarantine(_FakeCatalog()).load("github.events") == {}


def test_save_replaces_rather_than_merges():
    """Forgetting is the mechanism - a merge would keep files armed forever."""
    quarantine = OrphanQuarantine(_FakeCatalog())

    quarantine.save("github.events", {"a.parquet": 1, "b.parquet": 2})
    quarantine.save("github.events", {"b.parquet": 2})

    assert quarantine.load("github.events") == {"b.parquet": 2}


def test_review_persists_the_held_set():
    quarantine = OrphanQuarantine(_FakeCatalog(), min_age_ms=DAY_MS)

    quarantine.review("github.events", {"a.parquet"}, now_ms=1_000)
    assert quarantine.load("github.events") == {"a.parquet": 1_000}

    decision = quarantine.review("github.events", {"a.parquet"}, now_ms=1_000 + DAY_MS)
    assert decision.to_delete == {"a.parquet"}
    assert quarantine.load("github.events") == {}


def test_review_without_persist_leaves_the_record_untouched():
    """A dry run must not advance any file towards deletion."""
    quarantine = OrphanQuarantine(_FakeCatalog(), min_age_ms=DAY_MS)

    quarantine.review("github.events", {"a.parquet"}, persist=False, now_ms=1_000)

    assert quarantine.load("github.events") == {}


def test_run_finding_nothing_still_clears_the_record():
    quarantine = OrphanQuarantine(_FakeCatalog(), min_age_ms=DAY_MS)
    quarantine.save("github.events", {"a.parquet": 1_000})

    quarantine.review("github.events", set(), now_ms=2_000)

    assert quarantine.load("github.events") == {}


def test_unreadable_record_raises_rather_than_reading_as_empty():
    """An empty record and an unreadable one mean opposite things.

    Treating a failure as "nothing quarantined" would send every file back to
    strike one on a read failure, or - if the failure were on the write side -
    let a file be deleted on its first sighting.
    """

    class _BrokenCatalog(_FakeCatalog):
        def _dataset_doc_ref(self, collection, dataset_name):
            raise RuntimeError("firestore unavailable")

    with pytest.raises(QuarantineUnavailable):
        OrphanQuarantine(_BrokenCatalog()).load("github.events")


def test_catalog_without_document_support_is_unavailable():
    class _NoDocs:
        pass

    with pytest.raises(QuarantineUnavailable):
        OrphanQuarantine(_NoDocs()).load("github.events")


def test_oversized_record_refuses_to_truncate():
    """Truncating would silently arm or disarm an arbitrary subset."""
    quarantine = OrphanQuarantine(_FakeCatalog())
    entries = {f"f{i}.parquet": i for i in range(MAX_QUARANTINE_ENTRIES + 1)}

    with pytest.raises(QuarantineUnavailable):
        quarantine.save("github.events", entries)


def test_malformed_entries_are_dropped_back_to_strike_one():
    catalog = _FakeCatalog()
    quarantine = OrphanQuarantine(catalog)
    catalog.store["github/events/maintenance/orphan-quarantine"] = {
        "entries": [
            {"path": "good.parquet", "first-seen-ms": 5},
            {"path": "no-timestamp.parquet"},
            {"first-seen-ms": 7},
        ]
    }

    assert quarantine.load("github.events") == {"good.parquet": 5}


def test_default_quarantine_period_is_a_day():
    assert ORPHAN_QUARANTINE_MIN_AGE_MS == DAY_MS
