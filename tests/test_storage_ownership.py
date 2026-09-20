"""A dataset deletes only the bytes it domiciles.

`FORKS_DESIGN.md` S5.2. Every physical deleter in the catalog is scoped to the
dataset's own `location`, and three of them were already scoped that way by
accident - they enumerate candidates by LISTING that prefix, so a foreign path
is never a candidate. `SnapshotExpiration` is the one that was not: it
enumerates from manifest entries, which name whatever path `add_files` was
given, and it deleted them.

That is a bug on its own - a dataset has no business deleting bytes another
dataset owns - and it is also the single thing that made a fork's borrowed
entries unsafe. These tests hold the rule at both levels: the predicate, and
the deleter that is now the only way expiration reaches storage.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.ownership import is_own_path

LOCATION = "gs://bucket/ws/coll/events"
FOREIGN = "gs://bucket/samples/tpch_sf1/lineitem/part-00000.parquet"


# --------------------------------------------------------------------------
# 1. The predicate
# --------------------------------------------------------------------------


def test_a_file_under_the_location_is_owned():
    assert is_own_path(LOCATION, f"{LOCATION}/data/part-00000.parquet")


def test_a_file_in_another_dataset_is_not_owned():
    assert not is_own_path(LOCATION, FOREIGN)


def test_a_sibling_sharing_a_name_prefix_is_not_owned():
    # The separator is why this test exists: `startswith(location)` alone would
    # hand `events` the right to delete `events_v2`, which is a different
    # dataset that merely sorts next to it.
    assert not is_own_path(LOCATION, f"{LOCATION}_v2/data/part-00000.parquet")


def test_the_location_may_carry_a_trailing_slash():
    assert is_own_path(LOCATION + "/", f"{LOCATION}/data/part-00000.parquet")


def test_a_relative_path_is_owned():
    # Bare names are what fixtures and test doubles use to mean "inside this
    # dataset". They make no location-scoped claim, so there is nothing to
    # refuse - and refusing them would turn the guard into a rewrite of every
    # in-memory test in this suite.
    assert is_own_path(LOCATION, "manifest-3.parquet")


def test_an_unknown_location_owns_nothing_absolute():
    # Fail closed. A dataset that cannot say where it lives must not delete on
    # the strength of not knowing.
    assert not is_own_path(None, FOREIGN)
    assert not is_own_path("", FOREIGN)


def test_an_empty_path_is_never_owned():
    assert not is_own_path(LOCATION, "")
    assert not is_own_path(LOCATION, None)


# --------------------------------------------------------------------------
# 2. The deleter
# --------------------------------------------------------------------------


class _RecordingIO:
    """Records what was actually handed to storage."""

    def __init__(self):
        self.deleted = []

    def delete(self, path):
        self.deleted.append(path)


def test_expiration_deletes_a_file_the_dataset_owns():
    io = _RecordingIO()
    expirer = SnapshotExpiration(catalog=None)
    own = f"{LOCATION}/data/part-00000.parquet"

    assert expirer._delete_file(io, own, LOCATION) is True
    assert io.deleted == [own]


def test_expiration_does_not_delete_a_borrowed_file():
    # The case that makes forks safe: an over-age snapshot of a fork names its
    # upstream's files, no retained snapshot of the FORK references them, and
    # the orphan sweep proposes them. Dropping the entry was the whole of the
    # delete; the bytes are the upstream's.
    io = _RecordingIO()
    expirer = SnapshotExpiration(catalog=None)

    assert expirer._delete_file(io, FOREIGN, LOCATION) is False
    assert io.deleted == [], "expiration deleted another dataset's data file"


def test_a_dataset_with_no_location_deletes_nothing_absolute():
    io = _RecordingIO()
    expirer = SnapshotExpiration(catalog=None)

    assert expirer._delete_file(io, FOREIGN, None) is False
    assert io.deleted == []


def test_the_refusal_is_not_an_error_and_reports_as_not_deleted():
    # False, not a raise: the caller loops over orphan candidates and a raise
    # would stop it reclaiming the files it legitimately owns in the same pass.
    io = _RecordingIO()
    expirer = SnapshotExpiration(catalog=None)
    own = f"{LOCATION}/data/part-00001.parquet"

    results = [expirer._delete_file(io, p, LOCATION) for p in (FOREIGN, own, FOREIGN)]

    assert results == [False, True, False]
    assert io.deleted == [own], "one refusal stopped the pass reclaiming its own files"
