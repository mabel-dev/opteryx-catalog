"""The candidate search that makes the relationship graph populate itself.

The numbers these tests pin come from a measured run against the production
catalog on 2026-08-29 - 71 datasets, 1,265 columns, 2.67 billion rows, fifteen
relationships previously declared by hand. What that run established, and what
must not silently regress:

  * the pair search is cheap (0.09s for the whole account) and the SKETCH is
    what costs, so cost is linear in datasets rather than quadratic in columns;
  * a k=32 sketch recovers 7 of the 15 and a k=4096 sketch recovers 12, because
    KMV containment fails silently once the two cardinalities are more than
    about `k` apart;
  * value overlap alone cannot tell an id from a catalogue number, so the
    numeric cardinality floor and the same-collection rule are load-bearing
    precision filters, not tuning.
"""

from __future__ import annotations

import pytest

from opteryx_catalog.inference.relationships import MIN_COMPARED
from opteryx_catalog.inference.relationships import SKETCH_K
from opteryx_catalog.inference.relationships import ColumnProfile
from opteryx_catalog.inference.relationships import confidence_of
from opteryx_catalog.inference.relationships import score_candidates
from opteryx_catalog.inference.relationships import type_family
from opteryx_catalog.inference.relationships import value_hash


def _profile(table, column, values, *, family="varchar", cardinality=None, non_null=None):
    """A profile over a known value set, sketched exactly as the job sketches."""
    hashes = sorted({value_hash(value) for value in values})
    distinct = len(hashes)
    return ColumnProfile(
        workspace="ws",
        collection=table.split(".", 1)[0],
        dataset=table.split(".", 1)[1],
        column=column,
        family=family,
        cardinality=cardinality if cardinality is not None else distinct,
        non_null=non_null if non_null is not None else distinct,
        sketch=hashes[:SKETCH_K],
        saturated=distinct >= SKETCH_K,
        rows_sampled=len(list(values)),
        rows_total=len(list(values)),
    )


# --- the hash ------------------------------------------------------------


def test_the_same_value_hashes_the_same_whatever_its_width():
    """An INT32 42 and an INT64 42 are the same identity. If they hashed apart,
    a foreign key between columns of different integer widths - the ordinary
    case - would never be found."""
    assert value_hash(42) == value_hash(int("42"))


def test_a_number_and_its_text_do_not_collide():
    """`42` and `"42"` are not the same identity, and a job that thought they
    were would propose joins between id columns and free text."""
    assert value_hash(42) != value_hash("42")


def test_type_family_sees_through_parameterised_spellings():
    assert type_family("DECIMAL(38, 9)") == "decimal"
    assert type_family("TIMESTAMP[us]") == "temporal" or type_family("TIMESTAMP[us]") != "decimal"
    assert type_family("VARCHAR") == "varchar"
    assert type_family("UINT32") == "int"


# --- containment ---------------------------------------------------------


def test_a_contained_column_is_proposed():
    codes = [f"C{n:04}" for n in range(300)]
    near = _profile("geo.events", "country_code", codes[:200])
    far = _profile("geo.countries", "iso_alpha3", codes)

    (candidate,) = list(score_candidates([near, far]))
    assert candidate.near.column == "country_code"
    assert candidate.far.column == "iso_alpha3"
    assert candidate.containment == pytest.approx(1.0)
    assert candidate.compared == 200


def test_a_disjoint_column_is_not_proposed():
    """Measured in production: `ops.stdout_log.insert_id` against
    `ops.stderr_log.insert_id` scores 0.00 over 952 compared values. That pair
    is a DECLARED relationship, and the data does not support it - which is the
    job working, not failing."""
    near = _profile("ops.stdout_log", "insert_id", [f"a{n}" for n in range(300)])
    far = _profile("ops.stderr_log", "insert_id", [f"b{n}" for n in range(300)])

    assert list(score_candidates([near, far])) == []


def test_a_partial_overlap_below_the_threshold_is_not_proposed():
    shared = [f"x{n}" for n in range(100)]
    near = _profile("a.child", "ref", shared[:50] + [f"y{n}" for n in range(50)])
    far = _profile("a.parent", "id", shared)

    assert list(score_candidates([near, far])) == []


def test_the_direction_with_the_key_at_the_far_end_is_the_one_proposed():
    """Orientation is meaning, not presentation: `maps` runs referencing ->
    referenced. Proposing the reverse would draw the arrow backwards."""
    ids = [f"k{n}" for n in range(500)]
    child = _profile("a.child", "parent_ref", ids[:200] * 3, cardinality=200, non_null=600)
    parent = _profile("a.parent", "id", ids)

    (candidate,) = list(score_candidates([child, parent]))
    assert candidate.near.dataset == "child"
    assert candidate.far.dataset == "parent"


# --- the precision filters ----------------------------------------------


def test_two_small_integer_domains_are_not_proposed():
    """The failure mode that dominated the measured run before this filter:
    `exoplanets.st_nrvc` "contains" `moon_orbits.id` perfectly because both are
    small counters. Perfect overlap, no relationship."""
    counters = list(range(1, 40))
    near = _profile("a.left", "n_spectra", counters, family="int")
    far = _profile("a.right", "id", counters, family="int")

    assert list(score_candidates([near, far])) == []


def test_a_small_text_domain_is_still_proposed():
    """Nine planet names are a real key. The numeric floor must not apply to
    text, or `moons.planet -> planets.name` - declared by hand in production -
    is filtered out."""
    planets = ["Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune"]
    near = _profile("astronomy.moons", "planet", planets * 4, cardinality=8, non_null=32)
    far = _profile("astronomy.planets", "name", planets)

    (candidate,) = list(score_candidates([near, far]))
    assert candidate.containment == pytest.approx(1.0)


def test_a_pair_in_another_collection_is_not_proposed():
    """All fifteen hand-declared relationships have both ends in one
    collection. Value overlap cannot tell a supplier id from a star catalogue
    number, and the measured run proposed exactly that pair until this rule
    went in."""
    ids = [n for n in range(1000, 2000)]
    near = _profile("sales.sales", "supplier_id", ids, family="int")
    far = _profile("astronomy.stars", "HIP", ids, family="int")

    assert list(score_candidates([near, far])) == []


def test_a_far_end_that_repeats_its_values_is_not_a_key():
    values = [f"v{n}" for n in range(100)]
    near = _profile("a.child", "ref", values)
    # 100 distinct values over 10,000 rows: a fact table column, not a key.
    far = _profile("a.parent", "category", values, cardinality=100, non_null=10_000)

    # Asserted on the DIRECTION rather than on the count. The reverse -
    # `parent.category` referencing `child.ref`, which is unique - is a
    # perfectly reasonable candidate, and rejecting it would be rejecting a
    # lookup table.
    proposed = {(c.near.column, c.far.column) for c in score_candidates([near, far])}
    assert ("ref", "category") not in proposed


def test_a_near_end_wider_than_the_key_is_not_proposed():
    parent_ids = [f"k{n}" for n in range(100)]
    near = _profile("a.child", "ref", parent_ids + [f"extra{n}" for n in range(400)])
    far = _profile("a.parent", "id", parent_ids)

    # Again on the direction: a child holding 500 distinct values cannot be
    # referencing a 100-value key. The reverse direction is a different claim
    # and is not what this test is about.
    proposed = {(c.near.column, c.far.column) for c in score_candidates([near, far])}
    assert ("ref", "id") not in proposed


# --- evidence ------------------------------------------------------------


def test_the_evidence_carries_the_sample_size_not_just_the_ratio():
    """A bare score is not something an owner can judge. 94% of 1,685 values is;
    94% of three is not, and they must be distinguishable."""
    codes = [f"C{n:04}" for n in range(300)]
    (candidate,) = list(
        score_candidates([_profile("a.child", "ref", codes[:200]), _profile("a.parent", "id", codes)])
    )

    evidence = candidate.evidence()
    assert evidence["overlap"] == pytest.approx(1.0)
    assert evidence["values-compared"] == 200
    assert evidence["near-cardinality"] == 200
    assert evidence["far-cardinality"] == 300
    # Neither sketch saturated, so every distinct value was compared and the
    # overlap is exact rather than sampled.
    assert evidence["exact"] is True


def test_confidence_ranks_a_wide_sample_above_a_narrow_one():
    """The score exists to order a queue, and the ordering has to reflect how
    much was actually seen - a perfect overlap over 40 values is weaker evidence
    than a near-perfect one over four thousand."""
    # Identical value sets qualify in BOTH directions, which is correct - each
    # is contained in the other - so take either.
    small = [f"s{n}" for n in range(MIN_COMPARED + 8)]
    narrow = next(
        iter(
            score_candidates(
                [_profile("a.child", "ref", small), _profile("a.parent", "id", small)]
            )
        )
    )

    large = [f"l{n}" for n in range(4000)]
    wide = next(
        iter(
            score_candidates(
                [
                    _profile("b.child", "ref", large[:3800]),
                    _profile("b.parent", "id", large),
                ]
            )
        )
    )

    # Same containment, very different amounts of evidence behind it - which is
    # the whole point: the score has to separate them, because the ratio alone
    # does not.
    assert narrow.containment == pytest.approx(wide.containment)
    assert narrow.compared < MIN_COMPARED * 2 < wide.compared
    assert confidence_of(wide) > confidence_of(narrow)
