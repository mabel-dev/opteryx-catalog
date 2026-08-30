"""Relationships a job PROPOSED, and what an owner does with one.

Phase 1 declared fourteen relationships by hand, which proves the mechanism and
not the feature. These are the writes that let the graph populate itself: a job
proposes with evidence, an owner confirms or rejects, and a rejection sticks.

Three rules the tests below exist to hold:

  * A proposal never comes through DDL. `ADD CONSTRAINT` means asserted by
    definition - someone typed it - so proposals get their own entrance.
  * Confirmation UPGRADES the proposal in place. It cannot create, because the
    document is already there.
  * A rejection keys on the COLUMN PAIR, not on the generated constraint name,
    or the next run re-proposes it under a new name.
"""

from __future__ import annotations

import pytest

from opteryx_catalog.exceptions import ConstraintNotFound
from opteryx_catalog.opteryx_catalog import RELATIONSHIP_SUPPRESSIONS_SUBCOLLECTION
from opteryx_catalog.opteryx_catalog import generated_constraint_name
from opteryx_catalog.opteryx_catalog import relationship_pair_digest

# The fake Firestore lives beside the Phase 1 tests. Shared rather than copied:
# it reproduces rules real Firestore enforces (the property-path grammar, the
# collection group registry) that a second, drifting copy would stop enforcing.
from test_declared_relationships import _add_dataset
from test_declared_relationships import _catalog
from test_declared_relationships import _declare


EVIDENCE = {
    "overlap": 0.94,
    "near-cardinality": 1685,
    "far-cardinality": 1791,
    "values-compared": 1685,
    "method": "kmv-sketch",
}


def _seeded():
    catalog = _catalog()
    _add_dataset(catalog, "helpdesk.tickets")
    _add_dataset(catalog, "crm.customers")
    return catalog


def _propose(catalog, **overrides):
    kwargs = {
        "dataset_identifier": "helpdesk.tickets",
        "column": "customer_ref",
        "references_dataset": "crm.customers",
        "references_column": "id",
        "cardinality": "many_to_one",
        "confidence": 0.94,
        "evidence": EVIDENCE,
        "proposer": "inference-job",
    }
    kwargs.update(overrides)
    return catalog.propose_relationship(**kwargs)


# --- proposing ----------------------------------------------------------


def test_a_proposal_is_inferred_and_unverified_and_carries_its_evidence():
    catalog = _seeded()
    name = _propose(catalog)

    (row,) = catalog.list_relationships("helpdesk.tickets")
    assert row["name"] == name
    assert row["origin"] == "inferred"
    assert row["status"] == "unverified"
    assert row["confidence"] == 0.94
    # The evidence, not just the score. An owner cannot judge a bare number.
    assert row["evidence"]["overlap"] == 0.94
    assert row["evidence"]["values-compared"] == 1685
    assert row["proposed-by"] == "inference-job"
    # A proposal has no author and has not been verified. Filling either in
    # with the job's name would make a guess read as a person's statement.
    assert row["asserted-by"] is None
    assert row["verified-at-ms"] is None


def test_a_generated_name_is_stable_so_a_second_run_updates_rather_than_duplicates():
    catalog = _seeded()
    first = _propose(catalog, confidence=0.80)
    second = _propose(catalog, confidence=0.91)

    assert first == second
    rows = catalog.list_relationships("helpdesk.tickets")
    assert len(rows) == 1
    assert rows[0]["confidence"] == 0.91


def test_a_generated_name_distinguishes_two_targets_for_one_column():
    catalog = _seeded()
    _add_dataset(catalog, "crm.accounts")
    _propose(catalog)
    _propose(catalog, references_dataset="crm.accounts", references_column="id")

    names = {row["name"] for row in catalog.list_relationships("helpdesk.tickets")}
    assert len(names) == 2


def test_a_proposal_does_not_overwrite_a_relationship_a_person_asserted():
    catalog = _seeded()
    _declare(catalog)  # asserted, under a name of the owner's choosing

    assert _propose(catalog) is None
    (row,) = catalog.list_relationships("helpdesk.tickets")
    assert row["name"] == "tickets_customer_fk"
    assert row["origin"] == "asserted"


def test_confidence_outside_zero_to_one_is_refused():
    catalog = _seeded()
    with pytest.raises(ValueError, match="confidence"):
        _propose(catalog, confidence=1.4)


# --- confirming ---------------------------------------------------------


def test_confirming_upgrades_the_proposal_in_place_rather_than_creating():
    catalog = _seeded()
    name = _propose(catalog)

    _declare(catalog, constraint_name=name)

    rows = catalog.list_relationships("helpdesk.tickets")
    assert len(rows) == 1, "confirmation created a second row instead of upgrading"
    row = rows[0]
    assert row["origin"] == "asserted"
    assert row["status"] == "active"
    assert row["asserted-by"] == "olive"
    # What it was proposed on is kept: the row began as a machine proposal and
    # a reader of the graph is entitled to see that.
    assert row["confidence"] == 0.94
    assert row["evidence"]["overlap"] == 0.94
    assert row["proposed-by"] == "inference-job"


def test_confirming_under_a_name_of_the_owners_own_choosing_replaces_the_proposal():
    catalog = _seeded()
    _propose(catalog)

    _declare(catalog, constraint_name="tickets_customer_fk")

    rows = catalog.list_relationships("helpdesk.tickets")
    assert len(rows) == 1, "the proposal survived beside its own confirmation"
    assert rows[0]["name"] == "tickets_customer_fk"
    assert rows[0]["origin"] == "asserted"


def test_a_confirmed_pair_is_not_proposed_again():
    catalog = _seeded()
    _propose(catalog)
    _declare(catalog, constraint_name="tickets_customer_fk")

    assert _propose(catalog) is None
    assert len(catalog.list_relationships("helpdesk.tickets")) == 1


def test_confirming_takes_the_cardinality_from_the_statement_not_the_guess():
    catalog = _seeded()
    name = _propose(catalog, cardinality="many_to_many")

    _declare(catalog, constraint_name=name, cardinality="one_to_one")

    (row,) = catalog.list_relationships("helpdesk.tickets")
    assert row["cardinality"] == "one_to_one"


def test_a_genuine_duplicate_name_still_raises():
    catalog = _seeded()
    _declare(catalog)
    with pytest.raises(ValueError, match="already exists"):
        _declare(catalog, column="other_ref")


# --- rejecting ----------------------------------------------------------


def test_rejecting_removes_the_proposal_and_records_a_suppression():
    catalog = _seeded()
    name = _propose(catalog)

    assert catalog.reject_relationship(
        "helpdesk.tickets", name, author="olive", reason="different customer scheme"
    )

    assert catalog.list_relationships("helpdesk.tickets") == []
    (suppression,) = catalog.list_relationship_suppressions("helpdesk.tickets")
    assert suppression["rejected-by"] == "olive"
    assert suppression["reason"] == "different customer scheme"
    # What was rejected, so the record reads as a decision rather than a bare
    # prohibition.
    assert suppression["rejected-confidence"] == 0.94
    assert suppression["rejected-evidence"]["overlap"] == 0.94


def test_a_rejected_pair_is_not_proposed_again():
    catalog = _seeded()
    name = _propose(catalog)
    catalog.reject_relationship("helpdesk.tickets", name, author="olive")

    assert _propose(catalog) is None
    assert catalog.list_relationships("helpdesk.tickets") == []


def test_the_suppression_keys_on_the_pair_not_on_the_generated_name():
    catalog = _seeded()
    name = _propose(catalog)
    catalog.reject_relationship("helpdesk.tickets", name, author="olive")

    (suppression,) = catalog.list_relationship_suppressions("helpdesk.tickets")
    assert suppression["pair-digest"] == relationship_pair_digest(
        "customer_ref", "crm", "customers", "id"
    )
    # And the document is keyed by it, so a second rejection of the same pair
    # is the same record rather than a second one.
    docs = (
        catalog._dataset_doc_ref("helpdesk", "tickets")
        .collection(RELATIONSHIP_SUPPRESSIONS_SUBCOLLECTION)
        .stream()
    )
    assert [doc.id for doc in docs] == [suppression["pair-digest"]]


def test_dropping_a_proposal_rejects_it():
    """The Studio's dismiss button, and the reason it needs no new grammar.

    There is no reason to DROP a proposal except to answer "no" to it, and a
    removal that left no record would have the next run ask the same question
    again.
    """
    catalog = _seeded()
    name = _propose(catalog)

    assert catalog.drop_relationship("helpdesk.tickets", name, author="olive")

    assert catalog.list_relationships("helpdesk.tickets") == []
    (suppression,) = catalog.list_relationship_suppressions("helpdesk.tickets")
    assert suppression["rejected-by"] == "olive"
    assert _propose(catalog) is None


def test_rejecting_a_relationship_a_person_asserted_is_refused():
    catalog = _seeded()
    _declare(catalog)

    with pytest.raises(ValueError, match="DROP CONSTRAINT"):
        catalog.reject_relationship("helpdesk.tickets", "tickets_customer_fk", author="olive")


def test_rejecting_something_that_is_not_there_raises():
    catalog = _seeded()
    with pytest.raises(ConstraintNotFound):
        catalog.reject_relationship("helpdesk.tickets", "nope", author="olive")


def test_dropping_an_asserted_relationship_leaves_no_suppression():
    """DROP CONSTRAINT says what it does. Removing a relationship someone
    declared should not quietly forbid inference from ever raising it again -
    that is what rejecting a proposal is for."""
    catalog = _seeded()
    _declare(catalog)
    catalog.drop_relationship("helpdesk.tickets", "tickets_customer_fk", author="olive")

    assert catalog.list_relationship_suppressions("helpdesk.tickets") == []


# --- naming -------------------------------------------------------------


def test_a_generated_name_is_readable_and_marked_as_machine_made():
    name = generated_constraint_name("customer_ref", "crm", "customers", "id")
    assert name.startswith("inferred_customer_ref_")


def test_a_column_name_that_is_not_an_identifier_still_makes_a_usable_name():
    name = generated_constraint_name("customer ref/2", "crm", "customers", "id")
    assert name.startswith("inferred_customer_ref_2_")
    assert all(character.isalnum() or character == "_" for character in name)


def test_the_pair_digest_ignores_case_but_not_the_far_end():
    assert relationship_pair_digest("Customer_Ref", "CRM", "Customers", "ID") == (
        relationship_pair_digest("customer_ref", "crm", "customers", "id")
    )
    assert relationship_pair_digest("customer_ref", "crm", "customers", "id") != (
        relationship_pair_digest("customer_ref", "crm", "accounts", "id")
    )
