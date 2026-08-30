"""Verification and decay - design §9, and it is TWO mechanisms, not one job.

The design implied a single periodic checker. That is wrong for half of it:

  EXISTENCE is event-driven, caught on the write that removes the thing. A
  rename, a drop, a dropped column each know for certain, and for free, what
  just stopped existing - and `find_relationships_to` answers "what points at
  this" in one collection group query. Scanning for it later would re-derive,
  after a delay, what a write path had in hand.

  VALUES ride the inference job, because re-scoring a declared pair is the
  identical operation to scoring a candidate one - same sketches, same bounded
  read, same containment maths. The proof is in the history: the Phase 2
  inference run found `ops.stdout_log.insert_id -> ops.stderr_log.insert_id`
  scoring 0.00 over 952 values while INFERRING, not while verifying.

The rule that binds both: A BROKEN EDGE IS KEPT. "These used to correspond and
no longer do" is information. Deleting it hides a data problem.
"""

from __future__ import annotations

import pytest

from opteryx_catalog.inference.relationships import ColumnProfile
from opteryx_catalog.inference.relationships import observed_cardinality
from opteryx_catalog.inference.relationships import value_hash
from opteryx_catalog.inference.relationships import verify_declared

from test_declared_relationships import _add_dataset
from test_declared_relationships import _catalog
from test_declared_relationships import _declare


# --- helpers ------------------------------------------------------------


def _seeded():
    catalog = _catalog()
    _add_dataset(catalog, "helpdesk.tickets")
    _add_dataset(catalog, "crm.customers")
    return catalog


def _row(catalog, identifier, name):
    for row in catalog.list_relationships(identifier):
        if row["name"] == name:
            return row
    raise AssertionError(f"no relationship {name} on {identifier}")


def _profile(collection, dataset, column, values, *, cardinality=None, non_null=None):
    """One column, profiled from a literal list of values.

    Sketches are deliberately left UNSATURATED - far below SKETCH_K - so each
    one holds its column's complete distinct set and the containment below is
    exact rather than sampled. That keeps these tests about the decision being
    made, not about the estimator.
    """
    hashes = sorted({value_hash(value) for value in values})
    return ColumnProfile(
        workspace="ws",
        collection=collection,
        dataset=dataset,
        column=column,
        family="varchar",
        cardinality=len(hashes) if cardinality is None else cardinality,
        non_null=len(values) if non_null is None else non_null,
        sketch=hashes,
        saturated=False,
        rows_sampled=len(values),
        rows_total=len(values),
    )


# --- Half 1: existence, caught at DDL time ------------------------------


def test_dropping_a_dataset_breaks_the_relationships_that_pointed_at_it():
    catalog = _seeded()
    _declare(catalog)

    catalog.drop_dataset("crm.customers", author="olive")

    # KEPT, not deleted. The row is the only remaining record that anything
    # depended on the dropped dataset.
    row = _row(catalog, "helpdesk.tickets", "tickets_customer_fk")
    assert row["status"] == "broken"
    assert row["broken-reason"] == "dataset-dropped"
    assert "crm.customers" in row["broken-detail"]
    # An existence check IS a check, and the strongest kind - it observed
    # rather than sampled - so it stamps when it was made.
    assert row["verified-at-ms"] is not None


def test_a_break_keeps_the_evidence_the_row_already_carried():
    catalog = _seeded()
    name = catalog.propose_relationship(
        dataset_identifier="helpdesk.tickets",
        column="customer_ref",
        references_dataset="crm.customers",
        references_column="id",
        cardinality="many_to_one",
        confidence=0.94,
        evidence={"overlap": 0.94, "values-compared": 1685},
        proposer="inference-job",
    )

    catalog.drop_dataset("crm.customers", author="olive")

    row = _row(catalog, "helpdesk.tickets", name)
    assert row["status"] == "broken"
    # "These used to correspond" is only readable if what was measured survives.
    assert row["evidence"]["values-compared"] == 1685
    assert row["confidence"] == 0.94


def test_renaming_a_dataset_repoints_inbound_references_rather_than_breaking_them():
    # Driven through `_repoint_inbound_relationships` rather than through
    # `rename_dataset`, which moves every data file the dataset references and
    # wants a GCS bucket this harness does not have. What is under test is the
    # step `rename_dataset` now calls, not the copy around it.
    catalog = _seeded()
    _declare(catalog)

    catalog._repoint_inbound_relationships("crm", "customers", "crm", "accounts", author="olive")

    row = _row(catalog, "helpdesk.tickets", "tickets_customer_fk")
    # A renamed dataset is the SAME dataset. Breaking this would report a data
    # problem where there is none, and an owner who "fixed" it by dropping the
    # constraint would lose a true relationship to a bookkeeping event.
    assert row["status"] == "active"
    assert row["references-dataset"] == "accounts"
    assert row["references-collection"] == "crm"


def test_a_repointed_reference_is_found_by_the_reverse_lookup_at_its_new_name():
    catalog = _seeded()
    _declare(catalog)

    catalog._repoint_inbound_relationships("crm", "customers", "crm", "accounts", author="olive")

    assert catalog.find_relationships_to("crm.customers") == []
    (inbound,) = catalog.find_relationships_to("crm.accounts")
    assert inbound["name"] == "tickets_customer_fk"


def test_relationships_through_a_column_report_both_directions_and_which_is_which():
    catalog = _seeded()
    _declare(catalog)

    outbound = catalog.relationships_through_column("helpdesk.tickets", "customer_ref")
    assert [row["inbound"] for row in outbound] == [False]

    # The same relationship, seen from the end it references. Flagged inbound
    # because its near end may be a dataset the asker cannot read - which is
    # why a caller must not name it in a message (§8.2).
    inbound = catalog.relationships_through_column("crm.customers", "id")
    assert [row["inbound"] for row in inbound] == [True]


def test_a_column_lookup_ignores_columns_the_relationship_does_not_run_through():
    catalog = _seeded()
    _declare(catalog)

    assert catalog.relationships_through_column("helpdesk.tickets", "subject") == []


def test_dropping_a_column_breaks_relationships_in_both_directions_and_deletes_none():
    catalog = _seeded()
    _declare(catalog)

    broken = catalog.break_relationships_through_column(
        "crm.customers", "id", author="olive"
    )

    assert [row["inbound"] for row in broken] == [True]
    row = _row(catalog, "helpdesk.tickets", "tickets_customer_fk")
    assert row["status"] == "broken"
    assert row["broken-reason"] == "column-dropped"
    assert "crm.customers.id" in row["broken-detail"]


# --- Half 2: values, riding the inference job ---------------------------


def test_a_declared_pair_the_data_no_longer_supports_is_broken():
    # The production case this mechanism exists for: two log streams declared
    # to share insert ids, which score 0.00 over the values compared.
    catalog = _seeded()
    _declare(catalog)

    profiles = [
        _profile("helpdesk", "tickets", "customer_ref", [f"a{i}" for i in range(100)]),
        _profile("crm", "customers", "id", [f"b{i}" for i in range(100)]),
    ]

    (result,) = verify_declared(catalog, profiles)
    assert result.outcome == "broken"
    assert result.containment == 0.0
    assert result.compared == 100


def test_a_declared_pair_that_still_holds_is_stamped_active():
    catalog = _seeded()
    _declare(catalog)

    shared = [f"c{i}" for i in range(100)]
    profiles = [
        _profile("helpdesk", "tickets", "customer_ref", shared),
        _profile("crm", "customers", "id", shared + [f"d{i}" for i in range(50)]),
    ]

    (result,) = verify_declared(catalog, profiles)
    assert result.outcome == "active"
    assert result.containment == 1.0


def test_an_unprofiled_end_is_not_verifiable_and_is_never_marked_broken():
    catalog = _seeded()
    _declare(catalog)

    # Only the near end was profiled. The far one may be the wrong type, on a
    # dataset that failed to read, or in a collection this run was scoped away
    # from - and inferring "gone" from "not measured" would mark half a
    # catalog broken the first time a read failed.
    profiles = [
        _profile("helpdesk", "tickets", "customer_ref", [f"a{i}" for i in range(100)])
    ]

    (result,) = verify_declared(catalog, profiles)
    assert result.outcome == "not-verifiable"
    assert result.status_to_write is None


def test_too_few_values_compared_is_not_verifiable_rather_than_broken():
    catalog = _seeded()
    _declare(catalog)

    # A containment of 0.00 over three values is no more evidence of a break
    # than 1.00 over three is evidence of a relationship. Saturated, so the
    # complete-set waiver does not apply.
    near = _profile("helpdesk", "tickets", "customer_ref", ["a1", "a2", "a3"])
    near.saturated = True
    far = _profile("crm", "customers", "id", ["b1", "b2", "b3"])

    (result,) = verify_declared(catalog, [near, far])
    assert result.outcome == "not-verifiable"


def test_verification_does_not_promote_a_proposal_a_person_has_not_confirmed():
    catalog = _seeded()
    catalog.propose_relationship(
        dataset_identifier="helpdesk.tickets",
        column="customer_ref",
        references_dataset="crm.customers",
        references_column="id",
        cardinality="many_to_one",
        confidence=0.94,
        evidence={"overlap": 0.94},
        proposer="inference-job",
    )

    shared = [f"c{i}" for i in range(100)]
    profiles = [
        _profile("helpdesk", "tickets", "customer_ref", shared),
        _profile("crm", "customers", "id", shared),
    ]

    (result,) = verify_declared(catalog, profiles)
    assert result.outcome == "active"
    # A machine re-measuring its own guess does not turn it into a claim. Only
    # a person's confirmation does (§7.4).
    assert result.status_to_write is None


def test_a_mid_band_score_records_the_measurement_and_leaves_the_status_alone():
    catalog = _seeded()
    _declare(catalog)

    shared = [f"c{i}" for i in range(70)]
    profiles = [
        _profile("helpdesk", "tickets", "customer_ref", shared + [f"x{i}" for i in range(30)]),
        _profile("crm", "customers", "id", shared),
    ]

    (result,) = verify_declared(catalog, profiles)
    # Neither good enough to confirm nor bad enough to condemn. Breaking a
    # person's declaration is an accusation; it should not fire on noise.
    assert result.outcome == "inconclusive"
    assert result.status_to_write is None


def test_the_plausibility_filters_are_not_applied_to_a_pair_a_person_declared():
    # A cross-collection relationship is UNPROPOSABLE - `_plausible` requires
    # both ends in one collection - and perfectly true. Refusing to verify one
    # would leave exactly the relationships people had to declare by hand as
    # the ones nothing ever checks.
    catalog = _catalog()
    _add_dataset(catalog, "helpdesk.tickets")
    _add_dataset(catalog, "billing.accounts")
    _declare(catalog, references_dataset="billing.accounts")

    profiles = [
        _profile("helpdesk", "tickets", "customer_ref", [f"a{i}" for i in range(100)]),
        _profile("billing", "accounts", "id", [f"b{i}" for i in range(100)]),
    ]

    (result,) = verify_declared(catalog, profiles)
    assert result.outcome == "broken"


# --- cardinality, contradicted from the same profiles -------------------


def test_a_declared_many_to_one_that_is_really_many_to_many_is_contradicted():
    catalog = _seeded()
    _declare(catalog, cardinality="many_to_one")

    shared = [f"c{i}" for i in range(100)]
    profiles = [
        _profile("helpdesk", "tickets", "customer_ref", shared),
        # The far end repeats its values, so it is not a key: one near row
        # matches many far rows and every number joined through this inflates.
        _profile("crm", "customers", "id", shared, cardinality=100, non_null=5000),
    ]

    (result,) = verify_declared(catalog, profiles)
    assert result.outcome == "active"
    assert result.cardinality_observed == "many_to_many"
    assert result.cardinality_contradicted is True


def test_a_contradicted_cardinality_is_recorded_beside_the_declaration_not_over_it():
    catalog = _seeded()
    _declare(catalog, cardinality="many_to_one")

    catalog.record_relationship_verification(
        "helpdesk.tickets",
        "tickets_customer_fk",
        status="active",
        evidence={"overlap": 1.0},
        cardinality_observed="many_to_many",
        verifier="inference-job",
    )

    row = _row(catalog, "helpdesk.tickets", "tickets_customer_fk")
    # The person who wrote many_to_one is the authority on what the
    # relationship MEANS; the measurement says what the data currently IS.
    # Overwriting would destroy the disagreement, which is the finding.
    assert row["cardinality"] == "many_to_one"
    assert row["cardinality-observed"] == "many_to_many"


def test_an_agreeing_cardinality_is_not_reported_as_a_contradiction():
    catalog = _seeded()
    _declare(catalog, cardinality="many_to_one")

    shared = [f"c{i}" for i in range(100)]
    profiles = [
        _profile("helpdesk", "tickets", "customer_ref", shared, non_null=500),
        _profile("crm", "customers", "id", shared),
    ]

    (result,) = verify_declared(catalog, profiles)
    assert result.cardinality_observed == "many_to_one"
    assert result.cardinality_contradicted is False


def test_observed_cardinality_tests_the_far_end_rather_than_assuming_it_is_a_key():
    shared = [f"c{i}" for i in range(100)]
    near = _profile("helpdesk", "tickets", "customer_ref", shared)
    key = _profile("crm", "customers", "id", shared)
    not_key = _profile("crm", "customers", "id", shared, cardinality=100, non_null=5000)

    assert observed_cardinality(near, key) == "one_to_one"
    assert observed_cardinality(near, not_key) == "many_to_many"


# --- writing it back ----------------------------------------------------


def test_a_verification_write_is_narrow_and_keeps_everything_else_on_the_row():
    catalog = _seeded()
    _declare(catalog)

    catalog.record_relationship_verification(
        "helpdesk.tickets",
        "tickets_customer_fk",
        status="broken",
        evidence={"overlap": 0.0, "values-compared": 952},
        verifier="inference-job",
    )

    row = _row(catalog, "helpdesk.tickets", "tickets_customer_fk")
    assert row["status"] == "broken"
    assert row["broken-reason"] == "values-diverged"
    assert row["evidence"]["values-compared"] == 952
    assert row["verified-at-ms"] is not None
    # The declaration itself is untouched - a break is a statement about the
    # data, not a retraction of what someone said.
    assert row["asserted-by"] == "olive"
    assert row["column"] == "customer_ref"
    assert row["references-column"] == "id"


def test_a_relationship_that_comes_back_stops_reading_as_broken():
    catalog = _seeded()
    _declare(catalog)
    catalog.record_relationship_verification(
        "helpdesk.tickets", "tickets_customer_fk", status="broken", verifier="job"
    )

    catalog.record_relationship_verification(
        "helpdesk.tickets", "tickets_customer_fk", status="active", verifier="job"
    )

    row = _row(catalog, "helpdesk.tickets", "tickets_customer_fk")
    assert row["status"] == "active"
    # A repair is as much a fact as a break. A row that stayed marked broken
    # forever is how a status column stops being believed.
    assert row["broken-reason"] is None
    assert row["broken-at-ms"] is None


def test_an_inconclusive_re_score_does_not_erase_why_a_row_broke():
    catalog = _seeded()
    _declare(catalog)
    catalog.drop_dataset("crm.customers", author="olive")

    catalog.record_relationship_verification(
        "helpdesk.tickets", "tickets_customer_fk", status=None, verifier="job"
    )

    row = _row(catalog, "helpdesk.tickets", "tickets_customer_fk")
    assert row["status"] == "broken"
    # `status=None` knows nothing about why the row broke, so it must not
    # answer the question.
    assert row["broken-reason"] == "dataset-dropped"


def test_an_unknown_verification_status_is_refused():
    catalog = _seeded()
    _declare(catalog)

    with pytest.raises(ValueError):
        catalog.record_relationship_verification(
            "helpdesk.tickets", "tickets_customer_fk", status="probably", verifier="job"
        )


# --- the sweep must never be the thing that fails the statement ---------


def test_a_drop_survives_an_inbound_sweep_that_cannot_run(monkeypatch):
    catalog = _seeded()
    _declare(catalog)

    def _unavailable(_identifier):
        raise RuntimeError("collection group index is not available")

    monkeypatch.setattr(catalog, "find_relationships_to", _unavailable)
    catalog.drop_dataset("crm.customers", author="olive")

    # The drop had been decided; failing it because a METADATA sweep could not
    # run would make a dataset undroppable whenever the index is unavailable.
    # What is left behind is a stale inbound row - the shape `fsck` sweeps for.
    assert catalog.list_datasets("crm") == []
    row = _row(catalog, "helpdesk.tickets", "tickets_customer_fk")
    assert row["status"] == "active"


def test_the_repoint_helper_is_loud_and_rename_is_what_swallows_it(monkeypatch):
    catalog = _seeded()
    _declare(catalog)

    def _unavailable(_identifier):
        raise RuntimeError("collection group index is not available")

    monkeypatch.setattr(catalog, "find_relationships_to", _unavailable)

    # The helper raises; `rename_dataset` catches and logs. That split matters:
    # by the time rename calls this it has already copied every byte to the new
    # location, so raising through would abort it halfway - files at the new
    # prefix, catalog entry still at the old one. The catch itself is not
    # covered here because `rename_dataset` wants a GCS bucket this harness
    # does not have; what is pinned is that the helper does not fail silently
    # on its own.
    with pytest.raises(RuntimeError):
        catalog._repoint_inbound_relationships("crm", "customers", "crm", "accounts", author="o")
