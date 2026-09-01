"""SECURE: the narrow, sanctioned exemption from the egress lock.

`ALTER WORKSPACE <source> SET egress_protection TO OFF` is the only exemption
that exists without this, and it is all-or-nothing - it unlocks every copy out
of the workspace, for everyone, until somebody remembers to put it back. A
SECURE object is one named statement, into named workspaces, withdrawable on
its own.

The property that makes it worth having rather than a second way to say the
same thing: the record lives under the SOURCE workspace, so only the source can
write it. A flag on the object would be set by whoever may edit the object -
the party the lock protects against.
"""

from __future__ import annotations

import pytest

from opteryx_catalog import OpteryxCatalog
from opteryx_catalog.exceptions import EgressRestricted
from opteryx_catalog.opteryx_catalog import SECURE_OBJECTS_PROPERTY
from test_materialized_views import _add_dataset
from test_materialized_views import _catalog
from test_materialized_views import _register_mv
from test_materialized_views import _set_egress_restriction

TASK = "ws.ops.billing_events_ingest"


def _handle_on(catalog, workspace):
    """A second handle onto the SAME store, bound to another workspace.

    Sharing the store is the point. Two handles over separate fakes agree about
    nothing, so a test asserting a refusal would pass because the record was
    never visible rather than because it was never honoured - green for the
    wrong reason, which is the only kind of green worth nothing.
    """
    other = object.__new__(OpteryxCatalog)
    other.workspace = workspace
    other.firestore_client = catalog.firestore_client
    other._catalog_ref = catalog.firestore_client.collection(workspace)
    return other


# --- the exemption


def test_an_unsanctioned_task_is_refused():
    """The baseline the rest of this file moves off. `ws` is protected by
    default, so this needs no setup."""
    catalog = _catalog()

    with pytest.raises(EgressRestricted):
        catalog.enforce_task_egress(TASK, ["platform.billing.events"])


def test_a_sanctioned_task_copies_out():
    catalog = _catalog()
    catalog.mark_secure(TASK, ["platform"], author="owner")

    catalog.enforce_task_egress(TASK, ["platform.billing.events"])


def test_the_sanction_names_the_object_not_the_workspace():
    """The whole point of it being object-level: the lock stays on for
    everything else, so a second task gets no ride on the first one's sanction."""
    catalog = _catalog()
    catalog.mark_secure(TASK, ["platform"], author="owner")

    with pytest.raises(EgressRestricted):
        catalog.enforce_task_egress("ws.ops.something_else", ["platform.billing.events"])

    # And the workspace flag itself is untouched - this is not a back door to
    # clearing it.
    assert catalog.is_egress_restricted("ws") is True


def test_the_destination_is_pinned():
    """A task's `writes` can be changed by redefining it. An exemption that
    named only the object would follow that redefinition into a workspace its
    source never agreed to - so the destination is half of the key."""
    catalog = _catalog()
    catalog.mark_secure(TASK, ["platform"], author="owner")

    with pytest.raises(EgressRestricted):
        catalog.enforce_task_egress(TASK, ["elsewhere.mirror.events"])


def test_a_withdrawn_sanction_refuses_again():
    catalog = _catalog()
    catalog.mark_secure(TASK, ["platform"], author="owner")
    catalog.clear_secure(TASK, author="owner")

    with pytest.raises(EgressRestricted):
        catalog.enforce_task_egress(TASK, ["platform.billing.events"])


def test_a_materialized_view_can_be_sanctioned_the_same_way():
    """One implementation of what SECURE means, reached through both wrappers -
    the same reasoning that keeps `egress_verdict` and `enforce_egress_policy`
    from drifting.

    Note which workspace does the sanctioning. A view materializes into its own
    workspace, so the foreign end is its SOURCE: `ichnos` is being copied out of
    and `ichnos` is who has to agree. `ws`, which is doing the copying, cannot
    sanction itself into anything."""
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _register_mv(catalog, "mart.daily", sources=("src.a",))

    with pytest.raises(EgressRestricted):
        catalog.enforce_materialized_view_egress("mart.daily", ["ichnos.landing.orders"])

    _handle_on(catalog, "ichnos").mark_secure("ws.mart.daily", ["ws"], author="ichnos-owner")

    catalog.enforce_materialized_view_egress("mart.daily", ["ichnos.landing.orders"])


# --- only the source may sanction


def test_the_sanction_is_written_where_only_the_source_can_write_it():
    """`mark_secure` writes THIS workspace's properties, so a handle bound to
    the destination cannot sanction a copy out of the source. That is what makes
    "source owner only" a property of where the bytes live rather than a check
    someone can forget."""
    catalog = _catalog()
    catalog.mark_secure(TASK, ["platform"], author="owner")

    stored = catalog.firestore_client.collection("ws").document("$properties").get().to_dict()
    assert TASK in stored[SECURE_OBJECTS_PROPERTY]

    # Nothing was written into the destination's properties, which is where a
    # destination-side "we accept this copy" flag would have gone.
    destination_properties = (
        catalog.firestore_client.collection("platform").document("$properties").get().to_dict()
    )
    assert SECURE_OBJECTS_PROPERTY not in (destination_properties or {})


def test_a_destinations_sanction_of_itself_does_not_unlock_the_source():
    """The self-granting shape this design exists to refuse, written out: the
    destination marks the object secure in ITS OWN workspace, and the source
    still refuses because the source is where the answer is read from."""
    catalog = _catalog()
    _handle_on(catalog, "platform").mark_secure(TASK, ["ws"], author="not-the-source-owner")

    with pytest.raises(EgressRestricted):
        catalog.enforce_task_egress(TASK, ["platform.billing.events"])


def test_it_cannot_be_written_through_the_generic_property_setter():
    """`set_workspace_properties` writes whatever it is handed. An exemption is
    shaped, and an unshaped one fails OPEN - so it goes through `mark_secure`,
    which validates, or it does not go."""
    catalog = _catalog()

    with pytest.raises(ValueError, match="reserved"):
        catalog.set_workspace_properties(
            {SECURE_OBJECTS_PROPERTY: {TASK: {"destinations": ["platform"]}}}, author="owner"
        )


# --- what a sanction must say


def test_a_short_identifier_is_refused():
    """The object usually lives in another workspace, so there is no sensible
    workspace to complete a short name with - and completing it with this one
    would sanction a same-named object in the wrong place."""
    catalog = _catalog()

    with pytest.raises(ValueError, match="fully qualified"):
        catalog.mark_secure("ops.ingest", ["platform"], author="owner")


def test_an_exemption_to_nowhere_is_refused():
    catalog = _catalog()

    with pytest.raises(ValueError, match="at least one destination"):
        catalog.mark_secure(TASK, [], author="owner")


def test_destinations_are_workspaces_not_relations():
    """A relation here would never match: the comparison is against the
    destination WORKSPACE, so it would be an exemption that silently exempts
    nothing."""
    catalog = _catalog()

    with pytest.raises(ValueError, match="workspace names"):
        catalog.mark_secure(TASK, ["platform.billing.events"], author="owner")


def test_the_source_cannot_sanction_a_copy_into_itself():
    catalog = _catalog()

    with pytest.raises(ValueError, match="not egress"):
        catalog.mark_secure(TASK, ["ws"], author="owner")


# --- reading it back


def test_a_malformed_record_is_not_secure():
    """The permitting half of a default-closed rule has to be the conservative
    one about records it does not understand - the opposite of `_guard_is_on`,
    and for the same reason. `destinations` as a bare string would otherwise
    exempt every destination whose name it contains."""
    catalog = _catalog()
    catalog.firestore_client.collection("ws").document("$properties").set(
        {SECURE_OBJECTS_PROPERTY: {TASK: {"destinations": "platform"}}}, merge=True
    )

    assert catalog.is_secure(TASK, "platform") is False
    with pytest.raises(EgressRestricted):
        catalog.enforce_task_egress(TASK, ["platform.billing.events"])


def test_a_workspace_with_no_properties_has_sanctioned_nothing():
    catalog = _catalog()

    assert catalog.list_secure("never-heard-of-it") == {}
    assert catalog.is_secure(TASK, "platform", "never-heard-of-it") is False


def test_what_is_sanctioned_is_readable_and_says_who_and_when():
    """An exemption nobody can enumerate is an exemption nobody reviews."""
    catalog = _catalog()
    catalog.mark_secure(TASK, ["platform"], author="owner")

    (record,) = catalog.list_secure().values()
    assert record["destinations"] == ["platform"]
    assert record["secured-by"] == "owner"
    assert record["secured-at-ms"] > 0


def test_withdrawing_something_never_sanctioned_says_so():
    catalog = _catalog()

    with pytest.raises(KeyError):
        catalog.clear_secure(TASK, author="owner")

    catalog.clear_secure(TASK, author="owner", missing_ok=True)
