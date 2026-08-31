"""Task registration, and the trigger that points at one.

A task is a named catalog object addressed as
`<workspace>.<collection>.<task>` - so it lives beside `datasets` and `views`
under a collection, not inside a dataset. Which dataset's commits fire it is a
separate fact, recorded by a trigger on that dataset.

The Firestore stand-ins are the ones the materialized-view tests use.
"""

from __future__ import annotations

import pytest

from opteryx_catalog.exceptions import PlatformIdentityOwnerRefused
from opteryx_catalog.exceptions import TaskAlreadyExists
from opteryx_catalog.exceptions import TaskNotFound
from test_materialized_views import _add_dataset
from test_materialized_views import _catalog


# --- registration


def test_create_and_read_a_task():
    catalog = _catalog()
    catalog.create_task("ops.ingest", sql="SELECT 1", author="xb500")

    task = catalog.get_task("ops.ingest")
    assert task["identifier"] == "ws.ops.ingest"
    assert task["sql"] == "SELECT 1"
    # A task carries no identity: EXECUTE runs as the invoker, and an unattended
    # run carries the TRIGGER's owner. Storing one here would be a second answer.
    assert "runs-as" not in task
    assert task["created-by"] == "xb500"
    assert task["last-window-to"] is None


def test_a_task_is_not_a_dataset():
    """It must not be scannable or appear where relations are listed."""
    catalog = _catalog()
    catalog.create_task("ops.ingest", sql="SELECT 1", author="xb500")

    assert catalog.list_tasks("ops") == ["ingest"]
    assert list(catalog.list_datasets("ops")) == []


def test_creating_twice_is_refused_unless_asked():
    catalog = _catalog()
    catalog.create_task("ops.ingest", sql="SELECT 1", author="xb500")

    with pytest.raises(TaskAlreadyExists):
        catalog.create_task("ops.ingest", sql="SELECT 2", author="xb500")


def test_redefining_keeps_the_old_statement_as_a_version():
    catalog = _catalog()
    catalog.create_task("ops.ingest", sql="SELECT 1", author="xb500")
    first = catalog.get_task("ops.ingest")["statement-id"]

    catalog.create_task("ops.ingest", sql="SELECT 2", author="olive", update_if_exists=True)
    task = catalog.get_task("ops.ingest")

    assert task["sql"] == "SELECT 2"
    assert task["statement-id"] != first
    # The author of the CURRENT statement, distinct from who created the task.
    assert task["last-updated-by"] == "olive"
    assert task["created-by"] == "xb500"


def test_trigger_owner_is_pinned_across_re_registration():
    """Editing or repointing a trigger must never silently transfer whose
    authority its unattended runs carry - the confused-deputy hole."""
    catalog = _catalog()
    _add_dataset(catalog, "ops.src")
    catalog.create_trigger(
        "ops.src", name="t", target_task="ops.ingest", kind="task", author="olive"
    )
    # Re-registering as somebody else does NOT move the owner.
    catalog.create_trigger(
        "ops.src", name="t", target_task="ops.ingest", kind="task", author="mallory"
    )
    assert catalog.list_triggers("ops.src")[0]["runs-as"] == "olive"

    catalog.drop_trigger("ops.src", "t", author="mallory", missing_ok=True)
    catalog.create_trigger(
        "ops.src", name="t", target_task="ops.ingest", kind="task", author="mallory"
    )
    # A DROP genuinely clears it - that is a deliberate removal, not an edit.
    assert catalog.list_triggers("ops.src")[0]["runs-as"] == "mallory"

    catalog.set_trigger_owner("ops.src", "t", "olive", author="mallory")
    assert catalog.list_triggers("ops.src")[0]["runs-as"] == "olive"


def test_a_platform_identity_cannot_own_a_trigger():
    """`federator` and `xb500` are identities, not accounts: nothing bills them,
    and federator's credential is shipped to every service that commits a
    dataset. Refused at BOTH the pinning points reachable from this library, not
    only through the engine's binder - the two direct calls that pinned the ops
    ingest triggers to federator never went near a binder.

    This catalog is workspace `ws`, which is the ordinary case. `public` is the
    exception - see `test_xb500_may_own_a_trigger_in_public`."""
    catalog = _catalog()
    _add_dataset(catalog, "ops.src")

    # At creation, because the author is what gets pinned.
    with pytest.raises(PlatformIdentityOwnerRefused):
        catalog.create_trigger(
            "ops.src", name="t", target_task="ops.ingest", kind="task", author="federator"
        )
    with pytest.raises(PlatformIdentityOwnerRefused):
        catalog.create_trigger(
            "ops.src", name="t", target_task="ops.ingest", kind="task", author="XB500  "
        )

    # And on transfer.
    catalog.create_trigger(
        "ops.src", name="t", target_task="ops.ingest", kind="task", author="olive"
    )
    with pytest.raises(PlatformIdentityOwnerRefused):
        catalog.set_trigger_owner("ops.src", "t", "federator", author="olive")
    assert catalog.list_triggers("ops.src")[0]["runs-as"] == "olive"


def _public_catalog():
    """A catalog handle on the reserved `public` workspace."""
    catalog = _catalog()
    catalog.workspace = "public"
    return catalog


def test_xb500_may_own_a_trigger_in_public():
    """The one exemption, and the reason the rule cannot apply there.

    No billable account can hold WRITE over `public` - `implicit_grants` caps
    every non-platform identity at `reader` and `validate_pattern` refuses to
    write a policy over the workspace - so demanding a billable owner yields a
    trigger that fails on every fire rather than a safely-owned one. And the run
    is billed: an owner with no billing membership resolves to the house
    account, which is what "updating the public data is on the house" means in
    the metering.
    """
    catalog = _public_catalog()
    _add_dataset(catalog, "security.nvd_updates")

    catalog.create_trigger(
        "security.nvd_updates",
        name="t",
        target_task="security.nvd_merge",
        kind="task",
        author="xb500",
    )
    assert catalog.list_triggers("security.nvd_updates")[0]["runs-as"] == "xb500"


def test_federator_may_not_own_a_trigger_even_in_public():
    """Only the COSTING half of the rationale is answered in `public`. The other
    half is about federator specifically - its credential is shipped to every
    service that commits a dataset - and is untouched by where the trigger
    lives."""
    catalog = _public_catalog()
    _add_dataset(catalog, "security.nvd_updates")

    with pytest.raises(PlatformIdentityOwnerRefused):
        catalog.create_trigger(
            "security.nvd_updates",
            name="t",
            target_task="security.nvd_merge",
            kind="task",
            author="federator",
        )


def test_the_public_exemption_does_not_reach_materialized_views():
    """The MV paths do not pass `workspace`, so they get the unexempted rule.
    Nothing the platform maintains needs an xb500-owned view, and an exemption
    nothing needs is one nobody is checking."""
    catalog = _public_catalog()

    with pytest.raises(PlatformIdentityOwnerRefused):
        catalog.create_materialized_view(
            "security.mv",
            sql="SELECT 1",
            source_tables=["security.nvd_updates"],
            author="xb500",
        )


def test_the_ownership_gate_is_by_exemption_not_by_allowlist():
    """Only the MV refresh kind is exempt - it resolves its identity from the
    view's record and ignores `runs-as`. Any OTHER kind, including ones added
    after this test was written, is gated: a new kind must not arrive
    ungoverned because a list keyed on `task` was never extended."""
    catalog = _catalog()
    _add_dataset(catalog, "ops.src")

    with pytest.raises(PlatformIdentityOwnerRefused):
        catalog.create_trigger(
            "ops.src",
            name="t",
            target_task="ops.ingest",
            kind="http_endpoint",
            author="federator",
        )


def test_a_task_requires_a_statement_and_an_author():
    catalog = _catalog()
    with pytest.raises(ValueError):
        catalog.create_task("ops.ingest", sql="   ", author="xb500")
    with pytest.raises(ValueError):
        catalog.create_task("ops.ingest", sql="SELECT 1", author=None)


def test_missing_task_is_not_found():
    catalog = _catalog()
    with pytest.raises(TaskNotFound):
        catalog.get_task("ops.absent")


def test_a_fully_qualified_name_resolves_to_the_same_task():
    """The name a TRIGGER records is fully qualified, so the firing path hands
    `<workspace>.<collection>.<task>` in. Rejecting that spelling made every
    trigger-fired run raise TaskNotFound in production while the task existed."""
    catalog = _catalog()
    catalog.create_task("ops.ingest", sql="SELECT 1", author="xb500")

    short = catalog.get_task("ops.ingest")
    qualified = catalog.get_task("ws.ops.ingest")

    assert qualified["identifier"] == short["identifier"]
    assert qualified["sql"] == "SELECT 1"


def test_another_workspaces_task_is_refused():
    """A handle bound to one workspace must not silently read another's."""
    catalog = _catalog()
    catalog.create_task("ops.ingest", sql="SELECT 1", author="xb500")

    with pytest.raises(Exception, match="belongs to workspace"):
        catalog.get_task("elsewhere.ops.ingest")


def test_fully_qualified_names_work_across_the_task_api():
    """create/mark/drop must all accept the spelling a trigger records."""
    catalog = _catalog()
    catalog.create_task("ws.ops.ingest", sql="SELECT 1", author="xb500")
    assert catalog.list_tasks("ops") == ["ingest"]

    catalog.mark_task_fired("ws.ops.ingest", status="enqueued", window_to=42)
    assert catalog.get_task("ops.ingest")["last-window-to"] == 42

    catalog.drop_task("ws.ops.ingest", author="xb500")
    with pytest.raises(TaskNotFound):
        catalog.get_task("ops.ingest")


def test_drop_removes_the_task():
    catalog = _catalog()
    catalog.create_task("ops.ingest", sql="SELECT 1", author="xb500")
    catalog.drop_task("ops.ingest", author="xb500")

    with pytest.raises(TaskNotFound):
        catalog.get_task("ops.ingest")


# --- the fired breadcrumb


def test_a_successful_run_advances_the_window_breadcrumb():
    catalog = _catalog()
    catalog.create_task("ops.ingest", sql="SELECT 1", author="xb500")

    catalog.mark_task_fired("ops.ingest", status="enqueued", window_to=200)

    task = catalog.get_task("ops.ingest")
    assert task["last-window-to"] == 200
    assert task["last-fired-status"] == "enqueued"


def test_a_failed_run_leaves_the_breadcrumb_where_it_was():
    """Advancing past a window that was never consumed would hide the gap the
    breadcrumb exists to expose."""
    catalog = _catalog()
    catalog.create_task("ops.ingest", sql="SELECT 1", author="xb500")
    catalog.mark_task_fired("ops.ingest", status="enqueued", window_to=200)

    catalog.mark_task_fired("ops.ingest", status="failed")

    task = catalog.get_task("ops.ingest")
    assert task["last-window-to"] == 200
    assert task["last-fired-status"] == "failed"


# --- the trigger that points at a task


def test_a_trigger_can_target_a_task():
    catalog = _catalog()
    _add_dataset(catalog, "ops.catalog_changes")

    catalog.create_trigger(
        "ops.catalog_changes",
        name="task__ops__ingest",
        target_task="ops.ingest",
        kind="task",
        author="olive",
    )

    trigger = catalog.list_triggers("ops.catalog_changes")[0]
    assert trigger["kind"] == "task"
    assert trigger["target-task"] == "ws.ops.ingest"
    # Both fields are present, one empty, so a reader need not know the kind.
    assert trigger["target-view"] is None


def test_a_trigger_targets_a_view_or_a_task_never_both():
    catalog = _catalog()
    _add_dataset(catalog, "ops.catalog_changes")

    with pytest.raises(ValueError, match="never both"):
        catalog.create_trigger(
            "ops.catalog_changes",
            name="t",
            target_view="ws.m.d",
            target_task="ws.ops.ingest",
            author="xb500",
        )


def test_a_trigger_requires_a_target():
    catalog = _catalog()
    _add_dataset(catalog, "ops.catalog_changes")

    with pytest.raises(ValueError, match="requires a target"):
        catalog.create_trigger("ops.catalog_changes", name="t", author="xb500")


def test_a_task_trigger_will_not_be_repointed():
    """The same guard the view path has: a blind overwrite would leave the first
    target with no trigger and nothing to report it."""
    catalog = _catalog()
    _add_dataset(catalog, "ops.catalog_changes")
    catalog.create_trigger(
        "ops.catalog_changes",
        name="task__ops__ingest",
        target_task="ops.ingest",
        kind="task",
        author="olive",
    )

    with pytest.raises(Exception, match="refusing to repoint"):
        catalog.create_trigger(
            "ops.catalog_changes",
            name="task__ops__ingest",
            target_task="ops.other",
            kind="task",
            author="olive",
        )


# --- the namespace is shared


def test_a_task_cannot_take_a_dataset_name():
    """`workspace.collection.<object>` is ONE namespace. Datasets, views and
    tasks live in three separate subcollections, so nothing about the storage
    prevents a collision — this check is what does."""
    catalog = _catalog()
    _add_dataset(catalog, "ops.thing")

    with pytest.raises(Exception, match="already exists as a dataset"):
        catalog.create_task("ops.thing", sql="SELECT 1", author="xb500")


def test_a_dataset_cannot_take_a_task_name():
    """The check runs in both directions, or it is not a namespace."""
    catalog = _catalog()
    catalog.create_task("ops.thing", sql="SELECT 1", author="xb500")

    with pytest.raises(Exception, match="already exists as a task"):
        catalog.create_dataset("ops.thing", schema=None, author="olive")


def test_redefining_a_task_is_not_a_collision():
    """The same-kind case belongs to the creator's own replace logic."""
    catalog = _catalog()
    catalog.create_task("ops.thing", sql="SELECT 1", author="xb500")

    catalog.create_task("ops.thing", sql="SELECT 2", author="xb500", update_if_exists=True)

    assert catalog.get_task("ops.thing")["sql"] == "SELECT 2"


def test_name_holder_reports_the_kind():
    catalog = _catalog()
    assert catalog.name_holder("ops", "nothing") is None
    catalog.create_task("ops.t", sql="SELECT 1", author="xb500")
    assert catalog.name_holder("ops", "t") == "task"
    _add_dataset(catalog, "ops.d")
    assert catalog.name_holder("ops", "d") == "dataset"
