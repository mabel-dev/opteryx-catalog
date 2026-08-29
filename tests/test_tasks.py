"""Task registration, and the trigger that points at one.

A task is a named catalog object addressed as
`<workspace>.<collection>.<task>` - so it lives beside `datasets` and `views`
under a collection, not inside a dataset. Which dataset's commits fire it is a
separate fact, recorded by a trigger on that dataset.

The Firestore stand-ins are the ones the materialized-view tests use.
"""

from __future__ import annotations

import pytest

from opteryx_catalog.exceptions import TaskAlreadyExists
from opteryx_catalog.exceptions import TaskNotFound
from test_materialized_views import _add_dataset
from test_materialized_views import _catalog


# --- registration


def test_create_and_read_a_task():
    catalog = _catalog()
    catalog.create_task("ops.ingest", sql="SELECT 1", author="xb500", runs_as="federator")

    task = catalog.get_task("ops.ingest")
    assert task["identifier"] == "ws.ops.ingest"
    assert task["sql"] == "SELECT 1"
    assert task["runs-as"] == "federator"
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


def test_runs_as_is_pinned_across_redefinition():
    """Editing a task must never silently transfer whose authority it runs with."""
    catalog = _catalog()
    catalog.create_task("ops.ingest", sql="SELECT 1", author="xb500", runs_as="federator")

    catalog.create_task(
        "ops.ingest", sql="SELECT 2", author="mallory", runs_as="mallory", update_if_exists=True
    )

    assert catalog.get_task("ops.ingest")["runs-as"] == "federator"


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
        author="xb500",
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
        author="xb500",
    )

    with pytest.raises(Exception, match="refusing to repoint"):
        catalog.create_trigger(
            "ops.catalog_changes",
            name="task__ops__ingest",
            target_task="ops.other",
            kind="task",
            author="xb500",
        )
