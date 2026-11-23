"""Property graphs (SQL/PGQ): catalog text in the relation namespace, like a view.

The engine validates a graph and hands over its canonical CREATE PROPERTY GRAPH
statement; the catalog stores that text, shares the name with datasets, views
and tasks, and audits the change.
"""

from __future__ import annotations

import json

import pytest
from test_materialized_views import _add_dataset
from test_materialized_views import _catalog

from opteryx_catalog.exceptions import NameInUse
from opteryx_catalog.exceptions import PropertyGraphAlreadyExists
from opteryx_catalog.exceptions import PropertyGraphNotFound

STATEMENT = "CREATE PROPERTY GRAPH ws.ops.g\n  VERTEX TABLES (\n    ws.ops.t AS t KEY (id)\n      LABEL T NO PROPERTIES\n  );"


def _audit(capsys):
    return [
        json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")
    ]


def test_create_load_and_drop(capsys):
    catalog = _catalog()
    catalog.create_property_graph("ops.g", STATEMENT, author="alice")
    record = catalog.load_property_graph("ops.g")
    assert (record["statement"], record["author"], record["workspace"]) == (
        STATEMENT,
        "alice",
        "ws",
    )
    assert catalog.property_graph_exists(("ops", "g"))
    assert catalog.name_holder("ops", "g") == "property graph"

    catalog.drop_property_graph("ops.g", author="bob")
    assert not catalog.property_graph_exists("ops.g")
    with pytest.raises(PropertyGraphNotFound):
        catalog.load_property_graph("ops.g")

    actions = [(r["action"], r["resource_type"], r["author"]) for r in _audit(capsys)]
    assert actions == [
        ("create_property_graph", "property_graph", "alice"),
        ("drop_property_graph", "property_graph", "bob"),
    ]


def test_create_twice_needs_update_if_exists(capsys):
    catalog = _catalog()
    catalog.create_property_graph("ops.g", STATEMENT, author="alice")
    with pytest.raises(PropertyGraphAlreadyExists):
        catalog.create_property_graph("ops.g", STATEMENT, author="alice")
    catalog.create_property_graph(
        "ops.g", "CREATE PROPERTY GRAPH ...", author="carol", update_if_exists=True
    )
    assert catalog.load_property_graph("ops.g")["author"] == "carol"
    assert [r["action"] for r in _audit(capsys)] == [
        "create_property_graph",
        "update_property_graph",
    ]


def test_author_is_required():
    catalog = _catalog()
    with pytest.raises(ValueError, match="author must be provided"):
        catalog.create_property_graph("ops.g", STATEMENT)
    with pytest.raises(ValueError, match="author must be provided"):
        catalog.drop_property_graph("ops.g")


def test_dropping_a_missing_graph_is_a_no_op(capsys):
    catalog = _catalog()
    catalog.drop_property_graph("ops.nothing", author="alice")
    assert _audit(capsys) == []


def test_a_graph_cannot_take_a_dataset_view_or_task_name():
    catalog = _catalog()
    _add_dataset(catalog, "ops.d")
    catalog.create_view("ops.v", "SELECT 1", author="alice")
    catalog.create_task("ops.k", sql="SELECT 1", author="alice")
    for name, holder in (("d", "dataset"), ("v", "view"), ("k", "task")):
        with pytest.raises(NameInUse, match=f"already exists as a {holder}"):
            catalog.create_property_graph(f"ops.{name}", STATEMENT, author="alice")


def test_a_view_and_a_task_cannot_take_a_graph_name():
    catalog = _catalog()
    catalog.create_property_graph("ops.g", STATEMENT, author="alice")
    with pytest.raises(NameInUse, match="already exists as a property graph"):
        catalog.create_view("ops.g", "SELECT 1", author="alice")
    with pytest.raises(NameInUse, match="already exists as a property graph"):
        catalog.create_task("ops.g", sql="SELECT 1", author="alice")
