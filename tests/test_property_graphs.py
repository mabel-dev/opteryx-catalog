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


# --- The definition, taken apart: what a reader draws the graph from ---

# Exactly what the engine's binder renders (`_Element.rendered()`): properties
# as (column, name) pairs, endpoints as (key, vertex, references) triples.
VERTICES = [
    {
        "relation": ["ws", "ops", "people"],
        "name": "p",
        "key": ["id"],
        "label": "Person",
        "properties": [("id", "id"), ("full_name", "name")],
    },
    {
        "relation": ["ws", "ops", "cities"],
        "name": "c",
        "key": ["id"],
        "label": "City",
        "properties": [],
    },
]
EDGES = [
    {
        "relation": ["ws", "ops", "lives_in"],
        "name": "l",
        "key": ["person_id", "city_id"],
        "label": "LivesIn",
        "properties": [("since", "since")],
        "source": (["person_id"], "p", ["id"]),
        "destination": (["city_id"], "c", ["id"]),
    }
]


def _nested_arrays(value) -> bool:
    """True if any list sits directly inside a list - which Firestore refuses."""
    if isinstance(value, dict):
        return any(_nested_arrays(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return any(isinstance(v, (list, tuple)) or _nested_arrays(v) for v in value)
    return False


def test_element_tables_are_stored_beside_the_statement():
    catalog = _catalog()
    catalog.create_property_graph(
        "ops.g", STATEMENT, author="alice", vertex_tables=VERTICES, edge_tables=EDGES
    )
    record = catalog.load_property_graph("ops.g")

    assert record["statement"] == STATEMENT
    assert record["vertex-tables"][0] == {
        "name": "p",
        "label": "Person",
        "relation": ["ws", "ops", "people"],
        "key": ["id"],
        "properties": [
            {"column": "id", "name": "id"},
            {"column": "full_name", "name": "name"},
        ],
    }
    (edge,) = record["edge-tables"]
    assert edge["source"] == {"vertex": "p", "key": ["person_id"], "references": ["id"]}
    assert edge["destination"] == {"vertex": "c", "key": ["city_id"], "references": ["id"]}
    assert not _nested_arrays(record)


def test_a_caller_without_elements_stores_none_and_a_redefinition_drops_them():
    catalog = _catalog()
    catalog.create_property_graph("ops.old", STATEMENT, author="alice")
    assert "vertex-tables" not in catalog.load_property_graph("ops.old")

    catalog.create_property_graph(
        "ops.g", STATEMENT, author="alice", vertex_tables=VERTICES, edge_tables=EDGES
    )
    catalog.create_property_graph("ops.g", STATEMENT, author="alice", update_if_exists=True)
    record = catalog.load_property_graph("ops.g")
    assert "vertex-tables" not in record and "edge-tables" not in record


def test_a_graph_with_no_edges_records_an_empty_edge_list():
    catalog = _catalog()
    catalog.create_property_graph(
        "ops.g", STATEMENT, author="alice", vertex_tables=VERTICES[:1], edge_tables=[]
    )
    assert catalog.load_property_graph("ops.g")["edge-tables"] == []


@pytest.mark.parametrize(
    "vertices, edges, message",
    [
        (VERTICES, None, "given together"),
        (None, EDGES, "given together"),
        ([], [], "at least one vertex table"),
        (VERTICES, [{**EDGES[0], "source": (["person_id"], "nobody", ["id"])}], "not a vertex"),
        (VERTICES, [{**EDGES[0], "destination": None}], r"\(key, vertex, references\)"),
        ([{**VERTICES[0], "key": "id"}], [], "list of strings"),
        ([{**VERTICES[0], "properties": [("id",)]}], [], r"\(column, name\) pair"),
    ],
)
def test_malformed_element_tables_refuse_the_whole_create(vertices, edges, message, capsys):
    catalog = _catalog()
    with pytest.raises(ValueError, match=message):
        catalog.create_property_graph(
            "ops.g", STATEMENT, author="alice", vertex_tables=vertices, edge_tables=edges
        )
    assert not catalog.property_graph_exists("ops.g")
    assert _audit(capsys) == []


def test_list_property_graphs():
    catalog = _catalog()
    assert catalog.list_property_graphs("ops") == []
    catalog.create_property_graph("ops.a", STATEMENT, author="alice")
    catalog.create_property_graph("ops.b", STATEMENT, author="alice")
    catalog.create_view("ops.v", "SELECT 1", author="alice")
    catalog.drop_property_graph("ops.a", author="alice")
    assert catalog.list_property_graphs("ops") == ["b"]


@pytest.mark.parametrize("kind", ["property graph", "task"])
def test_a_collection_holding_a_graph_or_task_cannot_be_dropped(kind):
    """Firestore does not cascade a document delete to its subcollections, so
    dropping the collection document would strand these as ghost paths."""
    from opteryx_catalog.exceptions import CollectionNotEmpty

    catalog = _catalog()
    catalog._collection_ref("ops").set({})
    if kind == "task":
        catalog.create_task("ops.k", sql="SELECT 1", author="alice")
    else:
        catalog.create_property_graph("ops.g", STATEMENT, author="alice")
    with pytest.raises(CollectionNotEmpty):
        catalog.drop_collection("ops", author="alice")
    assert catalog._collection_ref("ops").get().exists
