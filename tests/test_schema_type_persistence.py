"""The stored `type` is the TYPE, not its dispatch CATEGORY.

CTAS hands `create_dataset` a relation schema whose columns carry Opteryx
`ColumnType` objects, and `_core_type_to_stored` decides what string lands in
the schema document. Storing `column_type.category.name` there is lossy in the
one direction nothing downstream can detect - it silently WIDENS:

    IPV4   -> "INTEGER" -> read back as INT64   (descriptor destroyed)
    UINT32 -> "INTEGER" -> read back as INT64   (unsigned becomes signed)

IPv4's category is INTEGER deliberately - that is what makes ordering, grouping
and joins run on the raw uint32 - so the category is exactly the information
that cannot round-trip an address, and every unsigned and narrow width collapses
the same way. Nothing errors; a `CREATE TABLE ... AS SELECT` over an IPV4 column
just produces a table the catalog describes as INT64, and the addresses render
as numbers from then on.

The stubs below mirror that trap on purpose: every type under test reports
`category.name == "INTEGER"`, so a category-based writer passes an equality
check against INT64 while an exact-spelling writer is the only thing that can
distinguish them. That is why these assert on the spelling rather than on
"something integer-ish".

No opteryx-core import: the catalog duck-types the schema it is handed
precisely so it does not depend on the engine (see `_schema_to_columns`), and
opteryx-core is not a test dependency. `test_round_trips_through_opteryx_core`
closes the loop against the real parser when it happens to be installed.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from opteryx_catalog.catalog.dataset import _stored_type_display
from opteryx_catalog.opteryx_catalog import OpteryxCatalog
from opteryx_catalog.opteryx_catalog import _core_type_to_stored

# The types this regression is about: an IPV4 and every unsigned width. Each is
# spelled exactly as `str(ColumnType)` renders it, because that spelling IS the
# persisted format - `parse_column_type` reads these back unchanged.
LOSSY_TYPES = ["IPV4", "UINT8", "UINT16", "UINT32", "UINT64"]


class _Category:
    def __init__(self, name):
        self.name = name


class _ColumnType:
    """Stands in for an Opteryx `ColumnType`.

    Only the two attributes `_core_type_to_stored` touches: `str()` (the exact
    spelling) and `.category.name` (the dispatch projection). Both are supplied,
    and they DISAGREE for every type here - that disagreement is the test.
    """

    def __init__(self, name, category="INTEGER"):
        self._name = name
        self.category = _Category(category)
        self.logical = None
        self.element = None

    def __str__(self):
        return self._name


class _SchemaColumn:
    def __init__(self, name, column_type):
        self.name = name
        self.column_type = column_type


class _RelationSchema:
    """A `.columns` list of `.name`/`.column_type` - the shape CTAS passes."""

    def __init__(self, columns):
        self.columns = columns


def _ctas_schema():
    """The schema a `CREATE TABLE ... AS SELECT` over an IPV4-and-unsigneds
    source produces, plus a plain INT64 as the control."""
    columns = [_SchemaColumn(name.lower(), _ColumnType(name)) for name in LOSSY_TYPES]
    columns.append(_SchemaColumn("plain", _ColumnType("INT64")))
    return _RelationSchema(columns)


# --- the writer --------------------------------------------------------------


@pytest.mark.parametrize("type_name", LOSSY_TYPES)
def test_core_type_to_stored_keeps_the_exact_spelling(type_name):
    stored, element_type, precision, scale = _core_type_to_stored(_ColumnType(type_name))

    assert stored == type_name
    # The category is INTEGER for all of these, so this is the assertion that
    # actually fails if the writer regresses to `column_type.category.name`.
    assert stored != "INTEGER"
    assert (element_type, precision, scale) == (None, None, None)


def test_schema_to_columns_persists_every_type_distinctly():
    cols = OpteryxCatalog._schema_to_columns(None, _ctas_schema())

    assert [c["type"] for c in cols] == LOSSY_TYPES + ["INT64"]
    # Distinctness is the property that widening destroys: collapsed to their
    # category these are five copies of "INTEGER" plus the control.
    assert len({c["type"] for c in cols}) == len(LOSSY_TYPES) + 1


# --- the full create path ----------------------------------------------------


class _Doc:
    def __init__(self, id_, data=None, exists=True):
        self.id = id_
        self.exists = exists
        self._data = data or {}

    def to_dict(self):
        return self._data


class _DocRef:
    def __init__(self, id_, log=None):
        self.id = id_
        self._doc = _Doc(id_, exists=False)
        self._subcollections = {}
        self.log = log if log is not None else []
        self.written = None

    def get(self):
        return self._doc

    def set(self, data):
        self.written = data
        self._doc = _Doc(self.id, dict(data), exists=True)

    def update(self, data):
        merged = dict(self._doc._data)
        merged.update(data)
        self._doc = _Doc(self.id, merged, exists=True)

    def collection(self, name):
        if name not in self._subcollections:
            self._subcollections[name] = _Collection(name, log=self.log)
        return self._subcollections[name]


class _Collection:
    def __init__(self, name, log=None):
        self.name = name
        self.log = log if log is not None else []
        self._docs = {}

    def document(self, doc_id):
        if doc_id not in self._docs:
            self._docs[doc_id] = _DocRef(doc_id, log=self.log)
        return self._docs[doc_id]

    def stream(self):
        return [ref._doc for ref in self._docs.values() if ref._doc.exists]


def _catalog():
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog.gcs_bucket = "bucket"
    catalog.io = None
    catalog._snapshot_cache = {}
    catalog._schema_cache = {}
    catalog._catalog_ref = _Collection("ws")
    return catalog


def _created_schema_columns(catalog):
    """The columns of the schema document `create_dataset` actually wrote -
    read back the way the dataset's `current-schema-id` finds it."""
    dataset_ref = catalog._catalog_ref.document("coll").collection("datasets").document("tbl")
    sid = dataset_ref.get().to_dict()["current-schema-id"]
    return dataset_ref.collection("schemas").document(sid).get().to_dict()["columns"]


def test_create_dataset_persists_the_ctas_types_unwidened():
    catalog = _catalog()

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.create_dataset("coll.tbl", schema=_ctas_schema(), author="alice")

    columns = _created_schema_columns(catalog)

    assert [c["name"] for c in columns] == [n.lower() for n in LOSSY_TYPES] + ["plain"]
    assert [c["type"] for c in columns] == LOSSY_TYPES + ["INT64"]


def test_created_schema_reads_back_with_the_same_types():
    """The catalog's own reader (`describe`, `get_dataset_schema`) must render
    what was written, not a widened stand-in."""
    catalog = _catalog()

    with patch("opteryx_catalog.opteryx_catalog.send_webhook"):
        catalog.create_dataset("coll.tbl", schema=_ctas_schema(), author="alice")

    displayed = [_stored_type_display(c) for c in _created_schema_columns(catalog)]

    assert displayed == LOSSY_TYPES + ["INT64"]


# --- against the real engine, when it is installed ---------------------------


def _opteryx_logical_type():
    """opteryx-core's `logical_type`, or None when it is absent or predates
    IPV4 - it is not a test dependency, and the older releases this catalog
    still supports have no IPV4 type to round-trip."""
    try:
        from opteryx.types import logical_type
    except ImportError:
        return None
    return logical_type if hasattr(logical_type, "IPV4") else None


@pytest.mark.parametrize("type_name", LOSSY_TYPES)
def test_round_trips_through_opteryx_core(type_name):
    """What the stubs assert about spelling, this asserts about meaning: the
    real parser must hand back the identical `ColumnType`, IPV4 descriptor and
    unsigned width included."""
    logical_type = _opteryx_logical_type()
    if logical_type is None:
        pytest.skip("opteryx-core with IPV4 support is not installed")

    original = logical_type.parse_column_type(type_name)
    stored, _element_type, _precision, _scale = _core_type_to_stored(original)
    restored = logical_type.parse_column_type(stored)

    assert restored == original
    # Belt and braces: equal types must also be the same physical width, so a
    # future `__eq__` that compares only categories cannot mask a regression.
    assert restored.physical == original.physical


def test_opteryx_core_categories_really_are_ambiguous():
    """Pins the premise the stubs encode - if IPV4 and the unsigned widths ever
    stop sharing INTEGER, these tests are asserting against a trap that no
    longer exists and should be revisited."""
    logical_type = _opteryx_logical_type()
    if logical_type is None:
        pytest.skip("opteryx-core with IPV4 support is not installed")

    categories = {logical_type.parse_column_type(name).category.name for name in LOSSY_TYPES}
    assert categories == {"INTEGER"}
