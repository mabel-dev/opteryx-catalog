from __future__ import annotations

from opteryx_catalog.opteryx_catalog import OpteryxCatalog


class _Doc:
    def __init__(self, id_):
        self.id = id_


def test_list_collections_excludes_properties():
    # Construct catalog without calling __init__ to avoid external I/O
    c = object.__new__(OpteryxCatalog)
    c.workspace = "w"

    class MockColl:
        # list_documents(), not stream(): a collection document that exists only
        # as the parent of subcollections holds no fields of its own and so is
        # invisible to stream(). Listing must not depend on it having any.
        def list_documents(self):
            return [_Doc("$properties"), _Doc("col_a"), _Doc("col_b")]

    c._catalog_ref = MockColl()

    cols = list(c.list_collections())
    assert "$properties" not in cols
    assert set(cols) == {"col_a", "col_b"}


def test_list_collections_handles_errors():
    c = object.__new__(OpteryxCatalog)
    c.workspace = "w"

    class BadColl:
        # ValueError, not RuntimeError: list_collections() only tolerates
        # (ValueError, KeyError, AttributeError) - anything else propagates.
        def list_documents(self):
            raise ValueError("boom")

    c._catalog_ref = BadColl()

    assert list(c.list_collections()) == []
