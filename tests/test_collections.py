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
        def stream(self):
            return [_Doc("$properties"), _Doc("col_a"), _Doc("col_b")]

    c._catalog_ref = MockColl()

    cols = list(c.list_collections())
    assert "$properties" not in cols
    assert set(cols) == {"col_a", "col_b"}


def test_list_collections_handles_errors():
    c = object.__new__(OpteryxCatalog)
    c.workspace = "w"

    class BadColl:
        def stream(self):
            raise RuntimeError("boom")

    c._catalog_ref = BadColl()

    assert list(c.list_collections()) == []
