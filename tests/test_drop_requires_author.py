"""Every drop entry point requires an author, exactly as its create counterpart
does.

A drop is the one mutation whose evidence disappears with it: once the document
is gone, the audit record (and, for datasets, the tombstone) is all that is left
to say who did it. A caller that omitted the author used to be invisible - the
drop succeeded and only the audit table showed the gap, weeks later. These pin
the omission as a loud, immediate failure instead.

The catalogs here are deliberately unwired - `object.__new__` with no Firestore
refs attached. That is the assertion: the check has to fire before any document
is touched, so a check that drifted below the does-it-exist lookup would surface
here as AttributeError rather than ValueError.
"""

from __future__ import annotations

import pytest

from opteryx_catalog.opteryx_catalog import OpteryxCatalog


def _unwired_catalog():
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    return catalog


def test_drop_dataset_requires_author():
    with pytest.raises(ValueError, match="author must be provided"):
        _unwired_catalog().drop_dataset("coll.tbl")


def test_drop_view_requires_author():
    with pytest.raises(ValueError, match="author must be provided"):
        _unwired_catalog().drop_view("coll.v")


def test_drop_collection_requires_author():
    with pytest.raises(ValueError, match="author must be provided"):
        _unwired_catalog().drop_collection("coll")


def test_drop_materialized_view_requires_author():
    with pytest.raises(ValueError, match="author must be provided"):
        _unwired_catalog().drop_materialized_view("coll.mv")


def test_drop_trigger_requires_author():
    with pytest.raises(ValueError, match="author must be provided"):
        _unwired_catalog().drop_trigger("coll.tbl", "trg")


def test_empty_string_author_is_not_accepted_as_attribution():
    """`author=""` is a missing author wearing a costume.

    Callers resolve the author with `session_user or DEFAULT`, and an empty
    string passes that `or` in some call chains but attributes the drop to
    nobody just as effectively as None.
    """
    with pytest.raises(ValueError, match="author must be provided"):
        _unwired_catalog().drop_dataset("coll.tbl", author="")
