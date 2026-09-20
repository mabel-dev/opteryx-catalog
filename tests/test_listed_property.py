"""`listed` decides whether a workspace appears in listings, and nothing else.

`LISTED_PROPERTY` is the declared form of "readable, but not worth putting in
front of everyone". It is read by odata.opteryx when it builds the service
document and `$metadata`; this suite holds the catalog half - that the flag
round-trips, that its default is the status quo, and that it is a property
anyone may set rather than one reserved to an internal writer.

The direction of the default is the part that matters. Its two siblings,
`deletion_protection` and `egress_protection`, default ON because unset must
mean PROTECT. This one defaults ON for the opposite reason: listing is what a
workspace has always done, and hiding one whose owner never asked for it hidden
is a person's data disappearing out of their own catalog.
"""

from __future__ import annotations

from unittest.mock import patch

from opteryx_catalog.opteryx_catalog import LISTED_PROPERTY
from opteryx_catalog.opteryx_catalog import OpteryxCatalog


class _Doc:
    def __init__(self, data=None, exists=True):
        self.exists = exists
        self._data = data

    def to_dict(self):
        return self._data


class _DocRef:
    def __init__(self, doc):
        self._doc = doc

    def get(self):
        return self._doc


def _catalog_reading(properties, exists=True):
    """A catalog whose `$properties` read answers with `properties`."""
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "samples"
    ref = _DocRef(_Doc(properties, exists=exists))
    with patch.object(OpteryxCatalog, "_foreign_properties_ref", return_value=ref):
        return catalog.is_listed()


def test_a_workspace_with_no_opinion_is_listed():
    assert _catalog_reading({}) is True


def test_listed_false_is_not_listed():
    assert _catalog_reading({LISTED_PROPERTY: False}) is False


def test_listed_true_is_listed():
    assert _catalog_reading({LISTED_PROPERTY: True}) is True


def test_an_unrecognised_value_reads_as_listed():
    # `_guard_is_on` only honours an explicit falsey value, so a hand-written
    # "off" - a truthy string - leaves the workspace listed. Hiding one on the
    # strength of a typo is the wrong way for this flag to be wrong.
    assert _catalog_reading({LISTED_PROPERTY: "off"}) is True


def test_a_workspace_with_no_properties_document_is_listed():
    assert _catalog_reading(None, exists=False) is True


def test_null_reads_as_unset():
    assert _catalog_reading({LISTED_PROPERTY: None}) is True


def test_it_is_not_a_reserved_property():
    # Unlike `secure_objects` and the lock fields, this is an ordinary setting
    # its workspace's owner writes with ALTER WORKSPACE - there is no separate
    # method guarding its shape, because it is one boolean.
    assert LISTED_PROPERTY not in OpteryxCatalog._RESERVED_WORKSPACE_PROPERTIES
