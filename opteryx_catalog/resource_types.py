"""Catalog resource types.

One enum instead of bare strings repeated at every audit/webhook call site.
The members are ``str`` subclasses whose *content* is the wire value, so they
pass through ``json.dumps`` and Firestore writes as the plain lowercase string
the downstream pipeline (``xb500`` log routing, ``ops.audit_log``) already
matches on - adding this enum changes no stored or emitted bytes.
"""

from __future__ import annotations

from enum import Enum


class ResourceType(str, Enum):
    """What kind of catalog resource a mutation touched."""

    DATASET = "dataset"
    VIEW = "view"
    COLLECTION = "collection"
    WORKSPACE = "workspace"
    MATERIALIZED_VIEW = "materialized_view"

    # `str(member)` must be the wire value. Without this, pre-3.12 Pythons
    # render "ResourceType.DATASET" from f-strings, which would corrupt any
    # message a member is interpolated into.
    __str__ = str.__str__
