"""`create_dataset` gives every dataset a location nobody has had and nobody can derive.

A location derived from the name is re-bindable: after a rename or a drop the same
name derives the same prefix, and two datasets share it. The location is minted
instead (an opaque id), stored on the document, and never re-derived
(RENAME_METADATA_ONLY_DESIGN.md).
"""

from __future__ import annotations

import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))

import pytest

from opteryx_catalog.integrity import audit_workspace
from opteryx_catalog.opteryx_catalog import OpteryxCatalog
from test_rename_dataset import _Client
from test_rename_dataset import _Coll
from test_rename_dataset import _NoStorage
from test_rename_dataset import _Ref

MINTED = re.compile(r"^gs://bucket/ws/[0-9a-f]{32}$")


@pytest.fixture
def catalog(monkeypatch):
    monkeypatch.setattr("opteryx_catalog.opteryx_catalog.send_webhook", lambda **k: None)
    client = _Client()
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog.gcs_bucket = "bucket"
    catalog.firestore_client = client
    catalog._catalog_ref = client.collection("ws")
    catalog.io = _NoStorage()
    catalog._storage_client = None
    catalog._repoint_inbound_relationships = lambda *a, **k: []
    return catalog


def _location(catalog, collection, name):
    return catalog.firestore_client.docs[("ws", collection, "datasets", name)]["location"]


def test_a_new_dataset_gets_an_opaque_location(catalog):
    dataset = catalog.create_dataset("coll.tbl", None, author="alice")

    stored = _location(catalog, "coll", "tbl")
    assert MINTED.match(stored)
    assert "tbl" not in stored and "coll" not in stored
    assert dataset.metadata.location == stored


def test_two_datasets_never_share_a_location(catalog):
    catalog.create_dataset("coll.a", None, author="alice")
    catalog.create_dataset("coll.b", None, author="alice")

    assert _location(catalog, "coll", "a") != _location(catalog, "coll", "b")


def test_a_name_that_is_dropped_and_recreated_gets_a_different_location(catalog):
    catalog.create_dataset("coll.tbl", None, author="alice")
    first = _location(catalog, "coll", "tbl")
    # Dropping is exercised elsewhere; here the document just goes.
    del catalog.firestore_client.docs[("ws", "coll", "datasets", "tbl")]

    catalog.create_dataset("coll.tbl", None, author="alice")

    assert _location(catalog, "coll", "tbl") != first


def test_the_vacated_name_of_a_rename_cannot_bind_the_renamed_datasets_files(catalog):
    """The scenario the whole change exists for."""
    catalog.create_dataset("coll.tbl", None, author="alice")
    original = _location(catalog, "coll", "tbl")

    catalog.rename_dataset("coll.tbl", "coll.renamed", author="alice")
    catalog.create_dataset("coll.tbl", None, author="alice")

    assert _location(catalog, "coll", "renamed") == original
    assert _location(catalog, "coll", "tbl") != original


def test_a_dataset_created_before_this_change_renames_and_its_name_is_not_rebound(catalog):
    """Its location was derived from its name; no create derives that prefix any more."""
    legacy = "gs://bucket/ws/coll/old"
    catalog.firestore_client.docs[("ws", "coll", "datasets", "old")] = {
        "name": "old", "collection": "coll", "location": legacy,
    }

    catalog.rename_dataset("coll.old", "coll.moved", author="alice")
    catalog.create_dataset("coll.old", None, author="alice")

    assert _location(catalog, "coll", "moved") == legacy
    assert _location(catalog, "coll", "old") != legacy


def test_a_ctas_reservation_is_the_location_create_takes(catalog):
    schema = {"columns": [{"name": "id", "type": "INTEGER"}]}
    writer_catalog_location = catalog._reserve_location("coll.ctas")
    assert catalog._reserve_location("coll.ctas") == writer_catalog_location

    catalog.create_dataset("coll.ctas", schema, author="alice")

    assert _location(catalog, "coll", "ctas") == writer_catalog_location
    # Consumed: a later create of the same name is a different dataset.
    assert "coll.ctas" not in catalog._pending_locations


def test_an_explicit_location_must_be_a_freshly_minted_one(catalog):
    for bad in (
        "gs://bucket/ws/coll/tbl",  # name-derived
        "gs://bucket/other/" + "a" * 32,  # another workspace
        "gs://elsewhere/ws/" + "a" * 32,  # another bucket
        "gs://bucket/ws/" + "a" * 32 + "/sub",  # not a bare id
    ):
        with pytest.raises(ValueError, match="not a freshly minted"):
            catalog.create_dataset("coll.tbl", None, author="alice", location=bad)
    assert ("ws", "coll", "datasets", "tbl") not in catalog.firestore_client.docs


def test_an_explicit_minted_location_is_used_and_settles_the_reservation(catalog):
    reserved = catalog._reserve_location("coll.tbl")

    catalog.create_dataset("coll.tbl", None, author="alice", location=reserved)

    assert _location(catalog, "coll", "tbl") == reserved
    assert "coll.tbl" not in catalog._pending_locations


def test_fsck_reports_two_datasets_on_one_location(catalog):
    docs = catalog.firestore_client.docs
    for name in ("a", "b"):
        docs[("ws", "coll", "datasets", name)] = {"location": "gs://bucket/ws/coll/shared"}
    docs[("ws", "coll", "datasets", "c")] = {"location": "gs://bucket/ws/" + "c" * 32}

    findings = [f for f in audit_workspace(_ListingClient(catalog.firestore_client), "ws")
                if f["kind"] == "duplicate-location"]

    assert sorted(f["path"] for f in findings) == [
        "ws/coll/datasets/a",
        "ws/coll/datasets/b",
    ]


class _ListingClient:
    """The path-keyed fake plus the `list_documents()` that `audit_workspace` walks."""

    def __init__(self, client):
        self._client = client

    def collection(self, name):
        return _Listing(self._client, (name,))


class _Listing:
    def __init__(self, client, path):
        self._client, self.path = client, path

    def list_documents(self):
        n = len(self.path)
        ids = sorted({p[n] for p in self._client.docs if len(p) > n and p[:n] == self.path})
        return [_ListingRef(self._client, self.path + (i,)) for i in ids]

    def stream(self):
        return _Coll(self._client, self.path).stream()


class _ListingRef:
    def __init__(self, client, path):
        self._client, self.path, self.id = client, path, path[-1]

    def get(self):
        return _Ref(self._client, self.path).get()

    def collection(self, name):
        return _Listing(self._client, self.path + (name,))
