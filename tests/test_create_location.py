"""`create_dataset` puts a dataset at its name location unless that location is in use.

The location is detached from the name - stored on the document, never re-derived,
so a rename leaves it where it is - but not forced to differ from it (architect,
2026-11-02). A new dataset gets `gs://<bucket>/<workspace>/<collection>/<name>`;
when a live dataset records that location (it was renamed away) or objects are
stored under it (a drop's files awaiting reconciliation), it gets a minted one.
"""

from __future__ import annotations

import os
import re
import sys

sys.path.insert(0, os.path.dirname(__file__))

import pytest
from test_rename_dataset import _Client
from test_rename_dataset import _Coll
from test_rename_dataset import _NoStorage
from test_rename_dataset import _Ref

from opteryx_catalog.integrity import audit_workspace
from opteryx_catalog.opteryx_catalog import OpteryxCatalog

MINTED = re.compile(r"^gs://bucket/ws/[0-9a-f]{32}$")


class _Storage:
    """A GCS client fake holding object names; a listing can be made to fail."""

    def __init__(self):
        self.objects = set()
        self.broken = False

    def list_blobs(self, bucket, prefix="", max_results=None):
        assert bucket == "bucket"
        if self.broken:
            raise RuntimeError("storage unavailable")
        names = sorted(n for n in self.objects if n.startswith(prefix))
        return iter(names[:max_results])


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
    catalog._storage_client = _Storage()
    catalog._repoint_inbound_relationships = lambda *a, **k: []
    return catalog


def _location(catalog, collection, name):
    return catalog.firestore_client.docs[("ws", collection, "datasets", name)]["location"]


def test_a_new_dataset_gets_its_name_location(catalog):
    dataset = catalog.create_dataset("coll.tbl", None, author="alice")

    assert _location(catalog, "coll", "tbl") == "gs://bucket/ws/coll/tbl"
    assert dataset.metadata.location == "gs://bucket/ws/coll/tbl"


def test_the_vacated_name_of_a_rename_does_not_bind_the_renamed_datasets_location(catalog):
    """A rename carries its location; a new dataset under the old name must not share it."""
    catalog.create_dataset("coll.tbl", None, author="alice")
    original = _location(catalog, "coll", "tbl")

    catalog.rename_dataset("coll.tbl", "coll.renamed", author="alice")
    catalog.create_dataset("coll.tbl", None, author="alice")

    assert _location(catalog, "coll", "renamed") == original
    assert MINTED.match(_location(catalog, "coll", "tbl"))


def test_a_rename_into_another_collection_still_claims_its_location(catalog):
    catalog.create_dataset("coll.tbl", None, author="alice")
    catalog.rename_dataset("coll.tbl", "other.tbl", author="alice")

    catalog.create_dataset("coll.tbl", None, author="alice")

    assert _location(catalog, "other", "tbl") == "gs://bucket/ws/coll/tbl"
    assert MINTED.match(_location(catalog, "coll", "tbl"))


def test_a_dropped_datasets_files_keep_the_name_location_out_of_use(catalog):
    catalog.create_dataset("coll.tbl", None, author="alice")
    catalog._storage_client.objects.add("ws/coll/tbl/data/part-0.parquet")
    # Dropping is exercised elsewhere; here the document just goes, the files stay.
    del catalog.firestore_client.docs[("ws", "coll", "datasets", "tbl")]

    catalog.create_dataset("coll.tbl", None, author="alice")

    assert MINTED.match(_location(catalog, "coll", "tbl"))


def test_a_sibling_prefix_is_not_mistaken_for_the_name_location(catalog):
    catalog._storage_client.objects.add("ws/coll/tbl2/data/part-0.parquet")

    catalog.create_dataset("coll.tbl", None, author="alice")

    assert _location(catalog, "coll", "tbl") == "gs://bucket/ws/coll/tbl"


def test_a_failed_storage_listing_fails_the_create(catalog):
    catalog._storage_client.broken = True

    with pytest.raises(RuntimeError, match="storage unavailable"):
        catalog.create_dataset("coll.tbl", None, author="alice")
    assert ("ws", "coll", "datasets", "tbl") not in catalog.firestore_client.docs


def test_a_ctas_reservation_is_the_location_create_takes(catalog):
    schema = {"columns": [{"name": "id", "type": "INTEGER"}]}
    writer_catalog_location = catalog._reserve_location("coll.ctas")
    assert writer_catalog_location == "gs://bucket/ws/coll/ctas"
    assert catalog._reserve_location("coll.ctas") == writer_catalog_location

    catalog.create_dataset("coll.ctas", schema, author="alice")

    assert _location(catalog, "coll", "ctas") == writer_catalog_location
    # Consumed: a later create of the same name is a different dataset.
    assert "coll.ctas" not in catalog._pending_locations


def test_an_explicit_location_must_be_one_the_catalog_allocates(catalog):
    for bad in (
        "gs://bucket/ws/coll/other",  # another name's location
        "gs://bucket/other/" + "a" * 32,  # another workspace
        "gs://elsewhere/ws/" + "a" * 32,  # another bucket
        "gs://bucket/ws/" + "a" * 32 + "/sub",  # not a bare id
    ):
        with pytest.raises(ValueError, match="not a location the catalog allocates"):
            catalog.create_dataset("coll.tbl", None, author="alice", location=bad)
    assert ("ws", "coll", "datasets", "tbl") not in catalog.firestore_client.docs


@pytest.mark.parametrize("given", ["gs://bucket/ws/coll/tbl", "gs://bucket/ws/" + "b" * 32])
def test_an_explicit_allocated_location_is_used_and_settles_the_reservation(catalog, given):
    catalog._reserve_location("coll.tbl")

    catalog.create_dataset("coll.tbl", None, author="alice", location=given)

    assert _location(catalog, "coll", "tbl") == given
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
