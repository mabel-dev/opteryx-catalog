"""Catalog handles share one Firestore/GCS connection per process.

The handle is still built per request - its constructor's workspace-exists read
is the point of building it - but the client, its credentials and its channel
are not rebuilt with it.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from opteryx_catalog import clients
from opteryx_catalog.iops import gcs


@pytest.fixture(autouse=True)
def _fresh_cache():
    clients.clear_shared_clients()
    yield
    clients.clear_shared_clients()


def test_same_project_and_database_share_one_client():
    with patch("google.cloud.firestore.Client") as factory:
        a = clients.shared_firestore_client("p", "db")
        b = clients.shared_firestore_client("p", "db")
    assert a is b
    factory.assert_called_once_with(project="p", database="db")


def test_different_databases_get_different_clients():
    with patch("google.cloud.firestore.Client", side_effect=lambda **_: MagicMock()):
        a = clients.shared_firestore_client("p", "one")
        b = clients.shared_firestore_client("p", "two")
    assert a is not b


def test_a_new_patch_never_receives_a_previously_cached_client():
    # Tests across the platform patch `firestore.Client` per test; each patch
    # is a new factory object and must get its own client back.
    with patch("google.cloud.firestore.Client") as first:
        a = clients.shared_firestore_client("p", "db")
    with patch("google.cloud.firestore.Client") as second:
        b = clients.shared_firestore_client("p", "db")
    assert a is first.return_value
    assert b is second.return_value


def test_a_forked_process_builds_its_own_client():
    with patch("google.cloud.firestore.Client", side_effect=lambda **_: MagicMock()):
        parent = clients.shared_firestore_client("p", "db")
        with patch.object(os, "getpid", return_value=os.getpid() + 1):
            child = clients.shared_firestore_client("p", "db")
    assert parent is not child


def test_catalog_handles_share_the_client():
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    client = MagicMock()
    client.collection.return_value.document.return_value.get.return_value.exists = True
    with patch("opteryx_catalog.opteryx_catalog.firestore.Client", return_value=client) as f:
        a = OpteryxCatalog("ws", firestore_project="p", firestore_database="db")
        b = OpteryxCatalog("ws", firestore_project="p", firestore_database="db")
    assert a.firestore_client is b.firestore_client is client
    f.assert_called_once()
    # The workspace gate is still read by every handle: sharing the connection
    # must not cache the answer to "does this workspace exist".
    assert client.collection.return_value.document.return_value.get.call_count == 2


def test_an_injected_client_is_used_and_passed_to_siblings():
    from opteryx_catalog.opteryx_catalog import OpteryxCatalog

    client = MagicMock()
    client.collection.return_value.document.return_value.get.return_value.exists = True
    with patch("opteryx_catalog.opteryx_catalog.firestore.Client") as factory:
        handle = OpteryxCatalog("ws", firestore_client=client)
        sibling = handle._catalog_for("other")
    factory.assert_not_called()
    assert handle.firestore_client is client
    assert sibling.firestore_client is client


def test_gcs_file_ios_share_credentials_and_session():
    credentials = MagicMock()
    storage_client = MagicMock(_credentials=credentials)
    with patch("google.cloud.storage.Client", return_value=storage_client) as factory:
        a = gcs.GcsFileIO()
        b = gcs.GcsFileIO()
    factory.assert_called_once()
    assert a._credentials is b._credentials is credentials
    assert a._session is b._session
    # The read cache is per handle: sharing it would change what the
    # expiration sweep's byte budget bounds.
    assert a._read_cache is not b._read_cache
