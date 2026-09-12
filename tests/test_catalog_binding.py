"""Workspace catalog bindings: read/write/clear on the `$properties` doc.

Covers the Phase 1 contract from WORKSPACE_CATALOG_RESOLUTION_PLAN.md:
round-trip including version monotonicity, shell-workspace provisioning with
the full lifecycle seed, write-time rejection of malformed bindings, and
clear-to-native semantics.
"""

from __future__ import annotations

import pytest
from google.cloud import firestore

from opteryx_catalog.binding import AUTH_MODE_STORED
from opteryx_catalog.binding import CatalogBinding
from opteryx_catalog.binding import clear_catalog_binding
from opteryx_catalog.binding import read_catalog_binding
from opteryx_catalog.binding import record_server_identity
from opteryx_catalog.binding import write_catalog_binding
from opteryx_catalog.exceptions import InvalidCatalogBinding
from opteryx_catalog.exceptions import WorkspaceNotFound


class _Snapshot:
    def __init__(self, data):
        self.exists = data is not None
        self._data = data

    def to_dict(self):
        return dict(self._data) if self._data is not None else None


class _Doc:
    def __init__(self):
        self._data = None

    def get(self):
        return _Snapshot(self._data)

    def set(self, data):
        self._data = dict(data)

    def update(self, fields):
        if self._data is None:
            raise KeyError("update on missing document")
        for key, value in fields.items():
            if value is firestore.DELETE_FIELD:
                self._data.pop(key, None)
            else:
                self._data[key] = value


class _Collection:
    def __init__(self):
        self._docs = {}

    def document(self, name):
        return self._docs.setdefault(name, _Doc())


class _FakeFirestore:
    def __init__(self):
        self._collections = {}

    def collection(self, name):
        return self._collections.setdefault(name, _Collection())


ICEBERG_CONFIG = {
    "catalog_type": "rest",
    "uri": "https://biglake.googleapis.com/iceberg/v1/restcatalog",
    "auth": {"type": "google", "google": {"scopes": ["scope-a"]}},  # nesting is fine
}


def test_read_missing_workspace_returns_none():
    assert read_catalog_binding(_FakeFirestore(), "nowhere") is None


def test_read_native_workspace_returns_none():
    fs = _FakeFirestore()
    fs.collection("native_ws").document("$properties").set({"timestamp-ms": 1})
    assert read_catalog_binding(fs, "native_ws") is None


def test_write_then_read_round_trips():
    fs = _FakeFirestore()
    version = write_catalog_binding(
        fs, "tarchia", kind="iceberg", config=ICEBERG_CONFIG, updated_by="alice"
    )

    binding = read_catalog_binding(fs, "tarchia")
    assert isinstance(binding, CatalogBinding)
    assert binding.kind == "iceberg"
    assert binding.config == ICEBERG_CONFIG
    assert binding.auth_mode == "ambient"
    assert binding.ciphertext is None
    assert binding.preserve_sql_case is False
    assert binding.version == version > 0
    assert binding.updated_by == "alice"


def test_write_provisions_shell_with_full_lifecycle_seed():
    # The same field set OpteryxCatalog.__init__ seeds with create_if_missing:
    # a shell workspace must lock and drop like any other.
    fs = _FakeFirestore()
    write_catalog_binding(fs, "shell_ws", kind="iceberg", updated_by="alice")

    doc = fs.collection("shell_ws").document("$properties")._data
    for lifecycle_field in (
        "timestamp-ms",
        "billing-account-id",
        "owner",
        "deleted-at-ms",
        "deleted-by",
        "locked-by",
        "locked-at-ms",
    ):
        assert lifecycle_field in doc, lifecycle_field
    assert doc["catalog"]["kind"] == "iceberg"


def test_write_on_existing_doc_touches_only_the_catalog_field():
    fs = _FakeFirestore()
    doc = fs.collection("ws").document("$properties")
    doc.set({"timestamp-ms": 42, "owner": "someone", "locked-by": "a-lock"})

    write_catalog_binding(fs, "ws", kind="iceberg", updated_by="alice")

    assert doc._data["timestamp-ms"] == 42
    assert doc._data["owner"] == "someone"
    assert doc._data["locked-by"] == "a-lock"
    assert doc._data["catalog"]["kind"] == "iceberg"


def test_version_is_monotonic_across_rewrites_and_rebinds():
    fs = _FakeFirestore()
    first = write_catalog_binding(fs, "ws", kind="iceberg", updated_by="alice")
    second = write_catalog_binding(fs, "ws", kind="iceberg", updated_by="alice")
    assert second > first

    # clear-then-rebind must not re-present an old version either - a cached
    # connector somewhere may still hold it.
    assert clear_catalog_binding(fs, "ws") is True
    third = write_catalog_binding(fs, "ws", kind="iceberg", updated_by="alice")
    assert third > second


def test_stored_auth_round_trips():
    fs = _FakeFirestore()
    write_catalog_binding(
        fs,
        "ws",
        kind="iceberg",
        auth_mode=AUTH_MODE_STORED,
        ciphertext="b64-envelope",
        kms_key="projects/p/locations/l/keyRings/r/cryptoKeys/k",
        inject_as="token",
        updated_by="alice",
    )
    binding = read_catalog_binding(fs, "ws")
    assert binding.auth_mode == "stored"
    assert binding.ciphertext == "b64-envelope"
    assert binding.kms_key.endswith("/cryptoKeys/k")
    assert binding.inject_as == "token"


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"kind": "Not-A-Kind"}, "not a usable name"),
        ({"kind": "iceberg", "config": {"workspace": "x"}}, "workspace"),
        ({"kind": "iceberg", "config": {"connector": "x"}}, "connector"),
        ({"kind": "iceberg", "config": {"prefix": "x"}}, "prefix"),
        ({"kind": "iceberg", "auth_mode": "magic"}, "auth mode"),
        ({"kind": "iceberg", "auth_mode": "stored"}, "missing"),
        ({"kind": "iceberg", "ciphertext": "ct"}, "ambient"),
    ],
)
def test_malformed_bindings_are_rejected_at_write_time(kwargs, match):
    fs = _FakeFirestore()
    with pytest.raises(InvalidCatalogBinding, match=match):
        write_catalog_binding(fs, "ws", updated_by="alice", **kwargs)
    # nothing reached Firestore
    assert read_catalog_binding(fs, "ws") is None


def test_write_requires_author():
    with pytest.raises(ValueError, match="updated_by"):
        write_catalog_binding(_FakeFirestore(), "ws", kind="iceberg", updated_by="")


def test_clear_reverts_to_native_and_keeps_the_doc():
    fs = _FakeFirestore()
    write_catalog_binding(fs, "ws", kind="iceberg", updated_by="alice")

    assert clear_catalog_binding(fs, "ws") is True
    assert read_catalog_binding(fs, "ws") is None
    # lifecycle fields survive: DROP WORKSPACE is the only thing that deletes
    # the doc itself.
    assert "timestamp-ms" in fs.collection("ws").document("$properties")._data


def test_clear_is_honest_about_what_it_did():
    fs = _FakeFirestore()
    fs.collection("native_ws").document("$properties").set({"timestamp-ms": 1})
    assert clear_catalog_binding(fs, "native_ws") is False

    with pytest.raises(WorkspaceNotFound):
        clear_catalog_binding(fs, "never_existed")


def test_postgres_binding_round_trips_with_a_stored_password():
    """kind = "postgres" is connector-only (no metastore): `config` is the
    PostgresConnector's constructor keywords and the password is a stored
    credential injected at `password` — the shape worker.opteryx's allowlist
    and scripts/bind_postgres_workspace.py both rely on."""
    fs = _FakeFirestore()
    config = {
        "host": "db.example.com",
        "port": 5432,
        "dbname": "app",
        "user": "reader",
        "sslmode": "require",
        "schema": "public",
        "timeout_s": 30,
    }
    version = write_catalog_binding(
        fs,
        "erp_test",
        kind="postgres",
        config=config,
        auth_mode=AUTH_MODE_STORED,
        ciphertext="b64-envelope",
        kms_key="projects/p/locations/l/keyRings/r/cryptoKeys/k",
        inject_as="password",
        updated_by="justin",
    )
    binding = read_catalog_binding(fs, "erp_test")
    assert binding.kind == "postgres"
    assert binding.config == config
    assert binding.auth_mode == "stored"
    assert binding.inject_as == "password"
    assert binding.version == version
    assert "password" not in binding.config  # only ever arrives via inject-as


# ---------------------------------------------------------------------------
# The observed server identity. `kind` names the CONNECTOR - a dozen engines
# speak the PostgreSQL wire protocol - so this is the only field that can say a
# `postgres` binding is really pointed at CockroachDB.
# ---------------------------------------------------------------------------


def test_the_observed_server_reads_back():
    fs = _FakeFirestore()
    fs.collection("ws").document("$properties").set(
        {
            "catalog": {
                "kind": "postgres",
                "config": {"host": "db.example.com"},
                "server-flavour": "cockroachdb",
                "server-version": "CockroachDB CCL v23.1.11",
            }
        }
    )
    binding = read_catalog_binding(fs, "ws")
    assert binding.kind == "postgres", "the connector is still postgres"
    assert binding.server_flavour == "cockroachdb"
    assert binding.server_version == "CockroachDB CCL v23.1.11"


def test_a_binding_with_no_observation_reads_as_none():
    """Never connected, or recorded by a deployment too old to write it. Either
    way a caller must fall back to the kind rather than assume PostgreSQL."""
    fs = _FakeFirestore()
    write_catalog_binding(fs, "ws", kind="postgres", config={"host": "h"}, updated_by="alice")
    binding = read_catalog_binding(fs, "ws")
    assert binding.server_flavour is None
    assert binding.server_version is None


def test_recording_the_server_targets_only_its_own_two_fields():
    """Targeted field paths, like stub_projection's listing stamp and for the
    same reason: a binding write may land between the connection that learned
    this and the call that records it, and must not be clobbered."""
    fs = _FakeFirestore()
    fs.collection("ws").document("$properties").set({"catalog": {"kind": "postgres"}})

    doc = fs.collection("ws").document("$properties")
    seen = {}
    doc.update = lambda fields: seen.update(fields)

    record_server_identity(fs, "ws", flavour="cockroachdb", version="CockroachDB v23")

    assert len(seen) == 2
    assert all("server-" in key for key in seen), seen
    assert set(seen.values()) == {"cockroachdb", "CockroachDB v23"}
    assert not any("kind" in key or "config" in key for key in seen)


def test_recording_the_server_is_a_no_op_for_an_unbound_workspace():
    """It annotates as a side effect of someone else's operation, so it must
    never be the call that creates a document - or that puts a catalog block on
    a native workspace."""
    fs = _FakeFirestore()
    record_server_identity(fs, "nowhere", flavour="cockroachdb")
    assert read_catalog_binding(fs, "nowhere") is None

    fs.collection("native_ws").document("$properties").set({"timestamp-ms": 1})
    record_server_identity(fs, "native_ws", flavour="cockroachdb")
    assert fs.collection("native_ws").document("$properties").get().to_dict() == {
        "timestamp-ms": 1
    }


def test_rebinding_drops_the_observation():
    """A rebind can point the workspace at a different server entirely, and
    carrying the old engine's name forward would mark it as something it is
    not. Same direction the listing stamps take."""
    fs = _FakeFirestore()
    fs.collection("ws").document("$properties").set(
        {"catalog": {"kind": "postgres", "server-flavour": "cockroachdb"}}
    )
    write_catalog_binding(fs, "ws", kind="postgres", config={"host": "other"}, updated_by="bob")
    assert read_catalog_binding(fs, "ws").server_flavour is None
