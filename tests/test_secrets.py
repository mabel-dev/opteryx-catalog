"""Customer secrets on the catalog (jobs.opteryx docs/design/secrets.md, SEC-1/4/13).

KMS is stubbed the way test_kms_envelope.py stubs it - reversibly, not as the
identity, so a path that skips the unwrap cannot pass. Firestore is a small
in-memory fake with the `create()` semantics the DDL flags depend on.
"""

from __future__ import annotations

import json
import pickle

import pytest

from opteryx_catalog.opteryx_catalog import OpteryxCatalog
from opteryx_catalog.secrets import SecretAlreadyExists
from opteryx_catalog.secrets import SecretInvalid
from opteryx_catalog.secrets import SecretKeyUnavailable
from opteryx_catalog.secrets import SecretNotFound
from opteryx_catalog.secrets import SecretRefused
from opteryx_catalog.secrets import scope_admits
from opteryx_catalog.secrets import set_kek_namer
from opteryx_catalog.secrets import validate_secret
from opteryx_catalog.secrets.types import normalise_scope
from opteryx_catalog.security import kms as kms_module

CANARY = "CANARY-6f1d2c"
SA_KEY = json.dumps(
    {
        "type": "service_account",
        "client_email": "reader@acme.iam.gserviceaccount.com",
        "private_key": f"-----BEGIN PRIVATE KEY-----\n{CANARY}\n-----END PRIVATE KEY-----\n",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
)
AWS_ID = "AKIAABCDEFGHIJKLMNOP"
AWS_SECRET = "abcdEFGHijklMNOPqrstUVWXyz0123456789+/AB"


# -- fakes -------------------------------------------------------------------


class AlreadyExists(Exception):
    pass


class _Snap:
    def __init__(self, id_, data):
        self.id = id_
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return None if self._data is None else dict(self._data)


class _DocRef:
    def __init__(self, store, path):
        self._store = store
        self._path = path
        self.id = path[-1]

    def get(self):
        return _Snap(self.id, self._store.get(self._path))

    def set(self, data, merge=False):
        if merge and self._path in self._store:
            self._store[self._path] = {**self._store[self._path], **data}
        else:
            self._store[self._path] = dict(data)

    def create(self, data):
        if self._path in self._store:
            raise AlreadyExists("exists")
        self._store[self._path] = dict(data)

    def update(self, data):
        if self._path not in self._store:
            raise KeyError("not found")
        current = self._store[self._path]
        for key, value in data.items():
            if type(value).__name__ == "Increment":
                current[key] = (current.get(key) or 0) + value._value
            else:
                current[key] = value

    def delete(self):
        self._store.pop(self._path, None)

    def collection(self, name):
        return _Coll(self._store, self._path + (name,))


class _Coll:
    def __init__(self, store, path):
        self._store = store
        self._path = path

    def document(self, id_):
        return _DocRef(self._store, self._path + (id_,))

    def stream(self):
        n = len(self._path) + 1
        for path, data in list(self._store.items()):
            if len(path) == n and path[:-1] == self._path:
                yield _Snap(path[-1], data)


def _catalog(workspace="analytics", account="acct_1"):
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = workspace
    store = {}
    catalog._store = store
    catalog._catalog_ref = _Coll(store, (workspace,))
    if account is not None:
        catalog._catalog_ref.document("$properties").set({"billing-account-id": account})
    return catalog


@pytest.fixture(autouse=True)
def stub_kms(monkeypatch):
    def _stream(kms_key, length):
        seed = kms_key.encode("utf-8")
        return bytes(seed[i % len(seed)] ^ (i & 0xFF) for i in range(length))

    def _xor(data, key):
        return bytes(a ^ b for a, b in zip(data, _stream(key, len(data))))

    calls = {"unwrap": 0}

    def _unwrap(wrapped, key):
        calls["unwrap"] += 1
        return _xor(wrapped, key)

    monkeypatch.setattr(kms_module, "_wrap_dek_versioned", lambda dek, key: (_xor(dek, key), f"{key}/cryptoKeyVersions/1"))
    monkeypatch.setattr(kms_module, "_unwrap_dek", _unwrap)
    kms_module.clear_dek_cache()
    set_kek_namer(lambda account: f"projects/p/locations/l/keyRings/customer-secrets/cryptoKeys/ba-{account}")
    import opteryx_catalog.secrets.store as store_module

    monkeypatch.setattr(store_module, "_is_already_exists", lambda err: isinstance(err, AlreadyExists))
    yield calls
    set_kek_namer(None)
    kms_module.clear_dek_cache()


@pytest.fixture
def audit(monkeypatch):
    records = []
    import opteryx_catalog.secrets.store as store_module

    monkeypatch.setattr(store_module, "emit_audit", lambda action, **kw: records.append((action, kw)))
    return records


def _public_resolver(host):
    return ["93.184.216.34"]


def _gcs(catalog, **kw):
    return catalog.create_secret(
        "billing_reader",
        "gcs_service_account",
        {"KEY": SA_KEY, "SCOPE": "gs://mabel_logs/gcp_billing/"},
        author="alice",
        **kw,
    )


# -- storage and envelope ----------------------------------------------------


def test_create_stores_ciphertext_only_at_the_designed_path(audit):
    catalog = _catalog()
    result = _gcs(catalog)
    assert result["created"] is True
    stored = catalog._store[("analytics", "$secrets", "items", "billing_reader")]
    assert stored["type"] == "gcs_service_account"
    assert stored["scope"] == "gs://mabel_logs/gcp_billing/"
    assert stored["alg"] == "AES256-GCM/KMS-wrapped-v1"
    assert stored["kek-version"].endswith("/cryptoKeyVersions/1")
    assert stored["kek"].endswith("/cryptoKeys/ba-acct_1")
    # The value is nowhere in the clear - not in the document, not in the result.
    assert CANARY not in repr(catalog._store)
    assert CANARY not in repr(result)
    assert "ciphertext" not in result and "wrapped-dek" not in result and "nonce" not in result
    assert audit[0][0] == "create_secret"
    assert CANARY not in repr(audit)


def test_plain_create_refuses_an_existing_name():
    catalog = _catalog()
    _gcs(catalog)
    with pytest.raises(SecretAlreadyExists):
        _gcs(catalog)


def test_if_not_exists_keeps_the_existing_value():
    catalog = _catalog()
    _gcs(catalog)
    before = dict(catalog._store[("analytics", "$secrets", "items", "billing_reader")])
    again = _gcs(catalog, if_not_exists=True)
    assert again["created"] is False
    assert catalog._store[("analytics", "$secrets", "items", "billing_reader")] == before


def test_or_replace_replaces_the_value_and_keeps_creation_stamp():
    catalog = _catalog()
    _gcs(catalog)
    first = dict(catalog._store[("analytics", "$secrets", "items", "billing_reader")])
    catalog.create_secret(
        "billing_reader",
        "aws_access_key",
        {"ACCESS_KEY_ID": AWS_ID, "SECRET_ACCESS_KEY": AWS_SECRET, "SCOPE": "s3://acme-lake/exports/"},
        author="bob",
        replace=True,
    )
    second = catalog._store[("analytics", "$secrets", "items", "billing_reader")]
    assert second["type"] == "aws_access_key"
    assert second["ciphertext"] != first["ciphertext"]
    assert second["created-by"] == "alice" and second["updated-by"] == "bob"


def test_or_replace_with_if_not_exists_is_a_contradiction():
    with pytest.raises(SecretInvalid):
        _gcs(_catalog(), replace=True, if_not_exists=True)


def test_a_workspace_without_a_billing_account_has_no_key():
    with pytest.raises(SecretKeyUnavailable):
        _gcs(_catalog(account=None))


def test_no_key_ring_configured_is_refused(monkeypatch):
    set_kek_namer(None)
    monkeypatch.delenv("OPTERYX_SECRETS_KEY_RING", raising=False)
    with pytest.raises(SecretKeyUnavailable):
        _gcs(_catalog())


def test_list_and_get_never_return_key_material():
    catalog = _catalog()
    _gcs(catalog)
    catalog.create_secret(
        "alerts", "http_endpoint", {"URL": f"https://hooks.example.com/{CANARY}"}, author="alice",
        resolve_host=_public_resolver,
    )
    listed = catalog.list_secrets()
    assert [r["name"] for r in listed] == ["alerts", "billing_reader"]
    for record in listed + [catalog.get_secret("alerts")]:
        assert not {"ciphertext", "nonce", "wrapped-dek"} & set(record)
    assert CANARY not in repr(listed)


def test_drop(audit):
    catalog = _catalog()
    _gcs(catalog)
    assert catalog.drop_secret("billing_reader", author="alice") is True
    with pytest.raises(SecretNotFound):
        catalog.get_secret("billing_reader")
    with pytest.raises(SecretNotFound):
        catalog.drop_secret("billing_reader", author="alice")
    assert catalog.drop_secret("billing_reader", author="alice", if_exists=True) is False


def test_secrets_are_invisible_from_another_workspace():
    a = _catalog("analytics")
    _gcs(a)
    b = _catalog("staging")
    b._store.update(a._store)
    b._catalog_ref = type(b._catalog_ref)(b._store, ("staging",))
    with pytest.raises(SecretNotFound):
        b.get_secret("billing_reader")
    assert b.list_secrets() == []


# -- use ---------------------------------------------------------------------


def test_use_decrypts_stamps_and_audits(audit, stub_kms):
    catalog = _catalog()
    _gcs(catalog)
    kms_module.clear_dek_cache()
    material = catalog.use_secret(
        "analytics.billing_reader".split(".")[1],
        used_by="alice",
        purpose="read",
        path="gs://mabel_logs/gcp_billing/x/a.parquet",
        query_id="q1",
    )
    assert material.payload["key"]["private_key"].count(CANARY) == 1
    assert CANARY not in repr(material) and CANARY not in str(material)
    with pytest.raises(TypeError):
        pickle.dumps(material)
    stored = catalog._store[("analytics", "$secrets", "items", "billing_reader")]
    assert stored["use-count"] == 1 and stored["last-used-at-ms"]
    assert audit[-1][0] == "use_secret"
    assert audit[-1][1]["path"] == "gs://mabel_logs/gcp_billing/x/a.parquet"
    # The second use is served from the DEK cache, not another KMS call.
    catalog.use_secret("billing_reader", used_by="alice", purpose="read", path="gs://mabel_logs/gcp_billing/y")
    assert stub_kms["unwrap"] == 1


def test_use_refuses_a_path_outside_scope_or_of_the_wrong_scheme():
    catalog = _catalog()
    _gcs(catalog)
    with pytest.raises(SecretRefused, match="outside"):
        catalog.use_secret("billing_reader", used_by="a", purpose="read", path="gs://mabel_logs/other/x")
    with pytest.raises(SecretRefused, match="outside"):
        catalog.use_secret("billing_reader", used_by="a", purpose="read", path="gs://mabel_logs_private/gcp_billing/x")
    with pytest.raises(SecretRefused, match="type"):
        catalog.use_secret("billing_reader", used_by="a", purpose="read", path="s3://mabel_logs/gcp_billing/x")


def test_a_sealed_record_does_not_open_at_another_address():
    catalog = _catalog()
    _gcs(catalog)
    items = ("analytics", "$secrets", "items")
    catalog._store[items + ("copy",)] = dict(catalog._store[items + ("billing_reader",)], name="copy")
    from opteryx_catalog.security.kms import SecretDecryptionError

    with pytest.raises(SecretDecryptionError):
        catalog.use_secret("copy", used_by="a", purpose="read", path="gs://mabel_logs/gcp_billing/x")


# -- validation --------------------------------------------------------------


@pytest.mark.parametrize(
    "secret_type, options, message",
    [
        ("webhook", {"URL": "https://x"}, "unknown secret TYPE"),
        ("http_endpoint", {"URL": "http://hooks.example.com/"}, "https"),
        ("http_endpoint", {"URL": "https://u:p@hooks.example.com/"}, "userinfo"),
        ("http_endpoint", {"URL": "https://10.0.0.1/"}, "private"),
        ("http_endpoint", {"URL": "https://169.254.169.254/"}, "private"),
        ("http_endpoint", {"URL": "https://hooks.example.com/", "HEADER_COOKIE": "x"}, "does not take"),
        ("http_endpoint", {"URLL": "https://hooks.example.com/"}, "does not take"),
        ("gcs_service_account", {"KEY": SA_KEY}, "requires SCOPE"),
        ("gcs_service_account", {"KEY": "not json", "SCOPE": "gs://bkt1/"}, "not valid JSON"),
        ("gcs_service_account", {"KEY": '{"type": "user"}', "SCOPE": "gs://bkt1/"}, "service_account"),
        ("gcs_service_account", {"KEY": SA_KEY, "SCOPE": "s3://bkt1/"}, "gs://"),
        ("gcs_service_account", {"KEY": SA_KEY, "SCOPE": "gs://bkt1/*.parquet"}, "not a glob"),
        ("gcs_service_account", {"KEY": SA_KEY, "SCOPE": "gs://bkt1/../x"}, "segments"),
        ("aws_access_key", {"ACCESS_KEY_ID": "nope", "SECRET_ACCESS_KEY": AWS_SECRET, "SCOPE": "s3://bkt1/"}, "shape"),
        ("aws_access_key", {"ACCESS_KEY_ID": AWS_ID, "SECRET_ACCESS_KEY": AWS_SECRET}, "requires SCOPE"),
        ("aws_access_key", {"ACCESS_KEY_ID": "ASIAABCDEFGHIJKLMNOP", "SECRET_ACCESS_KEY": AWS_SECRET, "SCOPE": "s3://bkt1/"}, "SESSION_TOKEN"),
    ],
)
def test_validation_refusals(secret_type, options, message):
    with pytest.raises(SecretInvalid, match=message) as raised:
        validate_secret(secret_type, options, resolve_host=_public_resolver)
    assert CANARY not in str(raised.value)
    for value in options.values():
        if len(value) > 12:
            assert value not in str(raised.value)


def test_http_endpoint_refuses_a_host_resolving_privately():
    with pytest.raises(SecretInvalid, match="private"):
        validate_secret("http_endpoint", {"URL": "https://internal.example.com/"}, resolve_host=lambda h: ["10.1.2.3"])


def test_valid_types_normalise_option_keys():
    v = validate_secret(
        "HTTP_ENDPOINT",
        {"url": "https://hooks.example.com/x", "header_authorization": "Bearer z"},
        resolve_host=_public_resolver,
    )
    assert v.payload == {"url": "https://hooks.example.com/x", "headers": {"Authorization": "Bearer z"}}
    a = validate_secret(
        "aws_access_key",
        {"ACCESS_KEY_ID": AWS_ID, "SECRET_ACCESS_KEY": AWS_SECRET, "REGION": "eu-west-2", "SCOPE": "s3://acme-lake"},
    )
    assert a.scope == "s3://acme-lake/"
    assert a.payload["region"] == "eu-west-2"


# -- scope matching ----------------------------------------------------------


@pytest.mark.parametrize(
    "scope, path, admitted",
    [
        ("gs://mabel_logs/", "gs://mabel_logs/a/b.parquet", True),
        ("gs://mabel_logs/", "gs://mabel_logs_private/a.parquet", False),
        ("gs://mabel_logs/gcp_billing/", "gs://mabel_logs/gcp_billing/x/y.parquet", True),
        ("gs://mabel_logs/gcp_billing/", "gs://mabel_logs/gcp_billing_old/y.parquet", False),
        ("gs://mabel_logs/gcp_billing/", "gs://mabel_logs/gcp_billing/../secret.parquet", False),
        ("gs://mabel_logs/gcp_billing/", "gs://mabel_logs/gcp_billing/./a", False),
        ("gs://mabel_logs/gcp_billing/", "gs://mabel_logs/gcp_billing//a", False),
        ("gs://mabel_logs/gcp_billing/", "s3://mabel_logs/gcp_billing/a", False),
        ("gs://mabel_logs/gcp_billing/", "GS://mabel_logs/gcp_billing/a", True),
        ("gs://mabel_logs/", "gs://mabel_logs", False),
        ("s3://acme-lake/exports/", "s3://acme-lake/exports/2026/a.csv", True),
    ],
)
def test_scope_admits_on_a_bucket_boundary(scope, path, admitted):
    assert scope_admits(scope, path) is admitted


def test_bare_bucket_scope_is_canonicalised_to_its_boundary():
    assert normalise_scope("gcs_service_account", "gs://mabel_logs") == "gs://mabel_logs/"
