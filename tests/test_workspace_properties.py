from __future__ import annotations

import json

import pytest

from opteryx_catalog.opteryx_catalog import OpteryxCatalog


class _Doc:
    """A stand-in for a Firestore document snapshot."""

    def __init__(self, data=None, exists=True):
        self.exists = exists
        self._data = data or {}

    def to_dict(self):
        return self._data


class _DocRef:
    """A stand-in for a Firestore DocumentReference. `set_workspace_properties`
    writes with set(merge=True), so that's the write op recorded here."""

    def __init__(self, data=None, exists=True):
        self._doc = _Doc(data, exists)
        self.written = None
        self.written_merge = None

    def get(self):
        return self._doc

    def set(self, data, merge=False):
        self.written = data
        self.written_merge = merge
        self._doc._data = {**self._doc._data, **data}
        self._doc.exists = True


class _Collection:
    def __init__(self, props_ref):
        self._props_ref = props_ref

    def document(self, doc_id):
        assert doc_id == "$properties", f"unexpected document: {doc_id}"
        return self._props_ref


def _catalog(props=None, exists=True):
    """A catalog whose $properties doc holds `props`."""
    props_ref = _DocRef(data=props, exists=exists)

    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog._catalog_ref = _Collection(props_ref)
    return catalog, props_ref


def _emitted(capsys):
    out = capsys.readouterr().out
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def test_get_returns_whole_document():
    catalog, _ = _catalog(
        {"owner": "alice", "billing-account-id": "acct-1", "deletion_protection": True}
    )

    assert catalog.get_workspace_properties() == {
        "owner": "alice",
        "billing-account-id": "acct-1",
        "deletion_protection": True,
    }


def test_get_missing_document_is_empty_not_an_error():
    """The constructor's $properties write is best-effort, so a workspace
    without one reads as 'no properties' rather than raising."""
    catalog, _ = _catalog(exists=False)

    assert catalog.get_workspace_properties() == {}


def test_get_does_not_hide_a_deleted_workspace():
    """Reading properties is how a caller discovers the workspace is deleted,
    so unlike the constructor this must not gate on deleted-at-ms."""
    catalog, _ = _catalog({"deleted-at-ms": 1700000000000, "deleted-by": "alice"})

    assert catalog.get_workspace_properties()["deleted-at-ms"] == 1700000000000


def test_set_merges_rather_than_replaces(capsys):
    """Setting one property must not blank the others by omission."""
    catalog, props_ref = _catalog({"owner": "alice", "billing-account-id": "acct-1"})

    catalog.set_workspace_properties({"deletion_protection": False}, author="alice")

    assert props_ref.written_merge is True
    assert props_ref.written["deletion_protection"] is False
    assert "owner" not in props_ref.written  # untouched, not rewritten
    assert catalog.get_workspace_properties()["owner"] == "alice"


def test_set_stamps_timestamp(capsys):
    catalog, props_ref = _catalog({})

    catalog.set_workspace_properties({"deletion_protection": True}, author="alice")

    assert isinstance(props_ref.written["timestamp-ms"], int)


def test_set_explicit_none_removes_a_property(capsys):
    catalog, props_ref = _catalog({"deletion_protection": True})

    catalog.set_workspace_properties({"deletion_protection": None}, author="alice")

    assert props_ref.written["deletion_protection"] is None


def test_set_rejects_empty_mapping():
    catalog, _ = _catalog({})

    with pytest.raises(ValueError, match="non-empty"):
        catalog.set_workspace_properties({}, author="alice")


@pytest.mark.parametrize(
    "field",
    ["deleted-at-ms", "deleted-by", "locked-by", "locked-at-ms", "timestamp-ms"],
)
def test_set_rejects_reserved_lifecycle_fields(field):
    """A generic setter must not be able to resurrect a deleted workspace or
    clear a lock - those have dedicated methods that audit and send webhooks."""
    catalog, props_ref = _catalog({"deleted-at-ms": 1700000000000})

    with pytest.raises(ValueError, match="reserved workspace lifecycle field"):
        catalog.set_workspace_properties({field: None}, author="alice")

    assert props_ref.written is None


def test_set_rejects_reserved_field_alongside_a_valid_one():
    """A reserved field must not slip through by being mixed with a legal one."""
    catalog, props_ref = _catalog({})

    with pytest.raises(ValueError, match="reserved workspace lifecycle field"):
        catalog.set_workspace_properties(
            {"deletion_protection": True, "locked-by": None}, author="alice"
        )

    assert props_ref.written is None


def test_set_emits_audit_record(capsys):
    catalog, _ = _catalog({})

    catalog.set_workspace_properties({"deletion_protection": False}, author="alice")

    record = _emitted(capsys)[0]
    assert record["action"] == "set_workspace_properties"
    assert record["resource_type"] == "workspace"
    assert record["workspace"] == "ws"
    assert record["author"] == "alice"
    assert record["detail"]["properties"] == ["deletion_protection"]


def test_set_unauthenticated_records_no_author(capsys):
    """No author means no author - not an invented one."""
    catalog, _ = _catalog({})

    catalog.set_workspace_properties({"deletion_protection": False})

    assert _emitted(capsys)[0]["author"] is None
