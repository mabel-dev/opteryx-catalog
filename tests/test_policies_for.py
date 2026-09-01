"""Which workspaces a run's policies are read from.

`policies_for` builds the `policies` list a job document carries, and that list
IS the authority the run executes with - the worker hands it straight to
`opteryx.session(access_policies=...)`.

It read exactly one workspace: the catalog handle's own. That was a silent
authority ceiling on any statement crossing a workspace. A minted token carries
a principal's policies from EVERY workspace (authenticate.opteryx builds the
claim with a collection-group query), so the same statement typed by a person
was bound against all of them and run unattended was bound against one - every
relation outside it denied, however the principal was actually granted.
"""

from __future__ import annotations

import pytest

from opteryx_catalog.trigger_firing import policies_for


class _Doc:
    def __init__(self, doc_id, data):
        self.id = doc_id
        self._data = data

    def to_dict(self):
        return dict(self._data)


class _Query:
    """A `where(principal in [...])` filter over one workspace's policies."""

    def __init__(self, docs, principals):
        self._docs = docs
        self._principals = principals

    def stream(self):
        return [
            doc for doc in self._docs if doc.to_dict().get("principal") in self._principals
        ]


class _Access:
    def __init__(self, docs):
        self._docs = docs

    def where(self, filter=None):
        # The fake reads the FieldFilter's value the way Firestore would apply
        # it, rather than ignoring it - a fake that returned everything would
        # let a query for the wrong principal pass this file.
        return _Query(self._docs, list(filter.value))


class _PoliciesDoc:
    def __init__(self, docs):
        self._docs = docs

    def collection(self, name):
        assert name == "access"
        return _Access(self._docs)


class _WorkspaceCollection:
    def __init__(self, docs):
        self._docs = docs

    def document(self, name):
        assert name == "$policies"
        return _PoliciesDoc(self._docs)


class _FirestoreClient:
    """Root collections are workspaces - the property that lets one handle read
    another workspace's policies without constructing a second catalog."""

    def __init__(self, by_workspace):
        self.by_workspace = by_workspace
        self.read = []

    def collection(self, name):
        self.read.append(name)
        return _WorkspaceCollection(self.by_workspace.get(name, []))


class _Catalog:
    def __init__(self, workspace, by_workspace):
        self.workspace = workspace
        self.firestore_client = _FirestoreClient(by_workspace)


def _catalog(**by_workspace):
    return _Catalog("opteryx", {ws: list(docs) for ws, docs in by_workspace.items()})


OPTERYX_POLICIES = [
    _Doc("p-read-logs", {"principal": "ingest", "role": "reader", "pattern": "opteryx.ops.*"}),
]
PLATFORM_POLICIES = [
    _Doc("p-write-billing", {"principal": "ingest", "role": "writer", "pattern": "platform.billing.*"}),
]


def test_the_default_is_this_workspace_alone():
    """Unchanged for every caller that names nothing - the refresh path, which
    cannot cross a workspace, still reads exactly one."""
    catalog = _catalog(opteryx=OPTERYX_POLICIES, platform=PLATFORM_POLICIES)

    policies = policies_for(catalog, "ingest")

    assert [p["pattern"] for p in policies] == ["opteryx.ops.*"]
    assert catalog.firestore_client.read == ["opteryx"]


def test_a_named_set_is_read_in_order():
    catalog = _catalog(opteryx=OPTERYX_POLICIES, platform=PLATFORM_POLICIES)

    policies = policies_for(catalog, "ingest", ["opteryx", "platform"])

    assert [p["pattern"] for p in policies] == ["opteryx.ops.*", "platform.billing.*"]


def test_the_write_targets_workspace_is_where_its_grant_lives():
    """The whole defect in one assertion: without `platform` the run carries no
    policy covering `platform.billing.events` and the binder denies the write,
    however the principal was actually granted."""
    catalog = _catalog(opteryx=OPTERYX_POLICIES, platform=PLATFORM_POLICIES)

    narrow = policies_for(catalog, "ingest", ["opteryx"])
    wide = policies_for(catalog, "ingest", ["opteryx", "platform"])

    assert not [p for p in narrow if p["pattern"].startswith("platform.")]
    assert [p for p in wide if p["pattern"].startswith("platform.")]


def test_no_workspace_is_read_twice():
    catalog = _catalog(opteryx=OPTERYX_POLICIES)

    policies_for(catalog, "ingest", ["opteryx", "opteryx"])

    assert catalog.firestore_client.read == ["opteryx"]


def test_the_wildcard_principal_is_still_carried():
    """Unchanged by the widening: a `*` policy grants every human user, and the
    principal would hold it interactively too."""
    catalog = _catalog(
        platform=[
            _Doc("p-all", {"principal": "*", "role": "reader", "pattern": "platform.public.*"})
        ]
    )

    policies = policies_for(catalog, "ingest", ["platform"])

    assert [p["pattern"] for p in policies] == ["platform.public.*"]


def test_another_principals_policies_are_never_carried():
    catalog = _catalog(
        platform=[
            _Doc("p-other", {"principal": "someone-else", "role": "owner", "pattern": "platform.*"})
        ]
    )

    assert policies_for(catalog, "ingest", ["platform"]) is None


def test_a_workspace_with_no_policies_contributes_nothing():
    catalog = _catalog(opteryx=OPTERYX_POLICIES)

    policies = policies_for(catalog, "ingest", ["opteryx", "never-heard-of-it"])

    assert [p["pattern"] for p in policies] == ["opteryx.ops.*"]


def test_the_same_document_id_in_two_workspaces_is_two_policies():
    """Document ids are unique per workspace, not across them. Keying on the id
    alone would drop one of these - and they say different things."""
    catalog = _catalog(
        opteryx=[_Doc("p1", {"principal": "ingest", "role": "reader", "pattern": "opteryx.*"})],
        platform=[_Doc("p1", {"principal": "ingest", "role": "writer", "pattern": "platform.*"})],
    )

    policies = policies_for(catalog, "ingest", ["opteryx", "platform"])

    assert sorted(p["pattern"] for p in policies) == ["opteryx.*", "platform.*"]


def test_no_principal_reads_nothing():
    catalog = _catalog(opteryx=OPTERYX_POLICIES)

    assert policies_for(catalog, None, ["opteryx"]) is None
    assert catalog.firestore_client.read == []


def test_a_malformed_policy_row_is_skipped():
    catalog = _catalog(
        opteryx=[
            _Doc("p-no-pattern", {"principal": "ingest", "role": "reader"}),
            _Doc("p-no-role", {"principal": "ingest", "pattern": "opteryx.*"}),
        ]
    )

    assert policies_for(catalog, "ingest", ["opteryx"]) is None
