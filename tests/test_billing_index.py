"""The billing index: `{workspace}/$billing/billing-account/current`.

`$properties['billing-account-id']` is the truth about who pays; the index is
the copy a collection-group query can reach across every workspace at once.
What these pin is the contract between them - written together or not at all,
absent for an unbilled workspace rather than defaulted, and gone when the
workspace is - because the readers (an account's rail, the storage-billing
sweep) are DRIVEN by it, and a wrong index does not merely litter: it lists a
workspace under the wrong account or prices its storage to the wrong payer.
"""

from __future__ import annotations

from unittest.mock import patch

from opteryx_catalog import billing_index
from opteryx_catalog.opteryx_catalog import OpteryxCatalog

# --- a Firestore stand-in with the three things this module needs ----------
# collection_group() over the whole tree, batch() with merge, and enough of
# the reference/snapshot surface to walk a hit back to its workspace.


class _Snap:
    def __init__(self, ref):
        self.id = ref.id
        self.reference = ref
        self.exists = ref._data is not None
        self._data = ref._data

    def to_dict(self):
        return dict(self._data) if self._data is not None else None


class _DocRef:
    def __init__(self, parent, doc_id):
        self.parent = parent
        self.id = doc_id
        self._data = None
        self._subcollections: dict[str, _CollRef] = {}

    def get(self):
        return _Snap(self)

    def set(self, data, merge=False):
        if merge and self._data is not None:
            self._data.update(data)
        else:
            self._data = dict(data)

    def delete(self):
        self._data = None

    def collection(self, name):
        return self._subcollections.setdefault(name, _CollRef(self, name))


class _CollRef:
    def __init__(self, parent, name):
        self.parent = parent
        self.id = name
        self._docs: dict[str, _DocRef] = {}

    def document(self, doc_id):
        return self._docs.setdefault(doc_id, _DocRef(self, doc_id))

    def stream(self):
        return [_Snap(d) for d in self._docs.values() if d._data is not None]

    def list_documents(self):
        return list(self._docs.values())


class _Query:
    def __init__(self, snaps):
        self._snaps = list(snaps)

    def where(self, field, op, value):
        assert op == "=="
        return _Query(s for s in self._snaps if (s.to_dict() or {}).get(field) == value)

    def limit(self, n):
        return _Query(self._snaps[:n])

    def stream(self):
        return iter(self._snaps)


class _Batch:
    """Records writes and applies them only on commit - the property the
    module leans on. A batch that applied eagerly would pass every test here
    while silently not being atomic."""

    def __init__(self):
        self._ops = []
        self.committed = False

    def set(self, ref, data, merge=False):
        self._ops.append(("set", ref, data, merge))

    def delete(self, ref):
        self._ops.append(("delete", ref, None, False))

    def commit(self):
        for op, ref, data, merge in self._ops:
            if op == "set":
                ref.set(data, merge=merge)
            else:
                ref.delete()
        self.committed = True


class _Client:
    def __init__(self):
        self._roots: dict[str, _CollRef] = {}
        self.batches: list[_Batch] = []

    def collection(self, name):
        return self._roots.setdefault(name, _CollRef(None, name))

    def batch(self):
        batch = _Batch()
        self.batches.append(batch)
        return batch

    def collection_group(self, name):
        hits = []

        def walk(coll):
            if coll.id == name:
                hits.extend(coll.stream())
            for doc in coll._docs.values():
                for sub in doc._subcollections.values():
                    walk(sub)

        for root in self._roots.values():
            walk(root)
        return _Query(hits)


def _index_doc(client, workspace):
    return billing_index.billing_index_ref(client, workspace).get().to_dict()


# --- the plain functions ----------------------------------------------------


def test_write_puts_the_workspace_name_in_the_document():
    """A collection-group hit must not need a path walk to know whose it is."""
    client = _Client()
    billing_index.write_billing_index(client, "erp", "acct_1", now_ms=5)
    assert _index_doc(client, "erp") == {"account_id": "acct_1", "workspace": "erp", "since_ms": 5}


def test_rewrite_replaces_rather_than_appends():
    """One document, fixed id: a rebill can never leave a workspace pointing at
    two accounts, and there is never a stale second document to reap."""
    client = _Client()
    billing_index.write_billing_index(client, "erp", "acct_1", now_ms=1)
    billing_index.write_billing_index(client, "erp", "acct_2", now_ms=2)

    assert billing_index.workspaces_billed_to(client, "acct_1") == []
    assert billing_index.workspaces_billed_to(client, "acct_2") == ["erp"]
    assert len(billing_index.billing_index_collection(client, "erp").list_documents()) == 1


def test_workspaces_billed_to_reaches_every_workspace():
    client = _Client()
    billing_index.write_billing_index(client, "erp", "acct_1", now_ms=1)
    billing_index.write_billing_index(client, "ops", "acct_1", now_ms=1)
    billing_index.write_billing_index(client, "other", "acct_2", now_ms=1)

    assert sorted(billing_index.workspaces_billed_to(client, "acct_1")) == ["erp", "ops"]
    assert billing_index.workspaces_billed_to(client, "nobody") == []
    assert billing_index.workspaces_billed_to(client, "") == []


def test_limit_one_is_the_deactivation_guards_question():
    client = _Client()
    billing_index.write_billing_index(client, "erp", "acct_1", now_ms=1)
    billing_index.write_billing_index(client, "ops", "acct_1", now_ms=1)
    assert len(billing_index.workspaces_billed_to(client, "acct_1", limit=1)) == 1


def test_workspace_accounts_omits_the_unbilled_rather_than_defaulting():
    """The storage-billing sweep's contract: no payer means no row, so the
    usage stays visible as a gap instead of charged to someone who did not
    incur it."""
    client = _Client()
    billing_index.write_billing_index(client, "erp", "acct_1", now_ms=1)
    # A namespace that exists and bills to nobody: $properties only.
    client.collection("sandbox").document("$properties").set({"billing-account-id": None})

    assert billing_index.workspace_accounts(client) == {"erp": "acct_1"}


def test_workspace_of_falls_back_to_the_path_for_a_document_without_the_field():
    """Belt and braces for a document this module did not write."""
    client = _Client()
    billing_index.billing_index_ref(client, "erp").set({"account_id": "acct_1"})
    assert billing_index.workspace_accounts(client) == {"erp": "acct_1"}
    assert billing_index.workspaces_billed_to(client, "acct_1") == ["erp"]


def test_read_billing_account_is_none_for_absent_or_blank():
    client = _Client()
    assert billing_index.read_billing_account(client, "erp") is None
    billing_index.billing_index_ref(client, "erp").set({"account_id": ""})
    assert billing_index.read_billing_account(client, "erp") is None


def test_clear_is_idempotent():
    client = _Client()
    billing_index.clear_billing_index(client, "never-indexed")
    billing_index.write_billing_index(client, "erp", "acct_1", now_ms=1)
    billing_index.clear_billing_index(client, "erp")
    billing_index.clear_billing_index(client, "erp")
    assert billing_index.workspace_accounts(client) == {}


def test_batch_defers_the_write_until_commit():
    client = _Client()
    batch = client.batch()
    billing_index.write_billing_index(client, "erp", "acct_1", batch=batch, now_ms=1)
    assert _index_doc(client, "erp") is None
    batch.commit()
    assert _index_doc(client, "erp")["account_id"] == "acct_1"


# --- the handle method: truth and index in one batch ------------------------


def _handle(client, workspace="erp"):
    client.collection(workspace).document("$properties").set({"billing-account-id": None})
    with patch("opteryx_catalog.opteryx_catalog.firestore.Client", return_value=client):
        return OpteryxCatalog(workspace=workspace)


def test_set_billing_account_writes_truth_and_index_in_one_batch():
    client = _Client()
    catalog = _handle(client)

    with patch("opteryx_catalog.opteryx_catalog.emit_audit") as audit:
        catalog.set_billing_account("acct_1", author="alice")

    props = client.collection("erp").document("$properties").get().to_dict()
    assert props["billing-account-id"] == "acct_1"
    assert _index_doc(client, "erp")["account_id"] == "acct_1"
    # ONE batch carried both, and it was committed - not two eager writes that
    # happen to agree today.
    assert len(client.batches) == 1
    assert client.batches[0].committed
    kinds = sorted(op for op, *_ in client.batches[0]._ops)
    assert kinds == ["set", "set"]
    audit.assert_called_once()
    assert audit.call_args.kwargs["billing_account"] == "acct_1"


def test_set_billing_account_none_is_the_unbilled_state():
    """Unbilled is legitimate - platform, test, reserved - and it means absent
    from every account's listing, not present under a default."""
    client = _Client()
    catalog = _handle(client)
    with patch("opteryx_catalog.opteryx_catalog.emit_audit"):
        catalog.set_billing_account("acct_1", author="alice")
        catalog.set_billing_account(None, author="alice")

    props = client.collection("erp").document("$properties").get().to_dict()
    assert props["billing-account-id"] is None
    assert _index_doc(client, "erp") is None
    assert billing_index.workspace_accounts(client) == {}


def test_set_billing_account_merges_rather_than_replacing_properties():
    """`$properties` carries the guards, the lock and the catalog binding; a
    billing write must not blank them."""
    client = _Client()
    catalog = _handle(client)
    client.collection("erp").document("$properties").set(
        {"deletion_protection": False, "catalog": {"kind": "postgres"}}, merge=True
    )
    with patch("opteryx_catalog.opteryx_catalog.emit_audit"):
        catalog.set_billing_account("acct_1")

    props = client.collection("erp").document("$properties").get().to_dict()
    assert props["deletion_protection"] is False
    assert props["catalog"] == {"kind": "postgres"}
    assert props["billing-account-id"] == "acct_1"
