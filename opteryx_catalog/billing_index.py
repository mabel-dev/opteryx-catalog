"""Which billing account a workspace bills to, indexed so it can be asked
across every workspace at once.

`$properties['billing-account-id']` is the truth: the create route stamps it
as the claim on the name, and `reserve_workspace_names.py` reads it to tell a
real workspace from a held one. But a workspace is a ROOT collection in this
database (`firestore_client.collection(workspace)`), so there is no parent
collection to run `where("billing-account-id", "==", X)` over, and "which
workspaces bill to this account?" - the manage-workspaces rail, the
account-deactivation guard, the storage-billing attribution sweep - has no
way to be answered from the truth alone.

This is that way. One document per workspace, under a subcollection whose ID
is fixed, so a collection-group query reaches every workspace's copy in one
round trip:

    {workspace}/$billing/billing-account/current
        account_id: "acct_123"
        workspace:  "erp"            # the name, so a hit needs no path walk
        since_ms:   1757808000000    # when it last changed payer

Plain field names, not `$properties`' kebab-case: these are QUERIED, and a
hyphen in a bare Firestore field path is a parse error ("Path account-id not
consumed") unless it is backtick-quoted everywhere it is used. The other
queried documents in this database - `$policies/access` (`principal`, `role`,
`pattern`) and `members` (`identity`, `status`) - already use plain names for
the same reason; kebab-case is the convention for `$properties`, which is
only ever read whole.

It mirrors `{workspace}/$policies/access` exactly - a `$`-prefixed document
the customer namespace cannot address, holding a subcollection with a
library-constant ID - and it is queried the way `impact.py`, `trigger_firing`
and the relationships paths already query across workspaces. The ID cannot
collide with customer data: user-supplied names only ever appear as DOCUMENT
ids in this database (a collection is a document at the workspace root, a
dataset a document under `datasets`), and every collection id is a constant.

It is DERIVED, never read as truth on its own, and it lives in the same
database as the truth for one reason: so the two can be written in one
Firestore batch and either both land or neither does. That is exactly what the
`workspaces/{name}` record in control's `(default)` database - which this
replaces - could not do at any price. See control.opteryx
`docs/design/workspace-billing-on-the-namespace.md`.

Plain functions over a Firestore client, like `binding.py`, and for the same
reason: the readers here (an account listing, a billing sweep) have no
workspace handle to construct and must not pay for one per row.

The collection-group query needs an index with collection-group scope on
`billing-account.account_id`. Declared in opteryx-infra
`firestore-terraform/main.tf` as `billing_account_id`; apply that config rather
than creating it by hand, so one place decides what exists. It must be READY
before this module's readers deploy.
"""

from __future__ import annotations

import time
from typing import Any

BILLING_DOC = "$billing"
BILLING_ACCOUNT_SUBCOLLECTION = "billing-account"
BILLING_ACCOUNT_CURRENT_DOC = "current"

ACCOUNT_FIELD = "account_id"
WORKSPACE_FIELD = "workspace"
SINCE_FIELD = "since_ms"


def billing_index_ref(firestore_client, workspace: str):
    """The one index document for `workspace`."""
    return (
        firestore_client.collection(workspace)
        .document(BILLING_DOC)
        .collection(BILLING_ACCOUNT_SUBCOLLECTION)
        .document(BILLING_ACCOUNT_CURRENT_DOC)
    )


def billing_index_collection(firestore_client, workspace: str):
    """The subcollection holding it - what `drop_workspace` empties."""
    return (
        firestore_client.collection(workspace)
        .document(BILLING_DOC)
        .collection(BILLING_ACCOUNT_SUBCOLLECTION)
    )


def write_billing_index(
    firestore_client,
    workspace: str,
    account_id: str,
    *,
    batch=None,
    now_ms: int | None = None,
) -> None:
    """Point `workspace`'s index at `account_id`.

    A single fixed-id document, overwritten rather than appended: a rebill
    replaces the pointer, so there is never a window in which a workspace bills
    to two accounts and never a stale second document to reap.

    `batch` is the whole point of this module. Pass the `WriteBatch` that is
    also carrying the `$properties` write and the two land together; call it
    bare only from a backfill, where `$properties` already says what this
    should say.
    """
    if not workspace:
        raise ValueError("workspace must be a non-empty name")
    if not account_id:
        raise ValueError("account_id must be a non-empty id")
    doc = {
        ACCOUNT_FIELD: account_id,
        WORKSPACE_FIELD: workspace,
        SINCE_FIELD: int(time.time() * 1000) if now_ms is None else int(now_ms),
    }
    ref = billing_index_ref(firestore_client, workspace)
    if batch is not None:
        batch.set(ref, doc)
    else:
        ref.set(doc)


def clear_billing_index(firestore_client, workspace: str, *, batch=None) -> None:
    """Remove the pointer. Idempotent: deleting an absent document is a no-op."""
    ref = billing_index_ref(firestore_client, workspace)
    if batch is not None:
        batch.delete(ref)
    else:
        ref.delete()


def read_billing_account(firestore_client, workspace: str) -> str | None:
    """`account-id` from the index, or None when the workspace is unbilled or
    has no index document. For one workspace prefer `$properties` - this
    exists for the readers that have no handle and want the index's answer."""
    snap = billing_index_ref(firestore_client, workspace).get()
    if not snap.exists:
        return None
    return (snap.to_dict() or {}).get(ACCOUNT_FIELD) or None


def _workspace_of(doc) -> str | None:
    """The workspace a collection-group hit belongs to.

    The field first: it is written with the document so a hit needs no path
    walk, and a document that lost it is one this module did not write. The
    path is the fallback for exactly that case - `{ws}/$billing/billing-account/
    current` puts the workspace three parents up.
    """
    data = doc.to_dict() or {}
    name = data.get(WORKSPACE_FIELD)
    if name:
        return str(name)
    try:
        return doc.reference.parent.parent.parent.id
    except AttributeError:
        return None


def workspaces_billed_to(firestore_client, account_id: str, *, limit: int | None = None) -> list[str]:
    """Every workspace whose index points at `account_id`, by name.

    `limit=1` is the account-deactivation guard's "is anything still attached?"
    - the cheapest question this index answers.
    """
    if not account_id:
        return []
    query = firestore_client.collection_group(BILLING_ACCOUNT_SUBCOLLECTION).where(
        ACCOUNT_FIELD, "==", account_id
    )
    if limit is not None:
        query = query.limit(limit)
    names: list[str] = []
    for doc in query.stream():
        name = _workspace_of(doc)
        if name:
            names.append(name)
    return names


def workspace_accounts(firestore_client) -> dict[str, str]:
    """`{workspace: account_id}` for every billed workspace, in one stream.

    A workspace with no index document is ABSENT, not defaulted. That is the
    contract the storage-billing sweep already holds itself to: an unbilled
    workspace's usage stays unpriced and visible as a gap rather than charged
    to someone who did not incur it. Unbilled is a legitimate state - platform,
    test and reserved namespaces all are - and this must not invent a payer
    for it.
    """
    out: dict[str, str] = {}
    for doc in firestore_client.collection_group(BILLING_ACCOUNT_SUBCOLLECTION).stream():
        data: dict[str, Any] = doc.to_dict() or {}
        account = data.get(ACCOUNT_FIELD)
        name = _workspace_of(doc)
        if account and name:
            out[name] = str(account)
    return out
