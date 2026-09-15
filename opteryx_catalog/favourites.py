"""Which workspaces a principal has starred, stored where the star is.

A favourite is an edge between a principal and a workspace. It is stored on
the workspace side, as one near-empty document whose EXISTENCE is the fact:

    {workspace}/$favourites/starred/{principal}
        starred_at_ms: 1757808000000

Presence means starred; absence means not. An unstar deletes the document.
There is deliberately no `starred: false` - a tri-state would invite a reader
that tells "explicitly unstarred" from "never starred" for no purpose anyone
has. See web.opteryx `FAVOURITES-DESIGN.md`.

It mirrors `{workspace}/$policies/access` exactly - a `$`-prefixed document
the customer namespace cannot address, holding a subcollection with a
library-constant ID, keyed by the SAME principal value the grants use. That
last point is why the key is the principal and not some other user id: the
`catalogs` database already holds this foreign key, in `$policies/access`, and
a second reference to a user must agree with the first.

── There is no query, so there is no index ──────────────────────────────────
The hot-path question - which workspaces has this principal starred? - looks
like a search across every workspace and would be a collection-group query
needing a composite index. It is not. The caller's policies already name
every workspace they can see, so every document path is known before the
read starts: `starred_workspaces` is N direct lookups in one `get_all()`,
which the service document already issues over `{workspace}/$properties` for
the same workspace set. One more reference per workspace in a batch that
already exists. Nothing here needs `firestore-terraform`.

── The wildcard trap ────────────────────────────────────────────────────────
`trigger_firing.fetch_policies_for_principal` deliberately appends
`WILDCARD_PRINCIPAL` so a wildcard grant reaches a human's token. Favourites
must NEVER do that: a wildcard grant means "anyone may read this workspace",
and the same pattern here would mean "everyone has starred this workspace" -
every user would open the Studio with strangers' shortcuts at the top of
their tree. Every lookup below keys on the concrete principal only; there is
no `in` clause anywhere in this module, on purpose.

── Deletion is not automatic ────────────────────────────────────────────────
Firestore deletes are not recursive and `drop_workspace` skips `$`-prefixed
documents in its generic loop, so this subcollection must be cleared by name
(`starred_collection`) in both `drop_workspace` and `_unlink_bound_workspace`
- beside `$policies/access`, for the reason that line gives: left behind, a
star silently reactivates if the workspace name is ever reused, and the next
workspace created under that name arrives already starred by strangers.

Plain functions over a Firestore client, like `billing_index.py` and for the
same reason: the readers (a service document, an API route) have no
workspace handle to construct and must not pay for one per row.
"""

from __future__ import annotations

import time
from typing import Iterable

FAVOURITES_DOC = "$favourites"
STARRED_SUBCOLLECTION = "starred"

STARRED_AT_FIELD = "starred_at_ms"

# Same constant `trigger_firing` and `opteryx_access` use. Named here so the
# guard in `_check_principal` reads as what it is rather than a magic string.
WILDCARD_PRINCIPAL = "*"


def _check_workspace(workspace: str) -> str:
    if not workspace or not isinstance(workspace, str):
        raise ValueError("workspace must be a non-empty name")
    return workspace


def _check_principal(principal: str) -> str:
    """The concrete principal a star belongs to - never the wildcard.

    Refused rather than silently ignored: a caller that reaches this with
    `*` has already confused a grant with a favourite, and the one thing
    worse than raising is writing a star that every user would then inherit.
    """
    if not principal or not isinstance(principal, str):
        raise ValueError("principal must be a non-empty identity")
    # Casefolded and trimmed, EXACTLY as `opteryx_access.patterns.normalize`
    # does to a grant's principal before matching it. The grants are matched
    # by normalized value (control's `_workspace_policies` reads them whole
    # for precisely this reason), so the star has to be keyed the same way or
    # `Alice` and `alice` would hold two different stars on one workspace and
    # the tree, reading by the token's `sub`, would find at most one of them.
    # Inlined rather than imported: opteryx_catalog does not depend on
    # opteryx_access, and two characters of semantics do not justify adding it.
    normalized = principal.strip().lower()
    if not normalized:
        raise ValueError("principal must be a non-empty identity")
    if normalized == WILDCARD_PRINCIPAL:
        raise ValueError("the wildcard principal cannot star a workspace")
    return normalized


def starred_collection(firestore_client, workspace: str):
    """The subcollection holding every star on `workspace` - what
    `drop_workspace` empties, and what a "who starred this?" reader lists."""
    return (
        firestore_client.collection(_check_workspace(workspace))
        .document(FAVOURITES_DOC)
        .collection(STARRED_SUBCOLLECTION)
    )


def starred_ref(firestore_client, workspace: str, principal: str):
    """The one document that is `principal`'s star on `workspace`."""
    return starred_collection(firestore_client, workspace).document(_check_principal(principal))


def star(firestore_client, workspace: str, principal: str, *, now_ms: int | None = None) -> None:
    """Star `workspace` for `principal`. Idempotent: starring twice is one star.

    `set`, not `create`: the second call must not fail, and the timestamp
    moving to "now" on a re-star is harmless - nothing reads it for ordering.
    """
    starred_ref(firestore_client, workspace, principal).set(
        {STARRED_AT_FIELD: int(now_ms if now_ms is not None else time.time() * 1000)}
    )


def unstar(firestore_client, workspace: str, principal: str) -> None:
    """Remove `principal`'s star on `workspace`. Idempotent: deleting a
    document that is not there is not an error in Firestore, and must not be
    one here - an unstar of something never starred is still "not starred"."""
    starred_ref(firestore_client, workspace, principal).delete()


def starred_workspaces(firestore_client, workspaces: Iterable[str], principal: str) -> list[str]:
    """Which of `workspaces` `principal` has starred, in one round trip.

    N direct document lookups via `get_all()`, NOT a collection-group query -
    see the module docstring for why that distinction is the whole design.
    The result keeps the input's order, deduplicated, so a caller sorting by
    it gets something stable.

    Fails CLOSED. Any error reading is an empty list, never a populated one:
    the tree falls back to alphabetical, which is what it did before this
    feature existed. It must never answer with another principal's stars, and
    the surest way to guarantee that is to never guess.
    """
    principal = _check_principal(principal)
    names = list(dict.fromkeys(w for w in workspaces if w))
    if not names:
        return []

    refs = [starred_ref(firestore_client, w, principal) for w in names]
    try:
        snapshots = firestore_client.get_all(refs)
        found = set()
        for snap in snapshots:
            if snap.exists:
                # parent = `starred`, parent.parent = `$favourites`,
                # parent.parent.parent = the workspace collection.
                found.add(snap.reference.parent.parent.parent.id)
    except Exception:
        return []

    return [w for w in names if w in found]


__all__ = [
    "FAVOURITES_DOC",
    "STARRED_SUBCOLLECTION",
    "STARRED_AT_FIELD",
    "WILDCARD_PRINCIPAL",
    "starred_collection",
    "starred_ref",
    "star",
    "unstar",
    "starred_workspaces",
]
