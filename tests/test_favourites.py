"""opteryx_catalog.favourites - the edge between a principal and a workspace.

Three things are pinned because each fails silently if broken:

  WILDCARD  A `*` principal must be refused, never written and never read.
            A wildcard grant means "anyone may read this"; the same value
            here would mean "everyone has starred this". See §5 of
            web.opteryx/FAVOURITES-DESIGN.md and the module docstring.
  CLOSED    A read that errors is an EMPTY list, never a populated one and
            never a raise. The tree falls back to alphabetical.
  NO QUERY  `starred_workspaces` is one `get_all()` over known paths - no
            `where`, no collection-group, no index.
"""

import pytest

from opteryx_catalog import favourites as F


# ---------------------------------------------------------------------------
# A Firestore client that is just enough: paths, get_all, set, delete.
# ---------------------------------------------------------------------------
class _Ref:
    def __init__(self, client, path):
        self.client = client
        self.path = path  # tuple of segments

    @property
    def id(self):
        return self.path[-1]

    @property
    def parent(self):
        return _Ref(self.client, self.path[:-1])

    def document(self, name):
        return _Ref(self.client, self.path + (name,))

    def collection(self, name):
        return _Ref(self.client, self.path + (name,))

    def set(self, data):
        self.client.docs[self.path] = dict(data)
        self.client.log.append(("set", "/".join(self.path)))

    def delete(self):
        self.client.docs.pop(self.path, None)
        self.client.log.append(("delete", "/".join(self.path)))

    def stream(self):
        """Documents directly under this collection path."""
        if self.client.fail:
            raise RuntimeError("firestore hiccup")
        self.client.stream_calls += 1
        depth = len(self.path)
        return [
            _Snap(_Ref(self.client, p), True)
            for p in self.client.docs
            if len(p) == depth + 1 and p[:depth] == self.path
        ]


class _Snap:
    def __init__(self, ref, exists):
        self.reference = ref
        self.exists = exists

    @property
    def id(self):
        return self.reference.id


class _Client:
    def __init__(self, *, fail=False):
        self.docs = {}
        self.log = []
        self.fail = fail
        self.get_all_calls = 0
        self.stream_calls = 0

    def collection(self, name):
        return _Ref(self, (name,))

    def get_all(self, refs):
        self.get_all_calls += 1
        if self.fail:
            raise RuntimeError("firestore hiccup")
        return [_Snap(r, r.path in self.docs) for r in refs]


# ---------------------------------------------------------------------------
# paths
# ---------------------------------------------------------------------------
def test_the_path_mirrors_policies_access():
    ref = F.starred_ref(_Client(), "erp", "alice")
    assert ref.path == ("erp", "$favourites", "starred", "alice")


def test_star_then_unstar_round_trips_and_is_idempotent():
    db = _Client()
    F.star(db, "erp", "alice", now_ms=5)
    F.star(db, "erp", "alice", now_ms=6)  # twice is one star
    assert db.docs[("erp", "$favourites", "starred", "alice")] == {"starred_at_ms": 6}

    F.unstar(db, "erp", "alice")
    F.unstar(db, "erp", "alice")  # unstar of nothing is not an error
    assert ("erp", "$favourites", "starred", "alice") not in db.docs


def test_the_principal_is_normalized_like_a_grant_principal():
    # `_workspace_policies` in control matches grants by casefolded, trimmed
    # principal. A star keyed any other way would be invisible to a lookup by
    # the token's `sub`.
    db = _Client()
    F.star(db, "erp", "  Alice.Example@Corp.IO ")
    assert ("erp", "$favourites", "starred", "alice.example@corp.io") in db.docs
    assert F.starred_workspaces(db, ["erp"], "ALICE.EXAMPLE@CORP.IO") == ["erp"]


# ---------------------------------------------------------------------------
# WILDCARD
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("principal", ["*", " * ", "", None])
def test_the_wildcard_and_the_empty_principal_are_refused_everywhere(principal):
    db = _Client()
    with pytest.raises(ValueError):
        F.star(db, "erp", principal)
    with pytest.raises(ValueError):
        F.unstar(db, "erp", principal)
    with pytest.raises(ValueError):
        F.starred_workspaces(db, ["erp"], principal)
    assert db.log == []  # nothing was written on the way to the raise


def test_a_wildcard_star_on_disk_is_never_read_back_for_a_real_principal():
    # Even if something wrote one by another path, a lookup for `alice`
    # touches only `alice`'s document - there is no `in` clause to widen it.
    db = _Client()
    db.docs[("erp", "$favourites", "starred", "*")] = {"starred_at_ms": 1}
    assert F.starred_workspaces(db, ["erp"], "alice") == []


# ---------------------------------------------------------------------------
# NO QUERY, and what comes back
# ---------------------------------------------------------------------------
def test_starred_workspaces_is_one_batched_read_over_known_paths():
    db = _Client()
    F.star(db, "erp", "alice")
    F.star(db, "ledger", "alice")
    F.star(db, "erp", "bob")  # someone else's star on a workspace alice also sees

    got = F.starred_workspaces(db, ["analytics", "ledger", "erp", "ops"], "alice")

    assert got == ["ledger", "erp"]  # input order kept, nobody else's stars
    assert db.get_all_calls == 1


def test_input_is_deduplicated_and_blanks_are_dropped():
    db = _Client()
    F.star(db, "erp", "alice")
    assert F.starred_workspaces(db, ["erp", "", "erp", None], "alice") == ["erp"]
    assert F.starred_workspaces(db, [], "alice") == []


# ---------------------------------------------------------------------------
# CLOSED
# ---------------------------------------------------------------------------
def test_a_failing_read_is_an_empty_list_not_a_raise():
    db = _Client(fail=True)
    assert F.starred_workspaces(db, ["erp", "ledger"], "alice") == []


# ---------------------------------------------------------------------------
# The other direction: who starred this workspace
# ---------------------------------------------------------------------------
def test_starred_principals_lists_one_workspaces_stars_without_a_query():
    db = _Client()
    F.star(db, "erp", "alice")
    F.star(db, "erp", "bob")
    F.star(db, "ledger", "carol")  # a different workspace is not included

    assert F.starred_principals(db, "erp") == ["alice", "bob"]
    assert db.stream_calls == 1  # a plain listing, not a collection-group scan


def test_starred_principals_is_sorted_and_empty_when_nobody_has():
    db = _Client()
    for who in ("zoe", "alice", "mo"):
        F.star(db, "erp", who)
    assert F.starred_principals(db, "erp") == ["alice", "mo", "zoe"]
    assert F.starred_principals(db, "never-starred") == []


def test_starred_principals_reports_what_is_on_disk_not_who_still_has_access():
    """A revoked principal leaves their star behind. Telling that from a live
    one needs the current grants, which only the caller has - so this returns
    storage and control filters it. Pinned so the boundary does not drift."""
    db = _Client()
    F.star(db, "erp", "revoked-last-week")
    assert F.starred_principals(db, "erp") == ["revoked-last-week"]


def test_a_failing_listing_is_empty_not_a_partial_answer():
    db = _Client()
    F.star(db, "erp", "alice")
    db.fail = True
    assert F.starred_principals(db, "erp") == []
