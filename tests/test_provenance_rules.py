"""The provenance rule set (catalog/provenance.py), in isolation.

What a receipt looks like, what a commit does to a dataset's standing source
list, and how that list is rebuilt from the receipts behind a snapshot. The
commit paths that APPLY these rules are covered in test_provenance_commits.py;
this file pins the rules themselves so the two cannot disagree about them.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from opteryx_catalog.catalog.metadata import MAX_READ_SOURCES
from opteryx_catalog.catalog.metadata import MAX_SOURCES
from opteryx_catalog.catalog.provenance import APPEND
from opteryx_catalog.catalog.provenance import CLEAR
from opteryx_catalog.catalog.provenance import REWRITE
from opteryx_catalog.catalog.provenance import UNCHANGED
from opteryx_catalog.catalog.provenance import effect_for_operation
from opteryx_catalog.catalog.provenance import is_self
from opteryx_catalog.catalog.provenance import merge_sources
from opteryx_catalog.catalog.provenance import normalize_read_sources
from opteryx_catalog.catalog.provenance import read_source_keys
from opteryx_catalog.catalog.provenance import recompute_sources

A = "ws.ops.a"
B = "ws.ops.b"
C = "ws.ops.c"


# ── the receipt ──────────────────────────────────────────────────────────────


def test_a_receipt_entry_has_exactly_three_keys():
    entries, truncated = normalize_read_sources([(A, 10, "version")])
    assert entries == [{"dataset": A, "snapshot-id": 10, "resolved-by": "version"}]
    assert truncated is False


def test_dict_entries_are_accepted_in_either_spelling():
    entries, _ = normalize_read_sources(
        [{"dataset": A, "snapshot_id": 10, "resolved_by": "tag"}, {"dataset": B, "snapshot-id": 5}]
    )
    assert entries == [
        {"dataset": A, "snapshot-id": 10, "resolved-by": "tag"},
        {"dataset": B, "snapshot-id": 5, "resolved-by": "current"},
    ]


def test_one_relation_read_twice_at_one_version_is_one_entry():
    entries, _ = normalize_read_sources([(A, 10), (A, 10, "current")])
    assert len(entries) == 1


def test_one_relation_read_at_two_versions_is_two_entries():
    """A self-join across versions is two reads, and collapsing them would drop
    the version somebody re-deriving the result needs."""
    entries, _ = normalize_read_sources([(A, 20, "version"), (A, 10, "version")])
    assert [e["snapshot-id"] for e in entries] == [10, 20]


def test_entries_are_sorted_by_dataset_then_snapshot():
    entries, _ = normalize_read_sources([(B, 1), (A, 2), (A, 1)])
    assert [(e["dataset"], e["snapshot-id"]) for e in entries] == [(A, 1), (A, 2), (B, 1)]


def test_a_relation_with_no_commits_yet_has_a_null_version():
    entries, _ = normalize_read_sources([(A, None)])
    assert entries[0]["snapshot-id"] is None
    assert read_source_keys(entries) == [A]


def test_keys_carry_the_bare_name_and_the_versioned_name():
    """One array, two questions: `array_contains A` finds every commit that
    read A at any version; `array_contains A@10` finds the ones that read
    that version."""
    entries, _ = normalize_read_sources([(A, 10), (A, 12), (B, 7)])
    assert read_source_keys(entries) == [A, B, f"{A}@10", f"{A}@12", f"{B}@7"]


def test_an_unqualified_name_is_refused():
    with pytest.raises(ValueError):
        normalize_read_sources([("ops.a", 10)])


def test_an_unknown_resolution_is_refused():
    with pytest.raises(ValueError):
        normalize_read_sources([(A, 10, "latest")])


def test_none_is_not_a_receipt():
    with pytest.raises(TypeError):
        normalize_read_sources(None)


def test_the_receipt_is_capped_and_says_so():
    many = [(f"ws.ops.t{i:04d}", 1) for i in range(MAX_READ_SOURCES + 5)]
    entries, truncated = normalize_read_sources(many)
    assert len(entries) == MAX_READ_SOURCES
    assert truncated is True


# ── what a commit does to the source list ────────────────────────────────────


@pytest.mark.parametrize(
    "operation, live, effect",
    [
        ("append", 10, APPEND),
        ("add-files", 10, APPEND),
        ("merge", 10, APPEND),
        ("update", 10, APPEND),
        ("delete", 10, APPEND),
        ("delete-files", 3, APPEND),
        ("delete", 0, CLEAR),
        ("merge", 0, CLEAR),
        ("truncate", 0, CLEAR),
        ("truncate", None, CLEAR),
        ("overwrite", 10, REWRITE),
        ("truncate-and-add-files", 10, REWRITE),
        ("compact", 10, UNCHANGED),
        ("statistics-refresh", 10, UNCHANGED),
        ("expire", 10, UNCHANGED),
    ],
)
def test_effect_by_operation(operation, live, effect):
    assert effect_for_operation(operation, live) == effect


def test_an_append_puts_the_new_sources_first_and_keeps_the_old():
    merged, dropped = merge_sources([C], [A, B], APPEND)
    assert merged == [C, A, B]
    assert dropped is False


def test_an_append_of_a_known_source_moves_it_to_the_front():
    merged, _ = merge_sources([B], [A, B], APPEND)
    assert merged == [B, A]


def test_a_rewrite_replaces_the_list():
    merged, _ = merge_sources([C], [A, B], REWRITE)
    assert merged == [C]


def test_a_clear_empties_the_list():
    assert merge_sources([C], [A, B], CLEAR) == ([], False)


def test_maintenance_leaves_the_list_alone():
    assert merge_sources([], [A, B], UNCHANGED) == ([A, B], False)


def test_the_list_keeps_the_most_recent_sixty_four_and_says_what_it_dropped():
    previous = [f"ws.ops.t{i:03d}" for i in range(MAX_SOURCES)]
    merged, dropped = merge_sources([C], previous, APPEND)
    assert len(merged) == MAX_SOURCES
    assert merged[0] == C
    assert previous[-1] not in merged, "the oldest is what the cap drops"
    assert dropped is True


def test_a_dataset_is_never_its_own_source():
    assert is_self("ws.ops.a", "ops.a", "ws") is True
    assert is_self("other.ops.a", "ops.a", "ws") is False
    # With no workspace to hand the relative name is the most that can be compared.
    assert is_self("ws.ops.a", "ops.a", None) is True


# ── rebuilding the list from the receipts behind a snapshot ──────────────────


def _doc(snapshot_id, parent, operation, reads, total=10, deleted=0, truncated=False):
    doc = {
        "snapshot-id": snapshot_id,
        "parent-snapshot-id": parent,
        "operation-type": operation,
        "summary": {"total-records": total, "total-deleted-records": deleted},
    }
    if reads is not None:
        doc["read-sources"] = [
            {"dataset": name, "snapshot-id": 1, "resolved-by": "current"} for name in reads
        ]
    if truncated:
        doc["read-sources-truncated"] = True
    return doc


def _chain(*docs):
    by_id = {d["snapshot-id"]: d for d in docs}
    return lambda sid: by_id.get(sid)


def test_recompute_unions_appends_newest_first_back_to_a_rewrite():
    fetch = _chain(
        _doc(1, None, "append", [A]),  # before the rewrite: not in the content
        _doc(2, 1, "overwrite", [B]),
        _doc(3, 2, "add-files", [C]),
        _doc(4, 3, "add-files", [B]),
    )
    assert recompute_sources(4, fetch, "ops.y", "ws") == ([B, C], True)


def test_recompute_stops_at_a_clear():
    fetch = _chain(_doc(1, None, "append", [A]), _doc(2, 1, "truncate", []), _doc(3, 2, "add-files", [C]))
    assert recompute_sources(3, fetch, "ops.y", "ws") == ([C], True)


def test_recompute_treats_a_delete_of_everything_as_a_clear():
    fetch = _chain(
        _doc(1, None, "append", [A]),
        _doc(2, 1, "delete", [], total=10, deleted=10),
        _doc(3, 2, "add-files", [C]),
    )
    assert recompute_sources(3, fetch, "ops.y", "ws") == ([C], True)


def test_recompute_walks_through_maintenance():
    fetch = _chain(_doc(1, None, "append", [A]), _doc(2, 1, "compact", []), _doc(3, 2, "statistics-refresh", []))
    assert recompute_sources(3, fetch, "ops.y", "ws") == ([A], True)


def test_recompute_reaching_the_chains_start_is_complete():
    fetch = _chain(_doc(1, None, "append", [A]))
    assert recompute_sources(1, fetch, "ops.y", "ws") == ([A], True)


def test_recompute_is_incomplete_when_a_receipt_is_missing():
    fetch = _chain(_doc(1, None, "append", None), _doc(2, 1, "add-files", [C]))
    assert recompute_sources(2, fetch, "ops.y", "ws") == ([C], False)


def test_recompute_stops_at_a_rewrite_with_no_receipt_and_is_incomplete():
    fetch = _chain(_doc(1, None, "append", [A]), _doc(2, 1, "overwrite", None), _doc(3, 2, "add-files", [C]))
    assert recompute_sources(3, fetch, "ops.y", "ws") == ([C], False)


def test_recompute_is_incomplete_when_a_snapshot_is_gone():
    fetch = _chain(_doc(2, 1, "add-files", [C]))
    assert recompute_sources(2, fetch, "ops.y", "ws") == ([C], False)


def test_recompute_is_incomplete_when_a_receipt_was_truncated():
    fetch = _chain(_doc(1, None, "append", [A], truncated=True))
    assert recompute_sources(1, fetch, "ops.y", "ws") == ([A], False)


def test_recompute_excludes_the_dataset_itself():
    fetch = _chain(_doc(1, None, "merge", ["ws.ops.y", A]))
    assert recompute_sources(1, fetch, "ops.y", "ws") == ([A], True)


def test_recompute_of_an_empty_head_is_empty_and_complete():
    assert recompute_sources(None, _chain(), "ops.y", "ws") == ([], True)
