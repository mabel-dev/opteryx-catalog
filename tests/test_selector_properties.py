"""Property tests for the compaction file selectors.

The selectors decide which manifest entries a compaction pass rewrites. Three
things must hold for any input, because the executor trusts the plan it is
handed: the plan only names files that were offered (never a fabricated or
duplicated entry), it never claims a single file (a one-file "merge" is a
pointless rewrite that also fails to reduce the file count), and the combined
input respects the size ceiling the executor sized its memory budget against.
"""

import os
import sys

# Ensure local package imports during test runs
sys.path.insert(0, os.path.join(sys.path[0], ".."))

from hypothesis import given
from hypothesis import strategies as st

from opteryx_catalog.catalog.compaction import MAX_SELECTED_BUDGET_BYTES
from opteryx_catalog.catalog.compaction import MIN_FILE_SIZE_BYTES
from opteryx_catalog.catalog.compaction import MIN_SIZE_BYTES
from opteryx_catalog.catalog.compaction import SMALL_FILE_BYTES
from opteryx_catalog.catalog.compaction import TARGET_SIZE_BYTES
from opteryx_catalog.catalog.compaction import DatasetCompactor
from opteryx_catalog.catalog.compaction import entry_size

MB = 1024 * 1024

# These three selectors read module constants and the entries they are handed,
# nothing off self; skip __init__ (which wants a real dataset).
_compactor = DatasetCompactor.__new__(DatasetCompactor)

# Sizes clustered on the thresholds the selectors branch on, since a uniform
# random spread over 0..5GB almost never lands on one.
_BOUNDARIES = [
    0,
    1,
    MIN_FILE_SIZE_BYTES - 1,
    MIN_FILE_SIZE_BYTES,
    MIN_SIZE_BYTES - 1,
    MIN_SIZE_BYTES,
    SMALL_FILE_BYTES - 1,
    SMALL_FILE_BYTES,
    TARGET_SIZE_BYTES - 1,
    TARGET_SIZE_BYTES,
    TARGET_SIZE_BYTES + 1,
]
_sizes = st.one_of(
    st.sampled_from(_BOUNDARIES),
    st.integers(min_value=0, max_value=5 * 1024 * MB),
    # NULL sizes are real: write_parquet_manifest fills absent keys with SQL
    # NULL, which is why entry_size exists. See entry_int's docstring.
    st.none(),
)


@st.composite
def _entries(draw, sizes=_sizes, min_size=0, max_size=24):
    """Distinct manifest-entry dicts. Distinct objects so a selector returning
    the same file twice is visible as a duplicate identity, not a coincidence."""
    drawn = draw(st.lists(sizes, min_size=min_size, max_size=max_size))
    return [
        {"file_path": f"mem://data/f{i}.parquet", "uncompressed_size_in_bytes": size}
        for i, size in enumerate(drawn)
    ]


def _assert_plan_is_sound(plan, offered):
    """Invariants every selector's plan shares."""
    if plan is None:
        return
    files = plan["files"]
    offered_ids = {id(e) for e in offered}

    assert len(files) >= 2, "a plan naming fewer than 2 files cannot reduce the file count"
    assert all(id(f) in offered_ids for f in files), "plan names a file that was not offered"
    assert len({id(f) for f in files}) == len(files), "plan names the same file twice"


@given(entries=_entries())
def test_brute_compaction_plan_is_sound(entries):
    plan = _compactor._select_brute_compaction(entries)
    _assert_plan_is_sound(plan, entries)

    if plan is not None:
        files = plan["files"]
        # Only merge candidates are eligible; a file already near target must
        # not be dragged into a rewrite.
        assert all(entry_size(f) < SMALL_FILE_BYTES for f in files)
        # The executor holds the combined input, so the budget is a memory bound.
        assert sum(entry_size(f) for f in files) <= MAX_SELECTED_BUDGET_BYTES


@given(sub_floor=_entries(sizes=st.integers(min_value=0, max_value=MIN_FILE_SIZE_BYTES - 1)))
def test_brute_consolidation_plan_is_sound(sub_floor):
    # Contract: the caller filters to sub-floor files (see _select_brute_merge).
    plan = _compactor._select_brute_consolidation(sub_floor, "col_a")
    _assert_plan_is_sound(plan, sub_floor)

    if plan is not None:
        # Rule 1: never build an input larger than one target output.
        assert sum(entry_size(f) for f in plan["files"]) <= TARGET_SIZE_BYTES


@st.composite
def _file_ranges(draw):
    """``{"min", "size", "entry"}`` records, the shape _select_binpack packs."""
    entries = draw(_entries(sizes=st.integers(min_value=0, max_value=5 * 1024 * MB)))
    return [
        {
            "min": draw(st.integers(min_value=-1000, max_value=1000)),
            "size": e["uncompressed_size_in_bytes"],
            "entry": e,
        }
        for e in entries
    ]


@given(file_ranges=_file_ranges())
def test_binpack_plan_is_sound(file_ranges):
    plan = _compactor._select_binpack(file_ranges, "col_a")
    _assert_plan_is_sound(plan, [fr["entry"] for fr in file_ranges])

    if plan is None:
        return

    files = plan["files"]
    # Settled files are left alone so packing converges - pulling one in would
    # rewrite a file that is already the right size, every pass, forever.
    assert all(entry_size(f) < MIN_SIZE_BYTES for f in files)
    assert sum(entry_size(f) for f in files) <= TARGET_SIZE_BYTES

    # Consecutive in sort-key order. Packing a non-consecutive set would union
    # a key range spanning files left behind, manufacturing new overlap for the
    # decluster tier to chase. `sorted` is stable, so equal mins keep input
    # order and this reproduces the selector's own ordering exactly.
    ordered = [id(fr["entry"]) for fr in sorted(file_ranges, key=lambda fr: fr["min"])]
    picked = [id(f) for f in files]
    start = ordered.index(picked[0])
    assert ordered[start : start + len(picked)] == picked


@given(entries=_entries(min_size=2, max_size=8, sizes=st.integers(min_value=1, max_value=64 * MB)))
def test_tiny_files_always_make_progress(entries):
    """Two or more tiny files must always produce a plan.

    "There is no volume threshold to wait for" (_select_brute_consolidation): a
    drip-fed dataset that accretes small files and never gets a plan is the
    small-files problem this rule exists to fix.
    """
    plan = _compactor._select_brute_consolidation(entries, "col_a")

    assert plan is not None
    assert len(plan["files"]) == len(entries)
