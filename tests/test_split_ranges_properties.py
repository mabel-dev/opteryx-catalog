"""Property tests for the row arithmetic that partitions a sorted morsel.

``_split_ranges`` decides which rows land in which output file during a
combine-split. A range that overlaps its neighbour duplicates rows; a gap drops
them. Neither shows up as an error - the compaction commits and the dataset is
silently wrong - so the invariants are asserted directly rather than inferred
from a few worked examples.
"""

import os
import sys

# Ensure local package imports during test runs
sys.path.insert(0, os.path.join(sys.path[0], ".."))

from hypothesis import given
from hypothesis import strategies as st

from opteryx_catalog.catalog.compaction import DatasetCompactor

# _split_ranges touches no instance state; skip __init__ (which wants a dataset).
_compactor = DatasetCompactor.__new__(DatasetCompactor)


@given(n=st.integers(min_value=0, max_value=10_000_000), k=st.integers(min_value=-4, max_value=64))
def test_ranges_partition_every_row_exactly_once(n, k):
    ranges = _compactor._split_ranges(n, k)

    assert ranges, "a partition of n rows is never empty"
    assert ranges[0][0] == 0, "must start at row 0"
    assert ranges[-1][1] == n, "must end at row n"
    # Contiguous: each range starts where the previous one stopped. With the
    # endpoints above this is exactly "no gaps and no overlaps".
    assert all(ranges[i][1] == ranges[i + 1][0] for i in range(len(ranges) - 1))
    assert sum(hi - lo for lo, hi in ranges) == n, "row count is conserved"


@given(n=st.integers(min_value=1, max_value=10_000_000), k=st.integers(min_value=1, max_value=64))
def test_no_empty_slice_and_never_more_than_k(n, k):
    ranges = _compactor._split_ranges(n, k)

    # An empty slice becomes a zero-row output file.
    assert all(hi > lo for lo, hi in ranges)
    # The docstring's contract: ceil step, so *at most* k ranges. Exceeding k
    # would mean more output files than the caller sized its plan for.
    assert len(ranges) <= k


@given(n=st.integers(min_value=1, max_value=10_000_000), k=st.integers(min_value=1, max_value=64))
def test_slices_are_near_equal(n, k):
    ranges = _compactor._split_ranges(n, k)
    sizes = [hi - lo for lo, hi in ranges]

    # All full-width but the last, which carries the remainder. A wider spread
    # means one output file is disproportionately large.
    assert len(set(sizes[:-1])) <= 1
    assert sizes[-1] <= sizes[0]


@given(
    rows=st.integers(min_value=0, max_value=2048),
    k=st.integers(min_value=-4, max_value=32),
)
def test_split_into_k_conserves_rows(rows, k):
    """The same conservation property one layer up, against a stand-in morsel.

    Mirrors ``_FakeTable`` in test_compaction.py: ``_split_into_k`` needs only
    ``num_rows`` and ``slice``, and building real draken morsels here would test
    draken's slicing rather than this module's arithmetic.
    """

    class _FakeTable:
        def __init__(self, num_rows, offset=0):
            self.num_rows = num_rows
            self.offset = offset

        def slice(self, offset, length):
            return _FakeTable(length, self.offset + offset)

    slices = _compactor._split_into_k(_FakeTable(rows), k)

    assert sum(s.num_rows for s in slices) == rows
    # Slices tile the parent in order, so offsets are contiguous from 0.
    expected_offset = 0
    for s in slices:
        assert s.offset == expected_offset
        expected_offset += s.num_rows
