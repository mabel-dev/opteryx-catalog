# type:ignore
# isort: skip_file
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx_catalog.maki_nage import distogram
import random
from pytest import approx


def _histogram(values, num_bins):
    """Pure Python histogram implementation for test validation."""
    if not values or num_bins < 1:
        return [], []

    min_val = min(values)
    max_val = max(values)

    if min_val == max_val:
        return [len(values)], [min_val, max_val]

    bin_width = (max_val - min_val) / num_bins
    bin_edges = [min_val + i * bin_width for i in range(num_bins + 1)]
    bin_edges[-1] = max_val

    counts = [0] * num_bins
    for val in values:
        if val == max_val:
            bin_idx = num_bins - 1
        else:
            bin_idx = int((val - min_val) / bin_width)
            bin_idx = min(bin_idx, num_bins - 1)
        counts[bin_idx] += 1

    return counts, bin_edges


def test_histogram():
    normal = [random.normalvariate(0.0, 1.0) for _ in range(10000)]
    h = distogram.Distogram(bin_count=64)

    for i in normal:
        distogram.update(h, i)

    # Verify distogram produces valid histogram output
    d_values, d_edges = distogram.histogram(h, 10)
    assert len(d_values) == 10
    assert len(d_edges) == 11

    h = distogram.Distogram(bin_count=3)
    distogram.update(h, 23)
    distogram.update(h, 28)
    distogram.update(h, 16)
    assert distogram.histogram(h, bin_count=3) == (
        approx([1.0714285714285714, 0.6285714285714286, 1.3]),
        [16.0, 20.0, 24.0, 28],
    )
    assert sum(distogram.histogram(h, bin_count=3)[0]) == approx(3.0)


def test_histogram_on_too_small_distribution():
    h = distogram.Distogram(bin_count=64)

    for i in range(5):
        distogram.update(h, i)

    assert distogram.histogram(h, 10) is None


def test_format_histogram():
    bin_count = 4
    h = distogram.Distogram(bin_count=bin_count)

    for i in range(4):
        distogram.update(h, i)

    hist = distogram.histogram(h, bin_count=bin_count)
    assert len(hist[1]) == len(hist[0]) + 1
