# type:ignore
# isort: skip_file
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx_catalog.maki_nage import distogram
from pytest import approx

import random


def _quantile(data, q):
    """Simple quantile function for test validation."""
    sorted_data = sorted(data)
    n = len(sorted_data)
    if q <= 0:
        return sorted_data[0]
    if q >= 1:
        return sorted_data[-1]

    pos = q * (n - 1)
    lower_idx = int(pos)
    upper_idx = lower_idx + 1

    if upper_idx >= n:
        return sorted_data[lower_idx]

    fraction = pos - lower_idx
    return sorted_data[lower_idx] * (1 - fraction) + sorted_data[upper_idx] * fraction


def test_quantile():
    h = distogram.Distogram(bin_count=3)
    distogram.update(h, 16, count=4)
    distogram.update(h, 23, count=3)
    distogram.update(h, 28, count=5)

    assert distogram.quantile(h, 0.5) == approx(23.625)


def test_quantile_not_enough_elemnts():
    h = distogram.Distogram(bin_count=10)

    for i in [12.3, 5.4, 8.2, 100.53, 23.5, 13.98]:
        distogram.update(h, i)

    assert distogram.quantile(h, 0.5) == approx(13.14)


def test_quantile_on_left():
    h = distogram.Distogram(bin_count=6)

    data = [12.3, 5.2, 5.4, 4.9, 5.5, 5.6, 8.2, 30.53, 23.5, 13.98]
    for i in data:
        distogram.update(h, i)

    assert distogram.quantile(h, 0.01) == approx(_quantile(data, 0.01), rel=0.01)
    assert distogram.quantile(h, 0.05) == approx(_quantile(data, 0.05), rel=0.05)
    assert distogram.quantile(h, 0.25) == approx(_quantile(data, 0.25), rel=0.05)


def test_quantile_on_right():
    h = distogram.Distogram(bin_count=6)

    data = [12.3, 8.2, 100.53, 23.5, 13.98, 200, 200.2, 200.8, 200.4, 200.1]
    for i in data:
        distogram.update(h, i)

    assert distogram.quantile(h, 0.99) == approx(_quantile(data, 0.99), rel=0.01)
    assert distogram.quantile(h, 0.85) == approx(_quantile(data, 0.85), rel=0.01)


def test_normal():
    normal = [random.normalvariate(0.0, 1.0) for _ in range(10000)]
    h = distogram.Distogram(bin_count=64)

    for i in normal:
        distogram.update(h, i)

    assert distogram.quantile(h, 0.5) == approx(_quantile(normal, 0.5), abs=0.2)
    assert distogram.quantile(h, 0.95) == approx(_quantile(normal, 0.95), abs=0.2)


def test_quantile_empty():
    h = distogram.Distogram()

    assert distogram.quantile(h, 0.3) is None


def test_quantile_out_of_bouns():
    h = distogram.Distogram()

    for i in [1, 2, 3, 4, 5, 6, 6.7, 6.1]:
        distogram.update(h, i)

    assert distogram.quantile(h, -0.2) is None
    assert distogram.quantile(h, 10) is None
