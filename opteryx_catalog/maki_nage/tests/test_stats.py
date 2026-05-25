# type:ignore
# isort: skip_file
import sys
import os
import statistics
import math

sys.path.insert(1, os.path.join(sys.path[0], "../../../.."))

from opteryx_catalog.maki_nage import distogram
from pytest import approx


import random


def test_stats():
    normal = [random.normalvariate(0.0, 1.0) for _ in range(10000)]
    h = distogram.Distogram()

    for i in normal:
        distogram.update(h, i)

    # Compare against pure Python statistical implementations
    py_mean = statistics.mean(normal)
    py_var = statistics.variance(normal)
    py_std = math.sqrt(py_var)

    assert distogram.mean(h) == approx(py_mean, abs=0.1)
    assert distogram.variance(h) == approx(py_var, abs=0.1)
    assert distogram.stddev(h) == approx(py_std, abs=0.1)
