"""`describe()`'s per-column distribution: per-file histograms re-binned onto the
dataset's range, by opteryx-core's distogram when opteryx is installed and by a
midpoint re-bin when it is not."""

import pytest

from opteryx_catalog.catalog import dataset

ADJACENT = [(0.0, 10.0, [5, 5]), (10.0, 20.0, [5, 5])]
OVERLAPPING = [(0.0, 100.0, [10] * 10), (50.0, 5000.0, [3, 0, 7, 1, 1, 1, 1, 1, 1, 4])]

MIDPOINT_OVERLAPPING = [103, 0, 7, 1, 1, 1, 1, 1, 1, 4]


def test_midpoint_rebin_places_each_bucket_by_its_midpoint():
    assert dataset._midpoint_distribution(ADJACENT, 0.0, 20.0, 4) == [5, 5, 5, 5]
    assert dataset._midpoint_distribution(OVERLAPPING, 0.0, 5000.0, 10) == MIDPOINT_OVERLAPPING


def test_distogram_rebin_keeps_the_total():
    pytest.importorskip("opteryx.third_party.maki_nage.distogram")
    assert dataset._distogram_distribution(ADJACENT, 0.0, 20.0, 4) == [5, 5, 5, 5]
    merged = dataset._distogram_distribution(OVERLAPPING, 0.0, 5000.0, 10)
    assert sum(merged) == 120
    assert merged[0] > 100


@pytest.mark.parametrize("installed", [True, False])
def test_the_rebin_follows_whether_opteryx_is_installed(monkeypatch, installed):
    if installed:
        pytest.importorskip("opteryx.third_party.maki_nage.distogram")
    monkeypatch.setattr(dataset, "_HAS_OPTERYX", installed)
    expected = (dataset._distogram_distribution if installed else dataset._midpoint_distribution)(
        OVERLAPPING, 0.0, 5000.0, 10
    )
    assert dataset._merged_distribution(OVERLAPPING, 0.0, 5000.0, 10) == expected
