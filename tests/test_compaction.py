"""
Test script for compaction functionality.

This tests the DatasetCompactor class with both brute and performance strategies.
"""
import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-catalog"))

from unittest.mock import Mock

from opteryx_catalog.catalog.compaction import (
    MIN_FILE_SIZE_BYTES,
    MIN_SIZE_BYTES,
    SMALL_FILE_BYTES,
    SORT_AWARE_FLOOR_BYTES,
    TARGET_SIZE_BYTES,
    DatasetCompactor,
)
from opteryx_catalog.catalog.metadata import DatasetMetadata, Snapshot

_MB = 1024 * 1024


class _FixedChoice:
    """Minimal ``random``-like stub for ``_select_overlap_decluster``'s
    injectable ``rng``: ``.choice()`` always returns whichever offered
    ``file_ranges`` entry has this ``file_path``, regardless of what else is
    offered. Matches by path (not identity) because callers like
    ``_select_sort_aware_merge`` rebuild ``file_ranges`` internally from
    ``entries``, so a pre-built object reference wouldn't survive the call.
    ``_select_overlap_decluster`` only calls ``.choice()`` (the seed-file
    pick) -- growth from there is deterministic (greedy by overlap amount),
    so fixing the seed is enough to make a test fully reproducible."""

    def __init__(self, file_path):
        self._file_path = file_path

    def choice(self, seq):
        return next(fr for fr in seq if fr["entry"]["file_path"] == self._file_path)


def create_test_table(num_rows: int, value_range: tuple = (0, 100)):
    """Create a simple test Morsel with a timestamp column for sorting."""
    import random

    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel

    timestamps = sorted([random.randint(value_range[0], value_range[1]) for _ in range(num_rows)])
    values = [f"value_{i}" for i in range(num_rows)]

    m = Morsel()
    m.append_vector("timestamp", vector_from_sequence(timestamps, dtype="INTEGER"))
    m.append_vector("value", vector_from_sequence(values, dtype="VARCHAR"))
    return m


def test_brute_compaction():
    """Test brute force compaction strategy."""
    print("Testing brute force compaction...")

    # Create mock dataset
    dataset = Mock()
    dataset.metadata = DatasetMetadata(
        dataset_identifier="test_dataset",
        location="/tmp/test_data",
    )
    dataset.metadata.sort_orders = []  # No sort order for brute
    dataset.metadata.snapshots = []
    dataset.metadata.current_snapshot = None

    # Two under-threshold files (merge candidates) plus one already at/above
    # SMALL_FILE_BYTES which must be left alone. Sizes are expressed relative to
    # the actual thresholds so the test can't go stale if the constants move.
    small = SMALL_FILE_BYTES // 4  # comfortably below threshold, two fit in memory
    mock_entries = [
        {
            "file_path": "/tmp/file1.parquet",
            "file_size_in_bytes": small // 2,
            "uncompressed_size_in_bytes": small,
            "record_count": 1000,
        },
        {
            "file_path": "/tmp/file2.parquet",
            "file_size_in_bytes": small // 2,
            "uncompressed_size_in_bytes": small,
            "record_count": 1200,
        },
        {
            "file_path": "/tmp/file3.parquet",
            # At the threshold -> NOT a merge candidate (condition is strict <).
            "file_size_in_bytes": SMALL_FILE_BYTES // 2,
            "uncompressed_size_in_bytes": SMALL_FILE_BYTES,
            "record_count": 3000,
        },
    ]

    # Create current snapshot with manifest
    dataset.metadata.current_snapshot = Snapshot(
        snapshot_id=1000,
        timestamp_ms=1000,
        manifest_list="/tmp/manifest.parquet",
    )

    # Mock IO and catalog
    dataset.io = Mock()
    dataset.catalog = Mock()

    # Create compactor
    compactor = DatasetCompactor(dataset, strategy="brute", author="test", agent="test-agent")

    # Verify strategy selection
    assert compactor.strategy == "brute", "Strategy should be brute"
    assert compactor.decision == "user", "Decision should be user"

    # Test selection logic directly
    plan = compactor._select_brute_compaction(mock_entries)

    assert plan is not None, "Should find files to compact"
    assert plan["type"] == "combine", "Should plan to combine small files"
    assert len(plan["files"]) == 2, "Should select 2 small files"
    selected = {f["file_path"] for f in plan["files"]}
    assert selected == {"/tmp/file1.parquet", "/tmp/file2.parquet"}
    assert "/tmp/file3.parquet" not in selected, "At-threshold file is not a candidate"

    print("✓ Brute force compaction test passed")


def _perf_entries():
    """Manifest entries carrying positional sort-column stats (the real shape).

    Parquet manifest entries expose per-column statistics as positional
    ``min_values``/``max_values`` lists aligned to ``field_ids`` — not the
    iceberg-style ``lower_bounds``/``upper_bounds`` dicts. Three sub-floor files
    (~190 MB each) which tier 1 BRUTE-force merges (rule 2). Used to exercise
    sort-column resolution end to end.
    """
    each = MIN_FILE_SIZE_BYTES // 3 + 20 * _MB  # ~190 MB; three sum > 500 MB trigger
    return [
        {
            "file_path": "/tmp/file1.parquet",
            "file_size_in_bytes": each // 2,
            "uncompressed_size_in_bytes": each,
            "record_count": 1000,
            "field_ids": [1, 2],
            "min_values": [1, "a"],
            "max_values": [100, "z"],
        },
        {
            "file_path": "/tmp/file2.parquet",
            "file_size_in_bytes": each // 2,
            "uncompressed_size_in_bytes": each,
            "record_count": 1200,
            "field_ids": [1, 2],
            "min_values": [50, "a"],
            "max_values": [150, "z"],
        },
        {
            "file_path": "/tmp/file3.parquet",
            "file_size_in_bytes": each // 2,
            "uncompressed_size_in_bytes": each,
            "record_count": 3000,
            "field_ids": [1, 2],
            "min_values": [200, "a"],
            "max_values": [300, "z"],
        },
    ]


def _perf_dataset(*, schema_via_method=True, field_id=1):
    """Build a mock dataset whose sort column is 'timestamp' at index 0.

    When ``schema_via_method`` is True the schema is only reachable through
    ``dataset.schema()`` (metadata.schema attribute is None) — the real
    freshly-loaded-dataset case.
    """
    from opteryx_catalog.catalog.dataset import RelationSchema, SchemaColumn

    dataset = Mock()
    dataset.metadata = DatasetMetadata(
        dataset_identifier="test_dataset",
        location="/tmp/test_data",
    )
    dataset.metadata.sort_orders = [0]  # Sort by first column (positional index)
    dataset.metadata.schema = None
    dataset.metadata.snapshots = []
    dataset.metadata.current_snapshot = Snapshot(
        snapshot_id=1000, timestamp_ms=1000, manifest_list="/tmp/manifest.parquet"
    )
    dataset.io = Mock()
    dataset.catalog = Mock()

    schema = RelationSchema(
        name="test_dataset",
        columns=[
            SchemaColumn(name="timestamp", type="INTEGER", id=field_id),
            SchemaColumn(name="value", type="VARCHAR", id=2),
        ],
    )
    if schema_via_method:
        dataset.schema = Mock(return_value=schema)
    else:
        dataset.schema = Mock(return_value=None)
        dataset.metadata.schema = schema
    return dataset


def test_performance_compaction():
    """Sub-floor files are BRUTE-force merged (rule A/2): selection resolves the
    sort column (for later declustering) but emits a no-sort ``combine`` plan."""
    print("Testing performance compaction...")

    dataset = _perf_dataset()
    compactor = DatasetCompactor(dataset, strategy=None, author="test", agent="test-agent")

    # Verify strategy selection (sort_orders present => performance)
    assert compactor.strategy == "performance", "Should auto-select performance strategy"
    assert compactor.decision == "auto", "Decision should be auto"

    plan = compactor._select_brute_merge(_perf_entries())

    assert plan is not None, "Should brute-merge the sub-floor files"
    assert plan["type"] == "combine", "sub-floor tier emits a brute combine plan"
    assert plan["mode"] == "brute", "rule 2: no sort on sub-floor files"
    assert plan["reason"] == "small-file-brute"
    assert plan["sort_column"] == "timestamp", "Should identify sort column"
    assert len(plan["files"]) == 3, "~570 MB total fits one bin -> all three"

    print("✓ Performance compaction test passed")


def test_performance_compaction_positional_fallback():
    """When entries carry no field_ids the sort column still resolves by schema
    position (brute merge doesn't need it, but selection resolves it anyway)."""
    entries = _perf_entries()
    for e in entries:
        e.pop("field_ids")  # force positional resolution

    dataset = _perf_dataset()
    compactor = DatasetCompactor(dataset, strategy="performance", author="t", agent="t")
    plan = compactor._select_brute_merge(entries)

    assert plan is not None
    assert plan["mode"] == "brute"
    assert plan["sort_column"] == "timestamp"
    assert len(plan["files"]) == 3


def test_performance_compaction_schema_via_metadata_attr():
    """Also works when the schema is only on the raw metadata.schema attribute."""
    dataset = _perf_dataset(schema_via_method=False)
    compactor = DatasetCompactor(dataset, strategy="performance", author="t", agent="t")
    plan = compactor._select_brute_merge(_perf_entries())

    assert plan is not None
    assert plan["sort_column"] == "timestamp"


def test_brute_leaves_large_file_alone():
    """Brute compaction only *combines* under-threshold files; it has no split
    path (splitting is a performance-mode concern). A lone already-large file
    yields no plan."""
    print("Testing brute leaves large file alone...")

    dataset = Mock()
    dataset.metadata = DatasetMetadata(
        dataset_identifier="test_dataset",
        location="/tmp/test_data",
    )
    dataset.metadata.sort_orders = []

    mock_entries = [
        {
            "file_path": "/tmp/large_file.parquet",
            "file_size_in_bytes": TARGET_SIZE_BYTES // 2,
            "uncompressed_size_in_bytes": TARGET_SIZE_BYTES + 100 * _MB,
            "record_count": 5000,
        }
    ]

    compactor = DatasetCompactor(dataset, strategy="brute")
    plan = compactor._select_brute_compaction(mock_entries)

    assert plan is None, "Brute should not act on a lone large file"

    print("✓ Brute leaves large file alone test passed")


def test_no_compaction_needed():
    """Test when no compaction is needed."""
    print("Testing no compaction scenario...")

    dataset = Mock()
    dataset.metadata = DatasetMetadata(
        dataset_identifier="test_dataset",
        location="/tmp/test_data",
    )
    dataset.metadata.sort_orders = []

    # Both files are already at/above SMALL_FILE_BYTES, so neither is a merge
    # candidate -> brute has nothing to do.
    mock_entries = [
        {
            "file_path": "/tmp/file1.parquet",
            "file_size_in_bytes": SMALL_FILE_BYTES // 2,
            "uncompressed_size_in_bytes": SMALL_FILE_BYTES,
            "record_count": 2000,
        },
        {
            "file_path": "/tmp/file2.parquet",
            "file_size_in_bytes": SMALL_FILE_BYTES // 2,
            "uncompressed_size_in_bytes": SMALL_FILE_BYTES + 100 * _MB,
            "record_count": 2500,
        },
    ]

    compactor = DatasetCompactor(dataset, strategy="brute")
    plan = compactor._select_brute_compaction(mock_entries)

    assert plan is None, "Should not find anything to compact"

    print("✓ No compaction test passed")


def test_normalize_sort_order_positional_int():
    """Positional int form (tests + production ops.* datasets)."""
    from opteryx_catalog.catalog.compaction import normalize_sort_order

    assert normalize_sort_order([0]) == {
        "name": None, "field_id": None, "index": 0, "ascending": True,
    }
    assert normalize_sort_order([7])["index"] == 7


def test_normalize_sort_order_iceberg_dict():
    """Iceberg dict form written by scripts/create_dataset.py — the shape that
    previously crashed compact() with an uncaught ``dict >= int`` TypeError."""
    from opteryx_catalog.catalog.compaction import normalize_sort_order

    got = normalize_sort_order(
        [{"order-id": 1, "fields": [{"name": "id", "direction": "asc"}]}]
    )
    assert got == {"name": "id", "field_id": None, "index": None, "ascending": True}

    desc = normalize_sort_order(
        [{"order-id": 1, "fields": [{"name": "ts", "direction": "desc"}]}]
    )
    assert desc["name"] == "ts" and desc["ascending"] is False


def test_normalize_sort_order_edge_shapes():
    """Degenerate/unusable shapes normalize to None (caller falls back to brute)."""
    from opteryx_catalog.catalog.compaction import normalize_sort_order

    assert normalize_sort_order([]) is None
    assert normalize_sort_order(None) is None
    assert normalize_sort_order([{}]) is None            # empty dict, no field
    assert normalize_sort_order([True]) is None          # bool is not a column index
    # source-id (field id) form
    assert normalize_sort_order([{"fields": [{"source-id": 42}]}]) == {
        "name": None, "field_id": 42, "index": None, "ascending": True,
    }
    # bare column name
    assert normalize_sort_order(["ts"])["name"] == "ts"


# Size (MB) at/above the small-file floor: exercises the DECLUSTER rule.
_BIG_MB = (MIN_FILE_SIZE_BYTES // _MB) + 100
# Size (MB) below the floor: exercises the CONSOLIDATE rule.
_SMALL_MB = (MIN_FILE_SIZE_BYTES // _MB) // 4


def _entry(path, lo, hi, mb):
    """Manifest entry with the sort key (field id 1) at positional index 0."""
    return {
        "file_path": path,
        "file_size_in_bytes": mb * _MB // 2,
        "uncompressed_size_in_bytes": mb * _MB,
        "record_count": 1000,
        "field_ids": [1, 2],
        "min_values": [lo, "a"],
        "max_values": [hi, "z"],
    }


def test_overlapping_large_files_decluster():
    """Rule 3: an OVERLAPPING group of at/above-floor files is sort-aware merged
    (combine-split) so the executor can split it into disjoint key ranges.

    Seed the random pick to A (a fixed rng): A genuinely overlaps B (its
    range partly contains B's start) but not C, so growing from A must find
    B and stop there, excluding the disjoint C."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    entries = [
        _entry("/tmp/A.parquet", 0, 100, _BIG_MB),
        _entry("/tmp/B.parquet", 50, 200, _BIG_MB),   # overlaps A -> declustered
        _entry("/tmp/C.parquet", 300, 400, _BIG_MB),  # disjoint from A/B
    ]
    plan = compactor._select_sort_aware_merge(entries, rng=_FixedChoice("/tmp/A.parquet"))
    assert plan is not None, "overlapping big files must be declustered (rule 3)"
    assert plan["type"] == "combine-split"
    assert plan["mode"] == "sort-aware"
    assert plan["reason"] == "overlap-decluster"
    assert plan["sort_column"] == "timestamp"
    selected = {f["file_path"] for f in plan["files"]}
    assert selected == {"/tmp/A.parquet", "/tmp/B.parquet"}, "only the overlapping pair"
    assert "/tmp/C.parquet" not in selected


def test_overlapping_target_sized_files_decluster():
    """The motivating case for the streaming writer: two OVERLAPPING ~4 GB
    (target-sized) files combine to ~8 GB and decluster into two disjoint files.
    This is well above the hold-everything RAM gate - streaming makes it work, so
    selection must NOT refuse it."""
    from opteryx_catalog.catalog.compaction import (
        DECLUSTER_MAX_COMBINED_BYTES,
        MAX_SELECTED_BUDGET_BYTES,
    )

    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    target_mb = TARGET_SIZE_BYTES // _MB  # ~4 GB each
    entries = [
        _entry("/tmp/A.parquet", 0, 100, target_mb),
        _entry("/tmp/B.parquet", 40, 140, target_mb),  # overlaps A
    ]
    combined = 2 * TARGET_SIZE_BYTES
    assert combined > MAX_SELECTED_BUDGET_BYTES, "precondition: exceeds the RAM gate"
    assert combined <= DECLUSTER_MAX_COMBINED_BYTES, "precondition: within decluster cap"

    plan = compactor._select_sort_aware_merge(entries)
    assert plan is not None, "target-sized overlapping files MUST decluster (streaming)"
    assert plan["type"] == "combine-split" and plan["mode"] == "sort-aware"
    assert plan["reason"] == "overlap-decluster"
    assert len(plan["files"]) == 2
    assert plan["expected_outputs"] == 2, "8 GB -> two disjoint ~4 GB outputs"


def test_touching_boundary_is_not_overlap():
    """Files that merely share a single boundary value (max_i == min_{i+1}) - the
    artifact of a prior split on a tie - are NOT overlapping, so declustering
    converges instead of re-merging its own disjoint outputs forever.

    Exercised directly on ``_select_overlap_decluster`` with gate-fitting medium
    files, so the strict ``<`` test - not the memory gate or the bin-pack tier -
    is what declines. A genuinely-overlapping pair of the same sizes DOES group,
    proving the strict boundary is the only difference."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    mb = 1024  # ~1 GB: two fit within MAX_SELECTED_BUDGET_BYTES

    def ranges(pairs):
        entries = [_entry(f"/tmp/{n}.parquet", lo, hi, mb) for n, lo, hi in pairs]
        return compactor._build_file_ranges(entries, sort_field_id=1, sort_index=0)

    touching = ranges([("A", 0, 100), ("B", 100, 200)])  # share only value 100
    assert compactor._select_overlap_decluster(touching, "timestamp") is None

    overlapping = ranges([("A", 0, 100), ("B", 99, 200)])  # truly intersect
    plan = compactor._select_overlap_decluster(overlapping, "timestamp")
    assert plan is not None and plan["reason"] == "overlap-decluster"


def test_hot_value_group_detected_regardless_of_tie_order():
    """A 'hot' sort-key value with more rows than fit in one file (e.g. a
    Zipfian-popular value like a single dominant `project` name in a PyPI
    download log) fills several files whose stats are all ``min == max ==
    <value>`` (zero-width ranges), plus a boundary file that starts at that
    same value and extends further. All of these genuinely overlap and must
    decluster together as one group.

    A zero-width file's min/max are REAL observed values (every row in it is
    that exact value), not synthetic split edges - so it genuinely overlaps
    ANY range that weakly contains that value, including one that starts
    exactly there. That's different from two non-degenerate ranges sharing a
    boundary (a clean prior split - see test_touching_boundary_is_not_overlap),
    which is deliberately NOT treated as overlap so declustering converges.

    Seeded so the random pick lands on the wide file - starting from a
    zero-width file stops immediately by design (see
    test_zero_width_seed_stops_immediately), so this test forces the
    non-degenerate seed to exercise the growth logic. Reproduces the exact
    structure found live in opteryx.test.pypi's `project` sort key: three
    files entirely filled by one popular package plus a fourth file that
    starts with the same package and continues into the next ones
    alphabetically.
    """
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    mb = 2048  # settled on both selectors, so only decluster (not bin-pack) can find this;
    # all 4 files (2 GB each) must fit comfortably under the 12 GB decluster cap

    zero_width = [_entry(f"/tmp/pure{i}.parquet", 100, 100, mb) for i in range(3)]
    wide = _entry("/tmp/wide.parquet", 100, 300, mb)
    entries = zero_width + [wide]

    file_ranges = compactor._build_file_ranges(entries, sort_field_id=1, sort_index=0)
    plan = compactor._select_overlap_decluster(
        file_ranges, "timestamp", rng=_FixedChoice("/tmp/wide.parquet")
    )

    assert plan is not None, "the wide file genuinely overlaps every zero-width file at value 100"
    selected = {f["file_path"] for f in plan["files"]}
    assert selected == {e["file_path"] for e in entries}, "all four files share real overlap"


def test_zero_width_seed_stops_immediately():
    """If the randomly-picked seed is itself a single-value file (min ==
    max), there's no reordering benefit to chase from it alone - the
    selector stops without even trying to find a group, regardless of
    whether a genuinely overlapping wider file exists elsewhere."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    mb = 4096
    pure = _entry("/tmp/pure.parquet", 100, 100, mb)
    wide = _entry("/tmp/wide.parquet", 100, 300, mb)  # genuinely overlaps `pure`, per the test above
    file_ranges = compactor._build_file_ranges([pure, wide], sort_field_id=1, sort_index=0)

    plan = compactor._select_overlap_decluster(
        file_ranges, "timestamp", rng=_FixedChoice("/tmp/pure.parquet")
    )
    assert plan is None


def test_oversized_overlap_chain_capped_by_declustler_max_bytes():
    """When the seed's overlapping candidates combined exceed
    DECLUSTER_MAX_COMBINED_BYTES, only as many as fit get included - the cap
    still applies exactly as before, now on top of the random-seed pick."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    from opteryx_catalog.catalog.compaction import DECLUSTER_MAX_COMBINED_BYTES

    anchor_mb = 1000
    candidate_mb = 3500
    assert anchor_mb * _MB + 3 * candidate_mb * _MB <= DECLUSTER_MAX_COMBINED_BYTES
    assert anchor_mb * _MB + 4 * candidate_mb * _MB > DECLUSTER_MAX_COMBINED_BYTES

    entries = [_entry("/tmp/anchor.parquet", 100, 300, anchor_mb)]
    entries += [_entry(f"/tmp/c{i}.parquet", 100, 100, candidate_mb) for i in range(5)]
    file_ranges = compactor._build_file_ranges(entries, sort_field_id=1, sort_index=0)

    plan = compactor._select_overlap_decluster(
        file_ranges, "timestamp", rng=_FixedChoice("/tmp/anchor.parquet")
    )
    assert plan is not None
    selected = {f["file_path"] for f in plan["files"]}
    assert "/tmp/anchor.parquet" in selected, "the seed is never dropped"
    assert len(selected) == 4, "anchor + exactly 3 of the 5 candidates fit under the cap"


def test_growth_prefers_most_overlapping_candidate_first():
    """Growth adds whichever remaining file overlaps the group's current
    range the MOST, not just the first one found. Construct candidates whose
    overlap amounts against the seed clearly differ, with a cap that only
    lets some of them in, and confirm the biggest-overlap ones win."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    from opteryx_catalog.catalog.compaction import DECLUSTER_MAX_COMBINED_BYTES

    seed_mb = 1000
    cand_mb = 4200
    assert seed_mb * _MB + 2 * cand_mb * _MB <= DECLUSTER_MAX_COMBINED_BYTES
    assert seed_mb * _MB + 3 * cand_mb * _MB > DECLUSTER_MAX_COMBINED_BYTES

    # Seed spans [0, 1000]. Candidates overlap it by decreasing amounts:
    # big overlaps [0,900] (900), medium overlaps [0,500] (500), small
    # overlaps [0,100] (100) - only 2 of the 3 fit under the cap, so the two
    # LARGEST overlaps (big, medium) must be the ones chosen, not `small`.
    seed = _entry("/tmp/seed.parquet", 0, 1000, seed_mb)
    big = _entry("/tmp/big.parquet", 0, 900, cand_mb)
    medium = _entry("/tmp/medium.parquet", 0, 500, cand_mb)
    small = _entry("/tmp/small.parquet", 0, 100, cand_mb)
    entries = [seed, big, medium, small]
    file_ranges = compactor._build_file_ranges(entries, sort_field_id=1, sort_index=0)

    plan = compactor._select_overlap_decluster(
        file_ranges, "timestamp", rng=_FixedChoice("/tmp/seed.parquet")
    )
    assert plan is not None
    selected = {f["file_path"] for f in plan["files"]}
    assert selected == {"/tmp/seed.parquet", "/tmp/big.parquet", "/tmp/medium.parquet"}, (
        "the two largest-overlap candidates must win over `small`, not whichever sorts first"
    )


def test_different_seed_choice_reaches_different_overlap_clusters():
    """The actual anti-starvation mechanism: picking the STARTING file at
    random (rather than always scanning from the smallest sort-key value)
    means different calls can address different overlapping regions of the
    dataset. Two independent, non-overlapping clusters exist here; forcing
    the seed into each one in turn must produce a plan scoped to THAT
    cluster only - proving a deterministic first-cluster-wins scan (the
    pre-fix behaviour) isn't what's happening anymore. This is what actually
    fixes the live starvation case: an earlier cluster that can never fully
    resolve no longer permanently blocks every other cluster from ever
    getting a turn."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    mb = 4096

    cluster1 = [
        _entry("/tmp/c1a.parquet", 0, 100, mb),
        _entry("/tmp/c1b.parquet", 50, 200, mb),
    ]
    cluster2 = [
        _entry("/tmp/c2a.parquet", 1000, 1100, mb),
        _entry("/tmp/c2b.parquet", 1050, 1200, mb),
    ]
    entries = cluster1 + cluster2
    file_ranges = compactor._build_file_ranges(entries, sort_field_id=1, sort_index=0)

    plan1 = compactor._select_overlap_decluster(
        file_ranges, "timestamp", rng=_FixedChoice("/tmp/c1a.parquet")
    )
    plan2 = compactor._select_overlap_decluster(
        file_ranges, "timestamp", rng=_FixedChoice("/tmp/c2a.parquet")
    )

    assert {f["file_path"] for f in plan1["files"]} == {"/tmp/c1a.parquet", "/tmp/c1b.parquet"}
    assert {f["file_path"] for f in plan2["files"]} == {"/tmp/c2a.parquet", "/tmp/c2b.parquet"}


def test_disjoint_settled_files_left_alone():
    """Disjoint files already near target (>= MIN_SIZE_BYTES) are left alone:
    no overlap to decluster, and bin-pack skips settled files."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    big = (MIN_SIZE_BYTES // _MB) + 100  # ~3.6 GB settled
    entries = [
        _entry("/tmp/A.parquet", 0, 100, big),
        _entry("/tmp/B.parquet", 200, 300, big),
        _entry("/tmp/C.parquet", 400, 500, big),
    ]
    plan = compactor._select_sort_aware_merge(entries)
    assert plan is None, "disjoint settled files are already optimal"


def test_binpack_medium_files_toward_target():
    """Rule 1: consecutive, disjoint MEDIUM files (floor..MIN_SIZE_BYTES) are
    packed toward target to cut file count, as a single sort-aware output."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    # ~1 GB each, disjoint and consecutive; two fit under the 4 GB target.
    mb = 1024
    entries = [
        _entry("/tmp/A.parquet", 0, 100, mb),
        _entry("/tmp/B.parquet", 200, 300, mb),
    ]
    plan = compactor._select_sort_aware_merge(entries)
    assert plan is not None
    assert plan["type"] == "combine-split"
    assert plan["mode"] == "sort-aware"
    assert plan["reason"] == "bin-pack"
    assert plan["expected_outputs"] == 1
    assert len(plan["files"]) == 2


def test_binpack_declines_when_pair_exceeds_target():
    """Two disjoint mediums whose combined size exceeds target can't be packed
    without violating rule 1, so they're left alone."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    mb = 3 * 1024  # 3 GB each: unsettled, but any pair is 6 GB > 4 GB target
    entries = [
        _entry("/tmp/A.parquet", 0, 100, mb),
        _entry("/tmp/B.parquet", 200, 300, mb),
    ]
    plan = compactor._select_sort_aware_merge(entries)
    assert plan is None


def test_subfloor_and_decluster_are_independent():
    """Rule A (brute) and rule B (sort-aware) are attempted independently every
    pass - not a priority chain where one starves the other. When both a
    sub-floor pair AND an overlapping big group exist in the same manifest,
    each selector finds its own plan, oblivious to the other."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    entries = [
        _entry("/tmp/big1.parquet", 0, 100, _BIG_MB),
        _entry("/tmp/big2.parquet", 50, 200, _BIG_MB),   # overlaps big1
        _entry("/tmp/s1.parquet", 0, 1, _SMALL_MB),
        _entry("/tmp/s2.parquet", 5, 6, _SMALL_MB),
    ]

    brute_plan = compactor._select_brute_merge(entries)
    assert brute_plan is not None
    assert brute_plan["mode"] == "brute"
    assert {f["file_path"] for f in brute_plan["files"]} == {
        "/tmp/s1.parquet", "/tmp/s2.parquet",
    }

    sort_aware_plan = compactor._select_sort_aware_merge(entries)
    assert sort_aware_plan is not None, "the big overlap is not starved by rule A"
    assert sort_aware_plan["reason"] == "overlap-decluster"
    assert {f["file_path"] for f in sort_aware_plan["files"]} == {
        "/tmp/big1.parquet", "/tmp/big2.parquet",
    }


def test_overlap_band_reachable_by_both_pools():
    """A file between SORT_AWARE_FLOOR_BYTES (500MB) and MIN_FILE_SIZE_BYTES
    (512MB) is deliberately visible to BOTH selectors - the pools overlap on
    purpose, they are not a single shared cutoff. Regression test for the two
    thresholds silently collapsing back into one."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    band_mb = (SORT_AWARE_FLOOR_BYTES // _MB) + 5  # inside (500MB, 512MB)
    assert band_mb * _MB < MIN_FILE_SIZE_BYTES, "precondition: still under the brute ceiling"
    assert band_mb * _MB > SORT_AWARE_FLOOR_BYTES, "precondition: over the sort-aware floor"

    entries = [
        _entry("/tmp/A.parquet", 0, 1, band_mb),
        _entry("/tmp/B.parquet", 2, 3, band_mb),
    ]

    brute_plan = compactor._select_brute_merge(entries)
    assert brute_plan is not None, "still under 512MB -> visible to rule A"
    assert {f["file_path"] for f in brute_plan["files"]} == {"/tmp/A.parquet", "/tmp/B.parquet"}

    sort_aware_plan = compactor._select_sort_aware_merge(entries)
    assert sort_aware_plan is not None, "over 500MB -> ALSO visible to rule B"
    assert {f["file_path"] for f in sort_aware_plan["files"]} == {"/tmp/A.parquet", "/tmp/B.parquet"}


def test_subfloor_brute_bin_packs_to_target():
    """Sub-floor tier bin-packs the smallest first toward TARGET, one bin per
    call - it does NOT merge a >4GB mass into a single oversized file (rule 1)."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    # ~9 GB of just-under-floor files: one call takes ~4 GB worth, not all.
    per_mb = (MIN_FILE_SIZE_BYTES // _MB) - 1  # just under floor
    n = (9 * 1024) // per_mb
    entries = [_entry(f"/tmp/s{i}.parquet", i * 100, i * 100 + 99, per_mb) for i in range(n)]
    plan = compactor._select_brute_merge(entries)
    assert plan is not None
    assert plan["type"] == "combine" and plan["mode"] == "brute"
    packed = sum(f["uncompressed_size_in_bytes"] for f in plan["files"])
    assert packed <= TARGET_SIZE_BYTES, "one bin never exceeds target (rule 1)"
    assert len(plan["files"]) >= 2
    assert len(plan["files"]) < n, "does not swallow the whole 9 GB mass in one op"


def test_tiny_files_brute_merge_immediately():
    """Even a handful of tiny files are merged NOW - there is no volume threshold
    to wait for. Drip-fed (gdelt) case: leaving them un-merged IS the small-files
    problem. Rule 2 says brute (no sort)."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    entries = [
        _entry("/tmp/A.parquet", 0, 1, 1),      # 1MB
        _entry("/tmp/B.parquet", 500, 501, 1),  # 1MB
        _entry("/tmp/C.parquet", 1000, 1001, 1),
    ]
    plan = compactor._select_brute_merge(entries)
    assert plan is not None, "tiny files must be merged, not left to pile up"
    assert plan["type"] == "combine" and plan["mode"] == "brute"
    assert plan["reason"] == "small-file-brute"
    assert len(plan["files"]) == 3


def test_single_small_file_left_alone():
    """One lone sub-floor file: nothing to combine it with, so no plan."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    entries = [_entry("/tmp/A.parquet", 0, 1, 1)]
    plan = compactor._select_brute_merge(entries)
    assert plan is None


class _StubColumn:
    def __init__(self, vals):
        self._vals = vals

    def min(self):
        return min(self._vals)

    def max(self):
        return max(self._vals)


class _StubMorsel:
    """Minimal stand-in for a draken Morsel exercising _split_into_k's row
    arithmetic without importing draken (the test harness puts an incompatible
    vendored draken on sys.path). Models a single key column over row offsets."""

    def __init__(self, keys):
        self._keys = keys

    @property
    def num_rows(self):
        return len(self._keys)

    def slice(self, offset, length):
        return _StubMorsel(self._keys[offset : offset + length])

    def column(self, name):
        return _StubColumn(self._keys)


def test_split_ranges_partition():
    """_split_ranges tiles [0, n) into <=k contiguous, gapless, ordered ranges."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")

    for n, k in [(20, 4), (10, 3), (7, 7), (5, 100), (0, 3), (100, 1)]:
        ranges = compactor._split_ranges(n, k)
        assert ranges[0][0] == 0 and ranges[-1][1] == n, "covers all rows"
        assert len(ranges) <= max(1, k)
        total = sum(hi - lo for lo, hi in ranges)
        assert total == n, "no rows lost or duplicated"
        for i in range(len(ranges) - 1):
            assert ranges[i][1] == ranges[i + 1][0], "contiguous, no gaps/overlap"


def test_split_into_k_disjoint_and_complete():
    """_split_into_k over a key-sorted morsel yields k slices that preserve all
    rows and have disjoint (non-overlapping) key ranges."""
    dataset = _perf_dataset()
    compactor = DatasetCompactor(dataset, strategy="performance", author="t", agent="t")

    sorted_keys = sorted([5, 1, 9, 3, 7, 2, 8, 4, 6, 0, 5, 3, 8, 1, 9])
    m = _StubMorsel(sorted_keys)

    k = 4
    parts = compactor._split_into_k(m, k)
    assert len(parts) == k
    assert sum(p.num_rows for p in parts) == len(sorted_keys), "all rows preserved"

    ranges = [(p.column("k").min(), p.column("k").max()) for p in parts]
    for i in range(len(ranges) - 1):
        # sorted-slice boundaries: each slice's max <= next slice's min
        assert ranges[i][1] <= ranges[i + 1][0], f"overlap between {ranges[i]} and {ranges[i+1]}"

    # k=1 is a no-op; k > rows stays bounded by row count and preserves all rows
    assert len(compactor._split_into_k(m, 1)) == 1
    many = compactor._split_into_k(m, 100)
    assert sum(p.num_rows for p in many) == len(sorted_keys)


def test_iceberg_dict_sort_order_does_not_crash():
    """End-to-end: the Iceberg dict shape now resolves the sort column instead
    of raising out of the rule selectors (_select_brute_merge /
    _select_sort_aware_merge, via the shared _resolve_sort_columns_for_entries)."""
    dataset = _perf_dataset()
    # replace the positional [0] with the crashing Iceberg dict, naming the real
    # sort column ("timestamp") from _perf_dataset's schema.
    dataset.metadata.sort_orders = [
        {"order-id": 1, "fields": [{"name": "timestamp", "direction": "asc"}]}
    ]
    compactor = DatasetCompactor(dataset, strategy=None, author="t", agent="t")
    assert compactor.strategy == "performance"

    plan = compactor._select_brute_merge(_perf_entries())
    assert plan is not None
    assert plan["sort_column"] == "timestamp"


def test_null_sizes_in_manifest_do_not_crash():
    """A manifest column an entry never carried is written as SQL NULL, so
    ``uncompressed_size_in_bytes`` reads back as None (the key is PRESENT,
    holding None - ``.get(key, 0)`` does not save you). Every size comparison
    used to raise ``'<' not supported between instances of 'NoneType' and
    'int'`` straight out of compact(). A NULL size is now read as 0, i.e. a
    sub-floor merge candidate: merging rewrites it with real stats."""
    entries = _perf_entries()
    for e in entries:
        e["uncompressed_size_in_bytes"] = None
        e["file_size_in_bytes"] = None

    dataset = _perf_dataset()
    compactor = DatasetCompactor(dataset, strategy=None, author="t", agent="t")

    # Both rule selectors, plus the no-sort-key legacy path.
    plan = compactor._select_brute_merge(entries)
    assert plan is not None, "zero-sized (NULL) files are sub-floor merge candidates"
    assert len(plan["files"]) == len(entries)
    assert compactor._select_sort_aware_merge(entries) is None, (
        "NULL sizes read as 0, which is below the sort-aware floor"
    )
    assert compactor._select_brute_compaction(entries) is not None


# --- streaming chunk groups, sort direction, and the commit invariant ---------


def _chunked(values, size=3):
    """Feed a value sequence to _compute_chunk_groups the way the executor
    does: in windows, so a run straddling a window boundary is exercised."""
    return [values[i : i + size] for i in range(0, len(values), size)]


def test_chunk_groups_cover_every_row_ascending():
    """Groups must partition the sorted keys exactly - no value uncovered, none
    counted twice - whatever the window boundaries fall on."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance")
    values = [1, 1, 2, 3, 3, 3, 4, 5, 5, 6, 7, 8, 8, 9]
    groups = compactor._compute_chunk_groups(_chunked(values))

    assert all(g["type"] == "range" for g in groups)
    covered = []
    for group in groups:
        lo, hi = group["lo"], group["hi"]
        covered.extend(v for v in values if v >= lo and (hi is None or v < hi))
    assert covered == values


def test_chunk_groups_cover_every_row_descending():
    """Same invariant for a descending sort, where lo is the LARGER bound.

    The bug this pins: predicates were built as `>= lo AND < hi` regardless of
    direction, so on a descending sort every range group selected an empty,
    inverted interval and its rows were dropped from the merge.
    """
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance")
    values = [9, 8, 8, 7, 6, 5, 5, 4, 3, 3, 3, 2, 1, 1]
    groups = compactor._compute_chunk_groups(_chunked(values))

    covered = []
    for group in groups:
        lo, hi = group["lo"], group["hi"]
        covered.extend(v for v in values if v <= lo and (hi is None or v > hi))
    assert covered == values


def test_group_predicates_follow_sort_direction():
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance")
    group = {"type": "range", "lo": 10, "hi": 20}

    assert compactor._group_predicates("ts", group, True) == [
        ("ts", ">=", 10),
        ("ts", "<", 20),
    ]
    # Descending: lo is the larger value, so the interval is (hi, lo].
    desc = {"type": "range", "lo": 20, "hi": 10}
    assert compactor._group_predicates("ts", desc, False) == [
        ("ts", "<=", 20),
        ("ts", ">", 10),
    ]
    # Final group carries no second bound in either direction.
    assert compactor._group_predicates("ts", {"lo": 5, "hi": None}, True) == [("ts", ">=", 5)]
    assert compactor._group_predicates("ts", {"lo": 5, "hi": None}, False) == [("ts", "<=", 5)]


def test_chunk_groups_isolate_nulls_and_hot_values():
    from opteryx_catalog.catalog import compaction as c

    compactor = DatasetCompactor(_perf_dataset(), strategy="performance")
    hot = [7] * (c.ROW_GROUP_HARD_CAP_ROWS + 1)
    values = [None, None, 1, 2] + hot + [8, 9]
    groups = compactor._compute_chunk_groups(_chunked(values, 1000))

    assert groups[0] == {"type": "nulls", "count": 2}
    hots = [g for g in groups if g["type"] == "hot"]
    assert hots == [{"type": "hot", "value": 7}]
    # The range before the hot value stops AT it, so the two never double-read.
    before = [g for g in groups if g["type"] == "range" and g["lo"] == 1]
    assert before and before[0]["hi"] == 7


def test_file_pruning_never_drops_a_candidate_it_cannot_rule_out():
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance")
    # [10, 20): a file ending at 9 and one starting at 20 cannot contribute.
    assert not compactor._file_can_contribute((0, 9), 10, True, 20, False)
    assert not compactor._file_can_contribute((20, 30), 10, True, 20, False)
    assert compactor._file_can_contribute((5, 15), 10, True, 20, False)
    # No stats, or values that can't be compared, always read.
    assert compactor._file_can_contribute(None, 10, True, 20, False)
    assert compactor._file_can_contribute(("a", "b"), 10, True, 20, False)


def test_commit_refused_when_rows_go_missing():
    """The invariant gate: outputs holding fewer rows than the inputs must
    abort the pass, leave the input files referenced, and clean up the orphans
    it wrote."""
    dataset = _perf_dataset()
    compactor = DatasetCompactor(dataset, strategy="performance", author="t", agent="t")

    inputs = [
        {"file_path": "/tmp/test_data/data/a.parquet", "record_count": 100},
        {"file_path": "/tmp/test_data/data/b.parquet", "record_count": 100},
    ]
    outputs = [{"file_path": "/tmp/test_data/data/c.parquet", "record_count": 150}]

    result = compactor._finalize_compaction_snapshot(
        list(inputs), inputs, outputs, 1234, 200, 0, "native"
    )

    assert result is None
    assert "row-count mismatch" in compactor._last_error
    dataset.catalog.save_dataset_metadata.assert_not_called()
    dataset.io.delete.assert_called_once_with("/tmp/test_data/data/c.parquet")


def test_commit_proceeds_when_rows_balance():
    dataset = _perf_dataset()
    # _perf_dataset stores current_snapshot as a value; the commit path calls it.
    dataset.metadata.current_snapshot = Mock(return_value=None)
    compactor = DatasetCompactor(dataset, strategy="performance", author="t", agent="t")
    dataset.catalog.write_parquet_manifest = Mock(return_value="/tmp/manifest2.parquet")

    inputs = [
        {"file_path": "/tmp/test_data/data/a.parquet", "record_count": 100},
        {"file_path": "/tmp/test_data/data/b.parquet", "record_count": 100},
    ]
    outputs = [{"file_path": "/tmp/test_data/data/c.parquet", "record_count": 200}]

    snapshot = compactor._finalize_compaction_snapshot(
        list(inputs), inputs, outputs, 1234, 200, 0, "native"
    )

    assert snapshot is not None
    assert snapshot.summary["deleted-records"] == 200
    dataset.io.delete.assert_not_called()


def test_reconcile_failure_aborts_instead_of_dropping_rows():
    """An unreconcilable morsel used to be skipped, so its rows vanished from
    the output while its source file was still deleted by the commit."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance")

    class _BrokenMorsel:
        """Schema-visible (so it needs reconciling against the other morsel)
        but its columns can't be read back out."""

        num_rows = 4
        column_names = ["timestamp"]  # missing "value" -> needs a rebuild
        column_types = ["INTEGER"]

        def column(self, name):
            raise RuntimeError("unreadable")

    assert compactor._reconcile_schemas([create_test_table(4), _BrokenMorsel()]) is None


def test_commit_refused_when_another_writer_committed_first():
    """save_dataset_metadata has no compare-and-swap, so a pass that started
    before a concurrent commit must discard its work rather than overwrite it."""
    dataset = _perf_dataset()
    dataset.metadata.current_snapshot = Mock(return_value=None)
    dataset.identifier = "test.dataset"
    compactor = DatasetCompactor(dataset, strategy="performance", author="t", agent="t")
    dataset.catalog.write_parquet_manifest = Mock(return_value="/tmp/manifest2.parquet")

    compactor._baseline_snapshot_id = 1000
    fresh = Mock()
    fresh.metadata.current_snapshot_id = 2000  # someone else committed meanwhile
    dataset.catalog.load_dataset = Mock(return_value=fresh)

    inputs = [
        {"file_path": "/tmp/test_data/data/a.parquet", "record_count": 100},
        {"file_path": "/tmp/test_data/data/b.parquet", "record_count": 100},
    ]
    outputs = [{"file_path": "/tmp/test_data/data/c.parquet", "record_count": 200}]

    result = compactor._finalize_compaction_snapshot(
        list(inputs), inputs, outputs, 1234, 200, 0, "native"
    )

    assert result is None
    assert "changed during compaction" in compactor._last_error
    dataset.catalog.save_dataset_metadata.assert_not_called()
    dataset.io.delete.assert_called_once_with("/tmp/test_data/data/c.parquet")

    # Unchanged dataset: the same pass commits normally.
    fresh.metadata.current_snapshot_id = 1000
    dataset.io.delete.reset_mock()
    assert compactor._finalize_compaction_snapshot(
        list(inputs), inputs, outputs, 1235, 200, 0, "native"
    ) is not None
    dataset.io.delete.assert_not_called()


if __name__ == "__main__":
    print("Running compaction tests...\n")
    test_brute_compaction()
    test_performance_compaction()
    test_brute_leaves_large_file_alone()
    test_no_compaction_needed()
    test_overlapping_large_files_decluster()
    test_overlapping_target_sized_files_decluster()
    test_touching_boundary_is_not_overlap()
    test_disjoint_settled_files_left_alone()
    test_binpack_medium_files_toward_target()
    test_binpack_declines_when_pair_exceeds_target()
    test_subfloor_and_decluster_are_independent()
    test_overlap_band_reachable_by_both_pools()
    test_subfloor_brute_bin_packs_to_target()
    test_tiny_files_brute_merge_immediately()
    test_single_small_file_left_alone()
    test_normalize_sort_order_positional_int()
    test_normalize_sort_order_iceberg_dict()
    test_normalize_sort_order_edge_shapes()
    test_iceberg_dict_sort_order_does_not_crash()
    test_null_sizes_in_manifest_do_not_crash()
    test_chunk_groups_cover_every_row_ascending()
    test_chunk_groups_cover_every_row_descending()
    test_group_predicates_follow_sort_direction()
    test_chunk_groups_isolate_nulls_and_hot_values()
    test_file_pruning_never_drops_a_candidate_it_cannot_rule_out()
    test_commit_refused_when_rows_go_missing()
    test_commit_proceeds_when_rows_balance()
    test_reconcile_failure_aborts_instead_of_dropping_rows()
    test_commit_refused_when_another_writer_committed_first()
    print("\n✅ All tests passed!")
