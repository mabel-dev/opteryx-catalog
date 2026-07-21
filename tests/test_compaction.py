"""
Test script for compaction functionality.

This tests the DatasetCompactor class with both brute and performance strategies.
"""
import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))
sys.path.insert(1, os.path.join(sys.path[0], "../pyiceberg-firestore-gcs"))

from unittest.mock import Mock

from opteryx_catalog.catalog.compaction import (
    MIN_FILE_SIZE_BYTES,
    MIN_SIZE_BYTES,
    SMALL_FILE_BYTES,
    TARGET_SIZE_BYTES,
    DatasetCompactor,
)
from opteryx_catalog.catalog.metadata import DatasetMetadata, Snapshot

_MB = 1024 * 1024


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
    """Sub-floor files are BRUTE-force merged (rule 2): selection resolves the
    sort column (for later declustering) but emits a no-sort ``combine`` plan."""
    print("Testing performance compaction...")

    dataset = _perf_dataset()
    compactor = DatasetCompactor(dataset, strategy=None, author="test", agent="test-agent")

    # Verify strategy selection (sort_orders present => performance)
    assert compactor.strategy == "performance", "Should auto-select performance strategy"
    assert compactor.decision == "auto", "Decision should be auto"

    plan = compactor._select_performance_compaction(_perf_entries())

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
    plan = compactor._select_performance_compaction(entries)

    assert plan is not None
    assert plan["mode"] == "brute"
    assert plan["sort_column"] == "timestamp"
    assert len(plan["files"]) == 3


def test_performance_compaction_schema_via_metadata_attr():
    """Also works when the schema is only on the raw metadata.schema attribute."""
    dataset = _perf_dataset(schema_via_method=False)
    compactor = DatasetCompactor(dataset, strategy="performance", author="t", agent="t")
    plan = compactor._select_performance_compaction(_perf_entries())

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
    (combine-split) so the executor can split it into disjoint key ranges."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    entries = [
        _entry("/tmp/A.parquet", 0, 100, _BIG_MB),
        _entry("/tmp/B.parquet", 50, 200, _BIG_MB),   # overlaps A -> declustered
        _entry("/tmp/C.parquet", 300, 400, _BIG_MB),  # disjoint from A/B
    ]
    plan = compactor._select_performance_compaction(entries)
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

    plan = compactor._select_performance_compaction(entries)
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
    plan = compactor._select_performance_compaction(entries)
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
    plan = compactor._select_performance_compaction(entries)
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
    plan = compactor._select_performance_compaction(entries)
    assert plan is None


def test_subfloor_takes_priority_over_decluster():
    """One operation per call, sub-floor tier first: when both a sub-floor pair
    and an overlapping big group exist, the brute sub-floor merge wins."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    entries = [
        _entry("/tmp/big1.parquet", 0, 100, _BIG_MB),
        _entry("/tmp/big2.parquet", 50, 200, _BIG_MB),   # overlaps big1
        _entry("/tmp/s1.parquet", 0, 1, _SMALL_MB),
        _entry("/tmp/s2.parquet", 5, 6, _SMALL_MB),
    ]
    plan = compactor._select_performance_compaction(entries)
    assert plan is not None
    assert plan["mode"] == "brute", "sub-floor consolidation is highest priority"
    selected = {f["file_path"] for f in plan["files"]}
    assert selected == {"/tmp/s1.parquet", "/tmp/s2.parquet"}


def test_subfloor_brute_bin_packs_to_target():
    """Sub-floor tier bin-packs the smallest first toward TARGET, one bin per
    call - it does NOT merge a >4GB mass into a single oversized file (rule 1)."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    # ~9 GB of just-under-floor files: one call takes ~4 GB worth, not all.
    per_mb = (MIN_FILE_SIZE_BYTES // _MB) - 1  # just under floor
    n = (9 * 1024) // per_mb
    entries = [_entry(f"/tmp/s{i}.parquet", i * 100, i * 100 + 99, per_mb) for i in range(n)]
    plan = compactor._select_performance_compaction(entries)
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
    plan = compactor._select_performance_compaction(entries)
    assert plan is not None, "tiny files must be merged, not left to pile up"
    assert plan["type"] == "combine" and plan["mode"] == "brute"
    assert plan["reason"] == "small-file-brute"
    assert len(plan["files"]) == 3


def test_single_small_file_left_alone():
    """One lone sub-floor file: nothing to combine it with, so no plan."""
    compactor = DatasetCompactor(_perf_dataset(), strategy="performance", author="t", agent="t")
    entries = [_entry("/tmp/A.parquet", 0, 1, 1)]
    plan = compactor._select_performance_compaction(entries)
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
    of raising out of _select_performance_compaction."""
    dataset = _perf_dataset()
    # replace the positional [0] with the crashing Iceberg dict, naming the real
    # sort column ("timestamp") from _perf_dataset's schema.
    dataset.metadata.sort_orders = [
        {"order-id": 1, "fields": [{"name": "timestamp", "direction": "asc"}]}
    ]
    compactor = DatasetCompactor(dataset, strategy=None, author="t", agent="t")
    assert compactor.strategy == "performance"

    plan = compactor._select_performance_compaction(_perf_entries())
    assert plan is not None
    assert plan["sort_column"] == "timestamp"


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
    test_subfloor_takes_priority_over_decluster()
    test_subfloor_brute_bin_packs_to_target()
    test_tiny_files_brute_merge_immediately()
    test_single_small_file_left_alone()
    test_normalize_sort_order_positional_int()
    test_normalize_sort_order_iceberg_dict()
    test_normalize_sort_order_edge_shapes()
    test_iceberg_dict_sort_order_does_not_crash()
    print("\n✅ All tests passed!")
