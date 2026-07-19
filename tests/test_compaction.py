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

from opteryx_catalog.catalog.compaction import DatasetCompactor
from opteryx_catalog.catalog.metadata import DatasetMetadata, Snapshot


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

    # Create mock entries - small files that should be combined
    mock_entries = [
        {
            "file_path": "/tmp/file1.parquet",
            "file_size_in_bytes": 30 * 1024 * 1024,  # 30MB compressed
            "uncompressed_size_in_bytes": 40 * 1024 * 1024,  # 40MB uncompressed
            "record_count": 1000,
        },
        {
            "file_path": "/tmp/file2.parquet",
            "file_size_in_bytes": 35 * 1024 * 1024,  # 35MB compressed
            "uncompressed_size_in_bytes": 50 * 1024 * 1024,  # 50MB uncompressed
            "record_count": 1200,
        },
        {
            "file_path": "/tmp/file3.parquet",
            "file_size_in_bytes": 110 * 1024 * 1024,  # 110MB compressed (acceptable)
            "uncompressed_size_in_bytes": 130 * 1024 * 1024,  # 130MB uncompressed
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

    print("✓ Brute force compaction test passed")


def _perf_entries():
    """Manifest entries carrying positional sort-column stats (the real shape).

    Parquet manifest entries expose per-column statistics as positional
    ``min_values``/``max_values`` lists aligned to ``field_ids`` — not the
    iceberg-style ``lower_bounds``/``upper_bounds`` dicts. Files 1 and 2 overlap
    on ``timestamp`` (field id 1); file 3 is disjoint.
    """
    return [
        {
            "file_path": "/tmp/file1.parquet",
            "file_size_in_bytes": 30 * 1024 * 1024,
            "uncompressed_size_in_bytes": 40 * 1024 * 1024,
            "record_count": 1000,
            "field_ids": [1, 2],
            "min_values": [1, "a"],
            "max_values": [100, "z"],
        },
        {
            "file_path": "/tmp/file2.parquet",
            "file_size_in_bytes": 35 * 1024 * 1024,
            "uncompressed_size_in_bytes": 50 * 1024 * 1024,
            "record_count": 1200,
            "field_ids": [1, 2],
            "min_values": [50, "a"],  # Overlaps with file1
            "max_values": [150, "z"],
        },
        {
            "file_path": "/tmp/file3.parquet",
            "file_size_in_bytes": 110 * 1024 * 1024,
            "uncompressed_size_in_bytes": 130 * 1024 * 1024,
            "record_count": 3000,
            "field_ids": [1, 2],
            "min_values": [200, "a"],  # No overlap
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
    """Order-aware selection reads positional min/max stats keyed by field id."""
    print("Testing performance compaction...")

    dataset = _perf_dataset()
    compactor = DatasetCompactor(dataset, strategy=None, author="test", agent="test-agent")

    # Verify strategy selection (sort_orders present => performance)
    assert compactor.strategy == "performance", "Should auto-select performance strategy"
    assert compactor.decision == "auto", "Decision should be auto"

    plan = compactor._select_performance_compaction(_perf_entries())

    assert plan is not None, "Should find overlapping files"
    assert plan["type"] == "combine-split", "Should plan to combine and split"
    assert len(plan["files"]) == 2, "Should select 2 overlapping files"
    assert plan["sort_column"] == "timestamp", "Should identify sort column"
    # The two overlapping files (ids 1 & 2), not the disjoint file 3.
    selected = {f["file_path"] for f in plan["files"]}
    assert selected == {"/tmp/file1.parquet", "/tmp/file2.parquet"}

    print("✓ Performance compaction test passed")


def test_performance_compaction_positional_fallback():
    """When entries carry no field_ids, fall back to schema column position."""
    entries = _perf_entries()
    for e in entries:
        e.pop("field_ids")  # force positional resolution

    dataset = _perf_dataset()
    compactor = DatasetCompactor(dataset, strategy="performance", author="t", agent="t")
    plan = compactor._select_performance_compaction(entries)

    assert plan is not None
    assert plan["sort_column"] == "timestamp"
    assert len(plan["files"]) == 2


def test_performance_compaction_schema_via_metadata_attr():
    """Also works when the schema is only on the raw metadata.schema attribute."""
    dataset = _perf_dataset(schema_via_method=False)
    compactor = DatasetCompactor(dataset, strategy="performance", author="t", agent="t")
    plan = compactor._select_performance_compaction(_perf_entries())

    assert plan is not None
    assert plan["sort_column"] == "timestamp"


def test_large_file_splitting():
    """Test that large files are identified for splitting."""
    print("Testing large file splitting...")

    dataset = Mock()
    dataset.metadata = DatasetMetadata(
        dataset_identifier="test_dataset",
        location="/tmp/test_data",
    )
    dataset.metadata.sort_orders = []

    # Create entry for a large file
    mock_entries = [
        {
            "file_path": "/tmp/large_file.parquet",
            "file_size_in_bytes": 180 * 1024 * 1024,
            "uncompressed_size_in_bytes": 200 * 1024 * 1024,  # 200MB > 196MB threshold
            "record_count": 5000,
        }
    ]

    compactor = DatasetCompactor(dataset, strategy="brute")
    plan = compactor._select_brute_compaction(mock_entries)

    assert plan is not None, "Should identify large file"
    assert plan["type"] == "split", "Should plan to split"
    assert plan["reason"] == "file-too-large", "Reason should be file too large"

    print("✓ Large file splitting test passed")


def test_no_compaction_needed():
    """Test when no compaction is needed."""
    print("Testing no compaction scenario...")

    dataset = Mock()
    dataset.metadata = DatasetMetadata(
        dataset_identifier="test_dataset",
        location="/tmp/test_data",
    )
    dataset.metadata.sort_orders = []

    # All files are in acceptable range
    mock_entries = [
        {
            "file_path": "/tmp/file1.parquet",
            "file_size_in_bytes": 100 * 1024 * 1024,
            "uncompressed_size_in_bytes": 110 * 1024 * 1024,
            "record_count": 2000,
        },
        {
            "file_path": "/tmp/file2.parquet",
            "file_size_in_bytes": 120 * 1024 * 1024,
            "uncompressed_size_in_bytes": 135 * 1024 * 1024,
            "record_count": 2500,
        },
    ]

    compactor = DatasetCompactor(dataset, strategy="brute")
    plan = compactor._select_brute_compaction(mock_entries)

    assert plan is None, "Should not find anything to compact"

    print("✓ No compaction test passed")


if __name__ == "__main__":
    print("Running compaction tests...\n")
    test_brute_compaction()
    test_performance_compaction()
    test_large_file_splitting()
    test_no_compaction_needed()
    print("\n✅ All tests passed!")
