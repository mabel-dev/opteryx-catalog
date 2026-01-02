"""
Test script for compaction functionality.

This tests the DatasetCompactor class with both brute and performance strategies.
"""

from unittest.mock import Mock

import pyarrow as pa

from opteryx_catalog.catalog.compaction import DatasetCompactor
from opteryx_catalog.catalog.metadata import DatasetMetadata, Snapshot


def create_test_table(num_rows: int, value_range: tuple = (0, 100)) -> pa.Table:
    """Create a simple test table with a timestamp column for sorting."""
    import random

    timestamps = sorted([random.randint(value_range[0], value_range[1]) for _ in range(num_rows)])
    values = [f"value_{i}" for i in range(num_rows)]

    return pa.table({"timestamp": timestamps, "value": values})


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


def test_performance_compaction():
    """Test performance compaction strategy."""
    print("Testing performance compaction...")

    # Create mock dataset with sort order
    dataset = Mock()
    dataset.metadata = DatasetMetadata(
        dataset_identifier="test_dataset",
        location="/tmp/test_data",
    )
    dataset.metadata.sort_orders = [0]  # Sort by first column
    dataset.metadata.schema = Mock()
    dataset.metadata.schema.fields = [Mock(name="timestamp")]
    dataset.metadata.snapshots = []
    dataset.metadata.current_snapshot = None

    # Create mock entries with overlapping ranges
    mock_entries = [
        {
            "file_path": "/tmp/file1.parquet",
            "file_size_in_bytes": 30 * 1024 * 1024,
            "uncompressed_size_in_bytes": 40 * 1024 * 1024,
            "record_count": 1000,
            "lower_bounds": {"timestamp": 1},
            "upper_bounds": {"timestamp": 100},
        },
        {
            "file_path": "/tmp/file2.parquet",
            "file_size_in_bytes": 35 * 1024 * 1024,
            "uncompressed_size_in_bytes": 50 * 1024 * 1024,
            "record_count": 1200,
            "lower_bounds": {"timestamp": 50},  # Overlaps with file1
            "upper_bounds": {"timestamp": 150},
        },
        {
            "file_path": "/tmp/file3.parquet",
            "file_size_in_bytes": 110 * 1024 * 1024,
            "uncompressed_size_in_bytes": 130 * 1024 * 1024,
            "record_count": 3000,
            "lower_bounds": {"timestamp": 200},  # No overlap
            "upper_bounds": {"timestamp": 300},
        },
    ]

    dataset.metadata.current_snapshot = Snapshot(
        snapshot_id=1000,
        timestamp_ms=1000,
        manifest_list="/tmp/manifest.parquet",
    )

    # Mock IO and catalog
    dataset.io = Mock()
    dataset.catalog = Mock()

    # Create compactor (auto-detect should choose performance)
    compactor = DatasetCompactor(dataset, strategy=None, author="test", agent="test-agent")

    # Verify strategy selection
    assert compactor.strategy == "performance", "Should auto-select performance strategy"
    assert compactor.decision == "auto", "Decision should be auto"

    # Test selection logic directly
    plan = compactor._select_performance_compaction(mock_entries)

    assert plan is not None, "Should find overlapping files"
    assert plan["type"] == "combine-split", "Should plan to combine and split"
    assert len(plan["files"]) == 2, "Should select 2 overlapping files"
    assert plan["sort_column"] == "timestamp", "Should identify sort column"

    print("✓ Performance compaction test passed")


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
