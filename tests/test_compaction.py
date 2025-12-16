"""Tests for compaction module."""

import pytest


def test_compaction_config_defaults():
    """Test CompactionConfig default values."""
    from pyiceberg_firestore_gcs.compaction import CompactionConfig

    config = CompactionConfig()

    assert config.target_file_size_bytes == 128 * 1024 * 1024  # 128 MB
    assert config.min_file_count == 10
    assert config.max_small_file_size_bytes == 32 * 1024 * 1024  # 32 MB
    assert config.strategy == "binpack"
    assert config.enabled is True


def test_compaction_config_from_properties():
    """Test creating CompactionConfig from table properties."""
    from pyiceberg_firestore_gcs.compaction import CompactionConfig

    properties = {
        "write.target-file-size-bytes": "256000000",
        "compaction.min-file-count": "20",
        "compaction.max-small-file-size-bytes": "64000000",
        "compaction.strategy": "sort",
        "compaction.enabled": "false",
    }

    config = CompactionConfig.from_table_properties(properties)

    assert config.target_file_size_bytes == 256000000
    assert config.min_file_count == 20
    assert config.max_small_file_size_bytes == 64000000
    assert config.strategy == "sort"
    assert config.enabled is False


def test_file_group_creation():
    """Test FileGroup dataclass."""
    from pyiceberg_firestore_gcs.compaction import FileGroup

    files = ["file1.parquet", "file2.parquet"]
    group = FileGroup(
        files=files,
        total_size=1024 * 1024 * 100,  # 100 MB
        file_count=2,
    )

    assert group.file_count == 2
    assert group.total_size == 1024 * 1024 * 100


def test_compaction_plan_needs_compaction():
    """Test CompactionPlan.needs_compaction property."""
    from pyiceberg_firestore_gcs.compaction import CompactionPlan, FileGroup

    # Plan with no file groups
    plan = CompactionPlan(file_groups=[], total_files=5, total_size=1024, estimated_output_files=0)
    assert not plan.needs_compaction

    # Plan with file groups
    file_group = FileGroup(files=[], total_size=1024, file_count=2)
    plan = CompactionPlan(
        file_groups=[file_group], total_files=10, total_size=10240, estimated_output_files=5
    )
    assert plan.needs_compaction


def test_group_files_binpack_empty():
    """Test binpack grouping with empty file list."""
    from pyiceberg_firestore_gcs.compaction import group_files_binpack

    groups = group_files_binpack([], target_size=128 * 1024 * 1024)
    assert len(groups) == 0


def test_group_files_binpack_single_file():
    """Test binpack grouping with single file."""
    from pyiceberg_firestore_gcs.compaction import group_files_binpack
    from unittest.mock import Mock

    file = Mock()
    file.file_size_in_bytes = 10 * 1024 * 1024  # 10 MB

    groups = group_files_binpack([file], target_size=128 * 1024 * 1024)

    # Single file should not be grouped (no point compacting alone)
    assert len(groups) == 0


def test_group_files_binpack_multiple_files():
    """Test binpack grouping with multiple small files."""
    from pyiceberg_firestore_gcs.compaction import group_files_binpack
    from unittest.mock import Mock

    # Create 5 files of 20 MB each
    files = []
    for i in range(5):
        file = Mock()
        file.file_size_in_bytes = 20 * 1024 * 1024
        files.append(file)

    # With 128 MB target, should group into 1 bin
    groups = group_files_binpack(files, target_size=128 * 1024 * 1024)

    assert len(groups) == 1
    assert groups[0].file_count == 5
    assert groups[0].total_size == 100 * 1024 * 1024


def test_group_files_binpack_large_files():
    """Test binpack with files larger than target."""
    from pyiceberg_firestore_gcs.compaction import group_files_binpack
    from unittest.mock import Mock

    # Create files larger than target
    files = []
    for i in range(3):
        file = Mock()
        file.file_size_in_bytes = 150 * 1024 * 1024  # 150 MB
        files.append(file)

    # Each file exceeds target, so no grouping
    groups = group_files_binpack(files, target_size=128 * 1024 * 1024)

    # Large single files won't be grouped
    assert len(groups) == 0


def test_compaction_result_creation():
    """Test CompactionResult dataclass."""
    from pyiceberg_firestore_gcs.compaction import CompactionResult

    result = CompactionResult(
        table_identifier=("namespace", "table_name"),
        files_before=100,
        files_after=10,
        bytes_before=1024 * 1024 * 1000,
        bytes_after=1024 * 1024 * 1000,
        duration_seconds=45.5,
        files_rewritten=90,
        success=True,
    )

    assert result.success
    assert result.files_rewritten == 90
    assert result.error is None


def test_compaction_result_with_error():
    """Test CompactionResult with error."""
    from pyiceberg_firestore_gcs.compaction import CompactionResult

    result = CompactionResult(
        table_identifier=("namespace", "table_name"),
        files_before=0,
        files_after=0,
        bytes_before=0,
        bytes_after=0,
        duration_seconds=1.0,
        files_rewritten=0,
        success=False,
        error="Table not found",
    )

    assert not result.success
    assert result.error == "Table not found"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
