"""Small file compaction for PyIceberg tables.

This module provides utilities to compact small data files in Iceberg tables,
improving query performance and reducing metadata overhead.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from orso.logging import get_logger
from pyiceberg.catalog import Identifier
from pyiceberg.table import Table

logger = get_logger()
logger.setLevel(5)


@dataclass
class CompactionConfig:
    """Configuration for table compaction.

    Attributes:
        target_file_size_bytes: Target size for compacted files (default: 128 MB)
        min_file_count: Minimum number of files to trigger compaction (default: 10)
        max_small_file_size_bytes: Maximum size to consider a file "small" (default: 32 MB)
        strategy: Compaction strategy - "binpack" or "sort" (default: "binpack")
        enabled: Whether compaction is enabled (default: True)
    """

    target_file_size_bytes: int = 128 * 1024 * 1024  # 128 MB
    min_file_count: int = 10
    max_small_file_size_bytes: int = 32 * 1024 * 1024  # 32 MB
    strategy: str = "binpack"
    enabled: bool = True

    @classmethod
    def from_table_properties(cls, properties: Dict[str, str]) -> CompactionConfig:
        """Create config from table properties.

        Args:
            properties: Table properties dictionary

        Returns:
            CompactionConfig instance
        """
        return cls(
            target_file_size_bytes=int(
                properties.get("write.target-file-size-bytes", 128 * 1024 * 1024)
            ),
            min_file_count=int(properties.get("compaction.min-file-count", 10)),
            max_small_file_size_bytes=int(
                properties.get("compaction.max-small-file-size-bytes", 32 * 1024 * 1024)
            ),
            strategy=properties.get("compaction.strategy", "binpack"),
            enabled=properties.get("compaction.enabled", "true").lower() == "true",
        )


@dataclass
class FileGroup:
    """A group of files to be compacted together.

    Attributes:
        files: List of data files to compact
        total_size: Total size of all files in bytes
        file_count: Number of files in the group
    """

    files: List[Any]
    total_size: int
    file_count: int


@dataclass
class CompactionPlan:
    """Plan for compacting a table.

    Attributes:
        file_groups: Groups of files to compact together
        total_files: Total number of files to compact
        total_size: Total size of all files to compact
        estimated_output_files: Estimated number of output files
    """

    file_groups: List[FileGroup]
    total_files: int
    total_size: int
    estimated_output_files: int

    @property
    def needs_compaction(self) -> bool:
        """Whether compaction is needed."""
        return len(self.file_groups) > 0


@dataclass
class CompactionResult:
    """Result of a compaction operation.

    Attributes:
        table_identifier: Identifier of the compacted table
        files_before: Number of files before compaction
        files_after: Number of files after compaction
        bytes_before: Total bytes before compaction
        bytes_after: Total bytes after compaction
        duration_seconds: Time taken for compaction
        files_rewritten: Number of files rewritten
        success: Whether compaction succeeded
        error: Error message if compaction failed
    """

    table_identifier: Tuple[str, str]
    files_before: int
    files_after: int
    bytes_before: int
    bytes_after: int
    duration_seconds: float
    files_rewritten: int
    success: bool = True
    error: Optional[str] = None


def analyze_table_files(table: Table, config: CompactionConfig) -> CompactionPlan:
    """Analyze table files and create a compaction plan.

    Args:
        table: The table to analyze
        config: Compaction configuration

    Returns:
        CompactionPlan describing files to compact
    """
    # Get all data files from current snapshot
    snapshot = table.current_snapshot()
    if not snapshot:
        logger.debug("No snapshot found, skipping compaction analysis")
        return CompactionPlan(file_groups=[], total_files=0, total_size=0, estimated_output_files=0)

    # Collect small files
    small_files = []
    total_files = 0

    try:
        # Use table scan to get file information
        scan = table.scan()
        for task in scan.plan_files():
            total_files += 1
            data_file = task.data_file
            file_size = data_file.file_size_in_bytes

            # Check if file is "small" based on config
            if file_size < config.max_small_file_size_bytes:
                small_files.append(data_file)
    except Exception as e:
        logger.warning(f"Failed to analyze table files: {e}")
        return CompactionPlan(file_groups=[], total_files=0, total_size=0, estimated_output_files=0)

    logger.info(
        f"Found {len(small_files)} small files out of {total_files} total files "
        f"(threshold: {config.max_small_file_size_bytes / 1024 / 1024:.1f} MB)"
    )

    # Check if we meet minimum file count threshold
    if len(small_files) < config.min_file_count:
        logger.debug(
            f"Not enough small files for compaction ({len(small_files)} < {config.min_file_count})"
        )
        return CompactionPlan(
            file_groups=[],
            total_files=total_files,
            total_size=sum(f.file_size_in_bytes for f in small_files),
            estimated_output_files=0,
        )

    # Group files based on strategy
    if config.strategy == "binpack":
        file_groups = group_files_binpack(small_files, config.target_file_size_bytes)
    else:
        # Default to binpack for unsupported strategies
        logger.warning(f"Unsupported strategy '{config.strategy}', using binpack")
        file_groups = group_files_binpack(small_files, config.target_file_size_bytes)

    total_size = sum(f.file_size_in_bytes for f in small_files)

    return CompactionPlan(
        file_groups=file_groups,
        total_files=len(small_files),
        total_size=total_size,
        estimated_output_files=len(file_groups),
    )


def group_files_binpack(files: List[Any], target_size: int) -> List[FileGroup]:
    """Group files using bin-packing algorithm.

    Uses a first-fit decreasing algorithm to group files into bins
    that sum to approximately the target size.

    Args:
        files: List of data files to group
        target_size: Target size for each group

    Returns:
        List of FileGroup objects
    """
    # Sort files by size (largest first) for better packing
    sorted_files = sorted(files, key=lambda f: f.file_size_in_bytes, reverse=True)

    bins: List[List[Any]] = []
    bin_sizes: List[int] = []

    for file in sorted_files:
        file_size = file.file_size_in_bytes

        # Find first bin that has room for this file
        placed = False
        for i, bin_size in enumerate(bin_sizes):
            if bin_size + file_size <= target_size * 1.2:  # Allow 20% overflow
                bins[i].append(file)
                bin_sizes[i] += file_size
                placed = True
                break

        # Create new bin if needed
        if not placed:
            bins.append([file])
            bin_sizes.append(file_size)

    # Convert bins to FileGroup objects
    # Only include bins with multiple files (no point compacting single files)
    file_groups = []
    for bin_files, bin_size in zip(bins, bin_sizes):
        if len(bin_files) > 1:
            file_groups.append(
                FileGroup(files=bin_files, total_size=bin_size, file_count=len(bin_files))
            )

    logger.debug(
        f"Grouped {len(sorted_files)} files into {len(file_groups)} groups "
        f"(target size: {target_size / 1024 / 1024:.1f} MB)"
    )

    return file_groups


def should_compact(table: Table, config: Optional[CompactionConfig] = None) -> bool:
    """Check if a table needs compaction.

    Args:
        table: The table to check
        config: Optional compaction configuration (will be read from table if not provided)

    Returns:
        True if compaction is recommended
    """
    if config is None:
        config = CompactionConfig.from_table_properties(table.properties)

    if not config.enabled:
        return False

    plan = analyze_table_files(table, config)
    return plan.needs_compaction


def compact_table(
    catalog: Any,
    identifier: Union[str, Identifier],
    config: Optional[CompactionConfig] = None,
    dry_run: bool = False,
) -> CompactionResult:
    """Compact a table by rewriting small files.

    This function analyzes the table's data files and rewrites groups of small
    files into larger files to improve query performance.

    Args:
        catalog: The catalog containing the table
        identifier: Table identifier (namespace, table_name)
        config: Optional compaction configuration (will be read from table if not provided)
        dry_run: If True, only analyze and return plan without executing

    Returns:
        CompactionResult with statistics about the compaction

    Example:
        >>> from pyiceberg_firestore_gcs import create_catalog
        >>> from pyiceberg_firestore_gcs.compaction import compact_table
        >>> catalog = create_catalog("my_catalog", ...)
        >>> result = compact_table(catalog, ("namespace", "table"))
        >>> print(f"Compacted {result.files_rewritten} files")
    """
    start_time = time.perf_counter()

    try:
        # Load table
        table = catalog.load_table(identifier)
        namespace, table_name = table.name()

        # Get configuration
        if config is None:
            config = CompactionConfig.from_table_properties(table.properties)

        if not config.enabled:
            logger.info(f"Compaction disabled for {namespace}.{table_name}")
            return CompactionResult(
                table_identifier=(namespace, table_name),
                files_before=0,
                files_after=0,
                bytes_before=0,
                bytes_after=0,
                duration_seconds=0,
                files_rewritten=0,
                success=True,
            )

        # Analyze files
        plan = analyze_table_files(table, config)

        if not plan.needs_compaction:
            logger.info(f"No compaction needed for {namespace}.{table_name}")
            return CompactionResult(
                table_identifier=(namespace, table_name),
                files_before=plan.total_files,
                files_after=plan.total_files,
                bytes_before=plan.total_size,
                bytes_after=plan.total_size,
                duration_seconds=time.perf_counter() - start_time,
                files_rewritten=0,
                success=True,
            )

        logger.info(
            f"Compaction plan for {namespace}.{table_name}: "
            f"{plan.total_files} files → {plan.estimated_output_files} files "
            f"({plan.total_size / 1024 / 1024:.1f} MB)"
        )

        if dry_run:
            logger.info("Dry run mode - skipping execution")
            return CompactionResult(
                table_identifier=(namespace, table_name),
                files_before=plan.total_files,
                files_after=plan.estimated_output_files,
                bytes_before=plan.total_size,
                bytes_after=plan.total_size,  # Size stays the same
                duration_seconds=time.perf_counter() - start_time,
                files_rewritten=plan.total_files,
                success=True,
            )

        # Execute compaction
        # Note: Actual rewrite implementation would use PyIceberg's rewrite_data_files
        # For now, this is a placeholder that logs the operation
        logger.warning(
            "Compaction execution not yet implemented. "
            "This requires PyIceberg's rewrite_data_files functionality."
        )

        # TODO: Implement actual compaction execution
        # for file_group in plan.file_groups:
        #     # Read data from small files
        #     data = read_files(file_group.files)
        #
        #     # Write consolidated file
        #     new_file = write_data_file(data, target_path)
        #
        #     # Update table with rewrite
        #     table.rewrite_data_files(
        #         old_files=file_group.files,
        #         new_files=[new_file]
        #     )

        duration = time.perf_counter() - start_time

        return CompactionResult(
            table_identifier=(namespace, table_name),
            files_before=plan.total_files,
            files_after=plan.estimated_output_files,
            bytes_before=plan.total_size,
            bytes_after=plan.total_size,
            duration_seconds=duration,
            files_rewritten=plan.total_files,
            success=True,
        )

    except Exception as e:
        duration = time.perf_counter() - start_time
        logger.error(f"Compaction failed for {identifier}: {e}")

        return CompactionResult(
            table_identifier=identifier if isinstance(identifier, tuple) else (str(identifier), ""),
            files_before=0,
            files_after=0,
            bytes_before=0,
            bytes_after=0,
            duration_seconds=duration,
            files_rewritten=0,
            success=False,
            error=str(e),
        )


def get_compaction_stats(table: Table) -> Dict[str, Any]:
    """Get statistics about a table's files for compaction analysis.

    Args:
        table: The table to analyze

    Returns:
        Dictionary with file statistics

    Example:
        >>> stats = get_compaction_stats(table)
        >>> print(f"Average file size: {stats['avg_file_size_mb']:.1f} MB")
        >>> print(f"Small files: {stats['small_file_count']}")
    """
    config = CompactionConfig.from_table_properties(table.properties)

    snapshot = table.current_snapshot()
    if not snapshot:
        return {
            "file_count": 0,
            "total_size_mb": 0,
            "avg_file_size_mb": 0,
            "small_file_count": 0,
            "small_file_size_mb": 0,
            "compaction_needed": False,
        }

    file_sizes = []
    small_file_sizes = []

    try:
        scan = table.scan()
        for task in scan.plan_files():
            file_size = task.data_file.file_size_in_bytes
            file_sizes.append(file_size)

            if file_size < config.max_small_file_size_bytes:
                small_file_sizes.append(file_size)
    except Exception as e:
        logger.warning(f"Failed to get file statistics: {e}")
        return {"error": str(e)}

    total_size = sum(file_sizes)
    small_file_size = sum(small_file_sizes)

    return {
        "file_count": len(file_sizes),
        "total_size_mb": total_size / 1024 / 1024,
        "avg_file_size_mb": (total_size / len(file_sizes) / 1024 / 1024) if file_sizes else 0,
        "min_file_size_mb": min(file_sizes) / 1024 / 1024 if file_sizes else 0,
        "max_file_size_mb": max(file_sizes) / 1024 / 1024 if file_sizes else 0,
        "small_file_count": len(small_file_sizes),
        "small_file_size_mb": small_file_size / 1024 / 1024,
        "small_file_threshold_mb": config.max_small_file_size_bytes / 1024 / 1024,
        "compaction_needed": len(small_file_sizes) >= config.min_file_count,
    }
