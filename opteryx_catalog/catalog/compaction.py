"""
Compaction module for optimizing dataset file layout.

Provides incremental compaction strategies to address the small files problem.
"""

from __future__ import annotations

import os
import time
from typing import List
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .manifest import build_parquet_manifest_entry
from .metadata import Snapshot

# Constants
TARGET_SIZE_MB = 128
TARGET_SIZE_BYTES = TARGET_SIZE_MB * 1024 * 1024
MIN_SIZE_MB = 100
MIN_SIZE_BYTES = MIN_SIZE_MB * 1024 * 1024
MAX_SIZE_MB = 140
MAX_SIZE_BYTES = MAX_SIZE_MB * 1024 * 1024
SMALL_FILE_MB = 64
SMALL_FILE_BYTES = SMALL_FILE_MB * 1024 * 1024
LARGE_FILE_MB = 196
LARGE_FILE_BYTES = LARGE_FILE_MB * 1024 * 1024
MAX_MEMORY_FILES = 2  # Maximum files to hold in memory at once
MAX_MEMORY_BYTES = 280 * 1024 * 1024  # 280MB


class DatasetCompactor:
    """
    Incremental compaction for datasets to optimize file layout.

    Supports two strategies:
    - 'brute': Combines small files to reach target size (128MB)
    - 'performance': Optimizes pruning by merging overlapping ranges

    Each compact() call performs one compaction operation.
    """

    def __init__(
        self,
        dataset,
        strategy: Optional[str] = None,
        author: Optional[str] = None,
        agent: Optional[str] = None,
    ):
        """
        Initialize compactor for a dataset.

        Args:
            dataset: SimpleDataset instance to compact
            strategy: 'brute', 'performance', or None (auto-detect)
            author: Author name for snapshot metadata
            agent: Agent identifier for snapshot metadata
        """
        self.dataset = dataset
        self.author = author
        self.agent = agent or "compactor"

        # Auto-detect strategy if not specified
        if strategy is None:
            # Check if dataset has sort order - if so, performance mode is available
            sort_orders = getattr(dataset.metadata, "sort_orders", [])
            if sort_orders and len(sort_orders) > 0:
                self.strategy = "performance"
                self.decision = "auto"
            else:
                self.strategy = "brute"
                self.decision = "no-sort"
        else:
            self.strategy = strategy
            self.decision = "user"

        # Get sort column if available
        self.sort_column_id = None
        if self.strategy == "performance":
            sort_orders = getattr(dataset.metadata, "sort_orders", [])
            if sort_orders and len(sort_orders) > 0:
                self.sort_column_id = sort_orders[0]
            else:
                # Fallback to brute if performance requested but no sort order
                self.strategy = "brute"
                self.decision = "no-sort"

    def compact(self, dry_run: bool = False) -> Optional[Snapshot]:
        """
        Perform one incremental compaction operation.

        Args:
            dry_run: If True, return plan without executing

        Returns:
            New Snapshot if compaction was performed, None if nothing to compact
        """
        # Get current manifest entries
        current_snapshot = self.dataset.metadata.current_snapshot
        if not current_snapshot:
            return None

        manifest_path = current_snapshot.manifest_list
        if not manifest_path:
            return None

        # Read manifest entries
        entries = self._read_manifest(manifest_path)
        if not entries:
            return None

        # Select files to compact based on strategy
        if self.strategy == "brute":
            compaction_plan = self._select_brute_compaction(entries)
        else:  # performance
            compaction_plan = self._select_performance_compaction(entries)

        if not compaction_plan:
            return None

        if dry_run:
            # Return plan information (could extend this to return a structured plan)
            return compaction_plan

        # Execute compaction
        new_snapshot = self._execute_compaction(entries, compaction_plan)
        return new_snapshot

    def _read_manifest(self, manifest_path: str) -> List[dict]:
        """Read manifest entries from manifest file."""
        try:
            io = self.dataset.io
            inp = io.new_input(manifest_path)
            with inp.open() as f:
                data = f.read()
            table = pq.read_table(pa.BufferReader(data))
            return table.to_pylist()
        except Exception:
            return []

    def _select_brute_compaction(self, entries: List[dict]) -> Optional[dict]:
        """
        Select files for brute force compaction.

        Strategy:
        1. Find files < 64MB (small files to eliminate)
        2. Find files >= 196MB (large files to split)
        3. Combine small files up to 128MB target
        4. Split large files if any

        Returns:
            Compaction plan dict or None
        """
        small_files = []
        large_files = []
        acceptable_files = []

        for entry in entries:
            size = entry.get("uncompressed_size_in_bytes", 0)
            if size < SMALL_FILE_BYTES:
                small_files.append(entry)
            elif size >= LARGE_FILE_BYTES:
                large_files.append(entry)
            elif MIN_SIZE_BYTES <= size <= MAX_SIZE_BYTES:
                acceptable_files.append(entry)

        # Priority 1: Split large files
        if large_files:
            # Take first large file to split
            return {
                "type": "split",
                "files": [large_files[0]],
                "reason": "file-too-large",
            }

        # Priority 2: Combine small files
        if len(small_files) >= 2:
            # Find combination that gets close to target
            selected = []
            total_size = 0

            # Sort by size descending to fill efficiently
            sorted_files = sorted(
                small_files, key=lambda x: x.get("uncompressed_size_in_bytes", 0), reverse=True
            )

            for entry in sorted_files:
                entry_size = entry.get("uncompressed_size_in_bytes", 0)
                if total_size + entry_size <= MAX_MEMORY_BYTES and len(selected) < MAX_MEMORY_FILES:
                    selected.append(entry)
                    total_size += entry_size
                    # Stop if we've reached acceptable size
                    if total_size >= MIN_SIZE_BYTES:
                        break

            if len(selected) >= 2:
                return {
                    "type": "combine",
                    "files": selected,
                    "reason": "small-files",
                }

        # No compaction needed
        return None

    def _select_performance_compaction(self, entries: List[dict]) -> Optional[dict]:
        """
        Select files for performance-optimized compaction.

        Strategy:
        1. Find files >= 196MB to split
        2. Find overlapping or adjacent ranges on sort column
        3. Combine and split to eliminate overlap and reach target size

        Returns:
            Compaction plan dict or None
        """
        # Priority 1: Split large files (same as brute)
        large_files = []
        for entry in entries:
            size = entry.get("uncompressed_size_in_bytes", 0)
            if size >= LARGE_FILE_BYTES:
                large_files.append(entry)

        if large_files:
            return {
                "type": "split",
                "files": [large_files[0]],
                "reason": "file-too-large",
            }

        # Priority 2: Find overlapping ranges
        # Get schema to find sort column name
        schema = self.dataset.metadata.schema
        if not schema or not self.sort_column_id:
            # Fallback to brute logic
            return self._select_brute_compaction(entries)

        # Find sort column name from schema
        sort_column_name = None
        if hasattr(schema, "fields") and self.sort_column_id < len(schema.fields):
            sort_column_name = schema.fields[self.sort_column_id].name
        elif isinstance(schema, dict) and "fields" in schema:
            fields = schema["fields"]
            if self.sort_column_id < len(fields):
                sort_column_name = fields[self.sort_column_id].get("name")

        if not sort_column_name:
            # Can't find sort column, fallback to brute
            return self._select_brute_compaction(entries)

        # Extract ranges for each file
        file_ranges = []
        for entry in entries:
            lower_bounds = entry.get("lower_bounds", {})
            upper_bounds = entry.get("upper_bounds", {})

            if sort_column_name in lower_bounds and sort_column_name in upper_bounds:
                min_val = lower_bounds[sort_column_name]
                max_val = upper_bounds[sort_column_name]
                size = entry.get("uncompressed_size_in_bytes", 0)
                file_ranges.append(
                    {
                        "entry": entry,
                        "min": min_val,
                        "max": max_val,
                        "size": size,
                    }
                )

        if not file_ranges:
            # No range information, fallback to brute
            return self._select_brute_compaction(entries)

        # Sort by min value
        file_ranges.sort(key=lambda x: x["min"])

        # Find first overlapping or adjacent group
        for i in range(len(file_ranges) - 1):
            current = file_ranges[i]
            next_file = file_ranges[i + 1]

            # Check for overlap or adjacency
            if current["max"] >= next_file["min"]:
                # Found overlap or adjacency
                # Check if combining would be beneficial
                combined_size = current["size"] + next_file["size"]

                # Only combine if:
                # 1. Total size is within memory limits
                # 2. At least one file is below acceptable range
                # 3. Combined size would benefit from splitting OR result is in acceptable range
                if combined_size <= MAX_MEMORY_BYTES and (
                    current["size"] < MIN_SIZE_BYTES
                    or next_file["size"] < MIN_SIZE_BYTES
                    or (current["max"] >= next_file["min"])  # Overlap exists
                ):
                    return {
                        "type": "combine-split",
                        "files": [current["entry"], next_file["entry"]],
                        "reason": "overlapping-ranges",
                        "sort_column": sort_column_name,
                    }

        # No overlaps found, check for small files to combine
        small_files = [fr for fr in file_ranges if fr["size"] < SMALL_FILE_BYTES]
        if len(small_files) >= 2:
            # Combine adjacent small files
            selected = []
            total_size = 0

            for fr in small_files[:MAX_MEMORY_FILES]:
                if total_size + fr["size"] <= MAX_MEMORY_BYTES:
                    selected.append(fr["entry"])
                    total_size += fr["size"]
                    if total_size >= MIN_SIZE_BYTES:
                        break

            if len(selected) >= 2:
                return {
                    "type": "combine-split",
                    "files": selected,
                    "reason": "small-files",
                    "sort_column": sort_column_name,
                }

        # No compaction opportunities
        return None

    def _execute_compaction(self, all_entries: List[dict], plan: dict) -> Optional[Snapshot]:
        """
        Execute the compaction plan.

        Args:
            all_entries: All current manifest entries
            plan: Compaction plan from selection methods

        Returns:
            New Snapshot or None if failed
        """
        plan_type = plan["type"]
        files_to_compact = plan["files"]
        sort_column = plan.get("sort_column")

        # Read files to compact
        tables = []
        total_size = 0
        for entry in files_to_compact:
            file_path = entry.get("file_path")
            if not file_path:
                continue

            try:
                io = self.dataset.io
                inp = io.new_input(file_path)
                with inp.open() as f:
                    data = f.read()
                table = pq.read_table(pa.BufferReader(data))
                tables.append(table)
                total_size += entry.get("uncompressed_size_in_bytes", 0)
            except Exception:
                # Failed to read file, abort this compaction
                return None

        if not tables:
            return None

        # Combine tables
        combined = pa.concat_tables(tables)

        # Sort if performance mode
        if sort_column and plan_type == "combine-split":
            try:
                # Sort by the sort column
                combined = combined.sort_by([(sort_column, "ascending")])
            except Exception:
                # Sort failed, continue without sorting
                pass

        # Determine how to split
        output_tables = []
        if plan_type == "split" or (plan_type == "combine-split" and total_size > MAX_SIZE_BYTES):
            # Split into multiple files
            output_tables = self._split_table(combined, TARGET_SIZE_BYTES)
        else:
            # Single output file
            output_tables = [combined]

        # Write new files and build manifest entries
        new_entries = []
        snapshot_id = int(time.time() * 1000)

        for idx, table in enumerate(output_tables):
            # Generate file path
            file_name = f"data-{snapshot_id}-{idx:04d}.parquet"
            file_path = os.path.join(self.dataset.metadata.location, file_name)

            # Write parquet file
            try:
                io = self.dataset.io
                out = io.new_output(file_path)
                with out.create() as f:
                    pq.write_table(table, f)
            except Exception:
                # Failed to write, abort
                return None

            # Build manifest entry with full statistics
            entry_dict = build_parquet_manifest_entry(table, file_path)
            new_entries.append(entry_dict)

        # Create new manifest with updated entries
        # Remove old entries, add new entries
        old_file_paths = {f["file_path"] for f in files_to_compact}
        updated_entries = [e for e in all_entries if e.get("file_path") not in old_file_paths]
        updated_entries.extend(new_entries)

        # Write manifest
        manifest_path = self.dataset.catalog.write_parquet_manifest(
            snapshot_id, updated_entries, self.dataset.metadata.location
        )

        # Calculate summary statistics
        deleted_files = len(files_to_compact)
        deleted_size = sum(e.get("file_size_in_bytes", 0) for e in files_to_compact)
        deleted_data_size = sum(e.get("uncompressed_size_in_bytes", 0) for e in files_to_compact)
        deleted_records = sum(e.get("record_count", 0) for e in files_to_compact)

        added_files = len(new_entries)
        added_size = sum(e.get("file_size_in_bytes", 0) for e in new_entries)
        added_data_size = sum(e.get("uncompressed_size_in_bytes", 0) for e in new_entries)
        added_records = sum(e.get("record_count", 0) for e in new_entries)

        total_files = len(updated_entries)
        total_size = sum(e.get("file_size_in_bytes", 0) for e in updated_entries)
        total_data_size = sum(e.get("uncompressed_size_in_bytes", 0) for e in updated_entries)
        total_records = sum(e.get("record_count", 0) for e in updated_entries)

        # Build snapshot with agent metadata
        current = self.dataset.metadata.current_snapshot
        new_sequence = (current.sequence_number or 0) + 1 if current else 1

        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            timestamp_ms=snapshot_id,
            author=self.author,
            user_created=False,
            sequence_number=new_sequence,
            manifest_list=manifest_path,
            operation_type="compact",
            parent_snapshot_id=current.snapshot_id if current else None,
            schema_id=getattr(self.dataset.metadata.schema, "schema_id", None),
            commit_message=f"Compaction: {self.strategy} strategy, {deleted_files} files → {added_files} files",
            summary={
                "added-data-files": added_files,
                "added-files-size": added_size,
                "added-data-size": added_data_size,
                "added-records": added_records,
                "deleted-data-files": deleted_files,
                "deleted-files-size": deleted_size,
                "deleted-data-size": deleted_data_size,
                "deleted-records": deleted_records,
                "total-data-files": total_files,
                "total-files-size": total_size,
                "total-data-size": total_data_size,
                "total-records": total_records,
                "agent_meta": {
                    "committer": self.agent,
                    "compaction-algorithm": self.strategy,
                    "compaction-algorithm-decision": self.decision,
                    "compaction-files-combined": deleted_files,
                    "compaction-files-written": added_files,
                },
            },
        )

        # Commit snapshot
        try:
            self.dataset.metadata.snapshots.append(snapshot)
            self.dataset.metadata.current_snapshot = snapshot

            # Persist metadata via catalog
            if self.dataset.catalog:
                self.dataset.catalog.save_dataset_metadata(self.dataset.metadata)
        except Exception:
            return None

        return snapshot

    def _split_table(self, table: pa.Table, target_size: int) -> List[pa.Table]:
        """
        Split a table into multiple tables of approximately target size.

        Args:
            table: PyArrow table to split
            target_size: Target size in bytes (uncompressed)

        Returns:
            List of tables
        """
        if not table or table.num_rows == 0:
            return [table]

        # Estimate size per row
        total_size = sum(sum(chunk.size for chunk in col.chunks) for col in table.columns)

        if total_size <= target_size:
            return [table]

        # Calculate rows per split
        avg_row_size = total_size / table.num_rows
        rows_per_split = int(target_size / avg_row_size)

        if rows_per_split <= 0:
            rows_per_split = 1

        # Split into chunks
        splits = []
        offset = 0
        while offset < table.num_rows:
            end = min(offset + rows_per_split, table.num_rows)
            split = table.slice(offset, end - offset)
            splits.append(split)
            offset = end

        return splits if splits else [table]
