"""
Compaction module for optimizing dataset file layout.

Provides incremental compaction strategies to address the small files problem.
"""

from __future__ import annotations

import os
import time
import uuid
from typing import List, Optional

from .manifest import ParquetManifestEntry, build_parquet_manifest_entry_from_bytes
from .metadata import Snapshot

# Stable node identifier for this process (hex-mac-hex-pid)
_NODE = f"{uuid.getnode():x}-{os.getpid():x}"

# Constants
TARGET_SIZE_MB = 512
TARGET_SIZE_BYTES = TARGET_SIZE_MB * 1024 * 1024
MIN_SIZE_MB = 500
MIN_SIZE_BYTES = MIN_SIZE_MB * 1024 * 1024
MAX_SIZE_MB = 525
MAX_SIZE_BYTES = MAX_SIZE_MB * 1024 * 1024
SMALL_FILE_MB = 400
SMALL_FILE_BYTES = SMALL_FILE_MB * 1024 * 1024
LARGE_FILE_MB = 500
LARGE_FILE_BYTES = LARGE_FILE_MB * 1024 * 1024
MAX_MEMORY_FILES = 2  # Maximum output files to keep in memory during combine-and-split
MAX_MEMORY_BYTES = 1100 * 1024 * 1024  # 1GB - allows combining files just over target size


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
        current_snapshot = self.dataset.metadata.current_snapshot()
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
        # Prefer parsed-manifest cache to avoid repeated rugo parsing
        from .manifest import get_parsed_manifest

        try:
            return get_parsed_manifest(self.dataset.io, manifest_path)
        except Exception:
            return []

    def _select_brute_compaction(self, entries: List[dict]) -> Optional[dict]:
        """
        Select files for brute force compaction.

        Strategy:
        1. Combine files under SMALL_FILE_BYTES threshold to reach TARGET_SIZE_BYTES
        2. No splitting (file size is less critical with current read approach)

        Returns:
            Compaction plan dict or None
        """
        small_files = []

        for entry in entries:
            size = entry.get("uncompressed_size_in_bytes", 0)
            if size < SMALL_FILE_BYTES:
                small_files.append(entry)

        # Priority 1: Combine files under threshold
        if len(small_files) >= 2:
            # Find combination that gets close to target
            selected = []
            total_size = 0

            # Sort by size ascending to prioritize eliminating smallest files
            sorted_files = sorted(small_files, key=lambda x: x.get("uncompressed_size_in_bytes", 0))

            for entry in sorted_files:
                entry_size = entry.get("uncompressed_size_in_bytes", 0)
                if total_size + entry_size <= MAX_MEMORY_BYTES:
                    selected.append(entry)
                    total_size += entry_size
                    # Continue accumulating files until we hit target, don't stop early
                    if total_size >= TARGET_SIZE_BYTES and len(selected) >= 2:
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
        1. Find overlapping or adjacent ranges on sort column
        2. Combine files to eliminate overlap and reach target size
        3. No splitting (file size is less critical with current read approach)

        Returns:
            Compaction plan dict or None
        """

        # Priority 2: Find overlapping ranges on the sort column.
        #
        # Resolve the sort column from the dataset's *stored* schema. The raw
        # ``metadata.schema`` attribute is frequently None on a freshly loaded
        # dataset (the real schema lives in the schemas subcollection), so
        # prefer ``dataset.schema()`` which resolves it. ``sort_column_id`` is a
        # positional index into the schema's columns.
        sort_index = self.sort_column_id
        columns = None
        try:
            resolved = self.dataset.schema()
            if resolved is not None and getattr(resolved, "columns", None):
                columns = resolved.columns
        except Exception:
            columns = None
        if columns is None:
            schema = getattr(self.dataset.metadata, "schema", None)
            if getattr(schema, "columns", None):
                columns = schema.columns  # RelationSchema
            elif getattr(schema, "fields", None):
                columns = schema.fields
            elif isinstance(schema, dict) and "fields" in schema:
                columns = schema["fields"]

        if not columns or sort_index is None or sort_index >= len(columns):
            # Can't resolve the sort column; fall back to brute logic.
            return self._select_brute_compaction(entries)

        sort_col = columns[sort_index]
        sort_column_name = getattr(sort_col, "name", None)
        sort_field_id = getattr(sort_col, "id", None)
        if isinstance(sort_col, dict):
            sort_column_name = sort_column_name or sort_col.get("name")
            sort_field_id = sort_field_id if sort_field_id is not None else sort_col.get("id")

        if not sort_column_name:
            return self._select_brute_compaction(entries)

        # Extract the sort-column min/max for each file. Parquet manifest entries
        # store per-column statistics as positional lists (min_values/max_values)
        # aligned with field_ids, NOT as iceberg-style lower_bounds/upper_bounds
        # dicts. Read the positional stats, keyed by stable field-id when the
        # entry carries field_ids and falling back to schema column position.
        file_ranges = []
        for entry in entries:
            min_values = entry.get("min_values") or []
            max_values = entry.get("max_values") or []
            if not min_values or not max_values:
                continue

            field_ids = entry.get("field_ids") or []
            idx = None
            if sort_field_id is not None and sort_field_id in field_ids:
                idx = field_ids.index(sort_field_id)
            elif sort_index < len(min_values):
                idx = sort_index

            if idx is None or idx >= len(min_values) or idx >= len(max_values):
                continue

            min_val = min_values[idx]
            max_val = max_values[idx]
            if min_val is None or max_val is None:
                continue

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
            # No usable range information, fallback to brute
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

            for fr in small_files:
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

    def _reconcile_schemas(self, morsels: list) -> list:
        """
        Reconcile schemas across multiple draken Morsels.

        When morsels have incompatible schemas (e.g. one column is NULL-typed
        in one morsel because every value there was None, and a concrete type
        in another, or a column is missing entirely from one morsel), rebuild
        the mismatched columns so every morsel shares one unified schema
        before concatenation.

        Args:
            morsels: List of draken Morsels with potentially mismatched schemas

        Returns:
            List of morsels with unified schemas
        """
        if not morsels or len(morsels) <= 1:
            return morsels

        from draken.interop.vector_sequence import vector_from_sequence
        from draken.morsels.morsel import Morsel

        from .manifest import morsel_schema_dict

        # Build the unified per-column type: prefer the first non-NULL type
        # seen for each column name, across all morsels.
        unified_types: dict = {}
        for morsel in morsels:
            for name, dtype in morsel_schema_dict(morsel).items():
                dtype_name = getattr(dtype, "name", str(dtype))
                if name not in unified_types or unified_types[name] == "NULL":
                    unified_types[name] = dtype_name

        reconciled = []
        for morsel in morsels:
            morsel_types = {
                name: getattr(dtype, "name", str(dtype))
                for name, dtype in morsel_schema_dict(morsel).items()
            }
            if morsel_types == unified_types:
                reconciled.append(morsel)
                continue

            try:
                rebuilt = Morsel()
                for name, target_type in unified_types.items():
                    if name in morsel_types:
                        vec = morsel.column(name.encode("utf-8"))
                        if morsel_types[name] == target_type:
                            rebuilt.append_vector(name, vec)
                        else:
                            rebuilt.append_vector(
                                name, vector_from_sequence(vec.to_pylist(), dtype=target_type)
                            )
                    else:
                        # Column missing entirely: fill with nulls
                        rebuilt.append_vector(
                            name,
                            vector_from_sequence([None] * morsel.num_rows, dtype=target_type),
                        )
                reconciled.append(rebuilt)
            except Exception:
                # Failed to reconcile, skip this morsel
                continue

        return reconciled if reconciled else morsels

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

        from draken.morsels.morsel import Morsel

        # Read files to compact
        tables = []
        total_size = 0
        for entry in files_to_compact:
            file_path = entry.get("file_path")
            if not file_path:
                continue

            try:
                from rugo.parquet import read_parquet

                io = self.dataset.io
                inp = io.new_input(file_path)
                with inp.open() as f:
                    data = f.read()
                with read_parquet(bytes(data)) as reader:
                    row_group_morsels = list(reader)
                file_morsel = (
                    Morsel.combine(row_group_morsels) if len(row_group_morsels) > 1 else row_group_morsels[0]
                )
                tables.append(file_morsel)
                total_size += entry.get("uncompressed_size_in_bytes", 0)
            except Exception:
                # Failed to read file, abort this compaction
                return None

        if not tables:
            return None

        # Reconcile schemas before concatenation
        tables = self._reconcile_schemas(tables)

        # Combine morsels
        combined = Morsel.combine(tables) if len(tables) > 1 else tables[0]

        # Sort if performance mode
        if sort_column and plan_type == "combine-split":
            try:
                sort_values = combined.column(sort_column.encode("utf-8")).to_pylist()
                order = sorted(range(len(sort_values)), key=lambda i: (sort_values[i] is None, sort_values[i]))
                combined = combined.take(order)
            except Exception:
                # Sort failed, continue without sorting
                pass

        # Determine how to split
        output_tables = []
        if plan_type == "split" or (plan_type == "combine-split" and total_size > MAX_SIZE_BYTES):
            # Split into multiple files, but at most MAX_MEMORY_FILES
            output_tables = self._split_table(
                combined, TARGET_SIZE_BYTES, max_files=MAX_MEMORY_FILES
            )
        else:
            # Single output file
            output_tables = [combined]

        # Write new files and build manifest entries
        new_entries = []
        snapshot_id = int(time.time() * 1000)

        for idx, table in enumerate(output_tables):
            # Generate collision-resistant file path using nanosecond precision timestamp and node id
            file_name = f"{time.time_ns():x}-{_NODE}.parquet"
            file_path = os.path.join(self.dataset.metadata.location, "data", file_name)

            # Write parquet file and upload (so we can reuse bytes)
            try:
                from rugo.parquet import write_parquet

                from ..iops.fileio import WRITE_PARQUET_OPTIONS

                pdata = write_parquet(table, **WRITE_PARQUET_OPTIONS)
                io = self.dataset.io
                out = io.new_output(file_path).create()
                out.write(pdata)
                out.close()
            except Exception:
                # Failed to write or upload, abort
                return None

            # Build manifest entry with full statistics directly from the
            # in-memory Morsel (avoids re-reading and losing temporal/decimal
            # semantic types to Parquet's physical-int round-trip).
            entry_obj = build_parquet_manifest_entry_from_bytes(
                pdata,
                file_path,
                len(pdata),
                orig_morsel=table,
                field_id_by_name=self.dataset._field_id_by_name(),
            )
            entry_dict = self._to_dict(entry_obj)
            new_entries.append(entry_dict)

        # Create new manifest with updated entries
        # Remove old entries, add new entries
        # Also validate remaining entries - recover corrupted ones by reading files
        old_file_paths = {f["file_path"] for f in files_to_compact}
        updated_entries = []

        for e in all_entries:
            if e.get("file_path") not in old_file_paths:
                # Validate entry before including
                if self._is_valid_entry(e):
                    # Entry is valid, use as-is (100%)
                    updated_entries.append(e)
                else:
                    # Entry is corrupted, rebuild from source (100%)
                    recovered = self._recover_entry(e)
                    if not recovered:
                        # Rebuild failed - catastrophic, abort entire compaction
                        return None
                    updated_entries.append(recovered)

        updated_entries.extend(new_entries)

        # Ensure all entries are dicts (convert any ParquetManifestEntry objects)
        final_entries = []
        for entry in updated_entries:
            if isinstance(entry, dict):
                final_entries.append(entry)
            else:
                converted = self._to_dict(entry)
                if isinstance(converted, dict):
                    final_entries.append(converted)

        # Write manifest
        manifest_path = self.dataset.catalog.write_parquet_manifest(
            snapshot_id, final_entries, self.dataset.metadata.location
        )

        # Calculate summary statistics from actual data
        # Don't rely on potentially corrupted manifest entries
        try:
            deleted_files = len(files_to_compact)
            deleted_size = 0
            deleted_data_size = 0
            deleted_records = 0

            # Get actual stats from the tables we read
            for table in tables:
                deleted_records += table.num_rows
                # Estimate data size from actual table
                deleted_data_size += table.nbytes

            added_files = len(new_entries)
            added_size = sum(e.get("file_size_in_bytes", 0) for e in new_entries)
            added_data_size = sum(e.get("uncompressed_size_in_bytes", 0) for e in new_entries)
            added_records = sum(e.get("record_count", 0) for e in new_entries)

            total_files = len(final_entries)
            total_size = sum(e.get("file_size_in_bytes", 0) for e in final_entries)
            total_data_size = sum(e.get("uncompressed_size_in_bytes", 0) for e in final_entries)
            total_records = sum(e.get("record_count", 0) for e in final_entries)
        except Exception:
            # If stats calculation fails, refresh the entire manifest from data files
            final_entries = self._refresh_manifest_from_data_files(final_entries)

            # Use what we know directly since we still have new_entries in scope
            deleted_files = len(files_to_compact)
            deleted_size = sum(e.get("file_size_in_bytes", 0) for e in files_to_compact)
            deleted_data_size = sum(
                e.get("uncompressed_size_in_bytes", 0) for e in files_to_compact
            )
            deleted_records = sum(e.get("record_count", 0) for e in files_to_compact)

            added_files = len(new_entries)
            added_size = sum(e.get("file_size_in_bytes", 0) for e in new_entries)
            added_data_size = sum(e.get("uncompressed_size_in_bytes", 0) for e in new_entries)
            added_records = sum(e.get("record_count", 0) for e in new_entries)

            total_files = len(final_entries)
            total_size = sum(e.get("file_size_in_bytes", 0) for e in final_entries)
            total_data_size = sum(e.get("uncompressed_size_in_bytes", 0) for e in final_entries)
            total_records = sum(e.get("record_count", 0) for e in final_entries)

        # Build snapshot with agent metadata
        current = self.dataset.metadata.current_snapshot()
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
            self.dataset.metadata.current_snapshot_id = snapshot.snapshot_id

            # Persist metadata via catalog
            if self.dataset.catalog:
                self.dataset.catalog.save_dataset_metadata(
                    self.dataset.identifier, self.dataset.metadata
                )
        except Exception as e:
            raise RuntimeError(
                f"Failed to persist compaction snapshot {snapshot_id} to metastore"
            ) from e

        return snapshot

    def _split_table(self, table, target_size: int, max_files: int = None) -> list:
        """
        Split a Morsel into multiple Morsels of approximately target size.

        Args:
            table: draken Morsel to split
            target_size: Target size in bytes (uncompressed)
            max_files: Maximum number of output files to create (optional)

        Returns:
            List of Morsels
        """
        if not table or table.num_rows == 0:
            return [table]

        # Estimate size per row
        total_size = table.nbytes

        if total_size <= target_size:
            return [table]

        # Calculate number of splits needed
        avg_row_size = total_size / table.num_rows
        rows_per_split = int(target_size / avg_row_size)

        if rows_per_split <= 0:
            rows_per_split = 1

        # Calculate how many splits we'd produce
        num_splits = (table.num_rows + rows_per_split - 1) // rows_per_split

        # If max_files is set and we'd exceed it, increase rows_per_split to stay within limit
        if max_files and num_splits > max_files:
            rows_per_split = (table.num_rows + max_files - 1) // max_files

        # Split into chunks
        splits = []
        offset = 0
        while offset < table.num_rows:
            end = min(offset + rows_per_split, table.num_rows)
            split = table.slice(offset, end - offset)
            splits.append(split)
            offset = end

        return splits if splits else [table]

    def _to_dict(self, obj):
        """
        Convert a ParquetManifestEntry or similar object to a dict.

        Handles various object types that might be returned from manifest operations.

        Args:
            obj: Object to convert (dict, ParquetManifestEntry, or dataclass)

        Returns:
            Dict representation of the object, or the original if already a dict
        """
        if isinstance(obj, dict):
            return obj
        elif hasattr(obj, "to_dict") and callable(obj.to_dict):
            return obj.to_dict()
        elif hasattr(obj, "__dict__"):
            return vars(obj)
        else:
            return obj

    def _is_valid_entry(self, entry: dict) -> bool:
        """
        Validate a manifest entry by attempting to instantiate the data class.

        Tries to construct a ParquetManifestEntry from the dict. If successful,
        the entry is valid. If any exception is raised (missing fields, type errors,
        corrupted values), the entry is considered invalid.

        Args:
            entry: Manifest entry dict

        Returns:
            True if entry can be successfully converted to ParquetManifestEntry, False otherwise
        """
        if not isinstance(entry, dict):
            return False

        try:
            # Try to instantiate the data class from the dict
            # This will validate all fields, types, and constraints
            ParquetManifestEntry(**entry)
            return True
        except (TypeError, ValueError, KeyError, AttributeError):
            # Any exception means the entry is corrupted or invalid
            return False

    def _recover_entry(self, corrupted_entry: dict) -> Optional[dict]:
        """
        Recover a corrupted manifest entry by reading the actual file.

        Args:
            corrupted_entry: The corrupted manifest entry dict

        Returns:
            Rebuilt manifest entry dict, or None if recovery failed
        """
        file_path = corrupted_entry.get("file_path")
        if not file_path:
            return None

        try:
            # Read the file
            io = self.dataset.io
            inp = io.new_input(file_path)
            with inp.open() as f:
                data = f.read()

            # Rebuild manifest entry from the actual file data
            rebuilt_entry = build_parquet_manifest_entry_from_bytes(
                data, file_path, len(data), field_id_by_name=self.dataset._field_id_by_name()
            )

            # Convert to dict
            entry_dict = self._to_dict(rebuilt_entry)
            if isinstance(entry_dict, dict):
                return entry_dict
            return None
        except Exception:
            # If we can't recover the file, return None
            return None

    def _refresh_manifest_from_data_files(self, all_entries: List[dict]) -> List[dict]:
        """
        Refresh entire manifest by reading all data files and rebuilding entries from scratch.

        Used as fallback when stats calculation fails. Rebuilds all manifest entries by reading
        the actual parquet files to ensure accuracy and correctness.

        Args:
            all_entries: All current manifest entries

        Returns:
            List of refreshed manifest entries
        """
        refreshed_entries = []

        for entry in all_entries:
            file_path = entry.get("file_path")
            if not file_path:
                refreshed_entries.append(entry)  # Skip entries without file paths
                continue

            try:
                # Read the file and rebuild the entry from scratch
                io = self.dataset.io
                inp = io.new_input(file_path)
                with inp.open() as f:
                    data = f.read()

                # Rebuild manifest entry from actual file data
                rebuilt_entry = build_parquet_manifest_entry_from_bytes(
                    data, file_path, len(data), field_id_by_name=self.dataset._field_id_by_name()
                )

                # Convert to dict
                entry_dict = self._to_dict(rebuilt_entry)
                if isinstance(entry_dict, dict):
                    refreshed_entries.append(entry_dict)
                else:
                    refreshed_entries.append(entry)  # Fall back to original if rebuild failed
            except Exception:
                # If we can't rebuild, keep the original entry
                refreshed_entries.append(entry)

        return refreshed_entries

    def _calculate_stats_from_entries(
        self, all_entries: List[dict], compacted_files: List[dict]
    ) -> tuple:
        """
        Calculate statistics from manifest entries.

        Used when direct calculation from PyArrow tables fails.

        Args:
            all_entries: All manifest entries after compaction
            compacted_files: Files that were compacted (to calculate deleted stats)

        Returns:
            Tuple of (deleted_files, deleted_size, deleted_data_size, deleted_records,
                    added_files, added_size, added_data_size, added_records,
                    total_files, total_size, total_data_size, total_records)
        """
        compacted_paths = {f.get("file_path") for f in compacted_files}

        deleted_files = len(compacted_files)
        deleted_size = 0
        deleted_data_size = 0
        deleted_records = 0

        # Sum stats for deleted files
        for entry in compacted_files:
            deleted_size += entry.get("file_size_in_bytes", 0)
            deleted_data_size += entry.get("uncompressed_size_in_bytes", 0)
            deleted_records += entry.get("record_count", 0)

        # The new files are those in all_entries that weren't compacted
        added_files = 0
        added_size = 0
        added_data_size = 0
        added_records = 0

        for entry in all_entries:
            if entry.get("file_path") not in compacted_paths:
                continue
            # This is a new (non-compacted) file
            added_files += 1
            added_size += entry.get("file_size_in_bytes", 0)
            added_data_size += entry.get("uncompressed_size_in_bytes", 0)
            added_records += entry.get("record_count", 0)

        # Total stats from all entries
        total_files = len(all_entries)
        total_size = sum(e.get("file_size_in_bytes", 0) for e in all_entries)
        total_data_size = sum(e.get("uncompressed_size_in_bytes", 0) for e in all_entries)
        total_records = sum(e.get("record_count", 0) for e in all_entries)

        return (
            deleted_files,
            deleted_size,
            deleted_data_size,
            deleted_records,
            added_files,
            added_size,
            added_data_size,
            added_records,
            total_files,
            total_size,
            total_data_size,
            total_records,
        )
