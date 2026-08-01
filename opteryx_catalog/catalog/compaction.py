"""
Compaction module for optimizing dataset file layout.

Provides incremental compaction strategies to address the small files problem.

HOW PERFORMANCE COMPACTION WORKS
--------------------------------
The ``performance`` strategy has two merge modes and gradually improves
partitioning. It never merges past the target size (there is no size-based
split step).

  1. Never merge to exceed ~4 GB (TARGET_SIZE_BYTES). Bin-pack up to the target;
     we simply do not create a file larger than target.
  2. Files under 512 MB (MIN_FILE_SIZE_BYTES): BRUTE-FORCE merge - concatenate,
     no sort. Sorting tiny scattered files is wasted work; they get sorted once
     they graduate past the floor.
  3. Files at/over ~512 MB: SORT-AWARE merge that gradually improves
     partitioning - combine OVERLAPPING files, sort on the sort key, and split
     the result into DISJOINT key ranges. The split granularity depends on size:
     if the merged result exceeds ~4 GB it becomes multiple FILES (each a
     disjoint range); if it stays under ~4 GB it is one file whose disjoint
     ranges are expressed as sorted ROW GROUPS only (never split into a new file
     under target). Each pass tightens the partitioning a little; repeated
     passes converge toward non-overlapping files.
  4. At most ONE file under 512 MB may remain: whenever two or more sub-floor
     files exist they are merged; a single leftover "remainder" file below the
     floor (e.g. a 200 MB tail) is acceptable and expected.
  5. Execution streams row groups (see ``_execute_compaction_streaming``):
     constant memory, so a merge can be larger than RAM.

``brute`` strategy is a separate, older fallback for datasets with no sort
order; it is unrelated to the above.
"""

from __future__ import annotations

import os
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

from .manifest import ParquetManifestEntry, build_parquet_manifest_entry_from_bytes
from .metadata import Snapshot

# Stable node identifier for this process (hex-mac-hex-pid)
_NODE = f"{uuid.getnode():x}-{os.getpid():x}"

# Concurrency for _refresh_manifest_from_data_files: that path can touch
# every file in a dataset (a full-manifest-rebuild fallback), and each
# file's read+stats is independent (network I/O plus native decode that
# releases the GIL), so overlapping them cuts wall time on any dataset
# bigger than one file. Kept small and fixed rather than scaled to file
# count or CPU count -- rebuilding one large file's stats can transiently
# need several GB of native memory (see build_parquet_manifest_entry_from_bytes),
# so a wide pool would turn a rare recovery path into a memory spike.
_REFRESH_MANIFEST_MAX_WORKERS = 8


def normalize_sort_order(sort_orders) -> Optional[dict]:
    """Reduce a ``sort_orders`` value to the primary sort key in canonical form.

    ``sort_orders`` has been written in two incompatible shapes:

    * positional ints — ``[0]`` — an index into the schema's columns (used by
      the tests and by production ``ops.*`` datasets); and
    * Iceberg-style dicts — ``[{"order-id": 1, "fields": [{"name": "id",
      "direction": "asc"}]}]`` — name-based with a direction (written by
      ``scripts/create_dataset.py``). The old code treated ``sort_orders[0]`` as
      an int unconditionally, so the dict shape raised an uncaught ``TypeError``
      (``dict >= int``) out of ``compact()``.

    Returns ``{"name", "field_id", "index", "ascending"}`` for the primary
    (first) sort key, with the unused resolution keys set to ``None``, or
    ``None`` when nothing usable can be extracted (caller falls back to brute).
    Resolution precedence downstream is field_id → name → index.
    """
    try:
        if not sort_orders:
            return None
        entry = sort_orders[0]

        # Positional int index.
        if isinstance(entry, bool):
            return None  # bool is an int subclass; never a valid column index
        if isinstance(entry, int):
            return {"name": None, "field_id": None, "index": entry, "ascending": True}

        # Bare column name.
        if isinstance(entry, str):
            return {"name": entry, "field_id": None, "index": None, "ascending": True}

        if isinstance(entry, dict):
            # Iceberg sort-order object: unwrap to its first field. Also accept a
            # bare field dict ({"name": ..., "direction": ...}).
            field = entry
            fields = entry.get("fields")
            if isinstance(fields, (list, tuple)) and fields:
                field = fields[0]
            if not isinstance(field, dict):
                return None

            name = field.get("name")
            # Iceberg identifies the source column by "source-id" (a field id);
            # accept it as field_id when present.
            field_id = field.get("source-id")
            if field_id is None:
                field_id = field.get("field-id")
            direction = str(field.get("direction", "asc")).lower()
            ascending = direction != "desc"

            if name is None and field_id is None:
                return None
            return {
                "name": name,
                "field_id": field_id,
                "index": None,
                "ascending": ascending,
            }
    except Exception:
        return None
    return None

# Constants
#
# All size thresholds below are compared against `uncompressed_size_in_bytes`,
# NOT the on-disk size. At the ~8.6x zstd ratio measured on github.events, a
# 4 GB uncompressed target lands at roughly 470 MB on disk.
#
# These were raised from ~512 MB to ~4 GB on the strength of measurements over a
# 187M-row github.events dataset: 4 GB-uncompressed files scanned ~5x faster per
# row than the ~160 MB files they replaced. The old 512 MB target (~60 MB on
# disk) produced far more, far smaller files than reads want.
TARGET_SIZE_MB = 4096  # 4.0 GB - ideal output size
TARGET_SIZE_BYTES = TARGET_SIZE_MB * 1024 * 1024
MIN_SIZE_MB = 3584  # 3.5 GB - lower bound of the acceptable band
MIN_SIZE_BYTES = MIN_SIZE_MB * 1024 * 1024

# --- Small-file floor ("no files under X") -------------------------------------
#
# The invariant that answers "who merges tiny files": THIS policy does. After
# compaction settles, no file persists below MIN_FILE_SIZE_BYTES. Whenever there
# are two or more sub-floor files, they are combined - there is NO volume
# threshold to wait for (waiting is just the small-files problem by another
# name; a drip-fed dataset would sit with its tiny files un-merged for weeks).
#
# The sub-floor files (below the floor) are the only ones this policy touches;
# files at/above the floor are left alone. Consolidation only consumes sub-floor
# files and produces >=floor files (once enough accumulate), so it converges and
# never fights itself.
#
# MIN_FILE_SIZE_BYTES (512 MB) is the floor. A consolidation of a small drip
# yields a sub-floor output that stays a candidate and keeps absorbing new tiny
# files until it crosses the floor and freezes - a bounded accretion up to ~one
# floor of rewrites, the accepted price for not leaving tiny files around. A
# large accumulated mass instead splits straight into target-sized files.
MIN_FILE_SIZE_MB = 512
MIN_FILE_SIZE_BYTES = MIN_FILE_SIZE_MB * 1024 * 1024

# --- Sort-aware pool floor (deliberately overlaps the brute ceiling) -----------
#
# Rule A (brute) claims files < MIN_FILE_SIZE_BYTES (512 MB). Rule B (sort-aware)
# claims files > SORT_AWARE_FLOOR_BYTES (500 MB) - a lower, separate threshold,
# not a reuse of the 512 MB floor. The two pools overlap on (500 MB, 512 MB]: a
# borderline file is visible to both selectors. If it has a real sort-aware
# opportunity (it overlaps a neighbour, or packs with other sub-target files),
# rule B's better-quality merge (sorted, disjoint) can claim it; otherwise rule
# A's brute merge still cleans it up as the coverage guarantee. A single shared
# boundary would starve rule B of exactly these files, since they'd only ever be
# `< 512` and never reachable by the sort-aware selectors.
SORT_AWARE_FLOOR_MB = 500
SORT_AWARE_FLOOR_BYTES = SORT_AWARE_FLOOR_MB * 1024 * 1024

MAX_SIZE_MB = 4198  # 4.1 GB - hard cap
MAX_SIZE_BYTES = MAX_SIZE_MB * 1024 * 1024
SMALL_FILE_MB = 3584  # anything under the lower bound is a merge candidate
SMALL_FILE_BYTES = SMALL_FILE_MB * 1024 * 1024
LARGE_FILE_MB = 4198
LARGE_FILE_BYTES = LARGE_FILE_MB * 1024 * 1024
# Deprecated: combine-split no longer caps output to one file. Splitting a
# key-sorted batch into k = ceil(total / TARGET) target-sized files is what
# converges a scattered sort key (see _split_into_k); forcing a single output
# produced one union-range file per pass and never converged. Retained only so
# any external reference still resolves.
MAX_MEMORY_FILES = 1

# --- Byte-aware memory budget --------------------------------------------------
#
# A merge holds the combined input in RAM, then the sorted `take` copy, on top of
# a one-time native runtime warmup. The gate must bound that PEAK RSS against the
# container's real memory - not compare a flat 4.3 GB against the manifest's
# `uncompressed_size_in_bytes`, which is a sum-of-sys.getsizeof estimate whose
# ratio to real RAM is data-dependent.
#
# Calibration (measured, real data): a 7.9M-row file with 8.7 GB budget-unit read
# to ~10.2 GB RSS. Per-slice output (below) keeps the sort/write stage at
# ~read-footprint (measured 10.5 GB, ~1.2x budget). For a MULTI-file merge the
# peak driver is instead `Morsel.combine`, which holds the inputs and the
# concatenated result together (~2x the combined budget-unit) before the inputs
# are freed. We size the gate to that ~2x transient (times the budget->RAM ratio,
# ~1.0-1.2 for string/array-heavy data) so we do not OOM on the combine step.
# (A streaming k-way combine would remove this 2x and let the factor drop.)
#
# We fold the container RAM back into a ceiling on a merge's *combined budget-unit
# size*, so all selection arithmetic stays in the one budget unit. Override the
# container size for non-16 GB deployments via OPTERYX_COMPACTION_RAM_MB.
CONTAINER_RAM_MB = int(os.environ.get("OPTERYX_COMPACTION_RAM_MB", 16 * 1024))
CONTAINER_RAM_BYTES = CONTAINER_RAM_MB * 1024 * 1024
RUNTIME_WARMUP_BYTES = 768 * 1024 * 1024   # native lib/arena/threadpool floor (~measured)
PEAK_RAM_PER_BUDGET_BYTE = 2.0             # combine transient dominates (~2x combined budget-unit)
RAM_SAFETY_FRACTION = 0.85                 # headroom for Python/GC/other allocations
# Largest combined (uncompressed/budget-unit) input a single merge may hold.
# Never gated below one TARGET, so a legitimate single-target merge always fits.
MAX_SELECTED_BUDGET_BYTES = int(
    max(
        TARGET_SIZE_BYTES,
        (CONTAINER_RAM_BYTES * RAM_SAFETY_FRACTION - RUNTIME_WARMUP_BYTES)
        / PEAK_RAM_PER_BUDGET_BYTE,
    )
)
# Deprecated name, retained for external references (scripts/compaction_quick_ref.py).
# Now the byte-aware ceiling rather than a flat 4.3 GB.
MAX_MEMORY_BYTES = MAX_SELECTED_BUDGET_BYTES

# --- Decluster (rule 3) combined-input cap -------------------------------------
#
# Declustering runs on the STREAMING executor, whose peak is bounded by one
# window (~ROW_GROUP_HARD_CAP_ROWS rows) regardless of total merge size or file
# count - that is the whole reason the streaming writer was built. So decluster
# must NOT be bound by the hold-everything RAM gate (MAX_SELECTED_BUDGET_BYTES):
# that would refuse the motivating case, two OVERLAPPING ~4 GB (target-sized)
# files, which combine to 8 GB and split back into two disjoint ~4 GB files.
#
# Instead the cap bounds WORK PER PASS (bytes rewritten in one snapshot), not
# memory: it keeps a single decluster op from rewriting the whole dataset at
# once (resumability, and less contention with a live compactor). A larger
# overlapping cluster is declustered a chunk at a time and converges over passes.
# At 3x target a pass declusters up to ~three target-sized files into ~three
# disjoint outputs. Tunable; raising it only trades bigger snapshots for fewer
# passes, never memory (streaming is window-bounded).
DECLUSTER_MAX_COMBINED_BYTES = 3 * TARGET_SIZE_BYTES

# --- Three-pass streaming execution --------------------------------------------
#
# The hold-everything path above (read all inputs -> Morsel.combine -> sort ->
# per-slice take -> write) needs the whole merge resident, gated by
# MAX_SELECTED_BUDGET_BYTES. That gate rejects real merges of already-target-sized
# files (measured: two ~4GB github.events files -> 14.9GB real peak, over budget)
# even though the merge is small relative to available disk/storage - the limit
# is RAM, not data size.
#
# The streaming path removes that ceiling by never holding more than ~one row
# group's worth of data (across all input files contributing to it) at once:
#   pass 1: project the sort column ONLY from all candidate files, combine
#           (small - one column), native sort. This gives the EXACT global
#           sorted key sequence, not an estimate.
#   pass 2+: walk that sorted sequence in ~ROW_GROUP_TARGET_ROWS windows, snapped
#           to distinct-value edges (never split a run of equal keys across a
#           predicate boundary - predicates match by VALUE, not row position).
#           For each window, predicate-read [lo, hi) from every candidate file
#           (row-group pruned), combine + sort just that window, and
#           write_row_group() it before moving on. Peak is bounded by one
#           window's real size, independent of total merge size - true
#           larger-than-memory handling, at the cost of re-reading each
#           candidate file once per window it contributes to (read
#           amplification, paid deliberately to avoid write amplification: see
#           the accretive-merge measurement showing a ~21x total-bytes-rewritten
#           blowup from repeated whole-file rewrites).
#
# Two cases a value predicate cannot express, both handled by falling back to
# row-group-native accumulation (stream row groups, slice by row count instead
# of by value, so the ROW_GROUP_HARD_CAP_ROWS cap is never exceeded):
#   * NULLs in the sort key never match any value predicate (SQL three-valued
#     logic - confirmed empirically: rugo has no IS NULL predicate op, and
#     `col < x` matches zero null rows). Nulls are placed at position 0 (the
#     start of the first output file) by policy, extracted via draken's own
#     is_null()+filter_mask() on the (small, per-row-group) source data.
#   * A single value with more rows than the hard cap ("hot" value) can't be
#     sliced further by value (all matching rows look identical to a
#     predicate). Its rows are pulled via an exact `= value` predicate (which
#     already row-filters correctly) and flushed every ROW_GROUP_TARGET_ROWS
#     rows via plain row-count slicing.
#
# Used for any combine-split plan with a resolvable sort column. (An earlier
# rugo predicate-read filter_mask bug on ARRAY columns forced array datasets
# onto hold-everything; that is fixed as of rugo 0.4.17, so no schema
# restriction remains.) On any failure it falls back to hold-everything.
ROW_GROUP_TARGET_ROWS = 256_000
ROW_GROUP_HARD_CAP_ROWS = 272_000


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

        # Resolve the sort key to a canonical shape up front. ``sort_orders`` is
        # stored either as positional ints or Iceberg-style dicts; see
        # ``normalize_sort_order``. ``self.sort_order`` is
        # ``{"name", "field_id", "index", "ascending"}`` or None.
        self.sort_order = None
        if self.strategy == "performance":
            self.sort_order = normalize_sort_order(
                getattr(dataset.metadata, "sort_orders", [])
            )
            if self.sort_order is None:
                # Performance mode needs a usable sort key; fall back to brute.
                self.strategy = "brute"
                self.decision = "no-sort"

    def compact(self, dry_run: bool = False, rule: Optional[str] = None) -> Optional[Snapshot]:
        """
        Perform ONE compaction pass: a single read -> select -> execute ->
        commit cycle, same critical-section shape as before rule A/B existed.

        Rule A (brute merge of sub-512MB files) and rule B (sort-aware merge
        of files over 500MB, toward the 4GB target) are independent - see
        SORT_AWARE_FLOOR_BYTES for why their pools deliberately overlap. To
        attempt both in one cron tick, call compact() twice IN SERIES -
        ``compact(rule="brute")`` then ``compact(rule="sort_aware")`` - rather
        than chaining them inside a single call. Each call re-reads the
        manifest fresh at its own start and commits independently, so neither
        call's read-to-write window is any longer than a single operation's;
        chaining them in one call would double that window and raise the odds
        of losing a concurrent writer's commit (``save_dataset_metadata`` has
        no compare-and-swap - a later write unconditionally clobbers an
        earlier one). Calling both in series bounds each commit's exposure to
        one operation, same as a single-rule pass always had, and a writer
        that commits between the two calls is picked up by the second call
        instead of silently overwritten by it.

        Args:
            dry_run: If True, return the plan found without executing it
            rule: "brute", "sort_aware", or None. None tries brute, falling
                  back to sort_aware, for a caller that only wants a single
                  shot - it is NOT a substitute for calling both explicitly
                  when both should be attempted this tick.

        Returns:
            New Snapshot if compaction was performed, None if nothing to compact
            (or the plan dict, if dry_run).
        """
        current_snapshot = self.dataset.metadata.current_snapshot()
        if not current_snapshot or not current_snapshot.manifest_list:
            return None

        entries = self._read_manifest(current_snapshot.manifest_list)
        if not entries:
            return None

        if self.strategy == "brute":
            compaction_plan = self._select_brute_compaction(entries)
        elif rule == "brute":
            compaction_plan = self._select_brute_merge(entries)
        elif rule == "sort_aware":
            compaction_plan = self._select_sort_aware_merge(entries)
        elif rule is not None:
            raise ValueError(f"rule must be 'brute', 'sort_aware', or None; got {rule!r}")
        else:
            compaction_plan = self._select_brute_merge(entries) or self._select_sort_aware_merge(
                entries
            )

        if not compaction_plan:
            return None
        if dry_run:
            return compaction_plan
        return self._execute_compaction(entries, compaction_plan)

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
                if total_size + entry_size <= MAX_SELECTED_BUDGET_BYTES:
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

    def _resolve_sort_column(self, sort_order: dict, columns):
        """Resolve a canonical sort key against schema ``columns``.

        Precedence: field_id → name → positional index. ``columns`` entries may
        be objects with ``.name``/``.id`` or dicts with ``"name"``/``"id"``.
        Returns ``(column_name, field_id, index)`` where ``index`` is the
        column's schema position (used to read positional min/max stats when a
        manifest entry carries no field_ids). ``column_name`` is None when the
        key cannot be resolved (caller falls back to brute).
        """

        def col_name(c):
            return getattr(c, "name", None) or (c.get("name") if isinstance(c, dict) else None)

        def col_id(c):
            cid = getattr(c, "id", None)
            if cid is None and isinstance(c, dict):
                cid = c.get("id")
            return cid

        target_fid = sort_order.get("field_id")
        target_name = sort_order.get("name")
        target_index = sort_order.get("index")

        sort_index = None
        if target_fid is not None:
            sort_index = next((i for i, c in enumerate(columns) if col_id(c) == target_fid), None)
        if sort_index is None and target_name is not None:
            sort_index = next((i for i, c in enumerate(columns) if col_name(c) == target_name), None)
        if sort_index is None and target_index is not None and 0 <= target_index < len(columns):
            sort_index = target_index

        if sort_index is None:
            return None, None, None
        sort_col = columns[sort_index]
        return col_name(sort_col), col_id(sort_col), sort_index

    def _resolve_sort_columns_for_entries(self, entries: List[dict]):
        """Shared prep for both rule selectors: resolve the sort column against
        the dataset's stored schema. Returns (sort_column_name, sort_field_id,
        sort_index), all None if it can't be resolved (caller falls back to
        brute for that rule).
        """
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

        if not columns or self.sort_order is None:
            return None, None, None

        return self._resolve_sort_column(self.sort_order, columns)

    def _select_brute_merge(self, entries: List[dict]) -> Optional[dict]:
        """Rule A (rules 2 & 4): two or more sub-floor (< MIN_FILE_SIZE_BYTES)
        files => BRUTE-force merge (no sort), bin-packed toward TARGET. Tiny
        scattered files hurt reads most and are cheapest to fix; sorting them
        is wasted until they graduate the floor.

        Independent of rule B - see ``_select_sort_aware_merge`` and
        ``compact()``, which attempt both every pass rather than picking one.
        """
        sort_column_name, _, _ = self._resolve_sort_columns_for_entries(entries)
        if not sort_column_name:
            # No usable sort key at all; the legacy brute strategy (no sort
            # column required) is the only thing that can make progress.
            return self._select_brute_compaction(entries)

        # No range stats needed here - brute merge doesn't sort, so files
        # without min/max still qualify.
        sub_floor = [
            e
            for e in entries
            if e.get("uncompressed_size_in_bytes", 0) < MIN_FILE_SIZE_BYTES
        ]
        return self._select_brute_consolidation(sub_floor, sort_column_name)

    def _select_sort_aware_merge(self, entries: List[dict], rng=None) -> Optional[dict]:
        """Rule B (rules 1 & 3): files over SORT_AWARE_FLOOR_BYTES (500 MB) -
        deliberately overlapping rule A's < 512 MB pool, see
        SORT_AWARE_FLOOR_BYTES - get sort-aware combine + split toward the 4GB
        TARGET. Two sub-checks, first applicable wins:

          1. An OVERLAPPING group of >= floor files => sort-aware combine-split
             that declusters it into disjoint key ranges.
          2. Consecutive, already-disjoint MEDIUM files (floor..MIN_SIZE_BYTES)
             => sort-aware bin-pack toward TARGET to reduce file count.

        A single file already over the 4.1 GB hard cap is NOT re-split on its
        own by this selector - being oversized alone is not sufficient reason
        to rewrite it; it's only touched if it participates in an overlapping
        group (sub-check 1) or a packable run (sub-check 2), same as any other
        file at/above the floor.

        Independent of rule A - see ``_select_brute_merge`` and ``compact()``.
        Returns None (not a brute fallback) when the sort column can't be
        resolved: without a sort key there is nothing sort-aware to do, and
        rule A already covers the no-sort-key dataset case.
        """
        sort_column_name, sort_field_id, sort_index = (
            self._resolve_sort_columns_for_entries(entries)
        )
        if not sort_column_name:
            return None

        big = [
            e
            for e in entries
            if e.get("uncompressed_size_in_bytes", 0) > SORT_AWARE_FLOOR_BYTES
        ]

        file_ranges = self._build_file_ranges(big, sort_field_id, sort_index)
        if not file_ranges:
            return None

        # Sub-check 1: decluster one overlapping group.
        plan = self._select_overlap_decluster(file_ranges, sort_column_name, rng=rng)
        if plan:
            return plan

        # Sub-check 2: bin-pack consecutive medium files toward target.
        return self._select_binpack(file_ranges, sort_column_name)

    def _build_file_ranges(self, entries, sort_field_id, sort_index):
        """Extract ``{entry, min, max, size}`` on the sort key for each entry
        that carries usable positional min/max stats.

        Parquet manifest entries expose per-column statistics as positional
        ``min_values``/``max_values`` lists aligned with ``field_ids`` (NOT
        iceberg-style ``lower_bounds``/``upper_bounds`` dicts). Resolve the sort
        column's slot by stable field-id when the entry carries field_ids,
        falling back to schema column position. Entries with no usable stats are
        skipped - they can't be reasoned about for overlap or key-ordered packing.
        """
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
            elif sort_index is not None and sort_index < len(min_values):
                idx = sort_index

            if idx is None or idx >= len(min_values) or idx >= len(max_values):
                continue

            min_val = min_values[idx]
            max_val = max_values[idx]
            if min_val is None or max_val is None:
                continue

            file_ranges.append(
                {
                    "entry": entry,
                    "min": min_val,
                    "max": max_val,
                    "size": entry.get("uncompressed_size_in_bytes", 0),
                }
            )
        return file_ranges

    def _select_brute_consolidation(self, sub_floor, sort_column_name) -> Optional[dict]:
        """Rules 2 & 4: whenever two or more sub-floor (< MIN_FILE_SIZE_BYTES)
        files exist, BRUTE-force merge them - concatenate, NO sort - bin-packing
        the smallest first toward TARGET_SIZE_BYTES (rule 1: never exceed target),
        one bin per ``compact()`` call.

        There is no volume threshold to wait for: a drip-fed dataset gets its
        handful of tiny files merged NOW (waiting is the small-files problem by
        another name). Sorting scattered tiny files is wasted work; the merged
        result is sort-aware declustered (tier 2) once it graduates past the
        floor. A single leftover sub-floor remainder is acceptable (rule 4): the
        merge only needs two files to make progress, and it accretes a sub-floor
        output over calls until it crosses the floor and freezes.

        Emits a ``combine`` (brute) plan; the executor's ``combine`` path never
        sorts and produces one output, so no ``morsel_sort`` runs on this path.
        """
        if len(sub_floor) < 2:
            return None  # need at least two sub-floor files to combine

        ordered = sorted(
            sub_floor, key=lambda e: e.get("uncompressed_size_in_bytes", 0)
        )
        selected = []
        total = 0
        for e in ordered:
            size = e.get("uncompressed_size_in_bytes", 0)
            if selected and total + size > TARGET_SIZE_BYTES:
                break
            selected.append(e)
            total += size

        if len(selected) < 2:
            return None
        return {
            "type": "combine",
            "mode": "brute",
            "files": selected,
            "reason": "small-file-brute",
            "sort_column": sort_column_name,
        }

    def _select_overlap_decluster(self, file_ranges, sort_column_name, rng=None) -> Optional[dict]:
        """Rule 3: pick a random file, grow it into an overlapping group, emit
        a sort-aware ``combine-split``. The (streaming) executor sorts the
        merged rows and splits them into k = ceil(combined / TARGET) disjoint
        key ranges: k == 1 -> one file whose disjoint ranges are sorted ROW
        GROUPS only (never a new file under target); k > 1 -> that many
        disjoint-range FILES.

        ``rng``: injectable ``random.Random`` (defaults to the ``random``
        module). Tests pass a seeded instance, or a stub with a fixed
        ``choice()``, for reproducibility; production leaves it unset.

        Algorithm:
          1. Pick one file at random from ``file_ranges``.
          2. If it's a single-value file (min == max), there's no reordering
             benefit to chase from it alone - stop, no plan this call.
          3. Otherwise repeatedly add whichever remaining file overlaps the
             group's current combined range the MOST (a strict test: touching
             at a shared boundary value, the artifact of a prior split on a
             tie, does not count - ``overlap <= 0`` is excluded), until either
             nothing overlaps at all, or the next file would push the
             combined size over DECLUSTER_MAX_COMBINED_BYTES (a work-per-pass
             bound, not a memory bound - the streaming executor is
             window-bounded, so two overlapping ~4 GB files fully fits:
             combined 8 GB -> two disjoint ~4 GB outputs).

        Picking the STARTING file at random (rather than always scanning from
        the smallest sort-key value) matters when one overlap region can
        never fully resolve in a single pass (e.g. a Zipfian-popular
        sort-key value whose boundary file structurally always overlaps its
        pure siblings): a deterministic scan-from-the-start would land on
        that same unresolvable region every single call and starve every
        other overlapping region in the dataset of a turn forever - observed
        live on opteryx.test.pypi, where a small, fully-resolvable cluster
        and a much larger one both sat completely untouched the entire
        session because an earlier, structurally-unresolvable cluster kept
        winning every call.
        """
        if not file_ranges:
            return None

        rng = rng or random
        seed = rng.choice(file_ranges)

        if seed["min"] == seed["max"]:
            return None  # single-value file: every other file at that same value adds nothing

        group = [seed]
        total = seed["size"]
        remaining = [fr for fr in file_ranges if fr is not seed]

        def _overlap_amount(fr, group_min, group_max):
            """How much ``fr`` overlaps [group_min, group_max]; <= 0 means it
            doesn't.

            ``fr`` degenerate (min == max, a single value with more rows than
            fit in one file - see the seed check above): min/max are REAL
            observed values, not synthetic split edges, so touching either
            edge of the group's range IS genuine overlap (the group provably
            contains that same value) - inclusive containment on both sides.

            ``fr`` non-degenerate: the standard strict interval-overlap test.
            A boundary shared with a non-degenerate neighbour is the artifact
            of a clean prior split (see test_touching_boundary_is_not_overlap),
            not real overlap - without excluding it, declustering would never
            converge, endlessly re-merging its own disjoint outputs.
            """
            if fr["min"] == fr["max"]:
                if group_min <= fr["min"] <= group_max:
                    return group_max - group_min  # always > 0: the seed is never degenerate
                return -1
            lo = max(fr["min"], group_min)
            hi = min(fr["max"], group_max)
            return hi - lo

        while remaining:
            try:
                group_min = min(fr["min"] for fr in group)
                group_max = max(fr["max"] for fr in group)
                best = max(remaining, key=lambda fr: _overlap_amount(fr, group_min, group_max))
                best_overlap = _overlap_amount(best, group_min, group_max)
            except TypeError:
                break  # sort-key values not mutually comparable

            if best_overlap <= 0:
                break  # nothing left genuinely overlaps the group

            if total + best["size"] > DECLUSTER_MAX_COMBINED_BYTES:
                remaining.remove(best)
                continue  # this one doesn't fit; a smaller, less-overlapping file still might

            group.append(best)
            total += best["size"]
            remaining.remove(best)

        if len(group) < 2:
            return None

        combined = sum(fr["size"] for fr in group)
        k = max(1, -(-combined // TARGET_SIZE_BYTES))
        return {
            "type": "combine-split",
            "mode": "sort-aware",
            "files": [fr["entry"] for fr in group],
            "reason": "overlap-decluster",
            "sort_column": sort_column_name,
            "expected_outputs": k,
        }

    def _select_binpack(self, file_ranges, sort_column_name) -> Optional[dict]:
        """Rule 1: reduce file count by packing CONSECUTIVE (in sort-key order),
        already-disjoint MEDIUM files toward TARGET. Only unsettled files (below
        MIN_SIZE_BYTES, the lower edge of the acceptable band) are packed; a file
        already near target is left alone so packing converges.

        Packing only *consecutive* files keeps the merged key range tight
        (``[first.min, last.max]``), so the result stays disjoint from its
        neighbours and does NOT manufacture new overlap for tier 2 to chase.
        Combined stays <= TARGET, so the sort-aware merge yields a single sorted
        output - no file over target (rule 1), and its disjoint ranges are
        expressed as sorted row groups.
        """
        try:
            ordered = sorted(file_ranges, key=lambda fr: fr["min"])
        except TypeError:
            return None

        i = 0
        m = len(ordered)
        while i < m:
            if ordered[i]["size"] >= MIN_SIZE_BYTES:
                i += 1
                continue  # already settled near target - leave alone
            group = [ordered[i]]
            total = ordered[i]["size"]
            j = i + 1
            while j < m:
                fr = ordered[j]
                if fr["size"] >= MIN_SIZE_BYTES:
                    break  # settled file breaks the packable run
                if total + fr["size"] > TARGET_SIZE_BYTES:
                    break
                group.append(fr)
                total += fr["size"]
                j += 1

            if len(group) >= 2:
                return {
                    "type": "combine-split",
                    "mode": "sort-aware",
                    "files": [fr["entry"] for fr in group],
                    "reason": "bin-pack",
                    "sort_column": sort_column_name,
                    "expected_outputs": 1,
                }
            i += 1
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
        # ``mode`` threads the merge kind through the plan: "brute" (rule 2 -
        # concatenate, NO sort) vs "sort-aware" (rules 1 & 3 - sort + disjoint
        # split). Brute plans are always type "combine" (single output, no
        # streaming, no sort), but we gate on ``mode`` too so a brute plan can
        # never trigger a sort regardless of type.
        mode = plan.get("mode", "sort-aware")
        sort_aware = mode != "brute"

        # Prefer the streaming path for combine-split plans: it holds only ~one
        # row group at a time (see the module comment above ROW_GROUP_TARGET_ROWS)
        # so it has no ceiling on combined input size, whereas hold-everything is
        # RAM-bound. Arrays used to be excluded (a rugo predicate-read filter_mask
        # bug on ARRAY columns) but that is fixed as of rugo 0.4.17, so streaming
        # is now the path for every combine-split with a resolvable sort column.
        if sort_aware and sort_column and plan_type == "combine-split":
            streamed = self._execute_compaction_streaming(all_entries, plan)
            if streamed is not None:
                return streamed
            # Streaming declined or failed (e.g. couldn't resolve a usable sort
            # column at execution time). Falling back to hold-everything is only
            # safe when the combined input fits the RAM gate; a decluster group
            # can now be far larger than RAM (that is the point of streaming), so
            # for an oversized merge we ABORT this compaction - leaving the data
            # intact for a later pass - rather than OOM by reading it all in.
            combined_budget = sum(
                e.get("uncompressed_size_in_bytes", 0) for e in files_to_compact
            )
            if combined_budget > MAX_SELECTED_BUDGET_BYTES:
                return None
            # Small enough to hold in memory - fall through to hold-everything
            # rather than losing the compaction outright.

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

        # Capture input stats now and release the input morsels. `combine` has
        # produced its own concatenated buffers, so the per-file inputs are dead
        # weight; freeing them here keeps peak RSS at ~one copy of the data
        # instead of inputs + combined. These feed the deleted-* snapshot stats.
        input_records = sum(t.num_rows for t in tables)
        input_data_size = sum(t.nbytes for t in tables)
        del tables

        # Sort if performance mode. Prefer draken's native sort (rugo >=0.4.16):
        # it computes the permutation in C (radix/stable) with no Python objects.
        # The old path materialised the whole sort column via to_pylist() and
        # sorted it in Python - a ~9 GB allocation at github.events scale.
        #
        # We keep only the permutation here and DO NOT apply a full take: a
        # `combined.take(perm)` would materialise an entire second sorted copy
        # (~2x peak). Instead each output slice is gathered on demand below via
        # `combined.take(perm[lo:hi])`, so peak stays at ~combined + one output.
        # The outcome is recorded in the snapshot (sort_status) - never silent.
        sort_status = "skipped"
        perm = None
        if sort_aware and sort_column and plan_type == "combine-split":
            ascending = True
            if isinstance(self.sort_order, dict):
                ascending = self.sort_order.get("ascending", True)
            try:
                from draken.morsels.sort import morsel_sort

                perm = morsel_sort(combined, [sort_column], [ascending])
                sort_status = "native"
            except ImportError:
                try:
                    sort_values = combined.column(sort_column.encode("utf-8")).to_pylist()
                    perm = sorted(
                        range(len(sort_values)),
                        key=lambda i: (sort_values[i] is None, sort_values[i]),
                    )
                    sort_status = "python-fallback"
                except Exception:
                    perm = None
                    sort_status = "failed"
            except Exception:
                # Native sort raised on this data; leave output unsorted but
                # record it rather than silently degrading.
                perm = None
                sort_status = "failed"

        # Determine the output row ranges. ``combined`` is emitted in sorted
        # order (via ``perm``), so slicing at row offsets yields output files with
        # DISJOINT key ranges (bar a shared boundary value on ties) - this is what
        # makes repeated compaction converge a scattered sort key instead of
        # merging it into ever-wider union ranges.
        #
        # Choose the number of outputs k in the budget unit (uncompressed size),
        # the SAME unit TARGET_SIZE_BYTES is expressed in. Do NOT derive k from
        # table.nbytes: draken's in-memory nbytes and the manifest's
        # uncompressed_size_in_bytes differ several-fold.
        n = combined.num_rows
        k = max(1, -(-total_size // TARGET_SIZE_BYTES))  # ceil(total / target)
        split_ok = plan_type in ("split", "combine-split") and k > 1 and n >= k
        ranges = self._split_ranges(n, k if split_ok else 1)

        # Write new files and build manifest entries. Each output is materialised
        # on demand and dropped before the next, so peak stays at ~combined + one
        # output rather than combined + a full sorted copy + all outputs.
        from rugo.parquet import write_parquet

        from ..iops.fileio import WRITE_PARQUET_OPTIONS

        new_entries = []
        snapshot_id = int(time.time() * 1000)

        for lo, hi in ranges:
            if perm is not None:
                # gather this slice's rows in sorted order (per-slice take)
                part = combined.take(list(perm[lo:hi]))
            elif len(ranges) == 1:
                part = combined  # single unsorted output: no copy needed
            else:
                part = combined.slice(lo, hi - lo)

            # Generate collision-resistant file path using nanosecond precision timestamp and node id
            file_name = f"{time.time_ns():x}-{_NODE}.parquet"
            file_path = os.path.join(self.dataset.metadata.location, "data", file_name)

            # Write parquet file and upload (so we can reuse bytes)
            try:
                pdata = write_parquet(part, **WRITE_PARQUET_OPTIONS)
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
                orig_morsel=part,
                field_id_by_name=self.dataset._field_id_by_name(),
            )
            entry_dict = self._to_dict(entry_obj)
            new_entries.append(entry_dict)

            # Drop this output before materialising the next.
            if part is not combined:
                del part
            del pdata

        return self._finalize_compaction_snapshot(
            all_entries, files_to_compact, new_entries, snapshot_id,
            input_records, input_data_size, sort_status,
        )

    def _finalize_compaction_snapshot(
        self,
        all_entries: List[dict],
        files_to_compact: List[dict],
        new_entries: List[dict],
        snapshot_id: int,
        input_records: int,
        input_data_size: int,
        sort_status: str,
    ) -> Optional[Snapshot]:
        """Shared tail for both execution paths (hold-everything and streaming):
        prune/validate the surviving old entries, write the new manifest, compute
        summary stats, and commit the snapshot. Neither execution path needs to
        know about manifest/Firestore mechanics - they just produce
        ``new_entries`` (already-built manifest dicts for the files they wrote)
        and call this.
        """
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
            # Input stats captured before the input morsels were freed.
            deleted_records = input_records
            deleted_data_size = input_data_size

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
                    "compaction-sort": sort_status,
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

    # --- Streaming execution (see the module comment above ROW_GROUP_TARGET_ROWS) ---

    def _read_sort_column_combined(self, files_to_compact: List[dict], sort_column: str):
        """Pass 1: project ONLY the sort column from every candidate file,
        streaming row-group by row-group, and combine into one small morsel.
        Cheap regardless of total merge size (measured: 60MB for 7.9M rows on
        one column, vs 10GB+ for the same rows with all columns). Returns None
        if any file can't be read (caller falls back to hold-everything).
        """
        from draken.morsels.morsel import Morsel
        from rugo.parquet import read_parquet

        parts = []
        for entry in files_to_compact:
            file_path = entry.get("file_path")
            if not file_path:
                return None
            try:
                with self.dataset.io.new_input(file_path).open() as f:
                    data = f.read()
                with read_parquet(bytes(data), columns=[sort_column]) as reader:
                    parts.extend(reader)
            except Exception:
                return None
        if not parts:
            return None
        return Morsel.combine(parts) if len(parts) > 1 else parts[0]

    def _compute_chunk_groups(self, sorted_keys: list, ascending: bool) -> list:
        """Turn the exact, fully sorted key sequence into an ordered list of
        chunk-group descriptors for pass 2+. Each group is one of:

          {"type": "nulls", "count": N}            - always first, if N>0
          {"type": "range", "lo": v, "hi": v|None}  - half-open [lo,hi); hi=None
                                                       for the final group (no
                                                       upper bound needed)
          {"type": "hot", "value": v}               - a single value whose run
                                                       alone exceeds the hard cap

        Boundaries snap to distinct-value edges so a predicate range never
        needs to split a run of equal keys (predicates match by value, not row
        position). Target ROW_GROUP_TARGET_ROWS per group; hard cap
        ROW_GROUP_HARD_CAP_ROWS - if extending to the next distinct value would
        exceed the cap, cut earlier instead (undershoot, never overshoot),
        except for a single run that alone exceeds the cap, which becomes its
        own "hot" group (pass 2+ falls back to row-count slicing for that one,
        since no value-based predicate can split it further).
        """
        n = len(sorted_keys)
        # draken's morsel_sort is NULLS-FIRST on ascending, NULLS-LAST on
        # descending; nulls cluster entirely to one end either way. Bring them
        # to logical position 0 regardless of sort direction (project policy,
        # not draken's own semantics).
        if ascending:
            null_count = 0
            while null_count < n and sorted_keys[null_count] is None:
                null_count += 1
            values = sorted_keys[null_count:]
        else:
            null_count = 0
            while null_count < n and sorted_keys[n - 1 - null_count] is None:
                null_count += 1
            values = sorted_keys[: n - null_count] if null_count else sorted_keys

        groups = []
        if null_count > 0:
            groups.append({"type": "nulls", "count": null_count})

        m = len(values)
        i = 0
        while i < m:
            run_end = i + 1
            while run_end < m and values[run_end] == values[i]:
                run_end += 1
            run_len = run_end - i
            if run_len > ROW_GROUP_HARD_CAP_ROWS:
                # A single value spans more rows than one chunk can safely
                # hold, and predicates can't slice within it - hand the whole
                # run to the row-count-slicing fallback.
                groups.append({"type": "hot", "value": values[i]})
                i = run_end
                continue

            # Normal case: extend from i, snapping to run boundaries, until
            # adding the next run would cross the hard cap.
            j = run_end
            count = run_len
            while j < m:
                next_end = j + 1
                while next_end < m and values[next_end] == values[j]:
                    next_end += 1
                next_run_len = next_end - j
                if count + next_run_len > ROW_GROUP_HARD_CAP_ROWS:
                    break
                j = next_end
                count += next_run_len
            hi = values[j] if j < m else None
            groups.append({"type": "range", "lo": values[i], "hi": hi})
            i = j

        return groups

    def _entry_field_idx(self, entry: dict, sort_field_id, sort_index):
        """Resolve the sort column's positional index within one manifest
        entry's stat lists (field-id keyed when available, else schema
        position) - the same precedence used during selection."""
        field_ids = entry.get("field_ids") or []
        if sort_field_id is not None and sort_field_id in field_ids:
            return field_ids.index(sort_field_id)
        if sort_index is not None and sort_index < len(entry.get("min_values") or []):
            return sort_index
        return None

    def _iter_group_morsels(
        self, files_to_compact, sort_column, ascending, group, null_bearing_paths
    ):
        """Yield one or more sorted, <=ROW_GROUP_HARD_CAP_ROWS-row Morsels for
        a single chunk group. Dispatches on group type:

          range - one predicate read across all candidate files (row-group
                  pruned), combine, native sort. Already <=hard-cap by
                  construction (see _compute_chunk_groups), so always exactly
                  one output morsel.
          hot   - an exact `= value` predicate read (already row-filtered,
                  no further per-row filtering needed - all matching rows
                  share the one value, so any relative order among them is
                  fine), flushed every ROW_GROUP_TARGET_ROWS rows by plain
                  row-count slicing across possibly many output morsels.
          nulls - no predicate can express IS NULL (confirmed: rugo raises on
                  `col = None`, and value predicates never match null rows -
                  SQL three-valued logic). Stream row groups from files known
                  to carry nulls in this column, extract just the null rows via
                  draken's own is_null()+filter_mask() per row group (safe:
                  eligibility already excludes ARRAY columns), accumulate and
                  flush at the same row-count cap.
        """
        from draken.morsels.morsel import Morsel
        from draken.interop.vector_sequence import vector_from_sequence
        from rugo.parquet import read_parquet

        def sort_and_yield(morsel):
            if morsel is None or morsel.num_rows == 0:
                return
            from draken.morsels.sort import morsel_sort

            perm = morsel_sort(morsel, [sort_column], [ascending])
            yield morsel.take(list(perm))

        if group["type"] == "range":
            parts = []
            lo, hi = group["lo"], group["hi"]
            preds = [(sort_column, ">=", lo)]
            if hi is not None:
                preds.append((sort_column, "<", hi))
            for entry in files_to_compact:
                file_path = entry.get("file_path")
                with self.dataset.io.new_input(file_path).open() as f:
                    data = f.read()
                with read_parquet(bytes(data), predicates=preds) as reader:
                    parts.extend(reader)
            if parts:
                combined = Morsel.combine(parts) if len(parts) > 1 else parts[0]
                yield from sort_and_yield(combined)
            return

        if group["type"] == "hot":
            value = group["value"]
            preds = [(sort_column, "=", value)]
            acc = []
            acc_rows = 0
            for entry in files_to_compact:
                file_path = entry.get("file_path")
                with self.dataset.io.new_input(file_path).open() as f:
                    data = f.read()
                with read_parquet(bytes(data), predicates=preds) as reader:
                    for rg in reader:
                        acc.append(rg)
                        acc_rows += rg.num_rows
                        while acc_rows >= ROW_GROUP_TARGET_ROWS:
                            combined = Morsel.combine(acc) if len(acc) > 1 else acc[0]
                            head = combined.slice(0, ROW_GROUP_TARGET_ROWS)
                            tail_len = combined.num_rows - ROW_GROUP_TARGET_ROWS
                            yield head
                            acc = [combined.slice(ROW_GROUP_TARGET_ROWS, tail_len)] if tail_len else []
                            acc_rows = tail_len
            if acc_rows:
                yield Morsel.combine(acc) if len(acc) > 1 else acc[0]
            return

        if group["type"] == "nulls":
            acc = []
            acc_rows = 0
            for entry in files_to_compact:
                file_path = entry.get("file_path")
                if file_path not in null_bearing_paths:
                    continue
                with self.dataset.io.new_input(file_path).open() as f:
                    data = f.read()
                with read_parquet(bytes(data)) as reader:
                    for rg in reader:
                        col = rg.column(sort_column)
                        mask_bytes = col.is_null()
                        if not any(mask_bytes):
                            continue
                        mask = vector_from_sequence([bool(b) for b in mask_bytes], dtype="BOOL")
                        nulls_only = rg.filter_mask(mask)
                        if nulls_only.num_rows == 0:
                            continue
                        acc.append(nulls_only)
                        acc_rows += nulls_only.num_rows
                        while acc_rows >= ROW_GROUP_TARGET_ROWS:
                            combined = Morsel.combine(acc) if len(acc) > 1 else acc[0]
                            head = combined.slice(0, ROW_GROUP_TARGET_ROWS)
                            tail_len = combined.num_rows - ROW_GROUP_TARGET_ROWS
                            yield head
                            acc = [combined.slice(ROW_GROUP_TARGET_ROWS, tail_len)] if tail_len else []
                            acc_rows = tail_len
            if acc_rows:
                yield Morsel.combine(acc) if len(acc) > 1 else acc[0]
            return

    def _execute_compaction_streaming(self, all_entries: List[dict], plan: dict) -> Optional[Snapshot]:
        """Three-pass streaming execution: project+sort the key column, derive
        chunk groups, stream row-group-sized sorted chunks per group, and roll
        them into target-sized output files via rugo's streaming writer
        (`write_parquet_stream`, undocumented as of rugo 0.4.17 - flagged here
        so it's easy to find when regression coverage lands upstream). Peak
        memory is bounded by one chunk's size, independent of merge size.

        Returns None (never raises) if anything about this path can't proceed,
        so the caller can fall back to hold-everything.
        """
        plan_type = plan["type"]
        files_to_compact = plan["files"]
        sort_column = plan.get("sort_column")
        if not sort_column or plan_type != "combine-split":
            return None

        ascending = True
        if isinstance(self.sort_order, dict):
            ascending = self.sort_order.get("ascending", True)

        # Pass 1: exact global sort of the key column alone.
        key_morsel = self._read_sort_column_combined(files_to_compact, sort_column)
        if key_morsel is None or key_morsel.num_rows == 0:
            return None
        try:
            from draken.morsels.sort import morsel_sort

            perm = morsel_sort(key_morsel, [sort_column], [ascending])
        except Exception:
            return None
        sorted_keys = key_morsel.take(list(perm)).column(sort_column).to_pylist()
        n = len(sorted_keys)
        del key_morsel, perm

        groups = self._compute_chunk_groups(sorted_keys, ascending)
        del sorted_keys
        if not groups:
            return None

        # Which candidate files actually carry nulls in the sort column -
        # lets the "nulls" group skip files that can't contribute (common
        # case: most files have none). ``plan`` only carries the sort
        # column's NAME, not its field_id/schema-index, so re-resolve those
        # against the schema (self.sort_order is always a dict here: a
        # combine-split plan only exists when performance-mode selection
        # already normalized and resolved it).
        try:
            columns = self.dataset.schema().columns
            _, sort_field_id, sort_index = self._resolve_sort_column(self.sort_order, columns)
        except Exception:
            sort_field_id, sort_index = None, None
        null_bearing_paths = set()
        for entry in files_to_compact:
            idx = self._entry_field_idx(entry, sort_field_id, sort_index)
            null_counts = entry.get("null_counts") or []
            if idx is not None and idx < len(null_counts) and null_counts[idx]:
                null_bearing_paths.add(entry.get("file_path"))
            elif idx is None:
                # Can't confirm zero nulls for this file - check it rather
                # than risk silently dropping null rows.
                null_bearing_paths.add(entry.get("file_path"))

        total_size = sum(e.get("uncompressed_size_in_bytes", 0) for e in files_to_compact)
        k = max(1, -(-total_size // TARGET_SIZE_BYTES))
        target_rows_per_file = max(1, -(-n // k))

        def master_chunks():
            for group in groups:
                yield from self._iter_group_morsels(
                    files_to_compact, sort_column, ascending, group, null_bearing_paths
                )

        def file_chunk_source(gen, target_rows):
            rows = 0
            for chunk in gen:
                yield chunk
                rows += chunk.num_rows
                if rows >= target_rows:
                    return

        def rechain(first, rest):
            yield first
            yield from rest

        from rugo.parquet import write_parquet_stream

        from ..iops.fileio import WRITE_PARQUET_OPTIONS

        gen = master_chunks()
        new_entries = []
        snapshot_id = int(time.time() * 1000)
        try:
            while True:
                sub = file_chunk_source(gen, target_rows_per_file)
                first = next(sub, None)
                if first is None:
                    break
                file_name = f"{time.time_ns():x}-{_NODE}.parquet"
                file_path = os.path.join(self.dataset.metadata.location, "data", file_name)
                out = self.dataset.io.new_output(file_path).create()
                try:
                    write_parquet_stream(rechain(first, sub), out.write, **WRITE_PARQUET_OPTIONS)
                finally:
                    out.close()

                # Re-read the just-written file to compute manifest stats. This
                # is the one place the streaming path re-materialises data -
                # the COMPRESSED bytes of one output file (a few hundred MB at
                # target size, not the multi-GB uncompressed dataset it
                # replaces) - reusing build_parquet_manifest_entry_from_bytes's
                # existing, correct, row-group-streaming stat computation
                # rather than hand-rolling a parallel (and error-prone)
                # incremental histogram/sketch merge.
                with self.dataset.io.new_input(file_path).open() as f:
                    written = f.read()
                written = bytes(written)
                entry_obj = build_parquet_manifest_entry_from_bytes(
                    written,
                    file_path,
                    len(written),
                    field_id_by_name=self.dataset._field_id_by_name(),
                )
                new_entries.append(self._to_dict(entry_obj))
                del written
        except Exception:
            return None

        if not new_entries:
            return None

        input_records = n
        input_data_size = sum(e.get("uncompressed_size_in_bytes", 0) for e in files_to_compact)

        return self._finalize_compaction_snapshot(
            all_entries, files_to_compact, new_entries, snapshot_id,
            input_records, input_data_size, "native",
        )

    def _split_ranges(self, n: int, k: int) -> list:
        """The ``k`` (lo, hi) row ranges that partition ``n`` rows into near-equal
        contiguous slices. ceil step, so at most ``k`` ranges are produced. Used
        by both the per-slice output loop and _split_into_k so the two never
        diverge on boundary arithmetic."""
        if k <= 1 or n == 0:
            return [(0, n)]
        step = -(-n // k)
        return [(off, min(off + step, n)) for off in range(0, n, step)]

    def _split_into_k(self, table, k: int) -> list:
        """Split a (sorted) Morsel into ``k`` slices of near-equal row count.

        Slicing a key-sorted morsel at row offsets gives outputs with disjoint
        key ranges (adjacent slices may share a single value where a run of
        equal keys straddles a boundary). ``slice`` returns a view over the
        parent buffer, so this holds no extra copy.
        """
        n = table.num_rows
        if k <= 1 or n == 0:
            return [table]
        return [table.slice(lo, hi - lo) for lo, hi in self._split_ranges(n, k)]

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

    def _refresh_one_entry_from_data_file(self, entry: dict) -> dict:
        """Rebuild a single manifest entry from its data file.

        Same per-entry fallback contract as
        ``_refresh_manifest_from_data_files`` (below), split out so it can
        run in a worker thread: any failure (missing path, unreadable file,
        malformed rebuild) returns ``entry`` unchanged rather than raising,
        so one bad file can't take down the whole refresh.
        """
        file_path = entry.get("file_path")
        if not file_path:
            return entry

        try:
            io = self.dataset.io
            inp = io.new_input(file_path)
            with inp.open() as f:
                data = f.read()

            rebuilt_entry = build_parquet_manifest_entry_from_bytes(
                data, file_path, len(data), field_id_by_name=self.dataset._field_id_by_name()
            )

            entry_dict = self._to_dict(rebuilt_entry)
            if isinstance(entry_dict, dict):
                return entry_dict
            return entry  # Fall back to original if rebuild failed
        except Exception:
            return entry  # If we can't rebuild, keep the original entry

    def _refresh_manifest_from_data_files(self, all_entries: List[dict]) -> List[dict]:
        """
        Refresh entire manifest by reading all data files and rebuilding entries from scratch.

        Used as fallback when stats calculation fails. Rebuilds all manifest entries by reading
        the actual parquet files to ensure accuracy and correctness.

        Files are refreshed concurrently (see ``_REFRESH_MANIFEST_MAX_WORKERS``)
        since each file's read+stats is independent of every other -- this can
        be the whole dataset, so doing it one file at a time serially wastes
        wall-clock on network wait that overlapping threads reclaim.
        ``ThreadPoolExecutor.map`` preserves ``all_entries`` order in the
        result regardless of which file finishes first.

        Args:
            all_entries: All current manifest entries

        Returns:
            List of refreshed manifest entries
        """
        if not all_entries:
            return []

        max_workers = min(_REFRESH_MANIFEST_MAX_WORKERS, len(all_entries))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            return list(pool.map(self._refresh_one_entry_from_data_file, all_entries))

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
