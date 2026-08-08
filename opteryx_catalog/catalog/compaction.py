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
  6. Nothing commits unless the outputs hold exactly the rows the inputs did
     (``_row_counts_balance``) and no other writer committed while the merge was
     running (``_dataset_moved_under_us``). Every other failure path aborts the
     pass, removes the files it wrote, and records why in ``_last_error`` - the
     inputs are always left intact for a later pass.

``brute`` strategy is a separate, older fallback for datasets with no sort
order; it is unrelated to the above.
"""

from __future__ import annotations

import logging
import os
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from ..alerts import report as _alert
from ..exceptions import CompactionInvariantError
from .manifest import ParquetManifestEntry
from .manifest import build_parquet_manifest_entry_from_bytes
from .metadata import Snapshot

logger = logging.getLogger(__name__)

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


def entry_int(entry, key: str) -> int:
    """Read a numeric manifest column off an entry, treating SQL NULL as 0.

    ``write_parquet_manifest`` writes a fixed column set and fills any key an
    entry dict didn't carry with NULL, so a manifest row can come back with
    ``uncompressed_size_in_bytes``/``file_size_in_bytes``/``record_count`` set
    to ``None`` rather than merely absent. ``.get(key, 0)`` does NOT cover that
    - the key IS present, holding None - and every size comparison in the
    selectors then raised ``'<' not supported between instances of 'NoneType'
    and 'int'`` out of ``compact()``. A missing size is treated as 0, which
    makes the file a sub-floor merge candidate; merging rewrites it with real
    stats, so the dataset heals itself. Same ``int(x or 0)`` convention the
    dataset/expiration modules already use on these columns.
    """
    return int(entry.get(key) or 0)


def entry_size(entry) -> int:
    """Uncompressed (in-memory) size of a manifest entry, NULL-safe. See ``entry_int``."""
    return int(entry.get("uncompressed_size_in_bytes") or 0)


def normalize_sort_order(sort_orders) -> dict | None:
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
    except (AttributeError, KeyError, TypeError, ValueError):
        # Reading an arbitrarily-shaped sort-order document: a missing key, a
        # non-dict where a dict was expected, a direction that will not stringify.
        # Any of those means "no usable sort order", which callers handle by
        # falling back to a brute-force merge.
        return None
    return None


def resolve_sort_column(sort_order: dict, columns):
    """Resolve a canonical sort key (from ``normalize_sort_order``) against
    schema ``columns``.

    Precedence: field_id → name → positional index. ``columns`` entries may
    be objects with ``.name``/``.id`` or dicts with ``"name"``/``"id"``.
    Returns ``(column_name, field_id, index)`` where ``index`` is the
    column's schema position (used to read positional min/max stats when a
    manifest entry carries no field_ids). ``column_name`` is None when the
    key cannot be resolved (caller falls back to brute/unsorted).

    Shared by compaction (``DatasetCompactor._resolve_sort_column``) and
    write-time sorting (``SimpleDataset._write_table_and_build_entry``) so
    the two stay consistent about how a stored sort order maps to a column.
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
CONTAINER_RAM_MB = int(os.environ.get("OPTERYX_COMPACTION_RAM_MB") or 16 * 1024)
CONTAINER_RAM_BYTES = CONTAINER_RAM_MB * 1024 * 1024
RUNTIME_WARMUP_BYTES = 768 * 1024 * 1024  # native lib/arena/threadpool floor (~measured)
PEAK_RAM_PER_BUDGET_BYTE = 2.0  # combine transient dominates (~2x combined budget-unit)
RAM_SAFETY_FRACTION = 0.85  # headroom for Python/GC/other allocations
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
#           blowup from repeated whole-file rewrites). That re-reading is served
#           from a local cache (_SourceFileCache) and pruned against each file's
#           manifest min/max, so the amplification lands on local storage, not
#           on repeated object-store downloads.
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

# Sorted keys are consumed from pass 1 in chunks of this many rows rather than
# as one `to_pylist()` of the whole sequence: at github.events scale the full
# list is tens of millions of Python objects (gigabytes) on the very path whose
# purpose is bounded memory. See ``DatasetCompactor._iter_key_runs``.
KEY_SCAN_CHUNK_ROWS = 1_000_000

# How much of the candidate files' COMPRESSED bytes the source cache may hold in
# RAM before spilling the rest to local disk. See ``_SourceFileCache``.
SOURCE_CACHE_RAM_BYTES = (
    int(os.environ.get("OPTERYX_COMPACTION_SOURCE_CACHE_MB") or 2048) * 1024 * 1024
)


class _SourceFileCache:
    """Fetch each candidate file's bytes from object storage AT MOST ONCE per
    compaction, then serve them locally for every window that needs them.

    Streaming execution reads each candidate file once per chunk group it
    contributes to. Reading straight from ``io`` meant re-DOWNLOADING the whole
    file every time: a 3-file, ~24M-row decluster walks ~90 windows, so ~270
    full-file fetches - hundreds of gigabytes of network traffic to compact 12
    GB. The read amplification is inherent to the design (it is what buys
    bounded memory and avoids write amplification), but it should be paid
    against local storage, not the network.

    Bytes are kept in RAM up to ``SOURCE_CACHE_RAM_BYTES`` and spilled to files
    under ``tmpdir`` beyond it, so the cache never competes with the window
    budget that makes streaming viable in the first place.
    """

    def __init__(self, io, tmpdir: str, ram_budget: int = SOURCE_CACHE_RAM_BYTES):
        self._io = io
        self._tmpdir = tmpdir
        self._ram_budget = ram_budget
        self._ram_used = 0
        self._memory: dict = {}
        self._spilled: dict = {}

    def read(self, file_path: str) -> bytes:
        cached = self._memory.get(file_path)
        if cached is not None:
            return cached

        local = self._spilled.get(file_path)
        if local is not None:
            with open(local, "rb") as fh:
                return fh.read()

        with self._io.new_input(file_path).open() as f:
            data = bytes(f.read())

        if self._ram_used + len(data) <= self._ram_budget:
            self._memory[file_path] = data
            self._ram_used += len(data)
        else:
            local = os.path.join(self._tmpdir, f"src-{len(self._spilled):x}.parquet")
            try:
                with open(local, "wb") as fh:
                    fh.write(data)
                self._spilled[file_path] = local
            except OSError as exc:
                # No local space; fall back to re-fetching this file next time.
                logger.debug("compaction: could not spill %s locally (%s)", file_path, exc)
        return data


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
        strategy: str | None = None,
        author: str | None = None,
        agent: str | None = None,
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
        # Why the last pass declined to commit, if it did. Nearly every failure
        # here is a deliberate "abort and leave the data alone", which is safe
        # but invisible - a dataset can go weeks without compacting and look
        # identical to one with nothing to compact. Callers (and the cron job)
        # can read this; everything is logged as well.
        self._last_error: str | None = None
        # Snapshot id this pass is based on; set by compact(), checked before
        # the commit. None means "no baseline recorded", which disables the
        # staleness check (e.g. an execute path driven directly by a test).
        self._baseline_snapshot_id = None

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
            self.sort_order = normalize_sort_order(getattr(dataset.metadata, "sort_orders", []))
            if self.sort_order is None:
                # Performance mode needs a usable sort key; fall back to brute.
                self.strategy = "brute"
                self.decision = "no-sort"

    def compact(self, dry_run: bool = False, rule: str | None = None) -> Snapshot | None:
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

        # The snapshot this pass is based on. Re-checked immediately before the
        # commit (see _dataset_moved_under_us) so a concurrent writer's work is
        # not clobbered by a merge that started before it landed.
        self._baseline_snapshot_id = current_snapshot.snapshot_id

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

    def _read_manifest(self, manifest_path: str) -> list[dict]:
        """Read manifest entries from manifest file."""
        # Prefer parsed-manifest cache to avoid repeated rugo parsing
        from .manifest import get_parsed_manifest

        try:
            return get_parsed_manifest(self.dataset.io, manifest_path)
        except Exception as exc:  # noqa: BLE001 - aborts the pass, see _abort
            self._abort(f"could not read manifest {manifest_path}", exc)
            return []

    def _select_brute_compaction(self, entries: list[dict]) -> dict | None:
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
            size = entry_size(entry)
            if size < SMALL_FILE_BYTES:
                small_files.append(entry)

        # Priority 1: Combine files under threshold
        if len(small_files) >= 2:
            # Find combination that gets close to target
            selected = []
            total_size = 0

            # Sort by size ascending to prioritize eliminating smallest files
            sorted_files = sorted(small_files, key=entry_size)

            for entry in sorted_files:
                size = entry_size(entry)
                if total_size + size <= MAX_SELECTED_BUDGET_BYTES:
                    selected.append(entry)
                    total_size += size
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
        """Resolve a canonical sort key against schema ``columns``. See
        module-level ``resolve_sort_column`` (shared with write-time sorting)."""
        return resolve_sort_column(sort_order, columns)

    def _resolve_sort_columns_for_entries(self, entries: list[dict]):
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
        except Exception:  # noqa: BLE001 - Firestore client boundary
            # `dataset.schema()` reaches Firestore. Falling back to the in-memory
            # schema below is the whole point of the guard; an unresolvable sort
            # column downgrades this pass to a brute-force merge, never to a
            # silently wrong one.
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

    def _select_brute_merge(self, entries: list[dict]) -> dict | None:
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
        sub_floor = [e for e in entries if entry_size(e) < MIN_FILE_SIZE_BYTES]
        return self._select_brute_consolidation(sub_floor, sort_column_name)

    def _select_sort_aware_merge(self, entries: list[dict], rng=None) -> dict | None:
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
        sort_column_name, sort_field_id, sort_index = self._resolve_sort_columns_for_entries(
            entries
        )
        if not sort_column_name:
            return None

        big = [e for e in entries if entry_size(e) > SORT_AWARE_FLOOR_BYTES]

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
                    "size": entry_size(entry),
                }
            )
        return file_ranges

    def _select_brute_consolidation(self, sub_floor, sort_column_name) -> dict | None:
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

        ordered = sorted(sub_floor, key=entry_size)
        selected = []
        total = 0
        for e in ordered:
            size = entry_size(e)
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

    def _select_overlap_decluster(self, file_ranges, sort_column_name, rng=None) -> dict | None:
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

    def _select_binpack(self, file_ranges, sort_column_name) -> dict | None:
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
            List of morsels with unified schemas, or None if any morsel could
            not be reconciled. Returning None (rather than dropping the
            offending morsel) is deliberate: a dropped morsel's rows would
            vanish from the merged output while its input file was still
            deleted by the commit - silent data loss. The caller aborts the
            whole compaction instead, leaving the inputs intact for a later
            pass.
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
            except Exception:  # noqa: BLE001 - aborts the pass rather than dropping rows
                # Cannot reconcile this morsel. Dropping it would silently lose
                # its rows while the commit still deletes its source file, so
                # abort the compaction instead.
                return None

        return reconciled

    def _execute_compaction(self, all_entries: list[dict], plan: dict) -> Snapshot | None:
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
            combined_budget = sum(entry_size(e) for e in files_to_compact)
            if combined_budget > MAX_SELECTED_BUDGET_BYTES:
                return self._abort(
                    f"streaming declined and the {combined_budget >> 20} MB merge "
                    f"exceeds the {MAX_SELECTED_BUDGET_BYTES >> 20} MB in-memory gate"
                )
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
                    Morsel.combine(row_group_morsels)
                    if len(row_group_morsels) > 1
                    else row_group_morsels[0]
                )
                tables.append(file_morsel)
                total_size += entry_size(entry)
            except Exception as exc:  # noqa: BLE001 - aborts the pass, see _abort
                # Failed to read file, abort this compaction
                return self._abort(f"could not read input file {file_path}", exc)

        if not tables:
            return self._abort("no input files could be read")

        # Reconcile schemas before concatenation. None means a morsel could not
        # be brought to the unified schema; abort rather than silently drop it.
        tables = self._reconcile_schemas(tables)
        if not tables:
            return self._abort("could not reconcile input schemas")

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
                except Exception:  # noqa: BLE001 - pure-Python fallback sort
                    # Comparing values of mixed or exotic types. Recorded as failed
                    # rather than swallowed: `sort_status` reaches the commit.
                    perm = None
                    sort_status = "failed"
            except Exception:  # noqa: BLE001 - draken native sort, C-ABI boundary
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
            except Exception as exc:  # noqa: BLE001 - cleans up, then aborts the pass
                # Failed to write or upload. Anything already written this pass
                # is unreferenced now, so clean it up before aborting.
                self._delete_written_files(new_entries)
                return self._abort(f"could not write output file {file_path}", exc)

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
            all_entries,
            files_to_compact,
            new_entries,
            snapshot_id,
            input_records,
            input_data_size,
            sort_status,
        )

    def _finalize_compaction_snapshot(
        self,
        all_entries: list[dict],
        files_to_compact: list[dict],
        new_entries: list[dict],
        snapshot_id: int,
        input_records: int,
        input_data_size: int,
        sort_status: str,
    ) -> Snapshot | None:
        """Shared tail for both execution paths (hold-everything and streaming):
        prune/validate the surviving old entries, write the new manifest, compute
        summary stats, and commit the snapshot. Neither execution path needs to
        know about manifest/Firestore mechanics - they just produce
        ``new_entries`` (already-built manifest dicts for the files they wrote)
        and call this.

        Every commit is gated on a row-count invariant: compaction only ever
        rewrites rows, so the outputs must hold exactly as many records as the
        inputs did. This is the one check that turns a silent data-loss bug into
        a no-op - an inverted predicate, a rugo regression on some column type,
        a mis-derived chunk group all show up here as a count mismatch, and the
        pass aborts with the input files still intact.
        """
        if not self._row_counts_balance(files_to_compact, new_entries):
            self._delete_written_files(new_entries)
            return None

        if self._dataset_moved_under_us():
            # ``save_dataset_metadata`` has no compare-and-swap, so committing
            # now would unconditionally clobber whatever landed while we were
            # reading and rewriting - including that writer's new data files,
            # which our manifest knows nothing about. This is a read-back, not
            # a real CAS: it cannot close the window between this check and the
            # write, only the (much longer) one covering the merge itself.
            self._delete_written_files(new_entries)
            return self._abort(
                "dataset changed during compaction; discarding this pass rather "
                "than overwriting a concurrent commit"
            )

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
                        # Rebuild failed - catastrophic, abort entire compaction.
                        #
                        # This used to be a bare `return None`. Every other abort
                        # in this function deletes what the pass wrote and records
                        # why; this one did neither, so the abort its own comment
                        # calls catastrophic left its output files orphaned in
                        # storage and read, to any caller, as "nothing to compact".
                        self._delete_written_files(new_entries)
                        reason = (
                            f"could not rebuild corrupted manifest entry for {e.get('file_path')}"
                        )
                        self._abort(reason)
                        _alert(
                            CompactionInvariantError(reason),
                            fingerprint=(
                                "compaction-entry-recovery",
                                self.dataset.identifier,
                            ),
                            context={
                                "dataset": self.dataset.identifier,
                                "file_path": e.get("file_path"),
                            },
                        )
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
            deleted_size = sum(entry_int(e, "file_size_in_bytes") for e in files_to_compact)
            # Input record/byte counts come from the morsels themselves (captured
            # before they were freed); on-disk size is only in the manifest.
            deleted_records = input_records
            deleted_data_size = input_data_size

            added_files = len(new_entries)
            added_size = sum(entry_int(e, "file_size_in_bytes") for e in new_entries)
            added_data_size = sum(entry_size(e) for e in new_entries)
            added_records = sum(entry_int(e, "record_count") for e in new_entries)

            total_files = len(final_entries)
            total_size = sum(entry_int(e, "file_size_in_bytes") for e in final_entries)
            total_data_size = sum(entry_size(e) for e in final_entries)
            total_records = sum(entry_int(e, "record_count") for e in final_entries)
        except Exception:  # noqa: BLE001 - falls back to a full manifest refresh
            # Summing statistics off entries whose shape may predate this code.
            # The fallback below rebuilds every entry from its data file, so this
            # trades time for certainty rather than accepting wrong totals.
            # If stats calculation fails, refresh the entire manifest from data files
            final_entries = self._refresh_manifest_from_data_files(final_entries)

            # Use what we know directly since we still have new_entries in scope
            deleted_files = len(files_to_compact)
            deleted_size = sum(entry_int(e, "file_size_in_bytes") for e in files_to_compact)
            deleted_data_size = sum(entry_size(e) for e in files_to_compact)
            deleted_records = sum(entry_int(e, "record_count") for e in files_to_compact)

            added_files = len(new_entries)
            added_size = sum(entry_int(e, "file_size_in_bytes") for e in new_entries)
            added_data_size = sum(entry_size(e) for e in new_entries)
            added_records = sum(entry_int(e, "record_count") for e in new_entries)

            total_files = len(final_entries)
            total_size = sum(entry_int(e, "file_size_in_bytes") for e in final_entries)
            total_data_size = sum(entry_size(e) for e in final_entries)
            total_records = sum(entry_int(e, "record_count") for e in final_entries)

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

    def _dataset_moved_under_us(self) -> bool:
        """Whether the dataset's current snapshot changed since this pass read
        the manifest.

        Compaction's read-to-commit window spans a whole multi-GB merge, so a
        concurrent append is entirely plausible. Re-reading the catalog just
        before the commit turns "silently overwrite the other writer" into
        "skip this pass and retry next tick", which is the right trade: the
        work is idempotent and cheap to redo, the lost data is not.

        Returns False when the check itself can't run (no catalog, no loader,
        read fails) - an unavailable check is not evidence of a conflict, and
        must not stall compaction on every pass.
        """
        baseline = getattr(self, "_baseline_snapshot_id", None)
        if baseline is None:
            return False
        catalog = getattr(self.dataset, "catalog", None)
        loader = getattr(catalog, "load_dataset", None)
        if not callable(loader):
            return False
        try:
            fresh = loader(self.dataset.identifier)
            current = fresh.metadata.current_snapshot_id
        except Exception as exc:  # noqa: BLE001 - advisory check, logged below
            logger.debug("compaction: staleness check unavailable (%s)", exc)
            return False
        return current is not None and current != baseline

    def _abort(self, reason: str, exc: BaseException | None = None):
        """Record and log why this pass is declining to commit, then return
        None so callers can ``return self._abort(...)``. Every abort path used
        to be a bare ``return None``, which made a compactor that had silently
        done nothing for weeks indistinguishable from one with no work to do."""
        self._last_error = reason
        if exc is not None:
            logger.warning("compaction aborted: %s (%s: %s)", reason, type(exc).__name__, exc)
        else:
            logger.warning("compaction aborted: %s", reason)

    def _row_counts_balance(self, files_to_compact: list[dict], new_entries: list[dict]) -> bool:
        """Whether the outputs account for exactly the input rows.

        Skipped (returns True) when any input entry carries no ``record_count``:
        an absent count is unknown, not zero, and refusing to compact on missing
        stats would be worse than not checking. Output entries are freshly built
        from the files just written, so their counts are always present.
        """
        input_counts = [e.get("record_count") for e in files_to_compact]
        if any(c is None for c in input_counts):
            return True
        expected = sum(input_counts)
        actual = sum(entry_int(e, "record_count") for e in new_entries)
        if expected == actual:
            return True
        reason = (
            f"row-count mismatch: {expected} input rows vs {actual} written "
            f"across {len(new_entries)} file(s)"
        )
        self._abort(reason)
        # The abort above is a warning, which is where this signal has sat
        # while being the one check that distinguishes a silent data-loss bug
        # from a no-op. The data is intact - the pass declined to commit - but
        # something is producing the wrong number of rows and nobody would know.
        _alert(
            CompactionInvariantError(reason),
            fingerprint=("compaction-row-count-mismatch", self.dataset.identifier),
            context={
                "dataset": self.dataset.identifier,
                "expected_rows": expected,
                "written_rows": actual,
                "input_files": len(files_to_compact),
                "output_files": len(new_entries),
            },
        )
        return False

    def _delete_written_files(self, new_entries: list[dict]) -> None:
        """Best-effort removal of output files written by an aborted pass.

        Nothing references them once the snapshot is not committed, so leaving
        them behind is pure orphaned storage. Failures are ignored: losing the
        cleanup is not a reason to turn a safe abort into an exception.
        """
        for entry in new_entries or ():
            path = entry.get("file_path") if isinstance(entry, dict) else None
            if not path:
                continue
            try:
                self.dataset.io.delete(path)
            except Exception as exc:  # noqa: BLE001 - best-effort cleanup, logged below
                logger.debug("compaction: could not remove orphan %s (%s)", path, exc)

    # --- Streaming execution (see the module comment above ROW_GROUP_TARGET_ROWS) ---

    def _read_sort_column_combined(
        self, files_to_compact: list[dict], sort_column: str, source_cache
    ):
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
                # Reading through the cache means pass 2 gets these files for
                # free - pass 1 already had to fetch every one of them.
                data = source_cache.read(file_path)
                with read_parquet(data, columns=[sort_column]) as reader:
                    parts.extend(reader)
            except Exception:  # noqa: BLE001 - caller falls back to a brute merge
                # One unreadable input means no reliable global key order, so the
                # sort-aware plan is abandoned wholesale. The pass still runs, it
                # just stops claiming the output is sorted.
                return None
        if not parts:
            return None
        return Morsel.combine(parts) if len(parts) > 1 else parts[0]

    @staticmethod
    def _iter_key_runs(key_chunks):
        """Collapse a stream of sorted key CHUNKS (lists of values) into a
        stream of ``(value, run_length)`` runs, correctly joining a run that
        straddles a chunk boundary.

        Consuming the sorted keys in chunks rather than as one list is what
        keeps this bounded: materialising every key via ``to_pylist()`` on a
        multi-GB merge is gigabytes of Python objects on the very path whose
        purpose is bounded memory.
        """
        have = False
        current = None
        length = 0
        for chunk in key_chunks:
            for value in chunk:
                if have and value == current:
                    length += 1
                    continue
                # `None == None` is True, so null runs coalesce here too.
                if have and value is None and current is None:
                    length += 1
                    continue
                if have:
                    yield current, length
                current = value
                length = 1
                have = True
        if have:
            yield current, length

    def _compute_chunk_groups(self, key_chunks) -> list:
        """Turn the exact, fully sorted key sequence into an ordered list of
        chunk-group descriptors for pass 2+. Each group is one of:

          {"type": "nulls", "count": N}            - always first, if N>0
          {"type": "range", "lo": v, "hi": v|None}  - the sort-key interval
                                                       between two distinct
                                                       values; hi=None on the
                                                       final group (no further
                                                       bound needed)
          {"type": "hot", "value": v}               - a single value whose run
                                                       alone exceeds the hard cap

        ``lo``/``hi`` are given in SORT ORDER, not in ascending value order: on
        a descending sort ``lo`` is the larger value and ``hi`` the smaller.
        ``_group_predicates`` is the single place that turns them into
        comparison operators, so the direction is handled once.

        Boundaries snap to distinct-value edges so a predicate range never
        needs to split a run of equal keys (predicates match by value, not row
        position). Hard cap ROW_GROUP_HARD_CAP_ROWS - if extending to the next
        distinct value would exceed the cap, cut earlier instead (undershoot,
        never overshoot), except for a single run that alone exceeds the cap,
        which becomes its own "hot" group (pass 2+ falls back to row-count
        slicing for that one, since no value-based predicate can split it
        further).

        ``key_chunks`` is an iterable of lists of keys in sorted order; see
        ``_iter_key_runs``.
        """
        groups = []
        null_count = 0
        pending_lo = None
        pending_count = 0

        for value, run_len in self._iter_key_runs(key_chunks):
            if value is None:
                # draken's morsel_sort is NULLS-FIRST ascending, NULLS-LAST
                # descending; either way the nulls form one contiguous run.
                # Project policy places them at logical position 0 regardless,
                # so the group is inserted at the front below.
                null_count += run_len
                continue

            if run_len > ROW_GROUP_HARD_CAP_ROWS:
                # A single value spans more rows than one chunk can safely
                # hold, and predicates can't slice within it - hand the whole
                # run to the row-count-slicing fallback. Close any open range
                # at this value first so the two stay disjoint.
                if pending_count:
                    groups.append({"type": "range", "lo": pending_lo, "hi": value})
                    pending_count = 0
                groups.append({"type": "hot", "value": value})
                continue

            if pending_count == 0:
                pending_lo = value
                pending_count = run_len
            elif pending_count + run_len > ROW_GROUP_HARD_CAP_ROWS:
                groups.append({"type": "range", "lo": pending_lo, "hi": value})
                pending_lo = value
                pending_count = run_len
            else:
                pending_count += run_len

        if pending_count:
            groups.append({"type": "range", "lo": pending_lo, "hi": None})
        if null_count:
            groups.insert(0, {"type": "nulls", "count": null_count})

        return groups

    @staticmethod
    def _group_predicates(sort_column: str, group: dict, ascending: bool) -> list:
        """Predicates selecting exactly the rows of a "range" group.

        ``lo``/``hi`` come out of ``_compute_chunk_groups`` in SORT order, so on
        a descending sort ``lo`` is the LARGER value. Emitting ``>= lo AND
        < hi`` unconditionally (as an earlier version did) therefore produced an
        empty, inverted interval on every descending-sorted dataset: each range
        group read zero rows, and if the merge also had a hot/null group the
        snapshot committed with only those rows and deleted the rest.
        """
        lo, hi = group["lo"], group["hi"]
        if ascending:
            preds = [(sort_column, ">=", lo)]
            if hi is not None:
                preds.append((sort_column, "<", hi))
        else:
            preds = [(sort_column, "<=", lo)]
            if hi is not None:
                preds.append((sort_column, ">", hi))
        return preds

    @staticmethod
    def _group_value_bounds(group: dict, ascending: bool):
        """The group's extent as ``(low, low_inclusive, high, high_inclusive)``
        in ASCENDING value space (None = unbounded), for pruning candidate files
        against their manifest min/max. Mirrors ``_group_predicates``."""
        lo, hi = group["lo"], group["hi"]
        if ascending:
            return lo, True, hi, False  # [lo, hi)
        return hi, False, lo, True  # (hi, lo]

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

    @staticmethod
    def _file_can_contribute(bounds, low, low_inclusive, high, high_inclusive) -> bool:
        """Whether a file whose sort-key extent is ``bounds`` ((min, max) from
        its manifest entry, or None when it carries no usable stats) can hold
        any row inside the given ascending-space interval.

        Pruning here is what stops pass 2 from touching every candidate file for
        every window: on a mostly-disjoint dataset each window is served by one
        or two files instead of all of them. ``None`` bounds (no stats) and
        incomparable values both fall through to "yes", so pruning can only ever
        avoid work, never drop rows.
        """
        if bounds is None:
            return True
        file_min, file_max = bounds
        if file_min is None or file_max is None:
            return True
        try:
            if low is not None and (file_max < low or (not low_inclusive and file_max == low)):
                return False
            if high is not None and (file_min > high or (not high_inclusive and file_min == high)):
                return False
        except TypeError:
            return True  # values not mutually comparable; read it to be safe
        return True

    def _iter_group_morsels(
        self,
        files_to_compact,
        sort_column,
        ascending,
        group,
        null_bearing_paths,
        source_cache,
        bounds_by_path,
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
        from draken.interop.vector_sequence import vector_from_sequence
        from draken.morsels.morsel import Morsel
        from rugo.parquet import read_parquet

        def sort_and_yield(morsel):
            if morsel is None or morsel.num_rows == 0:
                return
            from draken.morsels.sort import morsel_sort

            perm = morsel_sort(morsel, [sort_column], [ascending])
            yield morsel.take(list(perm))

        if group["type"] == "range":
            parts = []
            preds = self._group_predicates(sort_column, group, ascending)
            low, low_inc, high, high_inc = self._group_value_bounds(group, ascending)
            for entry in files_to_compact:
                file_path = entry.get("file_path")
                if not self._file_can_contribute(
                    bounds_by_path.get(file_path), low, low_inc, high, high_inc
                ):
                    continue
                data = source_cache.read(file_path)
                with read_parquet(data, predicates=preds) as reader:
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
                if not self._file_can_contribute(
                    bounds_by_path.get(file_path), value, True, value, True
                ):
                    continue
                data = source_cache.read(file_path)
                with read_parquet(data, predicates=preds) as reader:
                    for rg in reader:
                        acc.append(rg)
                        acc_rows += rg.num_rows
                        while acc_rows >= ROW_GROUP_TARGET_ROWS:
                            combined = Morsel.combine(acc) if len(acc) > 1 else acc[0]
                            head = combined.slice(0, ROW_GROUP_TARGET_ROWS)
                            tail_len = combined.num_rows - ROW_GROUP_TARGET_ROWS
                            yield head
                            acc = (
                                [combined.slice(ROW_GROUP_TARGET_ROWS, tail_len)]
                                if tail_len
                                else []
                            )
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
                data = source_cache.read(file_path)
                with read_parquet(data) as reader:
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
                            acc = (
                                [combined.slice(ROW_GROUP_TARGET_ROWS, tail_len)]
                                if tail_len
                                else []
                            )
                            acc_rows = tail_len
            if acc_rows:
                yield Morsel.combine(acc) if len(acc) > 1 else acc[0]
            return

    def _execute_compaction_streaming(self, all_entries: list[dict], plan: dict) -> Snapshot | None:
        """Three-pass streaming execution: project+sort the key column, derive
        chunk groups, stream row-group-sized sorted chunks per group, and roll
        them into target-sized output files via rugo's streaming writer
        (`write_parquet_stream`, undocumented as of rugo 0.4.17 - flagged here
        so it's easy to find when regression coverage lands upstream). Peak
        memory is bounded by one chunk's size, independent of merge size.

        Returns None (never raises) if anything about this path can't proceed,
        so the caller can fall back to hold-everything.
        """
        import tempfile

        # One cache for the whole execution: every candidate file is fetched
        # from object storage at most once, however many windows read it.
        with tempfile.TemporaryDirectory(prefix="opteryx-compact-") as tmpdir:
            source_cache = _SourceFileCache(self.dataset.io, tmpdir)
            return self._execute_compaction_streaming_inner(all_entries, plan, source_cache)

    def _execute_compaction_streaming_inner(
        self, all_entries: list[dict], plan: dict, source_cache
    ) -> Snapshot | None:
        """Body of ``_execute_compaction_streaming``, split out so the source
        cache's temp directory is torn down on every exit path."""
        plan_type = plan["type"]
        files_to_compact = plan["files"]
        sort_column = plan.get("sort_column")
        if not sort_column or plan_type != "combine-split":
            return None

        ascending = True
        if isinstance(self.sort_order, dict):
            ascending = self.sort_order.get("ascending", True)

        # Pass 1: exact global sort of the key column alone.
        key_morsel = self._read_sort_column_combined(files_to_compact, sort_column, source_cache)
        if key_morsel is None or key_morsel.num_rows == 0:
            return None
        try:
            from draken.morsels.sort import morsel_sort

            perm = morsel_sort(key_morsel, [sort_column], [ascending])
            sorted_key_morsel = key_morsel.take(list(perm))
        except Exception:  # noqa: BLE001 - draken native sort, C-ABI boundary
            return None
        n = sorted_key_morsel.num_rows
        del key_morsel, perm

        def key_chunks(morsel):
            # Materialise the sorted keys a window at a time; the full
            # to_pylist() is gigabytes of Python objects at scale.
            offset = 0
            while offset < n:
                take = min(KEY_SCAN_CHUNK_ROWS, n - offset)
                yield morsel.slice(offset, take).column(sort_column).to_pylist()
                offset += take

        # _compute_chunk_groups drains the generator, so the keys are free to
        # drop before pass 2 starts reading real data.
        groups = self._compute_chunk_groups(key_chunks(sorted_key_morsel))
        del sorted_key_morsel
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
        except Exception:  # noqa: BLE001 - Firestore client boundary, see above
            # Without the resolved field-id the null-bearing scan below simply
            # finds nothing, which costs a planning refinement, not correctness.
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

        # Per-file sort-key extent, so each window only reads the files that can
        # actually hold rows in its range (see ``_file_can_contribute``). Files
        # with no usable stats are simply absent and are always read.
        bounds_by_path = {
            fr["entry"].get("file_path"): (fr["min"], fr["max"])
            for fr in self._build_file_ranges(files_to_compact, sort_field_id, sort_index)
        }

        total_size = sum(entry_size(e) for e in files_to_compact)
        k = max(1, -(-total_size // TARGET_SIZE_BYTES))
        target_rows_per_file = max(1, -(-n // k))

        def master_chunks():
            for group in groups:
                yield from self._iter_group_morsels(
                    files_to_compact,
                    sort_column,
                    ascending,
                    group,
                    null_bearing_paths,
                    source_cache,
                    bounds_by_path,
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
        except Exception as exc:  # noqa: BLE001 - cleans up, then aborts the pass
            # Outputs written before the failure are unreferenced; remove them
            # rather than leaving orphans in the data directory.
            self._delete_written_files(new_entries)
            return self._abort("streaming execution failed", exc)

        if not new_entries:
            return self._abort("streaming execution produced no output files")

        input_records = n
        input_data_size = sum(entry_size(e) for e in files_to_compact)

        return self._finalize_compaction_snapshot(
            all_entries,
            files_to_compact,
            new_entries,
            snapshot_id,
            input_records,
            input_data_size,
            "native",
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

    def _recover_entry(self, corrupted_entry: dict) -> dict | None:
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
        except Exception:  # noqa: BLE001 - per-entry recovery, contract documented above
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
        except Exception:  # noqa: BLE001 - per-entry recovery, contract documented above
            return entry  # If we can't rebuild, keep the original entry

    def _refresh_manifest_from_data_files(self, all_entries: list[dict]) -> list[dict]:
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
