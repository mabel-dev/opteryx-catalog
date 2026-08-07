# Compaction Strategy for Firestore-GCS Catalog

## Overview

This document describes the compaction approaches designed for this catalog implementation, focusing on small file compaction as the primary optimization target.

## Background

### What is Compaction?

Compaction in data lake systems involves:

1. **Small File Compaction**: Merging many small data files into fewer, larger files to improve read performance and reduce metadata overhead
2. **Metadata Compaction**: Consolidating multiple metadata files/manifests into fewer files

### Why Compaction Matters

- **Query Performance**: Reading many small files has high overhead (network requests, file opens)
- **Metadata Overhead**: Each file requires manifest entries, increasing query planning time
- **Storage Efficiency**: Small files can have poor compression ratios
- **Cost**: Cloud storage often charges per-request, so fewer large files = lower costs

## Design Decisions

### 1. Metadata Compaction: Not Needed (For Now)

**Rationale**: This catalog already writes consolidated Parquet manifests alongside standard Avro manifests. The Parquet manifest approach provides:
- Single consolidated file per snapshot
- Fast query planning (10-50x faster than Avro)
- BRIN-style pruning for efficient filtering

**Conclusion**: Metadata compaction is redundant given the Parquet manifest optimization already in place.

### 2. Small File Compaction: Primary Focus

**Problem**: Over time, tables accumulate many small data files from:
- Streaming writes (small batches)
- High-frequency appends
- Partitioned writes creating many small partition files

**Solution**: Implement a compaction service that:
1. Identifies tables/partitions with too many small files
2. Rewrites small files into fewer, larger files
3. Uses transactional rewrite operations for ACID guarantees

## Small File Compaction Design

### Configuration

Compaction behavior is controlled via table properties:

```python
# Target file size (default: 128 MB)
"write.target-file-size-bytes": "134217728"

# Minimum number of files to trigger compaction (default: 10)
"compaction.min-file-count": "10"

# Maximum file size to consider "small" (default: 32 MB)
"compaction.max-small-file-size-bytes": "33554432"

# Compaction strategy: "binpack" or "sort" (default: "binpack")
"compaction.strategy": "binpack"
```

### Compaction Strategies

#### 1. Bin-Packing Strategy (Default)

Groups small files into bins that sum to approximately the target file size:

**Advantages**:
- Simple and fast
- Works well for unordered data
- Minimal memory usage

**Use Case**: General-purpose compaction for most tables

#### 2. Sort-Based Strategy

Reads and sorts data before rewriting:

**Advantages**:
- Improves data locality for sorted columns
- Enables better predicate pushdown
- Reduces file skipping overhead

**Use Case**: Tables with common sort/filter patterns

**Trade-offs**: Higher CPU and memory usage

### Compaction Triggers

#### 1. Manual Trigger

Explicit API call to compact a table:

```python
from opteryx_catalog.catalog import DatasetCompactor

dataset = catalog.load_dataset("my_collection.my_dataset")
DatasetCompactor(dataset, author="me").compact()
```

#### 2. Scheduled Trigger

Scheduling lives outside this library, in the `xb500.opteryx` housekeeping
service: Cloud Scheduler calls its `/housekeeping/trigger_compaction`
endpoint, which discovers workspaces from Firestore, walks the allowlisted
collections, and calls `compact()` per dataset — writing one audit row per
dataset evaluated to `opteryx.ops.compaction_log`. See
`xb500.opteryx/app/operations/trigger_compaction.py`.

Compaction there is opt-in per collection: only `workspace.collection` names
in `COMPACTION_ALLOWED_COLLECTIONS` (default `opteryx.ops`) are ever
rewritten, with `COMPACTION_BLOCKED_WORKSPACES` (default `benchmarks`)
applied on top, so a newly discovered workspace is inert by default.

For working through a backlog outside the request-timeout of a container,
`scripts/catchup_compaction.py` loops `compact()` per dataset on a
long-running VM until a pass finds nothing left to do.

#### 3. Threshold-Based Trigger

Thresholds are enforced inside `compact()` itself rather than by a separate
`should_compact()` check — file selection is driven by `SMALL_FILE_BYTES`,
`MIN_FILE_SIZE_BYTES`, and `SORT_AWARE_FLOOR_BYTES`, and the pass returns
`None` when no group clears them. So "compact when thresholds are exceeded"
is simply calling `compact()` and letting it decline:

```python
# `dry_run=True` returns the PLAN DICT (what would be compacted, and why),
# or None when nothing clears the thresholds. Only `dry_run=False` returns
# a Snapshot.
plan = compactor.compact(dry_run=True)
if plan is not None:
    print(plan["type"], plan["reason"])
    compactor.compact(dry_run=False)
```

### Implementation Components

#### 1. File Analysis (`compaction.py`)

```python
def analyze_files(table) -> CompactionPlan:
    """Analyze data files and determine compaction needs.
    
    Returns:
        CompactionPlan with file groups to compact
    """
```

#### 2. File Grouping

```python
def group_files_binpack(files, target_size) -> List[List[DataFile]]:
    """Group files using bin-packing algorithm."""
```

#### 3. Rewrite Execution

Uses native rewrite operations:

```python
from pyiceberg.table import Table


def execute_compaction(table: Table, plan: CompactionPlan):
    """Execute compaction using a table library's rewrite_data_files or equivalent."""
    with table.update_spec() as update:
        for file_group in plan.file_groups:
            # Read data from small files
            data = read_files(file_group)

            # Write consolidated file
            new_file = write_parquet(data, target_path)

            # Update table with transaction
            update.rewrite_files(old_files=file_group, new_files=[new_file])
```

### Safety and Correctness

1. **ACID Guarantees**: Uses transactional rewrite/update operations when available
2. **Snapshot Isolation**: Compaction operates on a snapshot, doesn't block reads
3. **Rollback**: Failed compaction can be rolled back without data loss
4. **Concurrent Writers**: Optimistic concurrency control handles conflicts

### Performance Considerations

1. **Memory**: Processes one file group at a time to limit memory usage
2. **Parallelism**: Can process multiple partitions in parallel
3. **I/O**: Reads and writes are streamed to avoid buffering entire files
4. **Network**: Uses GCS multi-part uploads for large files

### Monitoring

Track compaction metrics:

```python
{
    "table": "namespace.table_name",
    "files_before": 150,
    "files_after": 15,
    "bytes_before": 4800000000,
    "bytes_after": 4800000000,
    "duration_seconds": 45.2,
    "files_rewritten": 135,
}
```

## Usage Examples

### Basic Compaction

```python
from opteryx_catalog import OpteryxCatalog
from opteryx_catalog.catalog import DatasetCompactor

catalog = OpteryxCatalog(
    workspace="my_workspace",
    firestore_project="my-project",
    gcs_bucket="my-bucket",
)

# Compact a specific dataset. Valid strategies are 'brute' and 'performance';
# omit the argument to auto-detect from the dataset's sort order.
dataset = catalog.load_dataset("my_collection.my_dataset")
compactor = DatasetCompactor(dataset, strategy="brute", author="me")

snapshot = compactor.compact()
if snapshot is None:
    print("declined to commit:", compactor._last_error)
else:
    print("committed snapshot", snapshot.snapshot_id)
```

### Automatic Compaction

The per-dataset policy lives on the dataset's `maintenance_policy` block as
`compaction-policy` (defaulting to `"performance"`), alongside
`retained-snapshot-age-days`. It is persisted with the dataset metadata and
reported by `scripts/inspect_snapshots.py`.

> **Caveat:** `DatasetCompactor` does not currently read
> `maintenance_policy["compaction-policy"]`. Strategy comes from its
> `strategy` argument, or is auto-detected from the dataset's sort order when
> that argument is `None`; the housekeeping service passes
> `strategy="performance"` explicitly. So the stored policy is descriptive
> today rather than load-bearing — wiring it into `__init__` is the gap.

Sizing is not property-driven either: it comes from the module constants in
`opteryx_catalog.catalog.compaction` (`TARGET_SIZE_BYTES` 4 GB,
`MIN_SIZE_BYTES` 3.5 GB, `MAX_SIZE_BYTES` 4.1 GB), with
`OPTERYX_COMPACTION_RAM_MB` (default 16384) as the runtime memory budget.

### Scheduled Compaction

Driven by Cloud Scheduler against the `xb500.opteryx` housekeeping service —
see "Scheduled Trigger" above for the endpoint, allowlist, and audit log.

## Future Enhancements

1. **Partition-Aware Compaction**: Compact within partitions only
2. **Z-Order Compaction**: Multi-dimensional clustering for better pruning
3. **Incremental Compaction**: Only compact recent data
4. **Cost-Based Optimization**: Use query patterns to optimize compaction
5. **Auto-Tuning**: Adjust parameters based on workload characteristics

## References

<!-- External references to specific projects removed -->

## Summary

This catalog implementation prioritizes **small file compaction** as the main optimization strategy. Metadata compaction is unnecessary due to the existing Parquet manifest optimization. The design provides flexible configuration, multiple strategies, and safe execution using available transactional or rewrite mechanisms when supported by the target table implementation.
