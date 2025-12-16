# Compaction Strategy for PyIceberg-Firestore-GCS Catalog

## Overview

This document describes the compaction approaches designed for this catalog implementation, focusing on small file compaction as the primary optimization target.

## Background

### What is Compaction?

Compaction in data lake systems like Iceberg involves:

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
3. Uses Iceberg's built-in rewrite operations for ACID guarantees

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
from pyiceberg_firestore_gcs.compaction import compact_table

catalog = create_catalog(...)
compact_table(catalog, ("namespace", "table_name"))
```

#### 2. Scheduled Trigger

Background job that periodically scans tables:

```python
from pyiceberg_firestore_gcs.compaction import CompactionScheduler

scheduler = CompactionScheduler(
    catalog=catalog,
    check_interval_seconds=3600,  # Check every hour
    auto_compact=True
)
scheduler.start()
```

#### 3. Threshold-Based Trigger

Automatically compact when thresholds are exceeded:

```python
# After commit, check if compaction is needed
if should_compact(table):
    schedule_async_compaction(table)
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

Uses PyIceberg's native rewrite operations:

```python
from pyiceberg.table import Table

def execute_compaction(table: Table, plan: CompactionPlan):
    """Execute compaction using Iceberg's rewrite_data_files."""
    with table.update_spec() as update:
        for file_group in plan.file_groups:
            # Read data from small files
            data = read_files(file_group)
            
            # Write consolidated file
            new_file = write_parquet(data, target_path)
            
            # Update table with transaction
            update.rewrite_files(
                old_files=file_group,
                new_files=[new_file]
            )
```

### Safety and Correctness

1. **ACID Guarantees**: Uses Iceberg's transaction system
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
    "files_rewritten": 135
}
```

## Usage Examples

### Basic Compaction

```python
from pyiceberg_firestore_gcs import create_catalog
from pyiceberg_firestore_gcs.compaction import compact_table

catalog = create_catalog(
    "my_catalog",
    firestore_project="my-project",
    gcs_bucket="my-bucket"
)

# Compact a specific table
result = compact_table(
    catalog,
    identifier=("my_namespace", "my_table"),
    strategy="binpack"
)

print(f"Compacted {result.files_rewritten} files")
```

### Automatic Compaction

```python
# Enable auto-compaction when creating table
table = catalog.create_table(
    identifier=("my_namespace", "my_table"),
    schema=schema,
    properties={
        "compaction.enabled": "true",
        "compaction.min-file-count": "20",
        "compaction.strategy": "binpack"
    }
)
```

### Scheduled Compaction

```python
from pyiceberg_firestore_gcs.compaction import CompactionScheduler

# Run compaction hourly
scheduler = CompactionScheduler(catalog)
scheduler.run_periodic(interval_seconds=3600)
```

## Future Enhancements

1. **Partition-Aware Compaction**: Compact within partitions only
2. **Z-Order Compaction**: Multi-dimensional clustering for better pruning
3. **Incremental Compaction**: Only compact recent data
4. **Cost-Based Optimization**: Use query patterns to optimize compaction
5. **Auto-Tuning**: Adjust parameters based on workload characteristics

## References

- [Apache Iceberg Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
- [Iceberg File Compaction](https://iceberg.apache.org/docs/latest/spark-writes/#compact-data-files)
- [PyIceberg API Documentation](https://py.iceberg.apache.org/)

## Summary

This catalog implementation prioritizes **small file compaction** as the main optimization strategy. Metadata compaction is unnecessary due to the existing Parquet manifest optimization. The design provides flexible configuration, multiple strategies, and safe execution using Iceberg's built-in mechanisms.
