#!/usr/bin/env python3
"""
Quick reference: Compaction capability in 3 minutes

This script demonstrates the compaction API and how to use it.
"""


def quick_start():
    """Copy-paste this to get started with compaction."""

    example = """
    from opteryx_catalog import OpteryxCatalog
    from opteryx_catalog.catalog.compaction import DatasetCompactor
    
    # Load your dataset
    catalog = OpteryxCatalog(workspace="my_workspace", gcs_bucket="my_bucket")
    dataset = catalog.load_dataset("collection.dataset_name")
    
    # Create compactor (auto-selects best strategy)
    compactor = DatasetCompactor(
        dataset,
        author="system_admin",
        agent="my_compaction_service"
    )
    
    # Check what would be compacted (no I/O changes)
    plan = compactor.compact(dry_run=True)
    if plan:
        print(f"Will {plan['type']}: {plan['reason']}")
        print(f"Files to process: {len(plan['files'])}")
    
    # Execute compaction
    new_snapshot = compactor.compact(dry_run=False)
    if new_snapshot:
        print(f"✓ Compacted! {new_snapshot.summary}")
    else:
        print("ℹ No compaction needed")
    """

    return example


def what_is_compaction():
    """One sentence explanation."""
    return """
    Compaction combines many small data files into fewer, larger files to improve 
    query performance and reduce metadata overhead.
    """


def strategies():
    """The two strategies at a glance."""

    return {
        "brute": {
            "description": "Simple file combining",
            "best_for": "General-purpose, unordered data",
            "algorithm": "Combine sub-floor files toward TARGET_SIZE_MB (uncompressed)",
            "auto_used": "When dataset has no sort order",
        },
        "performance": {
            "description": "Respects data order",
            "best_for": "Sorted data with common filter patterns",
            "algorithm": "Merges overlapping ranges, maintains sort order",
            "auto_used": "When dataset has sort orders defined",
        },
    }


def api_reference():
    """Quick API reference."""

    return {
        "DatasetCompactor(dataset, strategy=None, author=None, agent=None)": {
            "description": "Create compactor for a dataset",
            "strategy": "None=auto, 'brute', or 'performance'",
            "author": "Name for metadata tracking",
            "agent": "Identifier for the compaction service",
        },
        "compactor.compact(dry_run=False)": {
            "description": "Execute compaction",
            "returns": "Snapshot or None",
            "side_effects": "Writes files, updates metadata (unless dry_run=True)",
            "dry_run": "True = plan only, False = execute",
        },
        "compactor.compact(dry_run=True)": {
            "description": "Preview compaction plan",
            "returns": "Dict with keys: type, files, reason, sort_column (optional)",
            "type_values": "'combine', 'split', or 'combine-split'",
            "reason_values": "'small-files', 'file-too-large', 'overlapping-ranges'",
        },
    }


def file_size_thresholds():
    """Constants used in compaction decisions.

    Read from `opteryx_catalog.catalog.compaction` rather than restated here:
    these previously drifted out of sync with the real values, which makes a
    quick-reference actively misleading. All thresholds are compared against
    uncompressed size, not on-disk size.
    """
    from opteryx_catalog.catalog import compaction as c

    return {
        "SMALL_FILE_MB": c.SMALL_FILE_MB,
        "SMALL_FILE_BYTES": c.SMALL_FILE_BYTES,
        "description_small": "Files below this are candidates for combining",
        "MIN_SIZE_MB": c.MIN_SIZE_MB,
        "MIN_SIZE_BYTES": c.MIN_SIZE_BYTES,
        "description_min": "Lower bound of the acceptable output band",
        "TARGET_SIZE_MB": c.TARGET_SIZE_MB,
        "TARGET_SIZE_BYTES": c.TARGET_SIZE_BYTES,
        "description_target": "Goal size for combined files",
        "MAX_SIZE_MB": c.MAX_SIZE_MB,
        "MAX_SIZE_BYTES": c.MAX_SIZE_BYTES,
        "description_max": "Hard cap on output size",
        "LARGE_FILE_MB": c.LARGE_FILE_MB,
        "LARGE_FILE_BYTES": c.LARGE_FILE_BYTES,
        "description_large": "Files above this are candidates for splitting",
        "MAX_MEMORY_MB": c.MAX_MEMORY_BYTES // (1024 * 1024),
        "MAX_MEMORY_BYTES": c.MAX_MEMORY_BYTES,
        "description_memory": "Max input accumulated in memory during compaction",
        "MAX_MEMORY_FILES": c.MAX_MEMORY_FILES,
        "description_memory_files": "Max output files per combine-and-split pass",
    }


def compaction_output():
    """What you get back from successful compaction."""

    return {
        "snapshot_object": {
            "operation_type": "compact",
            "snapshot_id": "Timestamp-based ID",
            "manifest_list": "Path to updated manifest",
            "commit_message": "Describes what was compacted",
            "summary": {
                "deleted-data-files": "Count of files removed",
                "added-data-files": "Count of new files created",
                "deleted-files-size": "Bytes of deleted files (compressed)",
                "added-files-size": "Bytes of new files (compressed)",
                "deleted-records": "Record count deleted",
                "added-records": "Record count added",
                "total-data-files": "Files in dataset now",
                "total-records": "Total records now",
                "agent_meta": {
                    "committer": "The agent that performed compaction",
                    "compaction-algorithm": "brute or performance",
                    "compaction-files-combined": "Files that were merged",
                    "compaction-files-written": "New files created",
                },
            },
        }
    }


def decision_tree():
    """How to decide if compaction is needed."""

    return """
    Is your dataset slow?
    ├─ YES → Check file count (catalog.load_dataset(...).metadata)
    │  ├─ > 50 files → Probably too many small files
    │  └─ < 50 files → Check individual file sizes
    │     ├─ Most < 64MB → GOOD candidate for compaction
    │     └─ Some > 196MB → Split needed
    └─ NO → Compaction probably not needed yet
    
    Best practices:
    - Compact after batch loads with many small files
    - Compact after streaming ingest creates 100+ files
    - DON'T compact after single large writes (waste of time)
    - Consider running nightly in low-traffic periods
    """


def when_to_use_each_strategy():
    """Decision matrix for strategy selection."""

    return """
    Auto-detection (recommended):
    ├─ Dataset has sort orders → uses PERFORMANCE
    └─ Dataset has no sort orders → uses BRUTE
    
    Manual selection:
    ├─ Time-series data (timestamp column) → PERFORMANCE
    ├─ Event logs with partition key → PERFORMANCE
    ├─ Unordered catalog data → BRUTE
    ├─ Append-only fact table → BRUTE
    └─ Uncertain? → Auto-detect is usually correct
    """


def troubleshooting():
    """Common issues and solutions."""

    return {
        "compact() returns None": {
            "cause": "No compaction opportunities found",
            "meaning": "All files already in acceptable size range",
            "action": "Wait for more data before trying again",
        },
        "plan shows no files to compact": {
            "cause": "Dataset already optimized",
            "meaning": "File count and sizes are within thresholds",
            "action": "No action needed",
        },
        "manifest not found after compaction": {
            "cause": "Firestore write failed",
            "meaning": "Snapshot was created but metadata didn't persist",
            "action": "Check Firestore connectivity and permissions",
        },
        "Memory error during compaction": {
            "cause": "Tried to combine too many large files",
            "meaning": "Hit 280MB memory limit",
            "action": "Compactor should auto-limit (check logs), retry",
        },
        "Record counts don't match": {
            "cause": "Data corruption during I/O",
            "meaning": "Records were lost during write",
            "action": "Check storage write permissions, retry",
        },
    }


def performance_notes():
    """Performance characteristics."""

    return """
    File selection:
    - Brute: O(n) where n = file count → ~1ms for 100 files
    - Performance: O(n log n) due to sorting → ~2ms for 100 files
    
    Compaction execution:
    - Reading files: I/O bound, ~100ms per 128MB
    - Combining: Fast, ~50ms per 128MB
    - Sorting (if performance mode): ~500ms per 128MB
    - Writing: I/O bound, ~100ms per 128MB
    
    Total for 2×128MB combine: ~300-400ms (mostly I/O)
    Total for 256MB split: ~300-400ms (mostly I/O)
    
    GCS operations:
    - Multiple small reads → slower than single large read
    - Sequential writes → faster with larger files
    - Recommendation: Compact when > 50 files of < 64MB each
    """


def integration_ideas():
    """How to integrate compaction into your system."""

    return [
        {
            "trigger": "After batch load",
            "code": """
            dataset.append(large_batch)
            if len(dataset.metadata.current_snapshot.manifest_list) > 20:
                DatasetCompactor(dataset).compact()
            """,
        },
        {
            "trigger": "Scheduled nightly",
            "code": """
            for dataset_id in catalog.list_datasets(collection):
                dataset = catalog.load_dataset(dataset_id)
                DatasetCompactor(dataset, agent="nightly-compactor").compact()
            """,
        },
        {
            "trigger": "On data quality check",
            "code": """
            if data_quality_issues_found:
                # Compact before re-analysis
                DatasetCompactor(dataset).compact()
            """,
        },
        {
            "trigger": "Custom policy",
            "code": """
            def should_compact(dataset):
                snap = dataset.metadata.current_snapshot
                # Check size, age, file count
                return file_count > 30 or size_mb < avg_file_size
            
            if should_compact(dataset):
                DatasetCompactor(dataset).compact()
            """,
        },
    ]


def print_all():
    """Print a comprehensive reference."""

    print("=" * 70)
    print("COMPACTION QUICK REFERENCE")
    print("=" * 70)

    print("\n📌 WHAT IS COMPACTION?")
    print(what_is_compaction())

    print("\n⚡ GET STARTED IN 3 LINES")
    print(quick_start())

    print("\n🎯 TWO STRATEGIES")
    for name, details in strategies().items():
        print(f"\n  {name.upper()}:")
        for k, v in details.items():
            print(f"    {k}: {v}")

    print("\n📚 API REFERENCE")
    for method, info in api_reference().items():
        print(f"\n  {method}")
        for k, v in info.items():
            print(f"    {k}: {v}")

    print("\n📏 FILE SIZE THRESHOLDS (Constants)")
    for k, v in file_size_thresholds().items():
        if not k.startswith("description"):
            print(f"  {k}: {v}")

    print("\n📊 COMPACTION OUTPUT")
    import json

    print(json.dumps(compaction_output(), indent=2))

    print("\n🌳 DECISION TREE")
    print(decision_tree())

    print("\n🎯 STRATEGY SELECTION")
    print(when_to_use_each_strategy())

    print("\n🔧 TROUBLESHOOTING")
    for issue, details in troubleshooting().items():
        print(f"\n  {issue}:")
        for k, v in details.items():
            print(f"    {k}: {v}")

    print("\n⏱️  PERFORMANCE NOTES")
    print(performance_notes())

    print("\n💡 INTEGRATION IDEAS")
    for i, idea in enumerate(integration_ideas(), 1):
        print(f"\n  {i}. {idea['trigger']}")
        print(f"     {idea['code'].strip()}")

    print("\n" + "=" * 70)
    print("For full details, see: COMPACTION_USAGE.md")
    print("=" * 70)


if __name__ == "__main__":
    print_all()
