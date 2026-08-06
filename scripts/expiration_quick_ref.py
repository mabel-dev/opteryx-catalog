#!/usr/bin/env python3
"""
Quick reference: Snapshot Expiration API

This shows the key methods and patterns for using the expiration module.
"""


def quick_example():
    """Copy-paste to get started."""

    example = """
    from opteryx_catalog import OpteryxCatalog
    from opteryx_catalog.catalog.expiration import SnapshotExpiration
    
    catalog = OpteryxCatalog(workspace="prod", gcs_bucket="data-lake")
    expiration = SnapshotExpiration(catalog, author="maintenance")
    
    # Plan: See what would be deleted
    plan = expiration.expire_dataset("analytics.events", dry_run=True)
    print(f"Snapshots to delete: {plan['snapshots_to_delete']}")
    print(f"Orphaned files: {plan['orphaned_files_count']}")
    
    # Execute: Actually delete
    result = expiration.expire_dataset("analytics.events", dry_run=False)
    print(f"Deleted {len(result['deleted_files'])} files")
    """
    return example


def api_methods():
    """All available methods."""
    return {
        "expire_dataset(identifier, dry_run=False)": {
            "description": "Clean one dataset",
            "identifier": "collection.dataset_name",
            "returns": "Dict with deleted_snapshots, deleted_manifests, deleted_files",
            "example": "expiration.expire_dataset('analytics.events', dry_run=False)",
        },
        "expire_collection(collection, dry_run=False)": {
            "description": "Clean all datasets in collection",
            "returns": "Dict with aggregate stats",
            "example": "expiration.expire_collection('analytics', dry_run=False)",
        },
        "expire_workspace(dry_run=False)": {
            "description": "Clean entire workspace",
            "returns": "Dict with workspace-wide stats",
            "example": "expiration.expire_workspace()",
        },
    }


def retention_policy_examples():
    """Common retention policies."""
    return {
        "Keep only latest": {
            "config": {"retained-snapshot-age-days": None},
            "use_case": "Dev/test datasets, unversioned data",
            "example": "Only current snapshot kept",
        },
        "Keep last week": {
            "config": {"retained-snapshot-age-days": 7},
            "use_case": "Production datasets, week of rollback",
            "example": "All snapshots from last 7 days kept",
        },
        "Keep last 30 days": {
            "config": {"retained-snapshot-age-days": 30},
            "use_case": "Production datasets, monthly retention",
            "example": "All snapshots from last 30 days kept",
        },
        "Unlimited": {
            "config": {"retained-snapshot-age-days": -1},
            "use_case": "Audit/compliance, never delete",
            "example": "All snapshots preserved forever",
        },
    }


def workflow_examples():
    """Real-world workflows."""

    return {
        "Inspect what needs cleanup": """
from opteryx_catalog.catalog.expiration import identify_expiring_datasets

results = identify_expiring_datasets(catalog)
for collection, datasets in results.items():
    for ds in datasets:
        print(f"{collection}.{ds['dataset']}: "
              f"{ds['outside_window']} snapshots can be deleted")
""",
        "Dry-run before deleting": """
expiration = SnapshotExpiration(catalog)
plan = expiration.expire_dataset("analytics.events", dry_run=True)

if plan and plan["orphaned_files_count"] > 1000:
    print(f"Would delete {plan[\"orphaned_files_count\"]} files!")
    # Manual approval before executing
else:
    result = expiration.expire_dataset("analytics.events", dry_run=False)
""",
        "Cleanup entire collection": """
expiration = SnapshotExpiration(catalog, agent="cleanup-service")
result = expiration.expire_collection("analytics", dry_run=False)

print(f"Processed {result[\"datasets_processed\"]} datasets")
print(f"Deleted {result[\"total_files_deleted\"]} orphaned files")
""",
        "Automated nightly cleanup": """
import schedule

def nightly_cleanup():
    expiration = SnapshotExpiration(catalog)
    result = expiration.expire_workspace(dry_run=False)
    logging.info(f"Cleanup: {result[\"total_files_deleted\"]} files deleted")

schedule.every().day.at("02:00").do(nightly_cleanup)
""",
    }


def how_orphaned_files_detected():
    """Explain orphaned file detection."""
    return """
    ALGORITHM:
    
    1. For each snapshot being DELETED:
       files_in_deleted = read all manifests → union of all file_path values
    
    2. For each snapshot being KEPT:
       files_in_kept = read all manifests → union of all file_path values
    
    3. Orphaned files = files_in_deleted - files_in_kept
       (Files that only appear in deleted snapshots)
    
    EXAMPLE:
    
    Snapshot 1 manifest: [FileA, FileB, FileC]
    Snapshot 2 manifest: [FileA, FileB, FileD]
    Snapshot 3 manifest: [FileA, FileE]      ← KEEP THIS ONE
    
    files_in_deleted = {A, B, C} ∪ {A, B, D} = {A, B, C, D}
    files_in_kept = {A, E}
    orphaned = {A, B, C, D} - {A, E} = {B, C, D}
    
    Result: Delete files B, C, D from storage
    Result: Keep file A (still referenced in snapshot 3)
    """


def safety_guarantees():
    """What's guaranteed to be safe."""
    return {
        "Won't delete files still referenced": "Checked against all retained snapshots",
        "Won't delete snapshots within retention window": "Only deletes snapshots outside age window",
        "Idempotent": "Safe to run multiple times",
        "Dry-run safe": "No changes when dry_run=True",
        "Partial failures safe": "If Phase N fails, don't continue to Phase N+1",
    }


def troubleshooting_table():
    """Common issues and solutions."""
    return {
        "Nothing to delete": {
            "cause": "All datasets within retention policy",
            "solution": "Create more snapshots first (append data, compact, etc)",
        },
        "Snapshots won't delete": {
            "cause": "Firestore permissions or connectivity",
            "solution": "Check GCP IAM, Firestore access, network",
        },
        "Files still exist after cleanup": {
            "cause": "Files weren't actually orphaned (still in some snapshot)",
            "solution": "Run dry_run=True to verify before executing",
        },
        "Cleanup takes too long": {
            "cause": "Processing entire workspace or large collections",
            "solution": "Run on per-collection basis, schedule during off-hours",
        },
    }


def performance_tips():
    """How to optimize expiration performance."""
    return """
    1. Run during low-traffic periods
       → Reduces contention with queries
       → Faster Firestore/GCS operations
    
    2. Process per-collection, not entire workspace
       → Can parallelize across collections
       → Easier to monitor progress
    
    3. Set retention policy appropriately
       → More lenient = fewer deletions
       → Stricter = more frequent cleanup
    
    4. Monitor before automating
       → Run dry_run=True first
       → Verify orphaned file detection works
       → Then automate
    
    5. Combine with compaction
       → Compact THEN expire (in that order)
       → Compaction creates many new snapshots
       → Expiration cleans up old ones
    """


def print_all():
    """Print complete reference."""

    print("=" * 80)
    print("SNAPSHOT EXPIRATION - QUICK REFERENCE")
    print("=" * 80)

    print("\n⚡ GET STARTED")
    print(quick_example())

    print("\n📚 API METHODS")
    for method, info in api_methods().items():
        print(f"\n  {method}")
        for k, v in info.items():
            print(f"    {k}: {v}")

    print("\n📋 RETENTION POLICIES")
    for name, config in retention_policy_examples().items():
        print(f"\n  {name}:")
        for k, v in config.items():
            print(f"    {k}: {v}")

    print("\n🔧 WORKFLOWS")
    for title, code in workflow_examples().items():
        print(f"\n  {title}:")
        for line in code.strip().split("\n"):
            print(f"    {line}")

    print("\n🔍 ORPHANED FILE DETECTION")
    print(how_orphaned_files_detected())

    print("\n✅ SAFETY GUARANTEES")
    for guarantee, detail in safety_guarantees().items():
        print(f"  ✓ {guarantee}: {detail}")

    print("\n🐛 TROUBLESHOOTING")
    for issue, info in troubleshooting_table().items():
        print(f"\n  {issue}:")
        for k, v in info.items():
            print(f"    {k}: {v}")

    print("\n⏱️  PERFORMANCE TIPS")
    print(performance_tips())

    print("\n" + "=" * 80)
    print("For full details, see: SNAPSHOT_EXPIRATION.md")
    print("=" * 80)


if __name__ == "__main__":
    print_all()
