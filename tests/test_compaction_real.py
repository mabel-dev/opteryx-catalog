#!/usr/bin/env python3
"""
Compact the public.github.events dataset.

This script:
1. Loads the catalog using GCP credentials
2. Loads the public.github.events dataset
3. Analyzes current file structure
4. Runs actual compaction (not dry-run)
5. Shows before/after metrics
"""
import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))
sys.path.insert(1, os.path.join(sys.path[0], "../pyiceberg-firestore-gcs"))

import time

# Load config from environment
FIRESTORE_DATABASE = os.environ.get("FIRESTORE_DATABASE")
BUCKET_NAME = os.environ.get("GCS_BUCKET")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
WORKSPACE = os.environ.get("OPTERYX_WORKSPACE", "public")

print(f"Configuration:")
print(f"  GCP_PROJECT_ID: {GCP_PROJECT_ID}")
print(f"  BUCKET_NAME: {BUCKET_NAME}")
print(f"  FIRESTORE_DATABASE: {FIRESTORE_DATABASE}")
print(f"  WORKSPACE: {WORKSPACE}")

if not all([GCP_PROJECT_ID, BUCKET_NAME]):
    print("\n❌ Missing required environment variables:")
    print("   GCP_PROJECT_ID, GCS_BUCKET")
    sys.exit(1)

from opteryx_catalog import OpteryxCatalog
from opteryx_catalog.catalog.compaction import DatasetCompactor


def load_github_events_dataset():
    """Load the public.github.events dataset.
    
    Returns:
        Tuple of (dataset, identifier) where identifier is the working format
    """
    print("\n" + "="*70)
    print("Loading public.github.events Dataset")
    print("="*70)
    
    # Initialize catalog
    catalog = OpteryxCatalog(
        workspace=WORKSPACE,
        firestore_project=GCP_PROJECT_ID,
        firestore_database=FIRESTORE_DATABASE,
        gcs_bucket=BUCKET_NAME,
    )
    
    dataset_id = "github.events"
    print(f"\nLoading dataset: {WORKSPACE}.{dataset_id}")
    
    try:
        dataset = catalog.load_dataset(dataset_id)
        print(f"✓ Successfully loaded: {WORKSPACE}.{dataset_id}")
        return dataset, dataset_id
    except Exception as e:
        print(f"❌ Failed to load dataset: {e}")
        sys.exit(1)


def analyze_dataset(dataset):
    """Show dataset file statistics."""
    metadata = dataset.metadata
    snapshot = metadata.current_snapshot()
    
    if not snapshot:
        print("❌ No current snapshot found")
        return None
    
    entries = []
    manifest_path = snapshot.manifest_list
    
    try:
        # Read manifest
        from opteryx_catalog.catalog.manifest import get_parsed_manifest
        entries = get_parsed_manifest(dataset.io, manifest_path)
    except Exception as e:
        print(f"Failed to read manifest: {e}")
        return None
    
    total_files = len(entries)
    total_uncompressed = sum(e.get("uncompressed_size_in_bytes", 0) for e in entries)
    total_compressed = sum(e.get("file_size_in_bytes", 0) for e in entries)
    
    small = [e for e in entries if e.get("uncompressed_size_in_bytes", 0) < 64 * 1024 * 1024]
    large = [e for e in entries if e.get("uncompressed_size_in_bytes", 0) >= 500 * 1024 * 1024]
    
    print(f"\nDataset Metrics:")
    print(f"  Files: {total_files}")
    print(f"  Uncompressed: {total_uncompressed / 1024 / 1024:.1f} MB")
    print(f"  Compressed: {total_compressed / 1024 / 1024:.1f} MB")
    print(f"  Ratio: {total_compressed / total_uncompressed * 100:.1f}%")
    print(f"  Small files (< 64 MB): {len(small)}")
    print(f"  Large files (≥ 500 MB): {len(large)}")
    
    return {
        "files": total_files,
        "uncompressed": total_uncompressed,
        "compressed": total_compressed,
    }


def run_compaction(dataset):
    """Execute compaction."""
    print("\n" + "="*70)
    print("Running Compaction")
    print("="*70)
    
    compactor = DatasetCompactor(dataset, author="test_compaction", agent="test_agent")
    
    # Check plan
    plan = compactor.compact(dry_run=True)
    if not plan:
        print("ℹ No compaction needed")
        return None
    
    print(f"\nCompaction Plan:")
    print(f"  Type: {plan['type']}")
    print(f"  Reason: {plan['reason']}")
    print(f"  Files to process: {len(plan['files'])}")
    
    total_size = sum(f.get("uncompressed_size_in_bytes", 0) for f in plan["files"])
    print(f"  Size to process: {total_size / 1024 / 1024:.1f} MB")
    
    # Execute
    print(f"\nExecuting compaction...")
    start = time.time()
    new_snapshot = compactor.compact(dry_run=False)
    elapsed = time.time() - start
    
    if new_snapshot:
        print(f"✓ Compaction completed in {elapsed:.1f}s")
        print(f"  New snapshot ID: {new_snapshot.snapshot_id}")
        if hasattr(new_snapshot, 'summary'):
            print(f"  Summary: {new_snapshot.summary}")
        return new_snapshot
    else:
        print(f"❌ Compaction failed")
        return None


def main():
    try:
        # Load github.events dataset
        dataset, dataset_id = load_github_events_dataset()
        
        # Analyze before
        print("\n" + "="*70)
        print("Before Compaction")
        print("="*70)
        before = analyze_dataset(dataset)
        
        if not before:
            print("❌ Could not analyze dataset")
            sys.exit(1)
        
        # Run compaction
        snapshot = run_compaction(dataset)
        
        if snapshot:
            # Reload and analyze after
            print("\n" + "="*70)
            print("After Compaction")
            print("="*70)
            dataset = OpteryxCatalog(
                workspace=WORKSPACE,
                firestore_project=GCP_PROJECT_ID,
                firestore_database=FIRESTORE_DATABASE,
                gcs_bucket=BUCKET_NAME,
            ).load_dataset(dataset_id)
            
            after = analyze_dataset(dataset)
            
            # Summary
            if before and after:
                print("\n" + "="*70)
                print("Summary")
                print("="*70)
                reduction = (before["files"] - after["files"]) / before["files"] * 100
                print(f"  Files reduced: {before['files']} → {after['files']} ({reduction:.1f}% reduction)")
                print(f"  Data preserved: {after['uncompressed'] == before['uncompressed']}")
        
        print("\n✓ Compaction completed successfully")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
