#!/usr/bin/env python3
"""
Snapshot expiration utility - inspect what needs cleanup.

Usage:
  python inspect_snapshots.py                 # Scan all collections
  python inspect_snapshots.py collection      # Scan one collection
  python inspect_snapshots.py collection.ds   # Check one dataset
"""

import os
import sys

from opteryx_catalog import OpteryxCatalog
from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.expiration import identify_expiring_datasets

# Add local paths to sys.path to use local code instead of installed packages
sys.path.insert(0, os.path.join(sys.path[0], ".."))  # Add parent dir for opteryx_catalog
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-catalog"))


FIRESTORE_DATABASE = os.environ.get("FIRESTORE_DATABASE")
BUCKET_NAME = os.environ.get("GCS_BUCKET")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")


def inspect_workspace(catalog):
    """Scan entire workspace for expiring datasets."""
    print("\n" + "=" * 80)
    print("SNAPSHOT EXPIRATION SCAN - WORKSPACE")
    print("=" * 80)

    results = identify_expiring_datasets(catalog)

    if not results:
        print("\n✓ All datasets are within retention policy")
        return

    total_excess = 0
    total_cleanup_snapshots = 0

    for collection, datasets in results.items():
        print(f"\n📦 Collection: {collection}")
        print("-" * 80)

        for ds in datasets:
            excess = ds["excess_snapshots"]
            total_excess += excess
            total_cleanup_snapshots += ds["current_snapshots"] - ds["retained_snapshots"]

            print(f"  {ds['dataset']}")
            print(f"    Current snapshots:  {ds['current_snapshots']}")
            print(f"    Retained snapshots: {ds['retained_snapshots']}")
            print(f"    Can delete:         {excess}")

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Collections with expiring datasets: {len(results)}")
    print(f"Total datasets needing cleanup: {sum(len(ds) for ds in results.values())}")
    print(f"Total excess snapshots: {total_excess}")
    print(f"Total snapshots deletable: {total_cleanup_snapshots}")


def inspect_collection(catalog, collection: str):
    """Scan one collection for expiring datasets."""
    print("\n" + "=" * 80)
    print(f"SNAPSHOT EXPIRATION SCAN - COLLECTION '{collection}'")
    print("=" * 80)

    try:
        datasets = list(catalog.list_datasets(collection))
    except Exception as e:
        print(f"✗ Error listing datasets: {e}")
        return

    print(f"\nFound {len(datasets)} datasets\n")

    expiring = []

    for dataset_name in datasets:
        identifier = f"{collection}.{dataset_name}"
        try:
            dataset = catalog.load_dataset(identifier, load_history=True)
            if not dataset or not dataset.metadata.snapshots:
                print(f"  {dataset_name}: NO SNAPSHOTS")
                continue

            snapshots = dataset.metadata.snapshots
            policy = dataset.metadata.maintenance_policy or {}
            retention_days = policy.get("retained-snapshot-age-days")

            # Check if any snapshots are outside retention window
            if retention_days is None or retention_days == 0:
                outside_window = len(snapshots) - 1  # All but current
            elif retention_days < 0:
                outside_window = 0  # Unlimited
            else:
                import time

                current_time_ms = int(time.time() * 1000)
                cutoff_time_ms = current_time_ms - (retention_days * 24 * 60 * 60 * 1000)
                outside_window = sum(1 for s in snapshots if (s.timestamp_ms or 0) < cutoff_time_ms)

            if outside_window == 0:
                policy_str = (
                    f"{retention_days} days"
                    if retention_days and retention_days > 0
                    else "current only"
                )
                print(f"  {dataset_name}: {len(snapshots)} snapshots (within policy: {policy_str})")
            else:
                expiring.append(
                    {
                        "dataset": dataset_name,
                        "current": len(snapshots),
                        "outside_window": outside_window,
                        "policy": retention_days,
                    }
                )
                print(
                    f"  {dataset_name}: {len(snapshots)} snapshots (⚠️  {outside_window} outside window, policy: {retention_days} days)"
                )
        except Exception as e:
            print(f"  {dataset_name}: ERROR - {e}")

    if expiring:
        print("\n" + "-" * 80)
        print(f"\n{len(expiring)} datasets need cleanup:\n")
        for ds in expiring:
            print(f"  {ds['dataset']:40s} {ds['outside_window']} snapshots can be deleted")

        total_excess = sum(ds["outside_window"] for ds in expiring)
        print(f"\nTotal snapshots deletable: {total_excess}")
    else:
        print("\n✓ All datasets in this collection are within retention policy")


def inspect_dataset(catalog, identifier: str):
    """Inspect one dataset in detail."""
    collection, dataset_name = identifier.split(".")

    print("\n" + "=" * 80)
    print(f"SNAPSHOT EXPIRATION PLAN - '{identifier}'")
    print("=" * 80)

    try:
        dataset = catalog.load_dataset(identifier, load_history=True)
    except Exception as e:
        print(f"✗ Error loading dataset: {e}")
        return

    if not dataset or not dataset.metadata.snapshots:
        print("✗ No snapshots found")
        return

    import time

    snapshots = dataset.metadata.snapshots
    policy = dataset.metadata.maintenance_policy or {}
    retention_days = policy.get("retained-snapshot-age-days")

    print("\nRetention Policy:")
    print(f"  retained-snapshot-age-days: {retention_days}")
    print(f"  compaction-policy: {policy.get('compaction-policy', 'N/A')}")

    # Determine what would be deleted
    if retention_days is None or retention_days == 0:
        snapshots_to_keep = [snapshots[-1]]
    elif retention_days < 0:
        snapshots_to_keep = snapshots
    else:
        current_time_ms = int(time.time() * 1000)
        cutoff_time_ms = current_time_ms - (retention_days * 24 * 60 * 60 * 1000)
        snapshots_to_keep = [s for s in snapshots if (s.timestamp_ms or 0) >= cutoff_time_ms]
        if snapshots[-1] not in snapshots_to_keep:
            snapshots_to_keep.append(snapshots[-1])

    print(f"\nSnapshots: {len(snapshots)} total")
    print(f"  Retained: {len(snapshots_to_keep)}")
    print(f"  Can delete: {max(0, len(snapshots) - len(snapshots_to_keep))}")

    if len(snapshots) > len(snapshots_to_keep):
        to_delete = [s for s in snapshots if s not in snapshots_to_keep]

        print(f"\n❌ Snapshots to delete ({len(to_delete)}):\n")
        for i, snap in enumerate(to_delete):
            print(f"  {i + 1}. ID {snap.snapshot_id} (timestamp: {snap.timestamp_ms})")
            if snap.manifest_list:
                print(f"     Manifest: {snap.manifest_list}")

        print(f"\n✓ Snapshots to keep ({len(snapshots_to_keep)}):\n")
        for i, snap in enumerate(snapshots_to_keep):
            print(f"  {i + 1}. ID {snap.snapshot_id} (timestamp: {snap.timestamp_ms})")
            if snap.manifest_list:
                print(f"     Manifest: {snap.manifest_list}")

        # Show plan
        print("\n" + "-" * 80)
        print("EXPIRATION PLAN (dry-run):\n")

        expiration = SnapshotExpiration(catalog, author="inspector")
        plan = expiration.expire_dataset(identifier, dry_run=False)

        if plan:
            print(f"  Snapshots to delete: {plan['snapshots_to_delete']}")
            print(f"  Manifests to delete: {len(plan.get('deleted_manifests', []))}")
            print(f"  Orphaned files: {plan.get('orphaned_files_count', 0)}")

            print("\nTo execute cleanup:")
            print(f"  expiration.expire_dataset('{identifier}', dry_run=False)")
        else:
            print("  No expiration plan (already within policy)")
    else:
        print("\n✓ Dataset is within retention policy, no cleanup needed")


def _confirm(prompt: str) -> bool:
    """Ask the user to confirm a destructive action. Returns True for yes."""
    try:
        ans = input(prompt + " [y/N]: ")
        return ans.strip().lower() in ("y", "yes")
    except Exception:
        return False


def main():
    """Main entry point."""
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Inspect and (optionally) expire snapshots")
    parser.add_argument(
        "identifier",
        nargs="?",
        help="Optional collection or collection.dataset identifier to inspect",
    )
    parser.add_argument(
        "--apply",
        "-a",
        action="store_true",
        help="Execute expiration for the inspected target (destructive)",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="When used with --apply, do not prompt for confirmation",
    )
    parser.add_argument(
        "--workspace", help="Workspace name to use (overrides default)", default=None
    )

    args = parser.parse_args()

    wk = args.workspace or os.environ.get("OPTERYX_WORKSPACE") or "opteryx"
    catalog = OpteryxCatalog(
        workspace=wk,
        firestore_project=GCP_PROJECT_ID,
        firestore_database=FIRESTORE_DATABASE,
        gcs_bucket=BUCKET_NAME,
    )

    # No identifier -> inspect workspace
    if not args.identifier:
        inspect_workspace(catalog)

        if args.apply:
            expiration = SnapshotExpiration(catalog, author="inspector")
            plan = expiration.expire_workspace(dry_run=True)
            print("\nWorkspace expiration plan (dry-run):")
            print(json.dumps(plan, indent=2))

            if not args.yes and not _confirm(
                "Proceed with workspace expiration? This will delete snapshots/manifests/files"
            ):
                print("Aborted")
                return

            result = expiration.expire_workspace(dry_run=False)
            print("\nWorkspace expiration result:")
            print(json.dumps(result, indent=2))

        return

    identifier = args.identifier

    # Dataset identifier
    if "." in identifier:
        inspect_dataset(catalog, identifier)

        if args.apply:
            expiration = SnapshotExpiration(catalog, author="inspector")
            plan = expiration.expire_dataset(identifier, dry_run=True)
            print("\nExpiration plan (dry-run):")
            print(json.dumps(plan, indent=2))

            if not plan:
                print("Nothing to do")
                return

            if not args.yes and not _confirm(
                f"Proceed and execute expiration for '{identifier}'? This is destructive"
            ):
                print("Aborted")
                return

            result = expiration.expire_dataset(identifier, dry_run=False)
            print("\nExpiration result:")
            print(json.dumps(result, indent=2))

        return

    # Collection identifier
    collection = identifier
    inspect_collection(catalog, collection)

    if args.apply:
        expiration = SnapshotExpiration(catalog, author="inspector")
        plan = expiration.expire_collection(collection, dry_run=True)
        print("\nCollection expiration plan (dry-run):")
        print(json.dumps(plan, indent=2))

        if not plan or not plan.get("datasets_expiring"):
            print("Nothing to do for collection")
            return

        if not args.yes and not _confirm(
            f"Proceed and execute expiration for collection '{collection}'? This is destructive"
        ):
            print("Aborted")
            return

        result = expiration.expire_collection(collection, dry_run=False)
        print("\nCollection expiration result:")
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
