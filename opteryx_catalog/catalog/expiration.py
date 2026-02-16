"""
Snapshot expiration and garbage collection module.

Manages snapshot retention policies and cleans up:
1. Old snapshots (keeping only retained-snapshot-age-days)
2. Associated manifest files
3. Orphaned data files no longer referenced by any retained snapshot
"""

from __future__ import annotations

import logging
import time
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

from .metadata import Snapshot

logger = logging.getLogger(__name__)


class SnapshotExpiration:
    """
    Manages snapshot retention and garbage collection for datasets.

    Keeps datasets within their configured retention policy by:
    1. Identifying snapshots eligible for deletion
    2. Removing snapshots from Firestore
    3. Deleting associated manifest files
    4. Identifying and deleting orphaned data files
    """

    def __init__(self, catalog, author: Optional[str] = None, agent: Optional[str] = None):
        """
        Initialize snapshot expiration manager.

        Args:
            catalog: OpteryxCatalog instance
            author: Author name for tracking
            agent: Agent identifier (e.g., "garbage-collector")
        """
        self.catalog = catalog
        self.author = author or "system"
        self.agent = agent or "snapshot-expiration"
        self.deleted_snapshots = []
        self.deleted_manifests = []
        self.deleted_files = []

    def expire_dataset(self, identifier: str, dry_run: bool = False) -> Optional[Dict]:
        """
        Apply retention policy to a single dataset.

        Keeps snapshots based on age. Null or missing value means keep only
        the current (latest) snapshot - data is unversioned.

        Args:
            identifier: Dataset identifier (collection.name)
            dry_run: If True, plan only without executing deletes

        Returns:
            Summary dict or None if no expiration needed
        """
        try:
            # Load dataset with full history
            dataset = self.catalog.load_dataset(identifier, load_history=True)
            if not dataset or not dataset.metadata.snapshots:
                return None

            # Get retention policy
            policy = dataset.metadata.maintenance_policy or {}
            retention_days = policy.get("retained-snapshot-age-days")

            snapshots = dataset.metadata.snapshots
            if not snapshots:
                return None

            # Determine which snapshots to keep
            if retention_days is None or retention_days == 0:
                # Keep only current (latest) snapshot - unversioned
                snapshots_to_keep = [snapshots[-1]]  # Last snapshot only
            elif retention_days < 0:
                # Negative means unlimited retention
                return None
            else:
                # Keep snapshots within the age window
                current_time_ms = int(time.time() * 1000)
                cutoff_time_ms = current_time_ms - (retention_days * 24 * 60 * 60 * 1000)

                snapshots_to_keep = [
                    s for s in snapshots if (s.timestamp_ms or 0) >= cutoff_time_ms
                ]

                # Always keep at least the current snapshot
                if snapshots[-1] not in snapshots_to_keep:
                    snapshots_to_keep.append(snapshots[-1])

            if len(snapshots_to_keep) >= len(snapshots):
                # All snapshots are within retention window
                return None

            # Identify snapshots to delete
            snapshots_to_delete = [s for s in snapshots if s not in snapshots_to_keep]

            summary = {
                "identifier": identifier,
                "retention_days": retention_days,
                "snapshots_to_delete": len(snapshots_to_delete),
                "snapshots_to_keep": len(snapshots_to_keep),
                "deleted_snapshots": [],
                "deleted_manifests": [],
                "deleted_files": [],
                "orphaned_files_count": 0,
            }

            if dry_run:
                # Plan only: identify what would be deleted
                summary["deleted_snapshots"] = [
                    {
                        "snapshot_id": s.snapshot_id,
                        "timestamp_ms": s.timestamp_ms,
                        "manifest": s.manifest_list,
                    }
                    for s in snapshots_to_delete
                ]

                # Find orphaned files
                kept_files = self._get_files_in_snapshots(snapshots_to_keep)
                deleted_files = self._get_files_in_snapshots(snapshots_to_delete)
                orphaned = deleted_files - kept_files
                summary["orphaned_files_count"] = len(orphaned)

                return summary

            # Execute deletion
            return self._execute_expiration(
                identifier, dataset, snapshots_to_delete, snapshots_to_keep
            )

        except (ValueError, KeyError, AttributeError) as e:
            logger.error(f"Error expiring dataset {identifier}: {e}")
            return None

    def expire_collection(self, collection: str, dry_run: bool = False) -> Dict[str, any]:
        """
        Apply retention policy to all datasets in a collection.

        Args:
            collection: Collection name
            dry_run: If True, plan only without executing

        Returns:
            Summary of expiration results
        """
        datasets = self.catalog.list_datasets(collection)
        results = {
            "collection": collection,
            "datasets_processed": 0,
            "datasets_expiring": 0,
            "total_snapshots_deleted": 0,
            "total_manifests_deleted": 0,
            "total_files_deleted": 0,
            "total_orphaned_files": 0,
            "details": [],
        }

        for dataset_name in datasets:
            identifier = f"{collection}.{dataset_name}"
            summary = self.expire_dataset(identifier, dry_run=dry_run)
            results["datasets_processed"] += 1

            if summary:
                results["datasets_expiring"] += 1
                results["total_snapshots_deleted"] += summary.get("snapshots_to_delete", 0)
                results["total_manifests_deleted"] += len(summary.get("deleted_manifests", []))
                results["total_files_deleted"] += len(summary.get("deleted_files", []))
                results["total_orphaned_files"] += summary.get("orphaned_files_count", 0)
                results["details"].append(summary)

        return results

    def expire_workspace(self, dry_run: bool = False) -> Dict[str, any]:
        """
        Apply retention policy to all datasets in workspace.

        Args:
            dry_run: If True, plan only without executing

        Returns:
            Summary of expiration results
        """
        collections = self.catalog.list_collections()
        results = {
            "workspace": self.catalog.workspace,
            "collections_processed": 0,
            "datasets_processed": 0,
            "datasets_expiring": 0,
            "total_snapshots_deleted": 0,
            "total_manifests_deleted": 0,
            "total_files_deleted": 0,
            "total_orphaned_files": 0,
            "details": [],
        }

        for collection in collections:
            collection_result = self.expire_collection(collection, dry_run=dry_run)
            results["collections_processed"] += 1
            results["datasets_processed"] += collection_result.get("datasets_processed", 0)
            results["datasets_expiring"] += collection_result.get("datasets_expiring", 0)
            results["total_snapshots_deleted"] += collection_result.get(
                "total_snapshots_deleted", 0
            )
            results["total_manifests_deleted"] += collection_result.get(
                "total_manifests_deleted", 0
            )
            results["total_files_deleted"] += collection_result.get("total_files_deleted", 0)
            results["total_orphaned_files"] += collection_result.get("total_orphaned_files", 0)

            if collection_result.get("details"):
                results["details"].extend(collection_result["details"])

        return results

    def _execute_expiration(
        self,
        identifier: str,
        dataset,
        snapshots_to_delete: List[Snapshot],
        snapshots_to_keep: List[Snapshot],
    ) -> Dict:
        """
        Execute snapshot expiration: delete snapshots, manifests, and orphaned files.

        Args:
            identifier: Dataset identifier
            dataset: Dataset metadata object
            snapshots_to_delete: Snapshots to remove
            snapshots_to_keep: Snapshots to retain

        Returns:
            Summary of deletions
        """
        collection, dataset_name = identifier.split(".")
        summary = {
            "identifier": identifier,
            "snapshots_to_delete": len(snapshots_to_delete),
            "snapshots_to_keep": len(snapshots_to_keep),
            "deleted_snapshots": [],
            "deleted_manifests": [],
            "deleted_files": [],
        }

        # Step 1: Find which files are kept and which are orphaned
        kept_files = self._get_files_in_snapshots(snapshots_to_keep)
        deleted_files_snapshot = self._get_files_in_snapshots(snapshots_to_delete)
        orphaned_files = deleted_files_snapshot - kept_files

        # Step 2: Delete snapshots from Firestore
        # pylint: disable=protected-access
        snaps_coll = self.catalog._snapshots_collection(collection, dataset_name)
        for snapshot in snapshots_to_delete:
            try:
                snaps_coll.document(str(snapshot.snapshot_id)).delete()
                summary["deleted_snapshots"].append(
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "timestamp_ms": snapshot.timestamp_ms,
                    }
                )
                logger.info(f"Deleted snapshot {snapshot.snapshot_id} from {identifier}")
            except (ValueError, OSError) as e:
                logger.error(f"Failed to delete snapshot {snapshot.snapshot_id}: {e}")

        # Step 3: Delete manifest files from storage
        for snapshot in snapshots_to_delete:
            if snapshot.manifest_list:
                try:
                    io = self.catalog.io or dataset.io
                    # Delete manifest file (it's just a GCS object)
                    # Note: We may not have a direct delete API, so we log intent
                    manifest_path = snapshot.manifest_list
                    self._delete_file(io, manifest_path)
                    summary["deleted_manifests"].append(manifest_path)
                    logger.info(f"Deleted manifest {manifest_path}")
                except (ValueError, OSError) as e:
                    logger.error(f"Failed to delete manifest {snapshot.manifest_list}: {e}")

        # Step 4: Delete orphaned data files
        for file_path in orphaned_files:
            try:
                io = self.catalog.io or dataset.io
                self._delete_file(io, file_path)
                summary["deleted_files"].append(file_path)
                logger.info(f"Deleted orphaned file {file_path}")
            except (ValueError, OSError) as e:
                logger.error(f"Failed to delete orphaned file {file_path}: {e}")

        return summary

    def _get_files_in_snapshots(self, snapshots: List[Snapshot]) -> Set[str]:
        """
        Get all data files referenced by a set of snapshots.

        Args:
            snapshots: List of snapshots

        Returns:
            Set of file paths referenced in all manifests
        """
        files = set()

        for snapshot in snapshots:
            if not snapshot.manifest_list:
                continue

            try:
                # Read and parse manifest
                io = self.catalog.io
                from .manifest import get_parsed_manifest

                entries = get_parsed_manifest(io, snapshot.manifest_list)

                # Extract file paths
                for entry in entries:
                    file_path = entry.get("file_path")
                    if file_path:
                        files.add(file_path)
            except (ValueError, OSError) as e:
                logger.error(f"Error reading manifest {snapshot.manifest_list}: {e}")

        return files

    def _delete_file(self, io, file_path: str) -> bool:
        """
        Delete a file from storage.

        Args:
            io: FileIO instance
            file_path: Path to file to delete

        Returns:
            True if successful, False otherwise
        """
        try:
            # Attempt to delete via FileIO
            # Note: Not all FileIO implementations support delete
            if hasattr(io, "delete"):
                io.delete(file_path)
                return True
            else:
                # Log that delete not supported
                logger.warning(f"FileIO does not support delete: {file_path}")
                return False
        except (AttributeError, ValueError, OSError) as e:
            logger.error(f"Error deleting file {file_path}: {e}")
            return False


def identify_expiring_datasets(catalog) -> Dict[str, List[str]]:
    """
    Scan workspace and find datasets with snapshots outside retention window.

    Args:
        catalog: OpteryxCatalog instance

    Returns:
        Dict mapping collection -> list of datasets needing expiration
    """
    results = {}

    for collection in catalog.list_collections():
        expiring_datasets = []

        for dataset_name in catalog.list_datasets(collection):
            identifier = f"{collection}.{dataset_name}"
            try:
                dataset = catalog.load_dataset(identifier, load_history=True)
                if not dataset or not dataset.metadata.snapshots:
                    continue

                snapshots = dataset.metadata.snapshots
                policy = dataset.metadata.maintenance_policy or {}
                retention_days = policy.get("retained-snapshot-age-days")

                # Determine expiration eligibility
                if retention_days is None or retention_days == 0:
                    # Keep only current - check if there are older snapshots
                    if len(snapshots) > 1:
                        expiring_datasets.append(
                            {
                                "dataset": dataset_name,
                                "current_snapshots": len(snapshots),
                                "retained_policy": "current only",
                                "excess_snapshots": len(snapshots) - 1,
                            }
                        )
                elif retention_days > 0:
                    # Age-based retention - check if any snapshots are outside window
                    current_time_ms = int(time.time() * 1000)
                    cutoff_time_ms = current_time_ms - (retention_days * 24 * 60 * 60 * 1000)

                    outside_window = [
                        s for s in snapshots if (s.timestamp_ms or 0) < cutoff_time_ms
                    ]

                    if outside_window:
                        expiring_datasets.append(
                            {
                                "dataset": dataset_name,
                                "current_snapshots": len(snapshots),
                                "retained_policy": f"{retention_days} days",
                                "outside_window": len(outside_window),
                            }
                        )
            except (ValueError, KeyError, AttributeError) as e:
                logger.error(f"Error checking {identifier}: {e}")

        if expiring_datasets:
            results[collection] = expiring_datasets

    return results
