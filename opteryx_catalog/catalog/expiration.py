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

from .dataset import select_last_user_snapshot
from .metadata import Snapshot

logger = logging.getLogger(__name__)

# Memory management: limit snapshot processing to avoid excessive memory usage
# when loading manifests for orphaned file detection. Kept well above a single
# day's snapshot count for datasets on 15-minutely maintenance schedules
# (~96/day) so same-day runs never trip it.
MAX_SNAPSHOTS_FOR_ORPHAN_DETECTION = 2500

# How far back expiration looks for a user commit to protect from deletion.
# See the call site in _expire_dataset: deep enough to survive a burst of
# maintenance snapshots landing on top of a real write, shallow enough that a
# write-once/maintain-forever dataset does not pin its first snapshot (and
# every data file that snapshot references) in storage permanently.
USER_SNAPSHOT_LOOKBACK = 10

# Manifest orphan cleanup: only delete manifest files older than this (ms)
MANIFEST_ORPHAN_MIN_AGE_MS = 24 * 60 * 60 * 1000  # 1 day

# Data-file deep-clean: a physical file must be at least this old before it's
# eligible for deletion as "orphaned". Data files have no reliable
# timestamp-in-filename convention (unlike manifests), so age comes from
# storage object metadata; a file whose age can't be determined is treated as
# not-old-enough. This guards against deleting a file that was just uploaded
# by an in-flight append/compaction whose snapshot commit hasn't landed yet.
DATA_FILE_ORPHAN_MIN_AGE_MS = MANIFEST_ORPHAN_MIN_AGE_MS


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
        # Timed here rather than by the caller: callers that expire a whole
        # workspace or collection only see the aggregate, so per-dataset cost
        # is measurable at this level alone.
        start = time.perf_counter()
        summary = self._expire_dataset(identifier, dry_run=dry_run)
        if summary is not None:
            summary["duration_ms"] = int((time.perf_counter() - start) * 1000)
        return summary

    def _expire_dataset(self, identifier: str, dry_run: bool = False) -> Optional[Dict]:
        """Apply retention policy to a single dataset (see `expire_dataset`)."""
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

            # Always retain the most recent USER commit, in both branches
            # above. Maintenance writes snapshots of its own — compaction,
            # statistics refresh, expiration — so the newest snapshot is
            # routinely not one anybody made by hand. Without this, a dataset
            # that is written rarely but maintained often loses the last
            # thing a user actually did, and the UI is left showing only
            # commits the user never made (or nothing at all).
            #
            # Bounded to the last USER_SNAPSHOT_LOOKBACK snapshots on
            # purpose: a dataset written once by a human and maintained
            # automatically forever would otherwise pin its very first
            # snapshot, and everything it references, in storage for good. A
            # user commit buried deeper than that window is allowed to
            # expire, leaving the latest snapshot (already retained above) as
            # what the UI shows. An imperfect trade, chosen deliberately.
            protected_user_snapshot = select_last_user_snapshot(
                snapshots, lookback=USER_SNAPSHOT_LOOKBACK
            )
            if (
                protected_user_snapshot is not None
                and protected_user_snapshot not in snapshots_to_keep
            ):
                snapshots_to_keep.append(protected_user_snapshot)
                logger.info(
                    "Retaining last user snapshot %s for %s (outside retention window)",
                    protected_user_snapshot.snapshot_id,
                    identifier,
                )

            # Decide whether to skip expensive orphan-detection based on snapshot count
            total_snapshots = len(snapshots)
            skip_orphan_detection = total_snapshots > MAX_SNAPSHOTS_FOR_ORPHAN_DETECTION

            if len(snapshots_to_keep) >= len(snapshots):
                # All snapshots are within retention window — still attempt manifest-only tidy-up
                logger.debug("ENTER MANIFEST-ONLY BLOCK, dry_run=%s", dry_run)
                if skip_orphan_detection:
                    # Skip heavy scanning when snapshot history is large
                    return None

                # Check for orphaned manifests older than the configured min age
                try:
                    import re

                    from .deep_clean import DatasetDeepClean

                    cleaner = DatasetDeepClean(self.catalog)
                    orphaned_manifests = cleaner.get_orphaned_manifests(identifier) or set()

                    now_ms = int(time.time() * 1000)
                    eligible = []
                    skipped_recent = []
                    for m in orphaned_manifests:
                        mname = m.rsplit("/", 1)[-1]
                        match = re.search(r"manifest-(\d+)\.parquet$", mname)
                        if match:
                            ts = int(match.group(1))
                            if now_ms - ts >= MANIFEST_ORPHAN_MIN_AGE_MS:
                                eligible.append(m)
                            else:
                                skipped_recent.append(m)
                        else:
                            skipped_recent.append(m)

                    # Full reconciliation for orphaned *data* files: the delta
                    # method below has nothing to diff against here (no
                    # snapshots are being deleted this run), so this is the
                    # only path that can ever catch data files orphaned by a
                    # past event (a run where detection was skipped, etc).
                    kept_files = self._get_files_in_snapshots(snapshots_to_keep)
                    full_orphans = self._find_full_orphaned_data_files(dataset, kept_files)

                    if not eligible and not full_orphans:
                        return None

                    # For dry-run we should *not* delete anything; return a plan only.
                    if dry_run:
                        return {
                            "identifier": identifier,
                            "retention_days": retention_days,
                            "snapshots_to_delete": 0,
                            "snapshots_to_keep": len(snapshots_to_keep),
                            "deleted_snapshots": [],
                            "deleted_manifests": [],
                            "deleted_files": [],
                            "bytes_reclaimed": 0,
                            "orphaned_files_count": len(full_orphans),
                            "data_files_to_delete": sorted(full_orphans),
                            "orphaned_manifests_count": len(eligible),
                            "manifests_to_delete": sorted(eligible),
                            "manifests_skipped_due_to_age": sorted(skipped_recent),
                            "orphan_detection_skipped": False,
                        }

                    # Execute mode: perform deletion of eligible manifests and
                    # full-reconciliation orphaned data files, return a summary.
                    deleted = []
                    io = self.catalog.io or dataset.io
                    for m in eligible:
                        try:
                            res = self._delete_file(io, m)
                            if res:
                                deleted.append(m)
                        except Exception:
                            # Continue deleting what we can, but don't fail the whole op
                            pass

                    deleted_data_files = []
                    for f in full_orphans:
                        try:
                            if self._delete_file(io, f):
                                deleted_data_files.append(f)
                        except Exception as e:
                            logger.error("Failed to delete orphaned data file %s: %s", f, e)

                    return {
                        "identifier": identifier,
                        "retention_days": retention_days,
                        "snapshots_to_delete": 0,
                        "snapshots_to_keep": len(snapshots_to_keep),
                        "deleted_snapshots": [],
                        "deleted_manifests": deleted,
                        "deleted_files": deleted_data_files,
                        "bytes_reclaimed": 0,
                        "orphaned_files_count": len(deleted_data_files),
                        "orphaned_manifests_count": len(deleted),
                        "manifests_to_delete": sorted(eligible),
                        "manifests_skipped_due_to_age": sorted(skipped_recent),
                        "orphan_detection_skipped": False,
                    }
                except Exception as e:
                    # If manifest tidy-up fails, log and continue (do not silently swallow)
                    logger.exception("Manifest-only tidy-up failed for %s: %s", identifier, e)
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
                "bytes_reclaimed": 0,
                "orphaned_files_count": 0,
                "orphaned_manifests_count": 0,
                "manifests_to_delete": [],
                "manifests_skipped_due_to_age": [],
                "orphan_detection_skipped": skip_orphan_detection,
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

                # Find orphaned files only if under snapshot limit
                if not skip_orphan_detection:
                    kept_files = self._get_files_in_snapshots(snapshots_to_keep)
                    deleted_file_sizes = self._get_file_sizes_in_snapshots(snapshots_to_delete)
                    orphaned = set(deleted_file_sizes) - kept_files
                    # Full reconciliation catches data files orphaned by any
                    # past event, not just this run's condemned snapshots.
                    full_orphans = self._find_full_orphaned_data_files(dataset, kept_files)
                    orphaned = orphaned | full_orphans
                    summary["orphaned_files_count"] = len(orphaned)
                    summary["data_files_to_delete"] = sorted(orphaned)
                    # Bytes that *would* be reclaimed, so a dry run reports the
                    # same measure the execute path does. Full-reconciliation
                    # orphans have no known size (physical listing carries no
                    # stats), so they contribute 0 here.
                    summary["bytes_reclaimed"] = sum(
                        deleted_file_sizes.get(p, 0) for p in orphaned
                    )

                    # Identify orphaned manifest files (storage manifests not referenced by any snapshot)
                    try:
                        from .deep_clean import DatasetDeepClean

                        cleaner = DatasetDeepClean(self.catalog)
                        orphaned_manifests = cleaner.get_orphaned_manifests(identifier) or set()
                        # Only consider manifests older than MIN age
                        import re

                        now_ms = int(time.time() * 1000)
                        eligible = []
                        skipped_recent = []
                        for m in orphaned_manifests:
                            mname = m.rsplit("/", 1)[-1]
                            match = re.search(r"manifest-(\d+)\.parquet$", mname)
                            if match:
                                ts = int(match.group(1))
                                if now_ms - ts >= MANIFEST_ORPHAN_MIN_AGE_MS:
                                    eligible.append(m)
                                else:
                                    skipped_recent.append(m)
                            else:
                                # Unable to parse age; skip for safety
                                skipped_recent.append(m)

                        summary["orphaned_manifests_count"] = len(eligible)
                        summary["manifests_to_delete"] = sorted(eligible)
                        summary["manifests_skipped_due_to_age"] = sorted(skipped_recent)
                    except Exception:
                        # If manifest listing fails, be conservative and skip
                        summary["orphaned_manifests_count"] = 0

                return summary

            # If there are no snapshots to delete but there *are* orphaned manifests,
            # perform a manifest-only tidy-up in execute mode (delete eligible manifests).
            if not dry_run and len(snapshots_to_delete) == 0 and not skip_orphan_detection:
                try:
                    import re

                    from .deep_clean import DatasetDeepClean

                    cleaner = DatasetDeepClean(self.catalog)
                    orphaned_manifests = cleaner.get_orphaned_manifests(identifier) or set()

                    now_ms = int(time.time() * 1000)
                    eligible = []
                    skipped_recent = []
                    for m in orphaned_manifests:
                        mname = m.rsplit("/", 1)[-1]
                        match = re.search(r"manifest-(\d+)\.parquet$", mname)
                        if match:
                            ts = int(match.group(1))
                            if now_ms - ts >= MANIFEST_ORPHAN_MIN_AGE_MS:
                                eligible.append(m)
                            else:
                                skipped_recent.append(m)
                        else:
                            skipped_recent.append(m)

                    if eligible:
                        deleted = []
                        io = self.catalog.io or dataset.io
                        for m in eligible:
                            try:
                                if self._delete_file(io, m):
                                    deleted.append(m)
                            except Exception:
                                # Continue deleting what we can
                                pass

                        summary["deleted_manifests"] = deleted
                        summary["orphaned_manifests_count"] = len(deleted)
                        summary["manifests_to_delete"] = sorted(eligible)
                        summary["manifests_skipped_due_to_age"] = sorted(skipped_recent)
                        return summary
                except Exception:
                    # If tidy-up fails, continue to normal execution path
                    logger.exception(
                        "Manifest-only tidy-up failed during execute for %s", identifier
                    )

            # Execute deletion
            return self._execute_expiration(
                identifier,
                dataset,
                snapshots_to_delete,
                snapshots_to_keep,
                skip_orphan_detection=skip_orphan_detection,
                retention_days=retention_days,
            )

        except (ValueError, KeyError, AttributeError) as e:
            logger.error("Error expiring dataset %s: %s", identifier, e)
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
            "total_bytes_reclaimed": 0,
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
                results["total_bytes_reclaimed"] += summary.get("bytes_reclaimed", 0)
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
            "total_bytes_reclaimed": 0,
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
            results["total_bytes_reclaimed"] += collection_result.get("total_bytes_reclaimed", 0)

            if collection_result.get("details"):
                results["details"].extend(collection_result["details"])

        return results

    def _execute_expiration(
        self,
        identifier: str,
        dataset,
        snapshots_to_delete: List[Snapshot],
        snapshots_to_keep: List[Snapshot],
        skip_orphan_detection: bool = False,
        retention_days: Optional[int] = None,
    ) -> Dict:
        """
        Execute snapshot expiration: delete snapshots, manifests, and orphaned files.

        Args:
            identifier: Dataset identifier
            dataset: Dataset metadata object
            snapshots_to_delete: Snapshots to remove
            snapshots_to_keep: Snapshots to retain
            skip_orphan_detection: Skip orphaned file detection to save memory
            retention_days: Retention window applied, echoed into the summary

        Returns:
            Summary of deletions
        """
        collection, dataset_name = identifier.split(".")
        summary = {
            "identifier": identifier,
            "retention_days": retention_days,
            "snapshots_to_delete": len(snapshots_to_delete),
            "snapshots_to_keep": len(snapshots_to_keep),
            "deleted_snapshots": [],
            "deleted_manifests": [],
            "deleted_files": [],
            # Storage reclaimed by deleting orphaned *data* files. Manifest files
            # are not counted: a manifest's own size is recorded nowhere, and
            # FileIO has no stat/size call, so including them would cost an extra
            # request per manifest for a rounding-error contribution.
            "bytes_reclaimed": 0,
            "orphan_detection_skipped": skip_orphan_detection,
        }

        # Step 1: Find which files are kept and which are orphaned (if not skipped)
        orphaned_files = set()
        orphaned_file_sizes: Dict[str, int] = {}
        if not skip_orphan_detection:
            kept_files = self._get_files_in_snapshots(snapshots_to_keep)
            deleted_file_sizes = self._get_file_sizes_in_snapshots(snapshots_to_delete)
            orphaned_files = set(deleted_file_sizes) - kept_files
            orphaned_file_sizes = {p: deleted_file_sizes[p] for p in orphaned_files}
            # Full reconciliation catches data files orphaned by any past
            # event, not just this run's condemned snapshots (e.g. a prior
            # run where orphan detection was skipped). No size info for
            # these - orphaned_file_sizes.get() below defaults to 0.
            orphaned_files |= self._find_full_orphaned_data_files(dataset, kept_files)
        else:
            logger.info(
                "Skipping orphaned file detection for %s (%d snapshots to delete)",
                identifier,
                len(snapshots_to_delete),
            )

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
                logger.info("Deleted snapshot %s from %s", snapshot.snapshot_id, identifier)
            except (ValueError, OSError) as e:
                logger.error("Failed to delete snapshot %s: %s", snapshot.snapshot_id, e)

        # Step 3: Delete manifest files from storage
        for snapshot in snapshots_to_delete:
            if snapshot.manifest_list:
                try:
                    io = self.catalog.io or dataset.io
                    # Delete manifest file (it's just a GCS object)
                    manifest_path = snapshot.manifest_list
                    if self._delete_file(io, manifest_path):
                        summary["deleted_manifests"].append(manifest_path)
                        logger.info("Deleted manifest %s", manifest_path)
                    else:
                        logger.warning(
                            "Failed to delete manifest (delete not supported): %s", manifest_path
                        )
                except (ValueError, OSError) as e:
                    logger.error("Failed to delete manifest %s: %s", snapshot.manifest_list, e)

        # Step 3b: Tidy up orphaned manifest files (storage files not referenced by any snapshot)
        # Only perform when orphan detection is allowed for this dataset
        if not skip_orphan_detection:
            try:
                from .deep_clean import DatasetDeepClean

                cleaner = DatasetDeepClean(self.catalog)
                orphaned_manifests = cleaner.get_orphaned_manifests(identifier) or set()

                if orphaned_manifests:
                    import re

                    now_ms = int(time.time() * 1000)
                    for m in sorted(orphaned_manifests):
                        mname = m.rsplit("/", 1)[-1]
                        match = re.search(r"manifest-(\d+)\.parquet$", mname)
                        if match:
                            ts = int(match.group(1))
                            if now_ms - ts >= MANIFEST_ORPHAN_MIN_AGE_MS:
                                try:
                                    if self._delete_file(self.catalog.io or dataset.io, m):
                                        summary["deleted_manifests"].append(m)
                                        logger.info("Deleted orphaned manifest %s", m)
                                except Exception as e:
                                    logger.error("Failed to delete orphaned manifest %s: %s", m, e)
                            else:
                                summary.setdefault("manifests_skipped_due_to_age", []).append(m)
                        else:
                            # Unknown format - skip for safety
                            summary.setdefault("manifests_skipped_due_to_age", []).append(m)
            except Exception as e:
                logger.error("Error during orphaned manifest tidy-up for %s: %s", identifier, e)

        # Step 4: Delete orphaned data files (if detection was performed)
        if not skip_orphan_detection:
            for file_path in orphaned_files:
                try:
                    io = self.catalog.io or dataset.io
                    if self._delete_file(io, file_path):
                        summary["deleted_files"].append(file_path)
                        summary["bytes_reclaimed"] += orphaned_file_sizes.get(file_path, 0)
                        logger.info("Deleted orphaned file %s", file_path)
                    else:
                        logger.warning("Failed to delete orphaned file %s", file_path)
                except (ValueError, OSError) as e:
                    logger.error("Failed to delete orphaned file %s: %s", file_path, e)
        else:
            logger.info(
                "Orphaned file deletion skipped for %s. "
                "Data files from deleted snapshots may still exist. "
                "Run expiration again after reducing snapshot count.",
                identifier,
            )

        return summary

    def _get_file_sizes_in_snapshots(self, snapshots: List[Snapshot]) -> Dict[str, int]:
        """
        Get all data files referenced by a set of snapshots, with their sizes.

        Sizes come from `file_size_in_bytes` on the manifest entries we already
        have to read here, so capturing them costs no extra IO. This is what
        lets expiration report the bytes actually reclaimed from storage.

        Args:
            snapshots: List of snapshots

        Returns:
            Mapping of file path -> on-disk size in bytes (0 when unrecorded)
        """
        files: Dict[str, int] = {}

        for snapshot in snapshots:
            if not snapshot.manifest_list:
                continue

            try:
                # Read and parse manifest
                io = self.catalog.io
                from .manifest import get_parsed_manifest

                entries = get_parsed_manifest(io, snapshot.manifest_list)

                # Extract file paths and their on-disk sizes
                for entry in entries:
                    file_path = entry.get("file_path")
                    if file_path:
                        files[file_path] = int(entry.get("file_size_in_bytes") or 0)
            except Exception as e:
                # Broad on purpose: a corrupt/unreadable manifest (including
                # native-decoder errors like RuntimeError) must not abort
                # expiration for the whole dataset - skip it and move on.
                logger.error("Error reading manifest %s: %s", snapshot.manifest_list, e)

        return files

    def _get_files_in_snapshots(self, snapshots: List[Snapshot]) -> Set[str]:
        """
        Get all data files referenced by a set of snapshots.

        Args:
            snapshots: List of snapshots

        Returns:
            Set of file paths referenced in all manifests
        """
        return set(self._get_file_sizes_in_snapshots(snapshots))

    def _find_full_orphaned_data_files(self, dataset, kept_files: Set[str]) -> Set[str]:
        """
        Full reconciliation: physical data files under the dataset location
        that aren't referenced by any currently-kept snapshot.

        Unlike the delta approach (`_get_files_in_snapshots` diffed against
        this run's condemned snapshots), this catches files orphaned by any
        past event - a run where orphan detection was skipped, a snapshot
        removed some other way, etc. Manifest files themselves are excluded
        here; those are handled separately by `get_orphaned_manifests` with
        its own age gate.

        A file whose storage age can't be determined, or that isn't at least
        DATA_FILE_ORPHAN_MIN_AGE_MS old, is left alone - it may be mid-write
        by an in-flight append/compaction whose snapshot commit hasn't landed.

        Args:
            dataset: Dataset object (used for its storage location)
            kept_files: File paths referenced by all currently-retained snapshots

        Returns:
            Set of file paths safe to delete
        """
        try:
            from .deep_clean import DatasetDeepClean

            cleaner = DatasetDeepClean(self.catalog)
            location = dataset.metadata.location
            if not location:
                return set()

            physical = cleaner.get_all_physical_files(location)
            candidates = {
                f for f in physical if f not in kept_files and "/metadata/manifest-" not in f
            }
            if not candidates:
                return set()

            ages = cleaner.get_physical_file_ages_ms(location)
            return {
                f
                for f in candidates
                if ages.get(f, 0) >= DATA_FILE_ORPHAN_MIN_AGE_MS
            }
        except Exception as e:
            logger.error("Error during full orphaned-data-file reconciliation: %s", e)
            return set()

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
                logger.warning("FileIO does not support delete: %s", file_path)
                return False
        except (AttributeError, ValueError, OSError) as e:
            logger.error("Error deleting file %s: %s", file_path, e)
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
                        # For "current only" retention we always keep 1 snapshot
                        retained_snapshots = 1
                        excess_snapshots = max(0, len(snapshots) - retained_snapshots)
                        expiring_datasets.append(
                            {
                                "dataset": dataset_name,
                                "current_snapshots": len(snapshots),
                                "retained_policy": "current only",
                                "retained_snapshots": retained_snapshots,
                                "excess_snapshots": excess_snapshots,
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
                        retained_snapshots = len(snapshots) - len(outside_window)
                        # Always keep at least the current snapshot
                        if retained_snapshots < 1:
                            retained_snapshots = 1
                        excess_snapshots = len(outside_window)

                        expiring_datasets.append(
                            {
                                "dataset": dataset_name,
                                "current_snapshots": len(snapshots),
                                "retained_policy": f"{retention_days} days",
                                "retained_snapshots": retained_snapshots,
                                "excess_snapshots": excess_snapshots,
                                "outside_window": len(outside_window),
                            }
                        )
            except (ValueError, KeyError, AttributeError) as e:
                logger.error("Error checking %s: %s", identifier, e)

        if expiring_datasets:
            results[collection] = expiring_datasets

    return results
