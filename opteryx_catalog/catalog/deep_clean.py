"""
Deep clean module for removing orphaned files from storage.

This module performs a comprehensive scan of dataset storage and removes
any files that are NOT referenced in any snapshot's manifest. This handles
edge cases where files end up in storage without manifest entries.

Distinct from expiration module:
- Expiration: Deletes old snapshots, then orphaned files
- Deep Clean: Scans all physical files, deletes any not in any manifest
"""

from __future__ import annotations

import logging

from ..alerts import report as _alert
from ..exceptions import ManifestProtectionError
from .orphan_quarantine import OrphanQuarantine

logger = logging.getLogger(__name__)

# Minimum age before an unreferenced file may be deleted. Shares expiration's
# constant deliberately: the two modules delete from the same storage on the
# same basis, and a file that is too new for one to reclaim is too new for the
# other. Imported lazily inside `_age_gate` to avoid a circular import.


class DatasetDeepClean:
    """
    Comprehensive garbage collection for dataset storage.

    Identifies and removes files that exist in storage but are not
    referenced by any snapshot manifest.
    """

    def __init__(
        self,
        catalog,
        author: str | None = None,
        agent: str | None = None,
        quarantine: OrphanQuarantine | None = None,
    ):
        """
        Initialize deep clean.

        Args:
            catalog: OpteryxCatalog instance
            author: Author name for tracking
            agent: Agent identifier (e.g., "deep-cleaner")
            quarantine: Two-strike record for orphaned files. Shares expiration's
                record for the dataset on purpose - see `clean_dataset`.
        """
        self.catalog = catalog
        self.author = author or "system"
        self.agent = agent or "deep-clean"
        self.quarantine = quarantine or OrphanQuarantine(catalog)

    def clean_dataset(self, identifier: str, *, dry_run: bool) -> dict | None:
        """
        Perform deep clean on a single dataset.

        Args:
            identifier: Dataset identifier (collection.dataset_name)
            dry_run: If True, plan only without deleting

        Returns:
            Summary dict or None if no cleanup needed
        """
        try:
            # Load dataset with full history
            dataset = self.catalog.load_dataset(identifier, load_history=True)
            if not dataset:
                return None

            metadata = dataset.metadata
            if not metadata.snapshots:
                return None

            dataset_location = metadata.location
            if not dataset_location:
                return None

            # Step 1: Get all files referenced in any manifest
            manifest_files = self.get_all_manifest_files(metadata.snapshots)

            # Step 2: Get all physical files in storage
            try:
                physical_files = self.get_all_physical_files(dataset_location)
            except Exception as e:  # noqa: BLE001 - storage listing boundary
                logger.error(f"Error listing files in {dataset_location}: {e}")
                return None

            if not physical_files:
                # Deliberately NOT treated as "nothing is orphaned":
                # `get_all_physical_files` returns an empty set on a listing
                # error too, so this is an ambiguous result. Leave the
                # quarantine record alone rather than exonerate from it.
                return None

            # Step 3: Find orphaned (physical but not in manifests)
            #
            # Age-gated on the same terms as expiration. Without this, a file
            # written between the snapshot read above and the storage listing
            # is unreferenced-but-live, and gets deleted seconds after it
            # lands; the gate also covers files an in-flight commit has
            # written but not yet referenced. A file whose age can't be
            # determined is kept.
            candidates = self._age_gate(dataset_location, physical_files - manifest_files)

            # Step 4: require a second, independent sighting before deleting.
            #
            # Shares expiration's record for this dataset rather than keeping
            # its own, so a file flagged by one pass and then by the other has
            # been condemned by two different implementations reading storage at
            # two different times - stronger evidence than the same code running
            # twice. The two disagree in one direction by design: deep clean
            # protects files referenced by EVERY snapshot, expiration only those
            # referenced by RETAINED ones, so deep clean's candidates are always
            # a subset. That means deep clean can exonerate a file expiration
            # flagged, whose snapshot is condemned but not yet deleted. This
            # costs a cycle or two of reclamation and converges once the
            # snapshot is gone; it never deletes anything early, which is the
            # direction that matters.
            #
            # Called even when `candidates` is empty. An empty set from a
            # complete observation is a real statement - "nothing here is
            # orphaned" - and it has to clear stale sightings, or a path that is
            # deleted and later recreated inherits the old file's strike.
            orphaned_files, quarantine_fields = self.quarantine.review_for_deletion(
                identifier, candidates, dry_run
            )

            if not candidates:
                # All files are accounted for
                return None

            summary = {
                "identifier": identifier,
                "physical_files_count": len(physical_files),
                "manifest_files_count": len(manifest_files),
                "orphaned_files_count": len(orphaned_files),
                "deleted_files": [],
                **quarantine_fields,
            }

            if dry_run:
                summary["orphaned_files"] = sorted(orphaned_files)
                return summary

            # Execute deletion
            return self._execute_cleanup(orphaned_files, dataset, summary)

        except (ValueError, KeyError, AttributeError) as e:
            logger.error(f"Error cleaning dataset {identifier}: {e}")
            return None

    def clean_collection(self, collection: str, *, dry_run: bool) -> dict[str, any]:
        """
        Deep clean all datasets in a collection.

        Args:
            collection: Collection name
            dry_run: If True, plan only without deleting

        Returns:
            Summary of cleanup results
        """
        datasets = self.catalog.list_datasets(collection)
        results = {
            "collection": collection,
            "datasets_processed": 0,
            "datasets_cleaned": 0,
            "total_physical_files": 0,
            "total_manifest_files": 0,
            "total_orphaned_files": 0,
            "total_deleted_files": 0,
            # Unreferenced files held back for a second sighting.
            "total_orphans_quarantined": 0,
            "datasets_skipped_unprotectable": [],
            "details": [],
        }

        for dataset_name in datasets:
            identifier = f"{collection}.{dataset_name}"
            # A dataset whose protected-file set can't be established is
            # skipped, not silently cleaned: the failure is confined to that
            # dataset so one unreadable manifest doesn't stop the sweep, but
            # nothing is deleted for it either.
            try:
                summary = self.clean_dataset(identifier, dry_run=dry_run)
            except ManifestProtectionError as e:
                logger.error("Skipping deep clean of %s: %s", identifier, e)
                # See the matching site in expiration.py: the exception is
                # raised and then absorbed here so the sweep can continue, so
                # without an alert the skip is invisible.
                _alert(
                    e,
                    fingerprint=("gc-unprotectable-deep-clean", identifier),
                    context={"dataset": identifier, "sweep": "deep-clean"},
                )
                results["datasets_skipped_unprotectable"].append(identifier)
                results["datasets_processed"] += 1
                continue
            results["datasets_processed"] += 1

            if summary:
                results["datasets_cleaned"] += 1
                results["total_physical_files"] += summary.get("physical_files_count", 0)
                results["total_manifest_files"] += summary.get("manifest_files_count", 0)
                results["total_orphaned_files"] += summary.get("orphaned_files_count", 0)
                results["total_deleted_files"] += len(summary.get("deleted_files", []))
                results["total_orphans_quarantined"] += summary.get("orphans_quarantined", 0)
                results["details"].append(summary)

        return results

    def clean_workspace(self, *, dry_run: bool) -> dict[str, any]:
        """
        Deep clean all datasets in workspace.

        Args:
            dry_run: If True, plan only without deleting

        Returns:
            Summary of cleanup results
        """
        collections = self.catalog.list_collections()
        results = {
            "workspace": self.catalog.workspace,
            "collections_processed": 0,
            "datasets_processed": 0,
            "datasets_cleaned": 0,
            "total_physical_files": 0,
            "total_manifest_files": 0,
            "total_orphaned_files": 0,
            "total_deleted_files": 0,
            # Unreferenced files held back for a second sighting.
            "total_orphans_quarantined": 0,
            "details": [],
        }

        for collection in collections:
            collection_result = self.clean_collection(collection, dry_run=dry_run)
            results["collections_processed"] += 1
            results["datasets_processed"] += collection_result.get("datasets_processed", 0)
            results["datasets_cleaned"] += collection_result.get("datasets_cleaned", 0)
            results["total_physical_files"] += collection_result.get("total_physical_files", 0)
            results["total_manifest_files"] += collection_result.get("total_manifest_files", 0)
            results["total_orphaned_files"] += collection_result.get("total_orphaned_files", 0)
            results["total_deleted_files"] += collection_result.get("total_deleted_files", 0)
            results["total_orphans_quarantined"] += collection_result.get(
                "total_orphans_quarantined", 0
            )

            if collection_result.get("details"):
                results["details"].extend(collection_result["details"])

        return results

    def _age_gate(self, dataset_location: str, candidates: set[str]) -> set[str]:
        """
        Drop candidates that are too new, or whose age can't be determined.

        Deep clean decides orphanhood by diffing storage against the manifests,
        and those two observations are taken at different moments - so anything
        committed in between looks orphaned while being perfectly live. The age
        gate is what makes that race survivable. A file whose age can't be
        determined is treated as too new and kept.

        Args:
            dataset_location: Base path of dataset
            candidates: File paths proposed for deletion

        Returns:
            The subset old enough to delete
        """
        if not candidates:
            return set()

        from .expiration import DATA_FILE_ORPHAN_MIN_AGE_MS

        ages = self.get_physical_file_ages_ms(dataset_location)
        eligible = {f for f in candidates if ages.get(f, 0) >= DATA_FILE_ORPHAN_MIN_AGE_MS}

        held_back = len(candidates) - len(eligible)
        if held_back:
            logger.info(
                "Deep clean holding back %d of %d unreferenced file(s) under %s as too new "
                "(or age unknown) to reclaim.",
                held_back,
                len(candidates),
                dataset_location,
            )
        return eligible

    def get_all_manifest_files(self, snapshots: list) -> set[str]:
        """
        Get all files referenced in any snapshot manifest, plus the manifest
        files themselves.

        `clean_dataset` diffs this set against *every* physical file under the
        dataset location, which includes the `metadata/manifest-*.parquet`
        objects - so a live manifest file must be counted as "referenced" here
        or it gets misidentified as orphaned and deleted out from under its
        still-retained snapshot.

        Args:
            snapshots: List of Snapshot objects

        Returns:
            Set of all file paths from all manifests, plus each manifest's own path
        """
        manifest_files = set()

        for snapshot in snapshots:
            if not snapshot.manifest_list:
                continue

            manifest_files.add(snapshot.manifest_list)

            try:
                io = self.catalog.io
                from .manifest import get_parsed_manifest

                entries = get_parsed_manifest(io, snapshot.manifest_list)

                for entry in entries:
                    file_path = entry.get("file_path")
                    if file_path:
                        manifest_files.add(file_path)
                    # Merge-on-read delete sidecar: referenced per data file
                    # via delete_file_path (see catalog/deletes.py). Missing
                    # it here would quarantine and then delete the live
                    # vector — resurrecting deleted rows dataset-wide.
                    delete_file = entry.get("delete_file_path")
                    if delete_file:
                        manifest_files.add(delete_file)

                logger.debug(f"Read manifest {snapshot.manifest_list}: {len(entries)} files")
            except Exception as e:
                # This set is the protection list: `clean_dataset` deletes
                # every physical file NOT in it. A manifest we failed to read
                # therefore doesn't mean "no files to protect", it means we
                # cannot tell what to protect - and continuing would delete
                # every file that manifest was holding. Note a missing object
                # raises FileNotFoundError, which is an OSError, so the old
                # narrow catch swallowed precisely the case that matters.
                raise ManifestProtectionError(
                    f"Cannot read manifest {snapshot.manifest_list} of snapshot "
                    f"{snapshot.snapshot_id}: {e}. Refusing to delete anything for this "
                    "dataset while the set of protected files is incomplete."
                ) from e

        return manifest_files

    def get_all_physical_files(self, dataset_location: str) -> set[str]:
        """
        Get all physical files in dataset storage location.

        Args:
            dataset_location: Base path of dataset (e.g., gs://bucket/dataset)

        Returns:
            Set of all file paths in storage
        """
        physical_files = set()

        # GCS (and similar) prefix listing is a raw string match, not a path-
        # boundary match: an un-slashed prefix like ".../test/tweets" also
        # matches ".../test/tweets_512/..." and pulls in an unrelated
        # dataset's files as false-positive "orphans". Force a trailing
        # separator so only true children of this dataset's location match.
        if dataset_location and not dataset_location.endswith("/"):
            dataset_location = dataset_location + "/"

        try:
            io = self.catalog.io
            if not io:
                logger.error("No FileIO available for listing files")
                return physical_files

            # List all files in dataset location
            # Note: Implementation depends on FileIO type (GCS, local, etc)
            if hasattr(io, "list_files"):
                # Custom list method
                files = io.list_files(dataset_location)
                physical_files = set(files)
            elif hasattr(io, "ls"):
                # Alternative listing method
                files = io.ls(dataset_location)
                physical_files = set(files)
            else:
                # Fallback: try to list via directory walking
                # This is limited and may not work for all FileIO types
                logger.warning(f"FileIO does not support list_files or ls: {type(io).__name__}")
                return physical_files

            logger.info(f"Found {len(physical_files)} physical files in {dataset_location}")
            return physical_files

        except (ValueError, OSError, AttributeError) as e:
            logger.error(f"Error listing physical files in {dataset_location}: {e}")
            return physical_files

    def get_physical_file_ages_ms(self, dataset_location: str) -> dict[str, int]:
        """
        Get the age (in ms) of each physical file in dataset storage.

        Best-effort: returns {} when the attached FileIO can't report object
        creation times, so callers must treat a missing entry as "age
        unknown" and skip that file rather than assume it's safe to delete.

        Args:
            dataset_location: Base path of dataset (e.g., gs://bucket/dataset)

        Returns:
            Dict of file path -> age in milliseconds
        """
        if dataset_location and not dataset_location.endswith("/"):
            dataset_location = dataset_location + "/"

        try:
            io = self.catalog.io
            if not io or not hasattr(io, "list_files_with_age_ms"):
                return {}
            return io.list_files_with_age_ms(dataset_location) or {}
        except (ValueError, OSError, AttributeError) as e:
            logger.error(f"Error listing physical file ages in {dataset_location}: {e}")
            return {}

    def _execute_cleanup(self, orphaned_files: set[str], dataset, summary: dict) -> dict:
        """
        Execute deletion of orphaned files.

        Args:
            orphaned_files: Set of file paths to delete
            dataset: Dataset object
            summary: Summary dict to update

        Returns:
            Updated summary
        """
        io = self.catalog.io or dataset.io
        deleted_count = 0

        for file_path in orphaned_files:
            try:
                if self._delete_file(io, file_path):
                    summary["deleted_files"].append(file_path)
                    deleted_count += 1
                    logger.info(f"Deleted orphaned file: {file_path}")
                else:
                    logger.warning(f"Failed to delete orphaned file: {file_path}")
            except (ValueError, OSError) as e:
                logger.error(f"Failed to delete {file_path}: {e}")

        return summary

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
            if hasattr(io, "delete"):
                io.delete(file_path)
                return True
            else:
                logger.warning(f"FileIO does not support delete: {file_path}")
                return False
        except (AttributeError, ValueError, OSError) as e:
            logger.error(f"Error deleting file {file_path}: {e}")
            return False

    def get_orphaned_manifests(self, identifier: str) -> set | None:
        """
        Identify manifest files present in storage that are NOT referenced by any snapshot.

        Args:
            identifier: Dataset identifier (collection.dataset_name)

        Returns:
            Set of orphaned manifest file paths, or None if none found / on error
        """
        try:
            dataset = self.catalog.load_dataset(identifier, load_history=True)
            if not dataset or not dataset.metadata.snapshots:
                return None

            # Gather manifest paths referenced by snapshots
            referenced_manifests = {
                s.manifest_list
                for s in dataset.metadata.snapshots
                if getattr(s, "manifest_list", None)
            }

            # List physical files and pick manifest files in metadata/ directory
            physical_files = self.get_all_physical_files(dataset.metadata.location)
            manifest_files_in_storage = {f for f in physical_files if "/metadata/manifest-" in f}

            # Orphaned manifests are storage manifests not referenced by any snapshot
            orphaned = manifest_files_in_storage - referenced_manifests
            return orphaned if orphaned else None

        except (ValueError, KeyError, AttributeError) as e:
            logger.error(f"Error finding orphaned manifests for {identifier}: {e}")
            return None


def find_orphaned_files(catalog, identifier: str) -> set[str] | None:
    """
    Find orphaned files in a dataset without deleting.

    Useful for analysis and reporting.

    Args:
        catalog: OpteryxCatalog instance
        identifier: Dataset identifier (collection.dataset_name)

    Returns:
        Set of orphaned file paths or None
    """
    try:
        dataset = catalog.load_dataset(identifier, load_history=True)
        if not dataset or not dataset.metadata.snapshots:
            return None

        cleaner = DatasetDeepClean(catalog)

        # Get manifest files
        manifest_files = cleaner.get_all_manifest_files(dataset.metadata.snapshots)

        # Get physical files
        physical_files = cleaner.get_all_physical_files(dataset.metadata.location)

        # Find orphaned
        orphaned = physical_files - manifest_files

        return orphaned if orphaned else None

    except (ValueError, KeyError, AttributeError) as e:
        logger.error(f"Error finding orphaned files in {identifier}: {e}")
        return None
