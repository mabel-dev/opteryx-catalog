"""
Reclaim storage for datasets that have been dropped.

Distinct from the other two garbage-collection modules, both of which only ever
visit datasets that still exist:
- Expiration: trims snapshots of a *live* dataset, then its orphaned files
- Deep Clean: reconciles a *live* dataset's physical files against its manifests
- Dropped Sweep (here): reclaims datasets that are gone from the catalog entirely

A dropped dataset disappears from `list_datasets()`, so neither of the other
passes can ever reach its storage again. The tombstone written by
`OpteryxCatalog.drop_dataset()` is the only remaining record of where its files
live, and this module is what consumes it.
"""

from __future__ import annotations

import logging
import time
from typing import Any
from typing import Dict
from typing import Optional

from .deep_clean import DatasetDeepClean
from .expiration import DATA_FILE_ORPHAN_MIN_AGE_MS

logger = logging.getLogger(__name__)


class DroppedDatasetSweep:
    """Delete the storage left behind by dropped datasets.

    Everything under a dropped dataset's location is unreferenced by
    definition - the dataset it belonged to no longer exists - so this deletes
    the whole prefix rather than diffing against retained snapshots the way
    expiration does.
    """

    def __init__(
        self,
        catalog,
        author: Optional[str] = None,
        agent: Optional[str] = None,
        min_age_ms: int = DATA_FILE_ORPHAN_MIN_AGE_MS,
    ):
        """
        Args:
            catalog: OpteryxCatalog instance
            author: Author name for tracking
            agent: Agent identifier (e.g. "dropped-sweep")
            min_age_ms: how long a tombstone must have existed before its
                storage is eligible for deletion.
        """
        self.catalog = catalog
        self.author = author or "system"
        self.agent = agent or "dropped-sweep"
        self.min_age_ms = min_age_ms
        self._deep_clean = DatasetDeepClean(catalog, author=author, agent=agent)

    def sweep(self, dry_run: bool = True) -> Dict[str, Any]:
        """Process every tombstone in the workspace.

        Returns a summary with a per-tombstone breakdown in `details`.
        """
        try:
            tombstones = list(self.catalog.list_dropped_datasets())
        except Exception as exc:
            logger.error("Could not list tombstones: %s", exc)
            return {
                "tombstones": 0,
                "reclaimed": 0,
                "skipped": 0,
                "errors": 1,
                "files_deleted": 0,
                "error": str(exc),
                "details": [],
            }

        details = [self._sweep_one(tombstone, dry_run=dry_run) for tombstone in tombstones]

        return {
            "tombstones": len(details),
            "reclaimed": sum(1 for d in details if d["action"] == "reclaimed"),
            "skipped": sum(1 for d in details if d["action"] == "skipped"),
            "errors": sum(1 for d in details if d["action"] == "error"),
            "files_deleted": sum(d["files_deleted"] for d in details),
            "dry_run": dry_run,
            "details": details,
        }

    def _sweep_one(self, tombstone: Dict[str, Any], dry_run: bool = True) -> Dict[str, Any]:
        """Reclaim one tombstoned location, then clear the tombstone."""
        start = time.perf_counter()
        row = {
            "id": tombstone.get("id"),
            "collection": tombstone.get("collection"),
            "dataset": tombstone.get("name"),
            "location": tombstone.get("location"),
            "dropped_by": tombstone.get("dropped-by"),
            "action": "skipped",
            "reason": None,
            "files_deleted": 0,
            "files_failed": 0,
            "tombstone_cleared": False,
        }

        def _done(action: str, reason: str) -> Dict[str, Any]:
            row["action"] = action
            row["reason"] = reason
            row["duration_ms"] = int((time.perf_counter() - start) * 1000)
            return row

        dropped_at = tombstone.get("dropped-at-ms")
        if not isinstance(dropped_at, int):
            # No usable drop time means no way to prove the grace period has
            # elapsed, so this never becomes eligible. Surfaced, not deleted.
            return _done("error", "no-dropped-at-ms")

        age_ms = int(time.time() * 1000) - dropped_at
        if age_ms < self.min_age_ms:
            # The grace period is what makes deleting the whole prefix safe: it
            # guarantees any write that was in flight when the drop landed has
            # long since finished. Per-file ages are deliberately not used -
            # a FileIO that cannot report them would make this a silent no-op.
            return _done("skipped", "within-grace")

        location = tombstone.get("location")
        if not location:
            # Nothing to reclaim and no way to find it. Kept so it stays visible.
            return _done("error", "no-location")

        try:
            files = self._deep_clean.get_all_physical_files(location)
        except Exception as exc:
            logger.error("Could not list files under %s: %s", location, exc)
            row["error"] = str(exc)
            return _done("error", "list-failed")

        if dry_run:
            row["files_deleted"] = len(files)
            return _done("reclaimed", "dry-run")

        io = self.catalog.io
        deleted = 0
        failed = 0
        for file_path in files:
            if self._delete_file(io, file_path):
                deleted += 1
            else:
                failed += 1

        row["files_deleted"] = deleted
        row["files_failed"] = failed

        if failed:
            # Keep the tombstone so the next run retries the remainder -
            # clearing it now would strand those files permanently.
            return _done("error", "partial-delete")

        try:
            self.catalog.delete_tombstone(tombstone["id"])
            row["tombstone_cleared"] = True
        except Exception as exc:
            logger.error("Reclaimed %s but could not clear tombstone: %s", location, exc)
            row["error"] = str(exc)
            return _done("error", "tombstone-clear-failed")

        return _done("reclaimed", "dropped-dataset")

    @staticmethod
    def _delete_file(io, file_path: str) -> bool:
        """Delete one object, reporting failure rather than raising."""
        try:
            if not hasattr(io, "delete"):
                logger.warning("FileIO does not support delete: %s", file_path)
                return False
            io.delete(file_path)
            return True
        except (AttributeError, ValueError, OSError) as exc:
            logger.error("Error deleting file %s: %s", file_path, exc)
            return False
