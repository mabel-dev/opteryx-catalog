"""
Snapshot expiration and garbage collection module.

Manages snapshot retention policies and cleans up:
1. Old snapshots (keeping only retained-snapshot-age-days)
2. Associated manifest files
3. Orphaned data files no longer referenced by any retained snapshot

Nothing is destroyed at the moment it is condemned. An expired snapshot's
Firestore document is tombstoned in place (see SNAPSHOT_EXPIRED_AT_KEY), not
deleted, and its manifest and data files pass through the orphan quarantine
(two independent sightings, a day apart) before reaching storage deletion -
which on GCS is itself a 7-day soft delete. The layered timeline for a
snapshot that expires at T:

    T           tombstoned; invisible to normal reads, files now orphan
                candidates
    T+1 day+    files pass quarantine and are deleted into GCS soft-delete
    T+7 days    tombstone purged; the record that it existed is gone
    ~T+8 days   files hard-deleted by the bucket; restore no longer possible

Until the files hard-delete, `scripts/restore_snapshot.py` can rebuild the
snapshot into a new dataset - via the tombstone's manifest path while the
record exists, or via the storage listing of soft-deleted manifests in the
final stretch after the tombstone is gone.

A TAGGED snapshot never enters that timeline at all. A tag pins its snapshot,
and every file the snapshot references, from expiry indefinitely - no
retention setting reaches past it (SNAPSHOT_TAGS_DESIGN.md S4). The pin is
released only by dropping the tag, at which point the snapshot rejoins normal
retention and expires on the next run if it is already past the window; that
is deliberate, and there is no grace period. See `pinned_snapshot_ids`.
"""

from __future__ import annotations

import logging
import time

from ..alerts import report as _alert
from ..exceptions import ManifestProtectionError
from .dataset import select_last_user_snapshot
from .dataset import visible_history
from .metadata import SNAPSHOT_EXPIRED_AT_KEY
from .metadata import Snapshot
from .metadata import snapshot_is_tombstoned
from .orphan_quarantine import OrphanQuarantine

logger = logging.getLogger(__name__)

# Memory management: limit snapshot processing to avoid excessive memory usage
# when loading manifests for orphaned file detection. Kept well above a single
# day's snapshot count for datasets on 15-minutely maintenance schedules
# (~96/day) so same-day runs never trip it. Counts LIVE snapshots only: the
# dataset loader filters tombstoned documents out of `metadata.snapshots`, so
# a month of retained tombstones (~2,880 on a 15-minutely dataset) cannot push
# a healthy dataset over this limit.
MAX_SNAPSHOTS_FOR_ORPHAN_DETECTION = 2500

# How long a tombstoned snapshot document is kept before being purged from
# Firestore. Matches the bucket's 7-day soft-delete window: while the files
# can still be produced, the tombstone is the restore pointer. The edge is
# accepted knowingly - files outlive the tombstone by about a day (the orphan
# quarantine delays their entry into soft-delete), but a last-day restore can
# still find them without the record, because `restore_snapshot.py inventory`
# lists soft-deleted manifests straight from storage and the manifest
# filename carries the snapshot id.
EXPIRED_SNAPSHOT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000

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


def pinned_snapshot_ids(catalog, identifier: str, metadata) -> set[int]:
    """Snapshot ids held alive by a tag.

    A tag pins its snapshot from expiry until the tag is dropped
    (SNAPSHOT_TAGS_DESIGN.md S4), so this set is a PROTECTED input: it decides
    what may not be touched.

    Two sources, in order, because pinning must not depend on how the caller
    happened to load the dataset - a tag that pins only sometimes is not a pin:

    * the metadata, when a history load already carried the tags (`tags_loaded`).
      This is the normal case and costs no extra read;
    * otherwise `catalog.list_tags`, a direct subcollection read.

    Neither path answers an unreadable tag set with an empty one, and a catalog
    that cannot answer at all is refused rather than assumed untagged. That
    reading is the one that deletes exactly the data the tag exists to keep,
    so the failure is raised as `ManifestProtectionError` and the run aborts -
    the same treatment, for the same reason, as an unreadable manifest on a
    retained snapshot.
    """
    if getattr(metadata, "tags_loaded", False):
        return metadata.pinned_snapshot_ids()

    lister = getattr(catalog, "list_tags", None)
    if lister is None:
        raise ManifestProtectionError(
            f"Cannot establish which snapshots of {identifier} are pinned by a tag: "
            "this catalog cannot list tags. Refusing to expire anything for it."
        )
    try:
        tags = lister(identifier)
    except Exception as exc:
        raise ManifestProtectionError(
            f"Cannot read the snapshot tags of {identifier}: {exc}. Refusing to expire "
            "anything for this dataset while it is unknown which snapshots are pinned."
        ) from exc
    return {int(tag["snapshot-id"]) for tag in tags if tag.get("snapshot-id") is not None}


class SnapshotExpiration:
    """
    Manages snapshot retention and garbage collection for datasets.

    Keeps datasets within their configured retention policy by:
    1. Identifying snapshots eligible for deletion
    2. Removing snapshots from Firestore
    3. Deleting associated manifest files
    4. Identifying and deleting orphaned data files
    """

    def __init__(
        self,
        catalog,
        author: str | None = None,
        agent: str | None = None,
        quarantine: OrphanQuarantine | None = None,
    ):
        """
        Initialize snapshot expiration manager.

        Args:
            catalog: OpteryxCatalog instance
            author: Author name for tracking
            agent: Agent identifier (e.g., "garbage-collector")
            quarantine: Two-strike record for orphaned data files. Injectable so
                the promotion rule can be driven directly in tests.
        """
        self.catalog = catalog
        self.author = author or "system"
        self.agent = agent or "snapshot-expiration"
        self.quarantine = quarantine or OrphanQuarantine(catalog)
        self.deleted_snapshots = []
        self.deleted_manifests = []
        self.deleted_files = []

    def expire_dataset(self, identifier: str, *, dry_run: bool) -> dict | None:
        """
        Apply retention policy to a single dataset.

        Keeps snapshots based on age. Null or missing value means keep only
        the current snapshot - data is unversioned.

        `dry_run` is required and keyword-only. It used to default to False, so
        the destructive behaviour was what you got by omitting it; a caller had
        to know to opt out of deleting. Callers now state which they want.

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

        # Purge tombstones past their record window BEFORE the run, so
        # the load below streams fewer dead documents. Never on a dry run - a
        # plan must not destroy records, even ones nobody can restore.
        purged = 0 if dry_run else len(self.purge_snapshot_tombstones(identifier))

        summary = self._expire_dataset(identifier, dry_run=dry_run)
        if summary is not None:
            summary["duration_ms"] = int((time.perf_counter() - start) * 1000)
            summary["snapshot_tombstones_purged"] = purged
        elif purged:
            logger.info(
                "Purged %d expired snapshot tombstone(s) from %s (no other expiration needed)",
                purged,
                identifier,
            )
        return summary

    def purge_snapshot_tombstones(self, identifier: str) -> list[str]:
        """Hard-delete tombstones whose record window has ended.

        This is the ONLY place a snapshot document is ever deleted, and it is
        a two-phase deletion by construction: `_execute_expiration` marks, and
        this removes the mark-bearing document no sooner than
        EXPIRED_SNAPSHOT_RETENTION_MS later. A document without the stamp, or
        with an unreadable one, is never touched here - the stamp is the proof
        the waiting period ran, so no stamp means no deletion.

        Failures are logged and skipped rather than raised: a tombstone that
        cannot be purged today is a stale record, not a risk, and will be
        caught by any later run.

        Args:
            identifier: Dataset identifier (collection.name)

        Returns:
            Document ids purged.
        """
        cutoff_ms = int(time.time() * 1000) - EXPIRED_SNAPSHOT_RETENTION_MS
        purged: list[str] = []
        try:
            collection, dataset_name = identifier.split(".", 1)
            # pylint: disable=protected-access
            snaps_coll = self.catalog._snapshots_collection(collection, dataset_name)
            for doc in snaps_coll.stream():
                data = doc.to_dict() or {}
                if not snapshot_is_tombstoned(data):
                    continue
                expired_at = data.get(SNAPSHOT_EXPIRED_AT_KEY)
                if isinstance(expired_at, int) and expired_at <= cutoff_ms:
                    try:
                        snaps_coll.document(doc.id).delete()
                        purged.append(doc.id)
                    except Exception as exc:  # noqa: BLE001 - GCS/Firestore client boundary
                        logger.error(
                            "Failed to purge snapshot tombstone %s of %s: %s",
                            doc.id,
                            identifier,
                            exc,
                        )
        except Exception as exc:  # noqa: BLE001 - GCS/Firestore client boundary
            # Includes catalogs/fakes whose snapshot collections cannot
            # stream. Purging is housekeeping; nothing downstream depends on
            # it having run.
            logger.debug("Tombstone purge unavailable for %s: %s", identifier, exc)
        return purged

    def _expire_dataset(self, identifier: str, dry_run: bool = False) -> dict | None:
        """Apply retention policy to a single dataset (see `expire_dataset`)."""
        try:
            # Load dataset with full history
            dataset = self.catalog.load_dataset(identifier, load_history=True)
            if not dataset or not dataset.metadata.snapshots:
                return None

            # Resolved BEFORE any retention maths, so a dataset whose pins
            # cannot be established is refused before it has any candidates.
            pinned_ids = pinned_snapshot_ids(self.catalog, identifier, dataset.metadata)

            # Get retention policy
            policy = dataset.metadata.maintenance_policy or {}
            retention_days = policy.get("retained-snapshot-age-days")

            snapshots = dataset.metadata.snapshots
            if not snapshots:
                return None

            # The HEAD, from the dataset's pointer - NOT `snapshots[-1]`. That
            # list arrives in Firestore document-id order, which is lexicographic
            # on the id string and only accidentally chronological; and after a
            # `rollback` the head is deliberately not the newest snapshot at all.
            # Expiring the head is expiring the data every reader sees, so this
            # is the one snapshot that must be identified exactly.
            current = dataset.metadata.current_snapshot() or snapshots[-1]

            # Determine which snapshots to keep
            if retention_days is None or retention_days == 0:
                # Keep only the current snapshot - unversioned
                snapshots_to_keep = [current]
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
                if current not in snapshots_to_keep:
                    snapshots_to_keep.append(current)

            # A tagged snapshot is never an expiry candidate, in EITHER branch
            # above - including the "keep only the current" one, where a tag is
            # the only thing standing between a snapshot and immediate
            # condemnation. The pin does not weaken with age and no retention
            # setting reaches past it: a tag is held until it is dropped.
            #
            # This one insertion is all three protections the design calls for.
            # Being in `snapshots_to_keep` keeps the snapshot out of
            # `snapshots_to_delete` (so it is not tombstoned - and a tombstoned
            # snapshot is invisible to reads AND to storage billing), puts its
            # files into `kept_files` at every site that computes a retained set
            # (so they are not orphan candidates), and makes its manifest a
            # `required=True` read (so an unreadable one raises
            # ManifestProtectionError and aborts, rather than yielding a short
            # protected set and deleting the tag's data).
            #
            # DROP TAG unpins immediately and deliberately: the snapshot comes
            # back here unpinned on the very next run and expires then if it is
            # already past the window. Dropping a tag is how you agree to lose
            # the data, so there is no grace period.
            for pinned in (s for s in snapshots if s.snapshot_id in pinned_ids):
                if pinned not in snapshots_to_keep:
                    snapshots_to_keep.append(pinned)
                    logger.info(
                        "Retaining tagged snapshot %s of %s (pinned by a tag)",
                        pinned.snapshot_id,
                        identifier,
                    )

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
            # expire, leaving the current snapshot (already retained above) as
            # what the UI shows. An imperfect trade, chosen deliberately.
            # Ranked over the HEAD's own line of descent only. A rollback
            # leaves the snapshots it moved off live, and protecting one of
            # those would pin the version the dataset's owner has just retired -
            # while leaving the user commit the head actually rests on
            # unprotected, which is the one this exists to keep.
            protected_user_snapshot = select_last_user_snapshot(
                visible_history(current, snapshots),
                lookback=USER_SNAPSHOT_LOOKBACK,
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
                    eligible, skipped_recent = self._eligible_orphaned_manifests(identifier)

                    # Full reconciliation for orphaned *data* files: the delta
                    # method below has nothing to diff against here (no
                    # snapshots are being deleted this run), so this is the
                    # only path that can ever catch data files orphaned by a
                    # past event (a run where detection was skipped, etc).
                    kept_files = self._get_files_in_snapshots(snapshots_to_keep, required=True)
                    candidate_orphans = self._find_full_orphaned_data_files(dataset, kept_files)

                    if not eligible and not candidate_orphans:
                        return None

                    # Reviewed before the dry-run branch, so a plan reports what
                    # an execute run would delete right now rather than every
                    # candidate. `dry_run` keeps the record itself untouched.
                    (
                        full_orphans,
                        manifests_to_delete,
                        quarantine_fields,
                    ) = self._review_run_candidates(
                        identifier, candidate_orphans, set(eligible), dry_run
                    )

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
                            "orphaned_manifests_count": len(manifests_to_delete),
                            "manifests_to_delete": sorted(manifests_to_delete),
                            "manifests_skipped_due_to_age": sorted(skipped_recent),
                            "orphan_detection_skipped": False,
                            **quarantine_fields,
                        }

                    # Execute mode: perform deletion of eligible manifests and
                    # full-reconciliation orphaned data files, return a summary.
                    deleted = []
                    io = self.catalog.io or dataset.io
                    for m in manifests_to_delete:
                        try:
                            res = self._delete_file(io, m)
                            if res:
                                deleted.append(m)
                        except Exception as exc:  # noqa: BLE001 - GCS client boundary
                            # Continue deleting what we can, but don't fail the whole op. An
                            # undeleted manifest is an orphan the next run re-proposes, so this
                            # is recoverable - but it is not nothing, and it now says so.
                            logger.error("Failed to delete orphaned manifest %s: %s", m, exc)
                            # Continue deleting what we can, but don't fail the whole op

                    deleted_data_files = []
                    for f in full_orphans:
                        try:
                            if self._delete_file(io, f):
                                deleted_data_files.append(f)
                        except Exception as e:  # noqa: BLE001 - GCS/Firestore client boundary
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
                        "manifests_to_delete": sorted(manifests_to_delete),
                        "manifests_skipped_due_to_age": sorted(skipped_recent),
                        "orphan_detection_skipped": False,
                        **quarantine_fields,
                    }
                except ManifestProtectionError:
                    # Not a tidy-up failure to absorb: we could not establish
                    # what this dataset's files are, so the caller has to know
                    # the dataset was skipped rather than found clean.
                    raise
                except Exception:
                    # Not annotated: `logger.exception` below records the error
                    # in full, which is what BLE001 asks for.
                    # If manifest tidy-up fails, log and continue (do not
                    # silently swallow). `logger.exception` already attaches
                    # the traceback, so the message must not repeat it.
                    logger.exception("Manifest-only tidy-up failed for %s", identifier)
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
                    kept_files = self._get_files_in_snapshots(snapshots_to_keep, required=True)
                    deleted_file_sizes = self._get_file_sizes_in_snapshots(
                        snapshots_to_delete, required=False
                    )
                    # Age-gated to match the execute path, so a dry run plans
                    # exactly the deletions the execute path would perform.
                    orphaned = self._age_gate(dataset, set(deleted_file_sizes) - kept_files)
                    # Full reconciliation catches data files orphaned by any
                    # past event, not just this run's condemned snapshots.
                    full_orphans = self._find_full_orphaned_data_files(dataset, kept_files)
                    orphaned = orphaned | full_orphans

                    # Identify orphaned manifest files (storage manifests not
                    # referenced by any snapshot), plus the manifests belonging
                    # to the snapshots this run condemns - see the execute path
                    # for why those are reclaimed as orphans rather than deleted
                    # alongside their snapshot.
                    try:
                        eligible, skipped_recent = self._eligible_orphaned_manifests(identifier)
                        summary["manifests_skipped_due_to_age"] = sorted(skipped_recent)
                    except Exception as exc:  # noqa: BLE001 - GCS listing boundary
                        # If manifest listing fails, be conservative and skip. Reporting zero
                        # orphans is safe (nothing gets deleted on the strength of it); the
                        # danger would be reporting a short list as if it were complete.
                        logger.error(
                            "Could not list orphaned manifests for %s: %s", identifier, exc
                        )
                        # If manifest listing fails, be conservative and skip
                        eligible = []
                        summary["orphaned_manifests_count"] = 0

                    # One review for the whole run - data files and manifests
                    # together. Reviewing them separately would make the second
                    # call exonerate everything the first had just recorded.
                    (
                        orphaned,
                        manifests_to_delete,
                        quarantine_fields,
                    ) = self._review_run_candidates(
                        identifier, orphaned, set(eligible), dry_run=True
                    )
                    summary.update(quarantine_fields)
                    summary["orphaned_files_count"] = len(orphaned)
                    summary["data_files_to_delete"] = sorted(orphaned)
                    summary["orphaned_manifests_count"] = len(manifests_to_delete)
                    summary["manifests_to_delete"] = sorted(manifests_to_delete)
                    # Bytes that *would* be reclaimed, so a dry run reports the
                    # same measure the execute path does. Full-reconciliation
                    # orphans have no known size (physical listing carries no
                    # stats), so they contribute 0 here.
                    summary["bytes_reclaimed"] = sum(deleted_file_sizes.get(p, 0) for p in orphaned)

                return summary

            # If there are no snapshots to delete but there *are* orphaned manifests,
            # perform a manifest-only tidy-up in execute mode (delete eligible manifests).
            if not dry_run and len(snapshots_to_delete) == 0 and not skip_orphan_detection:
                try:
                    eligible, skipped_recent = self._eligible_orphaned_manifests(identifier)

                    _, manifests_to_delete, quarantine_fields = self._review_run_candidates(
                        identifier, set(), set(eligible), dry_run=False
                    )
                    summary.update(quarantine_fields)

                    if manifests_to_delete:
                        deleted = []
                        io = self.catalog.io or dataset.io
                        for m in manifests_to_delete:
                            try:
                                if self._delete_file(io, m):
                                    deleted.append(m)
                            except Exception as exc:  # noqa: BLE001 - GCS client boundary, see above
                                # Continue deleting what we can
                                logger.error("Failed to delete orphaned manifest %s: %s", m, exc)
                                # Continue deleting what we can

                        summary["deleted_manifests"] = deleted
                        summary["orphaned_manifests_count"] = len(deleted)
                        summary["manifests_to_delete"] = sorted(manifests_to_delete)
                        summary["manifests_skipped_due_to_age"] = sorted(skipped_recent)
                        return summary
                except ManifestProtectionError:
                    # Falling through to the normal execution path would run
                    # the very deletions this error says we cannot justify.
                    raise
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

    def expire_collection(self, collection: str, *, dry_run: bool) -> dict[str, any]:
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
            # Files identified as orphaned but held back for a second sighting.
            # A non-zero count is normal; a count that never falls means the
            # candidates are not reappearing and something is flapping.
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
                summary = self.expire_dataset(identifier, dry_run=dry_run)
            except ManifestProtectionError as e:
                logger.error("Skipping expiration of %s: %s", identifier, e)
                # The log line stays: it is the per-occurrence record, and the
                # alert is deduplicated. Without the alert this exception is
                # raised and then absorbed here, so a dataset can be
                # unreclaimable for weeks with nothing surfacing it.
                _alert(
                    e,
                    fingerprint=("gc-unprotectable-expiration", identifier),
                    context={"dataset": identifier, "sweep": "expiration"},
                )
                results["datasets_skipped_unprotectable"].append(identifier)
                results["datasets_processed"] += 1
                continue
            results["datasets_processed"] += 1

            if summary:
                results["datasets_expiring"] += 1
                results["total_snapshots_deleted"] += summary.get("snapshots_to_delete", 0)
                results["total_manifests_deleted"] += len(summary.get("deleted_manifests", []))
                results["total_files_deleted"] += len(summary.get("deleted_files", []))
                results["total_orphaned_files"] += summary.get("orphaned_files_count", 0)
                results["total_bytes_reclaimed"] += summary.get("bytes_reclaimed", 0)
                results["total_orphans_quarantined"] += summary.get("orphans_quarantined", 0)
                results["details"].append(summary)

        return results

    def expire_workspace(self, *, dry_run: bool) -> dict[str, any]:
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
            "total_orphans_quarantined": 0,
            # Rolled up from the collection results rather than dropped: a
            # dataset skipped because its protected-file set was unreadable is
            # the one thing a workspace-wide run most needs to surface.
            "datasets_skipped_unprotectable": [],
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
            results["total_orphans_quarantined"] += collection_result.get(
                "total_orphans_quarantined", 0
            )
            results["datasets_skipped_unprotectable"].extend(
                collection_result.get("datasets_skipped_unprotectable", [])
            )

            if collection_result.get("details"):
                results["details"].extend(collection_result["details"])

        return results

    def _execute_expiration(
        self,
        identifier: str,
        dataset,
        snapshots_to_delete: list[Snapshot],
        snapshots_to_keep: list[Snapshot],
        skip_orphan_detection: bool = False,
        retention_days: int | None = None,
    ) -> dict:
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
        orphaned_file_sizes: dict[str, int] = {}
        if not skip_orphan_detection:
            kept_files = self._get_files_in_snapshots(snapshots_to_keep, required=True)
            deleted_file_sizes = self._get_file_sizes_in_snapshots(
                snapshots_to_delete, required=False
            )
            # The delta set is age-gated on the same terms as the full
            # reconciliation below. It previously went straight to deletion,
            # so half the orphan set was deletable the instant it became
            # unreferenced - including files an in-flight commit was about to
            # claim.
            orphaned_files = self._age_gate(dataset, set(deleted_file_sizes) - kept_files)
            orphaned_file_sizes = {p: deleted_file_sizes[p] for p in orphaned_files}
            # Full reconciliation catches data files orphaned by any past
            # event, not just this run's condemned snapshots (e.g. a prior
            # run where orphan detection was skipped). No size info for
            # these - orphaned_file_sizes.get() below defaults to 0.
            orphaned_files |= self._find_full_orphaned_data_files(dataset, kept_files)

            # Orphaned manifests are gathered here, BEFORE step 2 deletes the
            # snapshot documents, so `get_orphaned_manifests` is answering a
            # question about the state this run actually judged.
            try:
                eligible_manifests, skipped_recent = self._eligible_orphaned_manifests(identifier)
                summary["manifests_skipped_due_to_age"] = sorted(skipped_recent)
            except Exception as e:  # noqa: BLE001 - GCS/Firestore client boundary
                logger.error("Error listing orphaned manifests for %s: %s", identifier, e)
                eligible_manifests = []

            # Last gate before deletion, and the only one that can catch a
            # protected-file set that was readable but wrong. Everything this
            # run proposes to delete goes through one review - see
            # `_review_run_candidates`.
            (
                orphaned_files,
                orphaned_manifests,
                quarantine_fields,
            ) = self._review_run_candidates(
                identifier, orphaned_files, set(eligible_manifests), dry_run=False
            )
            summary.update(quarantine_fields)
        else:
            # No reconciliation means no candidate set, and no candidate set
            # means nothing can be shown to have been seen twice - so this run
            # reclaims neither data files nor manifests, including the manifests
            # of the snapshots it is about to delete. They are left in storage
            # and picked up by the first run that can afford to look, once the
            # snapshot count falls back under the limit.
            orphaned_manifests = set()
            logger.info(
                "Skipping orphaned file detection for %s (%d snapshots to delete); "
                "data files and manifests are left in storage for a later run",
                identifier,
                len(snapshots_to_delete),
            )

        # Step 2: Tombstone expired snapshots in Firestore.
        #
        # An update, never a delete. The document keeps every field it had -
        # most importantly the manifest path, which is the thread a restore
        # follows back to the data files while the bucket's soft-delete can
        # still produce them. The `expired-at-ms` stamp is what hides it from
        # the dataset loader, and what the purge sweep ages against; a
        # document deleted here instead would erase the only record of what
        # this snapshot was, before anyone had decided that was acceptable.
        # pylint: disable=protected-access
        now_ms = int(time.time() * 1000)
        snaps_coll = self.catalog._snapshots_collection(collection, dataset_name)
        for snapshot in snapshots_to_delete:
            try:
                snaps_coll.document(str(snapshot.snapshot_id)).update(
                    {
                        SNAPSHOT_EXPIRED_AT_KEY: now_ms,
                        "expired-by": self.author,
                    }
                )
                summary["deleted_snapshots"].append(
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "timestamp_ms": snapshot.timestamp_ms,
                    }
                )
                logger.info(
                    "Tombstoned snapshot %s of %s (record kept %d days)",
                    snapshot.snapshot_id,
                    identifier,
                    EXPIRED_SNAPSHOT_RETENTION_MS // (24 * 60 * 60 * 1000),
                )
            except Exception as e:  # noqa: BLE001 - deliberate, see below
                # Broad on purpose: unlike the old delete() (idempotent on a
                # missing document), update() raises NotFound - a
                # google.api_core exception, neither ValueError nor OSError -
                # if the document vanished since we streamed it. One
                # untombstonable snapshot must not abort the rest.
                logger.error("Failed to tombstone snapshot %s: %s", snapshot.snapshot_id, e)

        # Step 3: Delete manifest files from storage.
        #
        # A condemned snapshot's own manifest is NOT deleted here, even though
        # step 2 has just removed the document that referenced it. Deleting it
        # inline would be a first-sight deletion of a manifest, and a manifest
        # deleted while something still points at it is the truncation failure
        # this whole path exists to avoid - the next commit reads its parent,
        # gets a 404, and writes a manifest listing only the new file.
        #
        # Instead it is left to become an orphaned manifest: with the snapshot
        # document gone, the next run's `get_orphaned_manifests` finds it, and
        # it is deleted once the quarantine has seen it twice. One extra cycle
        # of storage in exchange for never deleting a manifest on a single
        # observation.
        for snapshot in snapshots_to_delete:
            if snapshot.manifest_list:
                logger.debug(
                    "Manifest %s left for orphan reclamation after snapshot %s",
                    snapshot.manifest_list,
                    snapshot.snapshot_id,
                )

        # Step 3b: Tidy up orphaned manifest files (storage files not referenced
        # by any snapshot). Candidates and the quarantine verdict were both
        # established before step 2 ran; this only performs the deletions.
        for m in sorted(orphaned_manifests):
            try:
                if self._delete_file(self.catalog.io or dataset.io, m):
                    summary["deleted_manifests"].append(m)
                    logger.info("Deleted orphaned manifest %s", m)
            except Exception as e:  # noqa: BLE001 - GCS/Firestore client boundary
                logger.error("Failed to delete orphaned manifest %s: %s", m, e)

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

    def _get_file_sizes_in_snapshots(
        self, snapshots: list[Snapshot], *, required: bool
    ) -> dict[str, int]:
        """
        Get all data files referenced by a set of snapshots, with their sizes.

        Sizes come from `file_size_in_bytes` on the manifest entries we already
        have to read here, so capturing them costs no extra IO. This is what
        lets expiration report the bytes actually reclaimed from storage.

        `required` selects what an unreadable manifest means, and the two cases
        are opposites:

        - Retained snapshots (`required=True`) define what must be PROTECTED. A
          short set here gets subtracted from the physical files and the
          remainder is deleted, so one unreadable manifest turns this pass into
          a delete-everything pass. Abort instead.
        - Condemned snapshots (`required=False`) define what may be RECLAIMED. A
          short set here just means less is reclaimed this run, which the next
          run's full reconciliation picks up. Log and continue.

        Args:
            snapshots: List of snapshots
            required: True when the result is used to protect files from
                deletion, False when it is used to select files for deletion

        Raises:
            ManifestProtectionError: `required` and a manifest could not be read

        Returns:
            Mapping of file path -> on-disk size in bytes (0 when unrecorded)
        """
        files: dict[str, int] = {}

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
                    # Merge-on-read delete sidecar (catalog/deletes.py): it
                    # must be in the protected set whenever any retained
                    # snapshot's manifest references it, or the sweep deletes
                    # the live vector and every row it deleted resurrects.
                    # Size 0: the sidecar's on-disk size is not recorded on
                    # the entry, and the reclaimed-bytes tally is reporting.
                    delete_file = entry.get("delete_file_path")
                    if delete_file and delete_file not in files:
                        files[delete_file] = 0
            except Exception as e:
                # Broad on purpose: a corrupt/unreadable manifest (including
                # native-decoder errors like RuntimeError) must be handled here
                # rather than crashing the caller mid-pass.
                if required:
                    raise ManifestProtectionError(
                        f"Cannot read manifest {snapshot.manifest_list} of retained snapshot "
                        f"{snapshot.snapshot_id}: {e}. Refusing to delete anything for this "
                        "dataset while the set of protected files is incomplete."
                    ) from e
                logger.error("Error reading manifest %s: %s", snapshot.manifest_list, e)

        return files

    def _get_files_in_snapshots(self, snapshots: list[Snapshot], *, required: bool) -> set[str]:
        """
        Get all data files referenced by a set of snapshots.

        Args:
            snapshots: List of snapshots
            required: See `_get_file_sizes_in_snapshots`

        Returns:
            Set of file paths referenced in all manifests
        """
        return set(self._get_file_sizes_in_snapshots(snapshots, required=required))

    def _find_full_orphaned_data_files(self, dataset, kept_files: set[str]) -> set[str]:
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
            return self._age_gate(dataset, candidates)
        except Exception as e:  # noqa: BLE001 - GCS/Firestore client boundary
            logger.error("Error during full orphaned-data-file reconciliation: %s", e)
            return set()

    def _age_gate(self, dataset, candidates: set[str]) -> set[str]:
        """
        Drop candidates that are too new, or whose age can't be determined.

        A file younger than DATA_FILE_ORPHAN_MIN_AGE_MS may be mid-write by an
        in-flight append or compaction whose snapshot commit hasn't landed, so
        being unreferenced right now doesn't make it garbage. An age that can't
        be determined is treated as too new: `ages.get(f, 0)` yields 0, which
        fails the comparison and keeps the file.

        Args:
            dataset: Dataset object (used for its storage location)
            candidates: File paths proposed for deletion

        Returns:
            The subset old enough to delete
        """
        if not candidates:
            return set()

        from .deep_clean import DatasetDeepClean

        location = dataset.metadata.location
        if not location:
            return set()

        ages = DatasetDeepClean(self.catalog).get_physical_file_ages_ms(location)
        return {f for f in candidates if ages.get(f, 0) >= DATA_FILE_ORPHAN_MIN_AGE_MS}

    def _quarantine_orphans(
        self, identifier: str, candidates: set[str], dry_run: bool
    ) -> tuple[set[str], dict[str, any]]:
        """
        Require a second, independent sighting before any file is deleted.

        The age gate answers "could this file still be in flight?"; it cannot
        answer "was this dataset's state readable when we judged it?". Both
        orphan tests here are subtractions from the retained-file set, and a
        set that is momentarily wrong produces confident, specific, wrong
        answers. Quarantine is what turns a single wrong observation into a
        delay rather than a deletion: a candidate is recorded and passed over,
        and only deleted when a later run reaches the same conclusion a day or
        more later. See `orphan_quarantine` for why non-reappearing entries are
        forgotten rather than left armed.

        Called with the union of both detectors' candidates, once per run, so
        the record is a complete statement of what this run believed - a
        partial call would exonerate files the other detector had flagged.

        Args:
            identifier: Dataset identifier (collection.name)
            candidates: Files this run judges orphaned, already age-gated
            dry_run: When True the record is left untouched, so planning a run
                never advances a file toward deletion

        Returns:
            (files to delete now, summary fields describing the quarantine)
        """
        return self.quarantine.review_for_deletion(identifier, candidates, dry_run)

    def _eligible_orphaned_manifests(self, identifier: str) -> tuple[list[str], list[str]]:
        """
        Manifest files in storage that no snapshot references, split by age.

        Age comes from the timestamp in the filename rather than from storage
        metadata, which is why manifests have their own gate instead of using
        `_age_gate`. A name that doesn't parse is treated as too new: an
        unrecognised file under `metadata/` is exactly the thing not to delete
        on a guess.

        Args:
            identifier: Dataset identifier (collection.name)

        Returns:
            (old enough to consider, held back as too new or unparseable)
        """
        import re

        from .deep_clean import DatasetDeepClean

        cleaner = DatasetDeepClean(self.catalog)
        orphaned_manifests = cleaner.get_orphaned_manifests(identifier) or set()

        now_ms = int(time.time() * 1000)
        eligible: list[str] = []
        skipped_recent: list[str] = []
        for manifest in orphaned_manifests:
            name = manifest.rsplit("/", 1)[-1]
            # Optional nonce suffix — see write_parquet_manifest. It must stay
            # OPTIONAL: manifests written before the nonce existed have bare
            # names, and a pattern that stopped matching them would leak every
            # one of those files forever (an unmatched name is treated as
            # too-recent-to-reclaim, not as a parse failure).
            match = re.search(r"manifest-(\d+)(?:-[0-9a-f]+)?\.parquet$", name)
            if match and now_ms - int(match.group(1)) >= MANIFEST_ORPHAN_MIN_AGE_MS:
                eligible.append(manifest)
            else:
                skipped_recent.append(manifest)

        return eligible, skipped_recent

    def _review_run_candidates(
        self,
        identifier: str,
        data_candidates: set[str],
        manifest_candidates: set[str],
        dry_run: bool,
    ) -> tuple[set[str], set[str], dict[str, any]]:
        """
        Put everything this run proposes to delete through one review.

        Data files and manifests go in together because the record is replaced
        wholesale on each write: reviewing them separately would make the second
        call exonerate everything the first had just recorded. Splitting the
        verdict back out afterwards is the caller's convenience, not two
        decisions.

        Args:
            identifier: Dataset identifier (collection.name)
            data_candidates: Orphaned data files, already age-gated
            manifest_candidates: Orphaned manifests, already age-gated
            dry_run: When True the record is left untouched

        Returns:
            (data files to delete, manifests to delete, summary fields)
        """
        data_candidates = set(data_candidates)
        manifest_candidates = set(manifest_candidates)

        to_delete, fields = self._quarantine_orphans(
            identifier, data_candidates | manifest_candidates, dry_run
        )
        return to_delete & data_candidates, to_delete & manifest_candidates, fields

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


def identify_expiring_datasets(catalog) -> dict[str, list[str]]:
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

                # Tagged snapshots are pinned and will not expire, so they are
                # not excess. This is a report, not the deleting path, but a
                # report that counts pinned snapshots as expiring tells an
                # operator their tagged data is about to go - the one thing
                # pinning guarantees will not happen.
                #
                # Unlike the deleting path this degrades rather than aborting:
                # nothing here removes anything, so an unreadable tag set costs
                # accuracy in one row instead of the whole scan.
                try:
                    pinned = pinned_snapshot_ids(catalog, identifier, dataset.metadata)
                except ManifestProtectionError as exc:
                    logger.error("Could not read tags of %s for reporting: %s", identifier, exc)
                    pinned = set()

                # Determine expiration eligibility
                if retention_days is None or retention_days == 0:
                    # Keep only current - check if there are older snapshots
                    pinned_count = sum(1 for s in snapshots[:-1] if s.snapshot_id in pinned)
                    if len(snapshots) - pinned_count > 1:
                        # For "current only" retention we always keep 1 snapshot,
                        # plus every tagged one.
                        retained_snapshots = 1 + pinned_count
                        excess_snapshots = max(0, len(snapshots) - retained_snapshots)
                        expiring_datasets.append(
                            {
                                "dataset": dataset_name,
                                "current_snapshots": len(snapshots),
                                "retained_policy": "current only",
                                "retained_snapshots": retained_snapshots,
                                "excess_snapshots": excess_snapshots,
                                "pinned_snapshots": pinned_count,
                            }
                        )
                elif retention_days > 0:
                    # Age-based retention - check if any snapshots are outside window
                    current_time_ms = int(time.time() * 1000)
                    cutoff_time_ms = current_time_ms - (retention_days * 24 * 60 * 60 * 1000)

                    outside_window = [
                        s
                        for s in snapshots
                        if (s.timestamp_ms or 0) < cutoff_time_ms and s.snapshot_id not in pinned
                    ]

                    if outside_window:
                        retained_snapshots = len(snapshots) - len(outside_window)
                        # Always keep at least the current snapshot
                        retained_snapshots = max(retained_snapshots, 1)
                        excess_snapshots = len(outside_window)

                        expiring_datasets.append(
                            {
                                "dataset": dataset_name,
                                "current_snapshots": len(snapshots),
                                "retained_policy": f"{retention_days} days",
                                "retained_snapshots": retained_snapshots,
                                "excess_snapshots": excess_snapshots,
                                "outside_window": len(outside_window),
                                "pinned_snapshots": len(
                                    pinned & {s.snapshot_id for s in snapshots}
                                ),
                            }
                        )
            except (ValueError, KeyError, AttributeError) as e:
                logger.error("Error checking %s: %s", identifier, e)

        if expiring_datasets:
            results[collection] = expiring_datasets

    return results
