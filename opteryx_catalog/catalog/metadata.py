from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any

# Field stamped onto a snapshot document when expiration retires it. The
# document is NOT deleted at that point: it becomes a tombstone that keeps the
# manifest path (and through it, the data-file paths) addressable while the
# storage tier can still produce the bytes - GCS soft-delete holds deleted
# objects for 7 days, so within that window an expired snapshot is restorable.
# Tombstones are purged for good only after EXPIRED_SNAPSHOT_RETENTION_MS
# (see expiration.py), which matches that recovery window - the record lives
# exactly as long as acting on it is possible.
SNAPSHOT_EXPIRED_AT_KEY = "expired-at-ms"


def snapshot_is_tombstoned(doc: dict) -> bool:
    """True when a snapshot document has been retired by expiration.

    Shared by the dataset loader (which must hide tombstones from normal
    reads - every consumer of `metadata.snapshots`, including expiration's own
    retention maths and the orphan-detection size threshold, means LIVE
    snapshots) and the purge sweep (which must see them).
    """
    return doc.get(SNAPSHOT_EXPIRED_AT_KEY) is not None


@dataclass
class Snapshot:
    snapshot_id: int
    timestamp_ms: int
    author: str | None = None
    # Indicates whether this snapshot was created by a user (True) or internally (False)
    user_created: bool | None = None
    # Monotonic sequence number for writes
    sequence_number: int | None = None
    manifest_list: str | None = None
    # Operation metadata
    operation_type: str | None = None  # e.g., 'append', 'overwrite', 'compact'
    parent_snapshot_id: int | None = None
    schema_id: str | None = None
    # Commit message for the snapshot
    commit_message: str | None = None
    # Summary metrics (store zeros when not applicable)
    summary: dict = field(
        default_factory=lambda: {
            "added-data-files": 0,
            "added-files-size": 0,
            "added-records": 0,
            "deleted-data-files": 0,
            "deleted-files-size": 0,
            "deleted-records": 0,
            "total-data-files": 0,
            "total-files-size": 0,
            "total-records": 0,
        }
    )


@dataclass
class DatasetMetadata:
    dataset_identifier: str
    format_version: int = 2
    location: str = ""
    schema: Any = None
    properties: dict = field(default_factory=dict)
    # Dataset-level created/updated metadata
    timestamp_ms: int | None = None
    author: str | None = None
    description: str | None = None
    describer: str | None = None
    sort_orders: list[int] = field(default_factory=list)
    # Maintenance policy: retention settings grouped under a single block
    maintenance_policy: dict = field(
        default_factory=lambda: {
            "retained-snapshot-age-days": None,
            "compaction-policy": "performance",
        }
    )
    # Compaction policy lives under maintenance_policy as 'compaction-policy'
    snapshots: list[Snapshot] = field(default_factory=list)
    # The HEAD: the snapshot an unqualified read sees. Called "current"
    # everywhere the word is written by hand - in code, in SQL, in messages -
    # and stored under the matching key, `current-snapshot-id`.
    #
    # "current" rather than "latest" because the pointer makes no claim about
    # recency, and "latest" asserted one it cannot keep. It is the same word
    # the rest of the field uses for this pointer: Iceberg's
    # `current-snapshot-id`, Delta and Hudi's current version, `is_current` in
    # SCD Type 2.
    #
    # It is NOT necessarily the newest snapshot: `rollback` moves it BACKWARDS,
    # and the snapshots it was moved off stay live and readable by id. Anything
    # asking "what is the current state of the data?" must read this pointer,
    # never `max(snapshots)` or `snapshots[-1]`.
    current_snapshot_id: int | None = None
    # Tags: normalized (lowercase) tag name -> the snapshot id it is bound to.
    # Stored in a `tags` subcollection beside `snapshots` and `schemas`, NOT on
    # the dataset document - `save_dataset_metadata` writes that document whole
    # with `set()`, and a tag is a retention pin, so losing one to a routine
    # commit would un-protect data somebody is paying to keep.
    #
    # The direction is deliberate (see SNAPSHOT_TAGS_DESIGN.md S3): a tag points
    # at a snapshot; a snapshot knows nothing about its tags. Snapshot documents
    # are written once and thereafter only tombstoned, so tag names must not
    # live on them.
    #
    # Populated only by a history load (see `tags_loaded`).
    tags: dict[str, int] = field(default_factory=dict)
    # True only when `tags` above was actually populated from the catalog.
    # It defaults to FALSE because that is the honest answer for metadata
    # nobody has fetched tags for - a non-history load, or a hand-built
    # object. "No tags found" and "tags not established" must never collapse
    # into the same answer: the first means nothing is pinned, the second
    # means the pins are invisible, and acting on the second deletes exactly
    # the data a tag exists to keep. Anything deciding what to delete reads
    # this and goes back to the catalog (or refuses) rather than assuming.
    tags_loaded: bool = False
    # Schema management: schemas are stored in a subcollection in Firestore.
    # `schemas` contains dicts with keys: schema_id, columns (list of {id,name,type}).
    # Each schema dict may also include `timestamp-ms` and `author`.
    schemas: list[dict] = field(default_factory=list)
    current_schema_id: str | None = None
    # Monotonically-increasing, never-reused counter for allocating stable per-column
    # field-ids (Iceberg-style). Used to key manifest min/max statistics so they
    # survive schema evolution without positional drift. Persisted on the dataset's
    # root Firestore doc alongside `current-schema-id`.
    next_field_id: int = 1
    # Annotations: list of annotation objects attached to this dataset
    # Each annotation is a dict with keys like 'key' and 'value'.
    annotations: list[dict] = field(default_factory=list)
    # Refresh frequency in minutes; None means no automatic refresh
    refresh_frequency_mins: int | None = None
    # What kind of dataset this is. None for a plain dataset (the field is
    # absent on their documents); "materialized_view" for the backing table of
    # a materialized view. Carried on the metadata so readers - the OData
    # service, describe, any UI - can tell them apart without a second lookup.
    dataset_type: str | None = None
    # Materialized-view registration, mirrored here for the same reason
    # sort_orders and maintenance_policy are: `save_dataset_metadata` writes
    # the whole dataset document with `set()`, so a field it does not carry is
    # DESTROYED by the next commit. For a materialized view that commit is its
    # own refresh - the registration would not survive the first one.
    statement_id: str | None = None
    source_tables: list[str] = field(default_factory=list)
    # LEGACY, carried and never written. The identity a refresh executes as
    # lives on each refresh TRIGGER now, not on the view; this field survives
    # only so that a commit to a view registered under the old model does not
    # destroy the value before `scripts/backfill_refresh_trigger_identity.py`
    # has copied it onto the triggers. Retired with that script's last run.
    runs_as: str | None = None
    # Refresh suspended by an operator. On the VIEW rather than on its triggers:
    # a view with four sources has four triggers, and suspending three of
    # them would not suspend the view, it would refresh from a subset of its
    # sources - silently partial data. One flag cannot be partially applied.
    suspended_at_ms: int | None = None
    suspended_by: str | None = None
    last_refreshed_at_ms: int | None = None
    last_refresh_status: str | None = None
    last_refresh_execution_id: str | None = None

    def pinned_snapshot_ids(self) -> set[int]:
        """Snapshot ids held alive by a tag.

        A tag pins its snapshot from expiry forever, until the tag is dropped
        (SNAPSHOT_TAGS_DESIGN.md S4). Dropping a tag unpins immediately - the
        snapshot returns to normal retention on the next expiration run - which
        is why this is derived from `tags` on every call rather than cached.
        """
        return {sid for sid in self.tags.values() if sid is not None}

    def current_snapshot(self) -> Snapshot | None:
        """The snapshot the head points at - what an unqualified read sees.

        The `snapshots[-1]` fallback applies only when NO pointer is recorded,
        which is a dataset written before the pointer existed. It is not a
        general "newest wins" rule: once a pointer is set it is authoritative,
        including when a rollback has moved it behind snapshots that are still
        in the list.
        """
        if self.current_snapshot_id is None:
            return self.snapshots[-1] if self.snapshots else None
        for s in self.snapshots:
            if s.snapshot_id == self.current_snapshot_id:
                return s
        return None


# Dataset terminology: TableMetadata renamed to DatasetMetadata
