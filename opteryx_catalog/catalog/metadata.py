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

# Provenance caps (PROVENANCE_DESIGN.md S4.2, S2.2). A receipt is capped far
# above anything a real statement reads; the dataset's standing source list is
# deliberately short - "up to 64, most recent" is a documented property of the
# field - and both record when the cap bit rather than truncating silently.
MAX_READ_SOURCES = 256
MAX_SOURCES = 64


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
    # When expiration retired this snapshot, in epoch ms; None for a live one.
    # Only ever set on the tombstone-including load path
    # (`load_dataset(..., include_expired=True)`), which puts these records in
    # `DatasetMetadata.expired_snapshots` and never in `snapshots` - so a
    # Snapshot reached through any normal read still cannot carry it. The
    # storage behind an expired snapshot is not guaranteed to exist: this is a
    # record for the restore window (SNAPSHOT_EXPIRED_AT_KEY), not a version
    # anything may read.
    expired_at_ms: int | None = None
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
    # THE RECEIPT (PROVENANCE_DESIGN.md S2.1): one entry per (catalog relation,
    # snapshot) the statement that produced this commit read, as
    # `{dataset, snapshot-id, resolved-by}`. `None` means the writer did not
    # report - a defect, alerted and audited at commit, never a state a new
    # commit may quietly be in. `[]` means the statement read no catalog
    # relation, and only a caller that knows that may say it.
    read_sources: list[dict] | None = None
    read_sources_truncated: bool = False
    # What made this commit: `task:<workspace.collection.name>`,
    # `view:<workspace.collection.name>`, or `upload:<channel>` for data that
    # arrived from outside the catalog. Absent for a hand-run statement, which
    # is the one provenance field where absent is a state, not a bug.
    #
    # NOT what the commit DID - `operation-type` records that, and a second
    # field restating it is a second field that can disagree with it. The
    # vocabulary and the rule that the segment after the colon is scoped by
    # the kind live in `provenance.PRODUCER_KINDS`.
    produced_by: str | None = None


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
    # A statistics manifest recorded on the DATASET DOCUMENT rather than on a
    # snapshot. Only a dataset PROJECTED from an external catalog has one: it
    # has no snapshots (nothing commits to it), so there is no version history
    # for the usual pointer to hang off. Same file format and same reader as a
    # snapshot's `manifest_list` - see stub_projection's `manifest-list`.
    manifest_list: str | None = None
    # Maintenance policy: retention settings grouped under a single block
    maintenance_policy: dict = field(
        default_factory=lambda: {
            "retained-snapshot-age-days": None,
            "compaction-policy": "performance",
        }
    )
    # Compaction policy lives under maintenance_policy as 'compaction-policy'
    snapshots: list[Snapshot] = field(default_factory=list)
    # TOMBSTONES, and only when they were asked for: expiration has retired
    # these and the files behind them are in quarantine or GCS soft-delete, so
    # they are records of what is still restorable rather than history that can
    # be read. Kept in a list of their own, never merged into `snapshots`,
    # because every consumer of that field means LIVE - expiration's retention
    # maths, the orphan-detection threshold, ancestry walks, `previous`
    # resolution and the head-pointer fallback all break if a tombstone is in
    # it (see the loader in opteryx_catalog.py for what each one does wrong).
    # Empty unless `load_dataset(..., include_expired=True)` filled it, which
    # `SHOW ALL SNAPSHOTS FOR` is the only caller of.
    expired_snapshots: list[Snapshot] = field(default_factory=list)
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
    # THE STANDING SOURCE LIST (PROVENANCE_DESIGN.md S2.2): the distinct,
    # fully-qualified names of every dataset whose data is in the CURRENT
    # content, most recent first, at most MAX_SOURCES. Maintained by the commit
    # path from each commit's receipt - an append adds, a rewrite replaces, a
    # truncate clears, maintenance leaves it alone, a rollback recomputes it.
    # Distinct from `source_tables`, which is what a materialized view is
    # DECLARED to read; this is what its refreshes HAVE read.
    #
    # Carried here for the reason every other field in this block is: the
    # dataset document is written whole with `set()`.
    sources: list[str] = field(default_factory=list)
    # False when the list may be missing a name: the cap dropped one, or a
    # commit in the chain carried no receipt. True again after the next
    # rewrite or clear, which start the list afresh. The loader sets it False
    # for a document without the field - pre-feature history is incomplete by
    # definition - and a fresh object with an empty list is complete.
    sources_complete: bool = True
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


# ── provenance fields on the snapshot document ──────────────────────────────
#
# The two functions below are the ONLY place the receipt's stored keys are
# named. Both readers of a snapshot document (`OpteryxCatalog._snapshot_from_dict`
# and `SimpleDataset.snapshot`'s by-id fetch) and its one writer
# (`_snapshot_to_document`) go through them, so a key cannot be produced by
# one side and not consumed by the other - which is how `operation-type` was
# lost for the catalog's whole history.

READ_SOURCES_KEY = "read-sources"
READ_SOURCE_KEYS_KEY = "read-source-keys"
READ_SOURCES_TRUNCATED_KEY = "read-sources-truncated"
PRODUCED_BY_KEY = "produced-by"


def read_source_keys(entries: list) -> list[str]:
    """The receipt as `array_contains` keys: `dataset@snapshot-id` for every
    entry AND the bare `dataset` once per name. One array answers both
    questions a consumer walk asks - "who read version V of X" (exact key)
    and "who reads X at all" (bare name) - and Firestore matches an array
    element on equality only, so neither can be answered from the entries
    themselves. Order: bare names first, then the versioned keys, both in
    entry order."""
    names: list[str] = []
    versioned: list[str] = []
    for entry in entries:
        dataset = entry["dataset"]
        if dataset not in names:
            names.append(dataset)
        snapshot_id = entry.get("snapshot-id")
        if snapshot_id is not None:
            versioned.append(f"{dataset}@{snapshot_id}")
    return names + versioned


def read_source_key(dataset: str, snapshot_id: int | None = None) -> str:
    """The key a consumer query asks for - see `read_source_keys`."""
    return dataset if snapshot_id is None else f"{dataset}@{int(snapshot_id)}"


def provenance_document_fields(snapshot: Snapshot) -> dict:
    """The receipt as stored. A key is written only when there is something to
    say: a `None` receipt is ABSENT on the document, not null, so the stored
    shape matches the claim (absent = not reported)."""
    fields: dict = {}
    read_sources = getattr(snapshot, "read_sources", None)
    if read_sources is not None:
        fields[READ_SOURCES_KEY] = [dict(entry) for entry in read_sources]
        fields[READ_SOURCE_KEYS_KEY] = read_source_keys(read_sources)
        if getattr(snapshot, "read_sources_truncated", False):
            fields[READ_SOURCES_TRUNCATED_KEY] = True
    produced_by = getattr(snapshot, "produced_by", None)
    if produced_by is not None:
        fields[PRODUCED_BY_KEY] = produced_by
    return fields


def provenance_fields_from_document(sd: dict) -> dict:
    """Constructor kwargs for `Snapshot` from a stored document. A missing
    receipt reads back as `None`; `read-source-keys` is derived and not read."""
    read_sources = sd.get(READ_SOURCES_KEY)
    return {
        "read_sources": None if read_sources is None else [dict(e) for e in read_sources],
        "read_sources_truncated": bool(sd.get(READ_SOURCES_TRUNCATED_KEY, False)),
        "produced_by": sd.get(PRODUCED_BY_KEY),
    }
