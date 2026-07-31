from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from ..audit import emit_audit
from .manifest import (
    ParquetManifestEntry,
    build_parquet_manifest_entry_from_bytes,
    build_parquet_manifest_entry_from_morsel,
)
from .metadata import DatasetMetadata, Snapshot
from .metastore import Dataset

# Stable node identifier for this process (hex-mac-hex-pid)
_NODE = f"{uuid.getnode():x}-{os.getpid():x}"


def select_last_user_snapshot(
    snapshots: List[Snapshot], lookback: Optional[int] = None
) -> Optional[Snapshot]:
    """The most recent USER-created snapshot in `snapshots`, or None.

    Shared by `SimpleDataset.last_user_snapshot` (UI: "when did a human last
    change this?") and snapshot expiration (which must not delete the last
    thing a user did). Both need the same answer from the same rule, so the
    rule lives here once.

    `lookback` bounds the search to that many most-recent snapshots; None
    searches all of them. Expiration passes a bound deliberately: a dataset
    written by a human once and then maintained automatically forever would
    otherwise pin its very first snapshot in storage indefinitely. Bounding
    the window means a user commit buried under a long tail of automated
    ones is eventually allowed to expire — imperfect, but a deliberate
    trade between honest history and unbounded retention.

    `user_created` must be explicitly True. None means "not known to be a
    user commit" and does not count — guessing would put a system commit in
    front of a user asking "what did I change?", which is the confusion this
    exists to prevent.

    Ordering is by `sequence_number` (the monotonic write counter), falling
    back to `snapshot_id` (a millisecond timestamp) on older rows that
    predate it — never by Firestore's document iteration order, which is
    lexicographic on the id string and only coincidentally numeric.
    """
    ordered = sorted(
        snapshots, key=lambda s: (s.sequence_number or 0, s.snapshot_id or 0)
    )
    window = ordered if lookback is None else ordered[-lookback:]
    user_snapshots = [s for s in window if s.user_created is True]
    return user_snapshots[-1] if user_snapshots else None


@dataclass
class SchemaColumn:
    """Dependency-free column descriptor returned by :meth:`SimpleDataset.schema`.

    Field names (``name``/``type``/``element_type``/``precision``/``scale``/
    ``nullable``) match the generic external-schema convention Opteryx's
    ``OpteryxConnector._normalize_schema`` duck-types against, so the query
    engine reconstructs its own typed ``ColumnType`` on the other side — this
    module never needs to import opteryx-core or draken.
    """

    name: str
    type: str
    element_type: Optional[str] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    nullable: bool = True
    # Stable, catalog-assigned field-id (see DatasetMetadata.next_field_id) —
    # None for schemas persisted before field-ids existed.
    id: Optional[int] = None


@dataclass
class RelationSchema:
    """Dependency-free stand-in for an Opteryx ``RelationSchema``."""

    name: str
    columns: list


def _stored_type_display(c: dict) -> str:
    """Render a stored column's type as the canonical display string (mirrors
    ``str(opteryx ColumnType)``), without needing any opteryx-core/draken
    import. Consumers that duck-type this string (this module's own
    ``describe()`` text-type check, or Opteryx's connector-side
    ``parse_column_type``) get the same result as the real type object would
    produce.
    """
    raw = c.get("type")
    name = getattr(raw, "name", None) or (str(raw) if raw is not None else "VARCHAR")
    name = name.upper()

    if name == "DECIMAL":
        precision = c.get("precision")
        scale = c.get("scale")
        if precision is not None and scale is not None:
            return f"DECIMAL({precision}, {scale})"
        return "DECIMAL(38, 9)"

    if name == "ARRAY":
        element = c.get("element-type") or c.get("element_type")
        return f"ARRAY<{element or 'VARIANT'}>"

    return name


@dataclass
class Datafile:
    """Wrapper for a manifest entry representing a data file."""

    entry: dict

    @property
    def file_path(self) -> Optional[str]:
        return self.entry.get("file_path")

    @property
    def record_count(self) -> int:
        return int(self.entry.get("record_count") or 0)

    @property
    def file_size_in_bytes(self) -> int:
        return int(self.entry.get("file_size_in_bytes") or 0)

    def to_dict(self) -> dict:
        return dict(self.entry)

    @property
    def min_k_hashes(self) -> list:
        return self.entry.get("min_k_hashes") or []

    @property
    def histogram_counts(self) -> list:
        return self.entry.get("histogram_counts") or []

    @property
    def histogram_bins(self) -> int:
        return int(self.entry.get("histogram_bins") or 0)

    @property
    def min_values(self) -> list:
        return self.entry.get("min_values") or []

    @property
    def max_values(self) -> list:
        return self.entry.get("max_values") or []

    @property
    def field_ids(self) -> list:
        """Stable field-id per position in min_values/max_values/etc., parallel
        to those lists. Empty for manifest rows written before field-ids
        existed."""
        return self.entry.get("field_ids") or []

    @property
    def lower_bounds(self) -> Optional[Dict[int, Any]]:
        """min_values keyed by field-id instead of position. None when this
        entry has no field-ids (pre-migration manifest row) — callers must
        fall back to positional indexing of min_values in that case."""
        field_ids, min_values = self.field_ids, self.min_values
        if not field_ids or len(field_ids) != len(min_values):
            return None
        return {fid: v for fid, v in zip(field_ids, min_values) if fid is not None}

    @property
    def upper_bounds(self) -> Optional[Dict[int, Any]]:
        """max_values keyed by field-id — see lower_bounds."""
        field_ids, max_values = self.field_ids, self.max_values
        if not field_ids or len(field_ids) != len(max_values):
            return None
        return {fid: v for fid, v in zip(field_ids, max_values) if fid is not None}


@dataclass
class SimpleDataset(Dataset):
    identifier: str
    _metadata: DatasetMetadata
    io: Any = None
    catalog: Any = None

    @property
    def metadata(self) -> DatasetMetadata:
        return self._metadata

    def _next_sequence_number(self) -> int:
        """Calculate the next sequence number.

        Uses the current snapshot's sequence number + 1. Works efficiently
        with load_history=False since we only need the most recent snapshot,
        not the full history.

        Returns:
            The next sequence number (current snapshot's sequence + 1, or 1 if no snapshots).
        """
        if not self.metadata.snapshots:
            # No snapshots yet - this is the first one
            return 1

        # Get the current (most recent) snapshot - should have the highest sequence number
        current = self.snapshot()
        if current:
            seq = getattr(current, "sequence_number", None)
            return int(seq) + 1 if seq is not None else 1

    def snapshot(
        self, snapshot_id: Optional[int] = None, user_only: bool = False
    ) -> Optional[Snapshot]:
        """Return a Snapshot.

        - If `snapshot_id` is None, return the in-memory current snapshot.
        - If a `snapshot_id` is provided, prefer a Firestore lookup via the
          attached `catalog` (O(1) document get). Fall back to the in-memory
          `metadata.snapshots` list only when no catalog is attached or the
          remote lookup fails.
        - If `user_only` is True (with no `snapshot_id`), return the most
          recent USER-created snapshot instead of the current one — see
          `last_user_snapshot`.
        """
        if user_only:
            if snapshot_id is not None:
                # Contradictory: one asks for a specific snapshot, the other
                # for whichever is the latest user one. Refuse rather than
                # silently honouring one and ignoring the other.
                raise ValueError(
                    "snapshot(): `user_only=True` cannot be combined with an explicit "
                    "`snapshot_id`; pass one or the other"
                )
            return self.last_user_snapshot()

        # Current snapshot: keep in memory for fast access
        if snapshot_id is None:
            return self.metadata.current_snapshot()

        # Try Firestore document lookup when catalog attached
        if self.catalog:
            try:
                collection, dataset_name = self.identifier.split(".")
                doc = (
                    self.catalog._dataset_doc_ref(collection, dataset_name)
                    .collection("snapshots")
                    .document(str(snapshot_id))
                    .get()
                )
                if doc.exists:
                    sd = doc.to_dict() or {}
                    snap = Snapshot(
                        snapshot_id=int(sd.get("snapshot-id") or snapshot_id),
                        timestamp_ms=int(sd.get("timestamp-ms", 0)),
                        author=sd.get("author"),
                        sequence_number=sd.get("sequence-number", 0),
                        user_created=sd.get("user-created"),
                        manifest_list=sd.get("manifest"),
                        schema_id=sd.get("schema-id"),
                        summary=sd.get("summary", {}),
                        operation_type=sd.get("operation-type"),
                        parent_snapshot_id=sd.get("parent-snapshot-id"),
                        commit_message=sd.get("commit-message"),
                    )
                    return snap
            except Exception:
                # Be conservative: fall through to in-memory fallback
                pass

        # Fallback: search in-memory snapshots (only used when no catalog)
        for s in self.metadata.snapshots:
            if s.snapshot_id == snapshot_id:
                return s

        return None

    def last_user_snapshot(self, lookback: Optional[int] = None) -> Optional[Snapshot]:
        """The most recent snapshot a USER created, or None if there is none.

        `lookback` bounds the search to that many most-recent snapshots
        (None = search all of them). See `select_last_user_snapshot`.

        The current snapshot is frequently NOT one: compaction, expiration and
        statistics refresh (`refresh_manifest`) all commit snapshots of their
        own, so a dataset nobody has written to for a week can still show a
        commit from minutes ago. Surfacing that in a UI as "your last commit"
        invites the reasonable question of why there are commits the user
        never made. This answers "when did a HUMAN last change this data?"
        instead.

        `user_created` is authoritative here: every writer sets it (True for
        `append`/`overwrite`/`truncate`, False for the maintenance
        operations), so only an explicit True counts — a missing or None
        value is treated as "not known to be a user commit" rather than
        assumed to be one, since guessing wrong reintroduces exactly the
        confusion this exists to remove.

        Cost: the in-memory `metadata.snapshots` list normally holds only the
        current snapshot (`load_dataset(load_history=False)` is the default),
        so when that one is not user-created this streams the snapshots
        subcollection and picks the winner client-side. That mirrors how the
        rest of this module reads snapshots — no `where`/`order_by` query, so
        no composite index to provision — and it is a UI-facing lookup, not a
        query-plan hot path. When the current snapshot IS user-created, which
        is the common case, it returns immediately with no extra read.
        """
        current = self.metadata.current_snapshot()
        if current is not None and current.user_created is True:
            return current

        candidates: list = []
        if self.catalog:
            try:
                collection, dataset_name = self.identifier.split(".")
                # pylint: disable=protected-access
                snaps_coll = self.catalog._snapshots_collection(collection, dataset_name)
                candidates = [
                    self.catalog._snapshot_from_dict(doc.to_dict() or {})
                    for doc in snaps_coll.stream()
                ]
            except Exception:
                # Fall through to whatever is already in memory rather than
                # failing a read-only lookup outright.
                candidates = []

        if not candidates:
            candidates = list(self.metadata.snapshots)

        return select_last_user_snapshot(candidates, lookback=lookback)

    def _get_node(self) -> str:
        """Return the stable node identifier for this process.

        Uses a module-level constant to avoid per-instance hashing/caching.
        """
        return _NODE

    def snapshots(self) -> Iterable[Snapshot]:
        return list(self.metadata.snapshots)

    def schema(self, schema_id: Optional[str] = None) -> Optional[RelationSchema]:
        """Return a stored schema description.

        If `schema_id` is None, return the current schema (by
        `metadata.current_schema_id` or last-known schema). If a
        specific `schema_id` is provided, attempt to find it in the
        in-memory `metadata.schemas` list and, failing that, fetch it
        from the catalog's `schemas` subcollection when a catalog is
        attached.

        Returns the stored schema dict (contains keys like `schema_id`,
        `columns`, `timestamp-ms`, etc.) or None if not found.
        """
        # Determine which schema id to use
        sid = schema_id or self.metadata.current_schema_id

        # If no sid and a raw schema is stored on the metadata, return it
        if sid is None:
            return getattr(self.metadata, "schema", None)

        # Fast path: if this is the current schema id, prefer the cached
        # current schema (99% case) rather than scanning the entire list.
        sdict = None
        if sid == self.metadata.current_schema_id:
            if getattr(self.metadata, "schemas", None):
                last = self.metadata.schemas[-1]
                if last.get("schema_id") == sid:
                    sdict = last
            else:
                # If a raw schema is stored directly on metadata, use it.
                raw = getattr(self.metadata, "schema", None)
                if raw is not None:
                    sdict = {"schema_id": sid, "columns": raw}

        # If not the current schema, or cached current not present,
        # prefer to load the schema document from the backend (O(1) doc get).
        if sdict is None and self.catalog:
            try:
                collection, dataset_name = self.identifier.split(".")
                doc = (
                    self.catalog._dataset_doc_ref(collection, dataset_name)
                    .collection("schemas")
                    .document(sid)
                    .get()
                )
                sdict = doc.to_dict() or None
            except Exception:
                sdict = None

        # As a last-resort when no catalog is attached, fall back to an
        # in-memory search for compatibility (offline/unit-test mode).
        if sdict is None and not self.catalog:
            for s in self.metadata.schemas or []:
                if s.get("schema_id") == sid:
                    sdict = s
                    break

        if sdict is None:
            return None

        # Build a dependency-free RelationSchema from the stored column
        # metadata (see SchemaColumn/RelationSchema above).
        raw = sdict.get("columns")

        columns = [
            SchemaColumn(
                name=c.get("name"),
                type=_stored_type_display(c),
                element_type=c.get("element-type") or c.get("element_type"),
                precision=c.get("precision"),
                scale=c.get("scale"),
                nullable=c.get("nullable", True),
                id=c.get("id"),
            )
            for c in raw
        ]
        return RelationSchema(name=self.identifier, columns=columns)

    def _field_id_by_name(self) -> Dict[str, int]:
        """Current schema's name->field_id mapping, for keying manifest stats.

        Returns an empty dict for schemas with no catalog-assigned ids (e.g.
        datasets created before field-ids existed and not yet backfilled) —
        callers must treat that as "no field-ids available", not an error.
        """
        schema = self.schema()
        if schema is None:
            return {}
        return {
            col.name: col.id for col in schema.columns if getattr(col, "id", None) is not None
        }

    def append(self, table: Any, author: str = None, commit_message: Optional[str] = None):
        """Append a draken Morsel:

        - write a Parquet data file via `self.io`
        - create a simple Parquet manifest (one entry)
        - persist manifest and snapshot metadata using the attached `catalog`
        """
        snapshot_id = int(time.time() * 1000)

        if not hasattr(table, "num_rows") or not hasattr(table, "column_names"):
            raise TypeError("append() expects a draken.morsels.morsel.Morsel-like object")

        # Write table and build manifest entry
        manifest_entry = self._write_table_and_build_entry(table)
        entries = [manifest_entry.to_dict()]

        # persist manifest: for append, merge previous manifest entries
        # with the new entries so the snapshot's manifest is cumulative.
        manifest_path = None
        if self.catalog and hasattr(self.catalog, "write_parquet_manifest"):
            merged_entries = list(entries)

            # If there is a previous snapshot with a manifest, try to read
            # it and prepend its entries. Any read error is non-fatal and we
            # fall back to writing only the new entries.
            prev_snap = self.snapshot(None)
            if prev_snap and getattr(prev_snap, "manifest_list", None):
                prev_manifest_path = prev_snap.manifest_list
                try:
                    # Prefer FileIO when available
                    inp = self.io.new_input(prev_manifest_path)
                    with inp.open() as f:
                        prev_data = f.read()
                    from .manifest import read_manifest_rows

                    prev_rows = read_manifest_rows(prev_data)
                    merged_entries = prev_rows + merged_entries
                except Exception:
                    # If we can't read the previous manifest, continue with
                    # just the new entries (don't fail the append).
                    pass

            manifest_path = self.catalog.write_parquet_manifest(
                snapshot_id, merged_entries, self.metadata.location
            )

        # snapshot metadata
        if author is None:
            raise ValueError("author must be provided when appending to a dataset")
        # update metadata author/timestamp for this append
        self.metadata.author = author
        self.metadata.timestamp_ms = snapshot_id
        # default commit message
        if commit_message is None:
            commit_message = f"commit by {author}"

        recs = int(table.num_rows)
        fsize = int(getattr(manifest_entry, "file_size_in_bytes", 0))
        # Calculate uncompressed size from the manifest entry
        added_data_size = manifest_entry.uncompressed_size_in_bytes
        added_data_files = 1
        added_files_size = fsize
        added_records = recs
        deleted_data_files = 0
        deleted_files_size = 0
        deleted_data_size = 0
        deleted_records = 0

        prev = self.snapshot()
        if prev and prev.summary:
            prev_total_files = int(prev.summary.get("total-data-files", 0))
            prev_total_size = int(prev.summary.get("total-files-size", 0))
            prev_total_data_size = int(prev.summary.get("total-data-size", 0))
            prev_total_records = int(prev.summary.get("total-records", 0))
        else:
            prev_total_files = 0
            prev_total_size = 0
            prev_total_data_size = 0
            prev_total_records = 0

        total_data_files = prev_total_files + added_data_files - deleted_data_files
        total_files_size = prev_total_size + added_files_size - deleted_files_size
        total_data_size = prev_total_data_size + added_data_size - deleted_data_size
        total_records = prev_total_records + added_records - deleted_records

        summary = {
            "added-data-files": added_data_files,
            "added-files-size": added_files_size,
            "added-data-size": added_data_size,
            "added-records": added_records,
            "deleted-data-files": deleted_data_files,
            "deleted-files-size": deleted_files_size,
            "deleted-data-size": deleted_data_size,
            "deleted-records": deleted_records,
            "total-data-files": total_data_files,
            "total-files-size": total_files_size,
            "total-data-size": total_data_size,
            "total-records": total_records,
        }

        # sequence number
        try:
            next_seq = self._next_sequence_number()
        except Exception:
            next_seq = 1

        parent_id = self.metadata.current_snapshot_id

        snap = Snapshot(
            snapshot_id=snapshot_id,
            timestamp_ms=snapshot_id,
            author=author,
            sequence_number=next_seq,
            user_created=True,
            operation_type="append",
            parent_snapshot_id=parent_id,
            manifest_list=manifest_path,
            schema_id=self.metadata.current_schema_id,
            commit_message=commit_message,
            summary=summary,
        )

        self.metadata.snapshots.append(snap)
        self.metadata.current_snapshot_id = snapshot_id

        # persist metadata (let errors propagate)
        if self.catalog and hasattr(self.catalog, "save_snapshot"):
            self.catalog.save_snapshot(self.identifier, snap)
        if self.catalog and hasattr(self.catalog, "save_dataset_metadata"):
            self.catalog.save_dataset_metadata(self.identifier, self.metadata)

        self._emit_audit(
            "append",
            author=author,
            snapshot_id=snapshot_id,
            record_count=recs,
            files_added=added_data_files,
            bytes_added=added_files_size,
        )

    def _emit_audit(self, action: str, *, author: Optional[str], **detail: Any) -> None:
        """Record a data-changing operation against this dataset."""
        collection, _, name = self.identifier.partition(".")
        emit_audit(
            action,
            resource_type="dataset",
            workspace=getattr(self.catalog, "workspace", None),
            collection=collection,
            resource=name or None,
            author=author,
            **detail,
        )

    def _write_table_and_build_entry(self, table: Any):
        """Write a draken Morsel to storage and return a ParquetManifestEntry.

        This centralizes the IO and manifest construction so other operations
        (e.g. `overwrite`) can reuse the same behavior as `append`.
        """
        # Write parquet file with collision-resistant name
        fname = f"{time.time_ns():x}-{self._get_node()}.parquet"
        data_path = f"{self.metadata.location}/data/{fname}"

        from rugo.parquet import write_parquet

        from ..iops.fileio import WRITE_PARQUET_OPTIONS

        pdata = write_parquet(table, **WRITE_PARQUET_OPTIONS)

        out = self.io.new_output(data_path).create()
        out.write(pdata)
        out.close()

        # Build manifest entry with statistics directly from the in-memory
        # Morsel (avoids re-reading the file and losing temporal/decimal
        # semantic types to Parquet's physical-int round-trip).
        manifest_entry = build_parquet_manifest_entry_from_morsel(
            table, pdata, data_path, len(pdata), self._field_id_by_name()
        )
        return manifest_entry

    def overwrite(self, table: Any, author: str = None, commit_message: Optional[str] = None):
        """Replace the dataset entirely with `table` in a single snapshot.

        Semantics:
        - Write the provided table as new data file(s)
        - Create a new parquet manifest that contains only the new entries
        - Create a snapshot that records previous files as deleted and the
          new files as added (logical replace)
        """
        # Similar validation as append
        snapshot_id = int(time.time() * 1000)

        if not hasattr(table, "num_rows") or not hasattr(table, "column_names"):
            raise TypeError("overwrite() expects a draken.morsels.morsel.Morsel-like object")

        if author is None:
            raise ValueError("author must be provided when overwriting a dataset")

        # Write new data and build manifest entries (single table -> single entry)
        manifest_entry = self._write_table_and_build_entry(table)
        new_entries = [manifest_entry.to_dict()]

        # Write manifest containing only the new entries
        manifest_path = None
        if self.catalog and hasattr(self.catalog, "write_parquet_manifest"):
            manifest_path = self.catalog.write_parquet_manifest(
                snapshot_id, new_entries, self.metadata.location
            )

        # Compute deltas: previous manifest becomes deleted
        prev = self.snapshot(None)
        prev_total_files = 0
        prev_total_size = 0
        prev_total_data_size = 0
        prev_total_records = 0
        if prev and prev.summary:
            prev_total_files = int(prev.summary.get("total-data-files", 0))
            prev_total_size = int(prev.summary.get("total-files-size", 0))
            prev_total_data_size = int(prev.summary.get("total-data-size", 0))
            prev_total_records = int(prev.summary.get("total-records", 0))

        deleted_data_files = prev_total_files
        deleted_files_size = prev_total_size
        deleted_data_size = prev_total_data_size
        deleted_records = prev_total_records

        added_data_files = len(new_entries)
        added_files_size = sum(e.get("file_size_in_bytes", 0) for e in new_entries)
        added_data_size = sum(e.get("uncompressed_size_in_bytes", 0) for e in new_entries)
        added_records = sum(e.get("record_count", 0) for e in new_entries)

        total_data_files = added_data_files
        total_files_size = added_files_size
        total_data_size = added_data_size
        total_records = added_records

        summary = {
            "added-data-files": added_data_files,
            "added-files-size": added_files_size,
            "added-data-size": added_data_size,
            "added-records": added_records,
            "deleted-data-files": deleted_data_files,
            "deleted-files-size": deleted_files_size,
            "deleted-data-size": deleted_data_size,
            "deleted-records": deleted_records,
            "total-data-files": total_data_files,
            "total-files-size": total_files_size,
            "total-data-size": total_data_size,
            "total-records": total_records,
        }

        # sequence number
        try:
            next_seq = self._next_sequence_number()
        except Exception:
            next_seq = 1

        parent_id = self.metadata.current_snapshot_id

        if commit_message is None:
            commit_message = f"overwrite by {author}"

        snap = Snapshot(
            snapshot_id=snapshot_id,
            timestamp_ms=snapshot_id,
            author=author,
            sequence_number=next_seq,
            user_created=True,
            operation_type="overwrite",
            parent_snapshot_id=parent_id,
            manifest_list=manifest_path,
            schema_id=self.metadata.current_schema_id,
            commit_message=commit_message,
            summary=summary,
        )

        # Replace in-memory snapshots
        self.metadata.snapshots.append(snap)
        self.metadata.current_snapshot_id = snapshot_id

        if self.catalog and hasattr(self.catalog, "save_snapshot"):
            self.catalog.save_snapshot(self.identifier, snap)
        if self.catalog and hasattr(self.catalog, "save_dataset_metadata"):
            self.catalog.save_dataset_metadata(self.identifier, self.metadata)

    def add_files(self, files: list[str], author: str = None, commit_message: Optional[str] = None):
        """Add filenames to the dataset manifest without writing the files.

        - `files` is a list of file paths (strings). Files are assumed to
          already exist in storage; this method only updates the manifest.
        - Does not add files that already appear in the current manifest
          (deduplicates by `file_path`).
        - Creates a cumulative manifest for the new snapshot (previous
          entries + new unique entries).
        """
        if author is None:
            raise ValueError("author must be provided when adding files to a dataset")

        snapshot_id = int(time.time() * 1000)

        # Gather previous summary and manifest entries
        prev = self.snapshot(None)
        prev_total_files = 0
        prev_total_size = 0
        prev_total_records = 0
        prev_entries = []
        if prev and prev.summary:
            prev_total_files = int(prev.summary.get("total-data-files", 0))
            prev_total_size = int(prev.summary.get("total-files-size", 0))
            prev_total_records = int(prev.summary.get("total-records", 0))
        if prev and getattr(prev, "manifest_list", None):
            # try to read prev manifest entries
            try:
                from .manifest import read_manifest_rows

                inp = self.io.new_input(prev.manifest_list)
                with inp.open() as f:
                    data = f.read()
                prev_entries = read_manifest_rows(data)
            except Exception:
                prev_entries = []

        existing = {
            e.get("file_path") for e in prev_entries if isinstance(e, dict) and e.get("file_path")
        }

        # Build new entries for files that don't already exist. Only accept
        # Parquet files and compute full statistics for each file.
        new_entries = []
        seen = set()
        for fp in files:
            if not fp or fp in existing or fp in seen:
                continue
            if not fp.lower().endswith(".parquet"):
                # only accept parquet files
                continue
            seen.add(fp)

            # Read file and compute full statistics
            try:
                inp = self.io.new_input(fp)
                with inp.open() as f:
                    data = f.read()

                if data:
                    # Compute statistics using a single read of the compressed bytes
                    file_size = len(data)
                    manifest_entry = build_parquet_manifest_entry_from_bytes(
                        data, fp, file_size, field_id_by_name=self._field_id_by_name()
                    )
                else:
                    # Empty file, create placeholder entry
                    manifest_entry = ParquetManifestEntry(
                        file_path=fp,
                        file_format="parquet",
                        record_count=0,
                        null_counts=[],
                        file_size_in_bytes=0,
                        uncompressed_size_in_bytes=0,
                        column_uncompressed_sizes_in_bytes=[],
                        min_k_hashes=[],
                        histogram_counts=[],
                        histogram_bins=0,
                        min_values=[],
                        max_values=[],
                    )
            except Exception:
                # If read fails, fall back to placeholders
                manifest_entry = ParquetManifestEntry(
                    file_path=fp,
                    file_format="parquet",
                    record_count=0,
                    null_counts=[],
                    file_size_in_bytes=0,
                    uncompressed_size_in_bytes=0,
                    column_uncompressed_sizes_in_bytes=[],
                    min_k_hashes=[],
                    histogram_counts=[],
                    histogram_bins=0,
                    min_values=[],
                    max_values=[],
                )
            new_entries.append(manifest_entry.to_dict())

        merged_entries = prev_entries + new_entries

        # write cumulative manifest
        manifest_path = None
        if self.catalog and hasattr(self.catalog, "write_parquet_manifest"):
            manifest_path = self.catalog.write_parquet_manifest(
                snapshot_id, merged_entries, self.metadata.location
            )

        # Build summary deltas
        added_data_files = len(new_entries)
        added_files_size = 0
        added_data_size = 0
        added_records = 0
        # Sum statistics from new entries
        for entry in new_entries:
            added_files_size += entry.get("file_size_in_bytes", 0)
            added_data_size += entry.get("uncompressed_size_in_bytes", 0)
            added_records += entry.get("record_count", 0)
        deleted_data_files = 0
        deleted_files_size = 0
        deleted_data_size = 0
        deleted_records = 0

        prev_total_data_size = (
            int(prev.summary.get("total-data-size", 0)) if prev and prev.summary else 0
        )

        total_data_files = prev_total_files + added_data_files - deleted_data_files
        total_files_size = prev_total_size + added_files_size - deleted_files_size
        total_data_size = prev_total_data_size + added_data_size - deleted_data_size
        total_records = prev_total_records + added_records - deleted_records

        summary = {
            "added-data-files": added_data_files,
            "added-files-size": added_files_size,
            "added-data-size": added_data_size,
            "added-records": added_records,
            "deleted-data-files": deleted_data_files,
            "deleted-files-size": deleted_files_size,
            "deleted-data-size": deleted_data_size,
            "deleted-records": deleted_records,
            "total-data-files": total_data_files,
            "total-files-size": total_files_size,
            "total-data-size": total_data_size,
            "total-records": total_records,
        }

        # Sequence number
        try:
            next_seq = self._next_sequence_number()
        except Exception:
            next_seq = 1

        parent_id = self.metadata.current_snapshot_id

        if commit_message is None:
            commit_message = f"add files by {author}"

        snap = Snapshot(
            snapshot_id=snapshot_id,
            timestamp_ms=snapshot_id,
            author=author,
            sequence_number=next_seq,
            user_created=True,
            operation_type="add-files",
            parent_snapshot_id=parent_id,
            manifest_list=manifest_path,
            schema_id=self.metadata.current_schema_id,
            commit_message=commit_message,
            summary=summary,
        )

        self.metadata.snapshots.append(snap)
        self.metadata.current_snapshot_id = snapshot_id

        if self.catalog and hasattr(self.catalog, "save_snapshot"):
            self.catalog.save_snapshot(self.identifier, snap)
        if self.catalog and hasattr(self.catalog, "save_dataset_metadata"):
            self.catalog.save_dataset_metadata(self.identifier, self.metadata)

    def truncate_and_add_files(
        self, files: list[str], author: str = None, commit_message: Optional[str] = None
    ):
        """Truncate dataset (logical) and set manifest to provided files.

        - Writes a manifest that contains exactly the unique filenames provided.
        - Does not delete objects from storage.
        - Useful for replace/overwrite semantics.
        """
        if author is None:
            raise ValueError("author must be provided when truncating/adding files")

        snapshot_id = int(time.time() * 1000)

        # Read previous summary for reporting deleted counts
        prev = self.snapshot(None)
        prev_total_files = 0
        prev_total_size = 0
        prev_total_records = 0
        if prev and prev.summary:
            try:
                prev_total_files = int(prev.summary.get("total-data-files", 0))
            except Exception:
                prev_total_files = 0
            try:
                prev_total_size = int(prev.summary.get("total-files-size", 0))
            except Exception:
                prev_total_size = 0
            try:
                prev_total_records = int(prev.summary.get("total-records", 0))
            except Exception:
                prev_total_records = 0

        # Build unique new entries (ignore duplicates in input). Only accept
        # parquet files and compute full statistics for each file.
        new_entries = []
        seen = set()
        for fp in files:
            if not fp or fp in seen:
                continue
            if not fp.lower().endswith(".parquet"):
                continue
            seen.add(fp)

            try:
                data = None
                if self.io and hasattr(self.io, "new_input"):
                    inp = self.io.new_input(fp)
                    with inp.open() as f:
                        data = f.read()
                else:
                    if (
                        self.catalog
                        and getattr(self.catalog, "_storage_client", None)
                        and getattr(self.catalog, "gcs_bucket", None)
                    ):
                        bucket = self.catalog._storage_client.bucket(self.catalog.gcs_bucket)
                        parsed = fp
                        if parsed.startswith("gs://"):
                            parsed = parsed[5 + len(self.catalog.gcs_bucket) + 1 :]
                        blob = bucket.blob(parsed)
                        data = blob.download_as_bytes()

                if data:
                    # Compute statistics using a single read of the compressed bytes
                    file_size = len(data)
                    manifest_entry = build_parquet_manifest_entry_from_bytes(
                        data, fp, file_size, field_id_by_name=self._field_id_by_name()
                    )
                else:
                    # Empty file, create placeholder entry
                    manifest_entry = ParquetManifestEntry(
                        file_path=fp,
                        file_format="parquet",
                        record_count=0,
                        null_counts=[],
                        file_size_in_bytes=0,
                        uncompressed_size_in_bytes=0,
                        column_uncompressed_sizes_in_bytes=[],
                        min_k_hashes=[],
                        histogram_counts=[],
                        histogram_bins=0,
                        min_values=[],
                        max_values=[],
                    )
            except Exception:
                # If read fails, create placeholder entry
                manifest_entry = ParquetManifestEntry(
                    file_path=fp,
                    file_format="parquet",
                    record_count=0,
                    null_counts=[],
                    file_size_in_bytes=0,
                    uncompressed_size_in_bytes=0,
                    column_uncompressed_sizes_in_bytes=[],
                    min_k_hashes=[],
                    histogram_counts=[],
                    histogram_bins=0,
                    min_values=[],
                    max_values=[],
                )
            new_entries.append(manifest_entry.to_dict())

        manifest_path = None
        if self.catalog and hasattr(self.catalog, "write_parquet_manifest"):
            manifest_path = self.catalog.write_parquet_manifest(
                snapshot_id, new_entries, self.metadata.location
            )

        # Build summary: previous entries become deleted
        deleted_data_files = prev_total_files
        deleted_files_size = prev_total_size
        deleted_data_size = (
            int(prev.summary.get("total-data-size", 0)) if prev and prev.summary else 0
        )
        deleted_records = prev_total_records

        added_data_files = len(new_entries)
        added_files_size = 0
        added_data_size = 0
        added_records = 0
        # Sum statistics from new entries
        for entry in new_entries:
            added_files_size += entry.get("file_size_in_bytes", 0)
            added_data_size += entry.get("uncompressed_size_in_bytes", 0)
            added_records += entry.get("record_count", 0)

        total_data_files = added_data_files
        total_files_size = added_files_size
        total_data_size = added_data_size
        total_records = added_records

        summary = {
            "added-data-files": added_data_files,
            "added-files-size": added_files_size,
            "added-data-size": added_data_size,
            "added-records": added_records,
            "deleted-data-files": deleted_data_files,
            "deleted-files-size": deleted_files_size,
            "deleted-data-size": deleted_data_size,
            "deleted-records": deleted_records,
            "total-data-files": total_data_files,
            "total-files-size": total_files_size,
            "total-data-size": total_data_size,
            "total-records": total_records,
        }

        # Sequence number
        try:
            next_seq = self._next_sequence_number()
        except Exception:
            next_seq = 1

        parent_id = self.metadata.current_snapshot_id

        if commit_message is None:
            commit_message = f"truncate and add files by {author}"

        snap = Snapshot(
            snapshot_id=snapshot_id,
            timestamp_ms=snapshot_id,
            author=author,
            sequence_number=next_seq,
            user_created=True,
            operation_type="truncate-and-add-files",
            parent_snapshot_id=parent_id,
            manifest_list=manifest_path,
            schema_id=self.metadata.current_schema_id,
            commit_message=commit_message,
            summary=summary,
        )

        # Replace in-memory snapshots: append snapshot and update current id
        self.metadata.snapshots.append(snap)
        self.metadata.current_snapshot_id = snapshot_id

        if self.catalog and hasattr(self.catalog, "save_snapshot"):
            self.catalog.save_snapshot(self.identifier, snap)
        if self.catalog and hasattr(self.catalog, "save_dataset_metadata"):
            self.catalog.save_dataset_metadata(self.identifier, self.metadata)

    def scan(self, row_filter=None, snapshot_id: Optional[int] = None) -> Iterable[Datafile]:
        """Return Datafile objects for the given snapshot.

        - If `snapshot_id` is None, use the current snapshot.

        A snapshot carrying no manifest is an empty dataset. A snapshot whose
        manifest exists but cannot be read is a failure, and raises: yielding
        no rows would be indistinguishable from an empty dataset.
        """
        # Determine snapshot to read using the dataset-level helper which
        # prefers the in-memory current snapshot and otherwise performs a
        # backend lookup for the requested id.
        snap = self.snapshot(snapshot_id)

        if snap is None or not getattr(snap, "manifest_list", None):
            return

        # Use Arrow-native manifest retrieval (30-50% faster than to_pylist)
        from .manifest_arrow import get_arrow_manifest_rows

        for r in get_arrow_manifest_rows(self.io, snap.manifest_list):
            yield Datafile(entry=r)

    def manifest_sketch_vectors(self, snapshot_id: Optional[int] = None) -> dict:
        """Whole-column native draken Vectors for the sketch columns of a snapshot.

        Returns ``{column_name: Vector}`` for ``min_k_hashes`` / ``histogram_counts``
        so the planner can reduce them with native kernels instead of the per-file
        boxed lists. Reads through the same cached manifest retrieval as ``scan``
        (a cache hit when scan ran first), so this adds no extra decode. Empty dict
        when the snapshot has no manifest.
        """
        snap = self.snapshot(snapshot_id)
        if snap is None or not getattr(snap, "manifest_list", None):
            return {}
        from .manifest_arrow import get_arrow_manifest

        return get_arrow_manifest(self.io, snap.manifest_list).sketch_vectors

    def describe(self, snapshot_id: Optional[int] = None, bins: int = 10) -> dict:
        """Describe all schema columns for the given snapshot.

        Returns a dict mapping column name -> statistics (same shape as
        the previous `describe` per-column output).
        """
        import heapq

        snap = self.snapshot(snapshot_id)
        if snap is None or not getattr(snap, "manifest_list", None):
            raise ValueError("No manifest available for this dataset/snapshot")

        manifest_path = snap.manifest_list

        # Read manifest once using Arrow-native retrieval (30-50% faster)
        try:
            from .manifest_arrow import get_arrow_manifest

            manifest = get_arrow_manifest(self.io, manifest_path)
            entries = manifest.to_pylist()  # Convert to list only when needed
            if not entries:
                raise ValueError("Empty manifest data")
        except Exception:
            raise

        # Resolve schema and describe all columns
        relation_schema = None
        try:
            relation_schema = self.schema()
        except Exception:
            relation_schema = None

        if relation_schema is None:
            raise ValueError("Schema unavailable; cannot describe all columns")

        # Map column name -> index for every schema column
        col_to_idx: dict[str, int] = {c.name: i for i, c in enumerate(relation_schema.columns)}

        # Initialize accumulators per column
        stats: dict[str, dict] = {}
        for name in col_to_idx:
            stats[name] = {
                "null_count": 0,
                "mins": [],
                "maxs": [],
                "hashes": set(),
                "file_hist_infos": [],
                "min_displays": [],
                "max_displays": [],
                "uncompressed_bytes": 0,
            }

        total_rows = 0

        def _decode_minmax(v):
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return v
            # For strings stored as string values (not bytes), return as-is
            if isinstance(v, str):
                # Try to parse as number for backward compatibility
                try:
                    return int(v)
                except Exception:
                    try:
                        return float(v)
                    except Exception:
                        # Not a number, return the string itself for display
                        return v
            try:
                if isinstance(v, (bytes, bytearray, memoryview)):
                    b = bytes(v)
                    if b and b[-1] == 0xFF:
                        b = b[:-1]
                    s = b.decode("utf-8")
                    try:
                        return int(s)
                    except Exception:
                        try:
                            return float(s)
                        except Exception:
                            # Decoded bytes that aren't numbers, return as string
                            return s
            except Exception:
                pass
            return None

        # Single pass through entries updating per-column accumulators
        for ent in entries:
            if not isinstance(ent, dict):
                continue
            total_rows += int(ent.get("record_count") or 0)

            # prefetch lists
            ncounts = ent.get("null_counts") or []
            mks = ent.get("min_k_hashes") or []
            hists = ent.get("histogram_counts") or []
            mv = ent.get("min_values") or []
            xv = ent.get("max_values") or []
            col_sizes = ent.get("column_uncompressed_sizes_in_bytes") or []

            for cname, cidx in col_to_idx.items():
                # nulls
                try:
                    stats[cname]["null_count"] += int((ncounts or [0])[cidx])
                except Exception:
                    pass

                # mins/maxs
                try:
                    raw_min = mv[cidx]
                except Exception:
                    raw_min = None
                try:
                    raw_max = xv[cidx]
                except Exception:
                    raw_max = None
                dmin = _decode_minmax(raw_min)
                dmax = _decode_minmax(raw_max)
                if dmin is not None:
                    stats[cname]["mins"].append(dmin)
                if dmax is not None:
                    stats[cname]["maxs"].append(dmax)

                # min-k hashes (tolerant to scalar/list/tuple shapes)
                try:
                    col_mk = mks[cidx]
                    if isinstance(col_mk, (int, float, bytes, bytearray, memoryview, str)):
                        col_mk = (col_mk,)
                    elif col_mk is None:
                        col_mk = ()
                except Exception:
                    col_mk = ()
                for h in col_mk:
                    try:
                        stats[cname]["hashes"].add(int(h))
                    except Exception:
                        pass

                # histograms (tolerant to scalar/list/tuple)
                try:
                    col_hist = hists[cidx]
                    if isinstance(col_hist, (int, float, str)):
                        col_hist = (col_hist,)
                    elif col_hist is None:
                        col_hist = ()
                except Exception:
                    col_hist = ()
                if col_hist:
                    try:
                        if dmin is not None and dmax is not None and dmin != dmax:
                            stats[cname]["file_hist_infos"].append(
                                (float(dmin), float(dmax), list(col_hist))
                            )
                    except Exception:
                        pass

                # uncompressed bytes for this column (sum across files)
                try:
                    stats[cname]["uncompressed_bytes"] += int((col_sizes or [0])[cidx])
                except Exception:
                    pass

        # Build results per column
        results: dict[str, dict] = {}
        for cname, cidx in col_to_idx.items():
            s = stats[cname]
            # Handle mixed types: separate strings from numbers
            mins_filtered = [v for v in s["mins"] if v is not None]
            maxs_filtered = [v for v in s["maxs"] if v is not None]

            # Group by type: strings vs numbers
            str_mins = [v for v in mins_filtered if isinstance(v, str)]
            num_mins = [v for v in mins_filtered if not isinstance(v, str)]
            str_maxs = [v for v in maxs_filtered if isinstance(v, str)]
            num_maxs = [v for v in maxs_filtered if not isinstance(v, str)]

            # Use whichever type has values (strings take precedence for text columns)
            global_min = None
            global_max = None
            if str_mins:
                global_min = min(str_mins)
            elif num_mins:
                global_min = min(num_mins)

            if str_maxs:
                global_max = max(str_maxs)
            elif num_maxs:
                global_max = max(num_maxs)

            # kmv approx
            cardinality = 0
            cardinality_is_exact = False
            try:
                collected = s["hashes"]
                if collected:
                    smallest = heapq.nsmallest(32, collected)
                    k = len(smallest)
                    if k < 31:
                        cardinality = len(set(smallest))
                        cardinality_is_exact = True
                    else:
                        MAX_HASH = (1 << 64) - 1
                        R = max(smallest)
                        if R == 0:
                            cardinality = len(set(smallest))
                        else:
                            cardinality = int((k - 1) * (MAX_HASH + 1) / (R + 1))
            except Exception:
                cardinality = 0

            # distribution via distogram
            distribution = None
            # A column with fewer distinct values than requested buckets doesn't have
            # enough real data to fill `bins` slots -- interpolating across `bins`
            # anyway manufactures empty/fractional buckets that don't correspond to
            # any actual value. Only shrink using an exact cardinality: an estimate
            # can undercount, and shrinking on it would truncate a wider distribution.
            effective_bins = bins
            if cardinality_is_exact and cardinality > 0:
                effective_bins = min(bins, cardinality)
            if (
                s["file_hist_infos"]
                and global_min is not None
                and global_max is not None
                and global_max > global_min
            ):
                try:
                    from opteryx_catalog.maki_nage.distogram import Distogram
                    from opteryx_catalog.maki_nage.distogram import count as _count_dist
                    from opteryx_catalog.maki_nage.distogram import count_up_to as _count_up_to
                    from opteryx_catalog.maki_nage.distogram import merge as _merge_distogram
                    from opteryx_catalog.maki_nage.distogram import update as _update_distogram

                    dist_bin_count = max(50, effective_bins * 5)
                    global_d = Distogram(bin_count=dist_bin_count)
                    for fmin, fmax, counts in s["file_hist_infos"]:
                        fbins = len(counts)
                        if fbins <= 0:
                            continue
                        temp = Distogram(bin_count=dist_bin_count)
                        span = float(fmax - fmin) if fmax != fmin else 0.0
                        for bi, cnt in enumerate(counts):
                            if cnt <= 0:
                                continue
                            if span == 0.0:
                                rep = float(fmin)
                            else:
                                rep = fmin + (bi + 0.5) * span / fbins
                            _update_distogram(temp, float(rep), int(cnt))
                        global_d = _merge_distogram(global_d, temp)

                    distribution = [0] * effective_bins
                    total = int(_count_dist(global_d) or 0)
                    if total == 0:
                        distribution = [0] * effective_bins
                    else:
                        prev = 0.0
                        gmin = float(global_min)
                        gmax = float(global_max)
                        for i in range(1, effective_bins + 1):
                            edge = gmin + (i / effective_bins) * (gmax - gmin)
                            cum = _count_up_to(global_d, edge) or 0.0
                            distribution[i - 1] = int(round(cum - prev))
                            prev = cum
                        diff = total - sum(distribution)
                        if diff != 0:
                            distribution[-1] += diff
                except Exception:
                    distribution = [0] * effective_bins
                    gspan = float(global_max - global_min)
                    for fmin, fmax, counts in s["file_hist_infos"]:
                        fbins = len(counts)
                        if fbins <= 0:
                            continue
                        for bi, cnt in enumerate(counts):
                            if cnt <= 0:
                                continue
                            rep = fmin + (bi + 0.5) * (fmax - fmin) / fbins
                            gi = int((rep - global_min) / gspan * effective_bins)
                            if gi < 0:
                                gi = 0
                            if gi >= effective_bins:
                                gi = effective_bins - 1
                            distribution[gi] += int(cnt)

            res = {
                "dataset": self.identifier,
                "description": getattr(self.metadata, "description", None),
                "row_count": total_rows,
                "column": cname,
                "min": global_min,
                "max": global_max,
                "null_count": s["null_count"],
                "uncompressed_bytes": s["uncompressed_bytes"],
                "cardinality": cardinality,
                "cardinality_is_exact": cardinality_is_exact,
                "distribution": distribution,
            }

            # If textual, attempt display prefixes like describe()
            try:
                is_text = False
                if relation_schema is not None:
                    col = relation_schema.columns[cidx]
                    ctype = getattr(col, "type", None)
                    if ctype is not None:
                        sctype = str(ctype).lower()
                        if "char" in sctype or "string" in sctype or "varchar" in sctype:
                            is_text = True
            except Exception:
                is_text = False

            if is_text:
                # Use only textual display values collected from manifests.
                # Decode bytes and strip truncation marker (0xFF) if present.
                def _decode_display_raw(v):
                    if v is None:
                        return None
                    try:
                        if isinstance(v, (bytes, bytearray, memoryview)):
                            b = bytes(v)
                            if b and b[-1] == 0xFF:
                                b = b[:-1]
                            s_val = b.decode("utf-8", errors="replace")
                            return s_val[:16]
                        if isinstance(v, str):
                            return v[:16]
                    except Exception:
                        return None
                    return None

                min_disp = None
                max_disp = None
                try:
                    if s.get("min_displays"):
                        for v in s.get("min_displays"):
                            dv = _decode_display_raw(v)
                            if dv:
                                min_disp = dv
                                break
                    if s.get("max_displays"):
                        for v in s.get("max_displays"):
                            dv = _decode_display_raw(v)
                            if dv:
                                max_disp = dv
                                break
                except Exception:
                    min_disp = None
                    max_disp = None

                if min_disp is not None or max_disp is not None:
                    res["min_display"] = min_disp
                    res["max_display"] = max_disp

            results[cname] = res

        return results

    def refresh_manifest(self, agent: str, author: Optional[str] = None) -> int:
        """Refresh manifest statistics and create a new snapshot.

        - `agent`: identifier for the agent performing the refresh (string)
        - `author`: optional author to record; if omitted uses current snapshot author

        This recalculates per-file statistics (min/max, record counts, sizes,
        null counts, histograms, char-class byte stats) for every file in the
        current manifest, writes a new manifest and creates a new snapshot
        with `user_created=False` and `operation_type='statistics-refresh'`.

        Returns the new `snapshot_id`.

        Raises `ManifestRefreshError` if the current manifest cannot be read,
        or if ANY file's statistics cannot be recomputed — naming every file
        that failed. No snapshot is committed in that case: a manifest mixing
        freshly-computed statistics with silently-retained stale ones is
        indistinguishable downstream from a fully-successful refresh, so this
        fails whole rather than committing a partial result.
        """
        from opteryx_catalog.exceptions import ManifestRefreshError

        prev = self.snapshot(None)
        if prev is None or not getattr(prev, "manifest_list", None):
            raise ValueError("No current manifest available to refresh")

        # Use same author/commit-timestamp as previous snapshot unless overridden
        use_author = author if author is not None else getattr(prev, "author", None)

        snapshot_id = int(time.time() * 1000)

        # Rebuild manifest entries by re-reading each data file
        entries = []
        try:
            # Read previous manifest entries using Arrow-native retrieval
            from .manifest_arrow import get_arrow_manifest

            prev_manifest = get_arrow_manifest(self.io, prev.manifest_list)
            prev_rows = prev_manifest.to_pylist()
        except Exception as exc:
            # An unreadable manifest previously degraded to `prev_rows = []`,
            # which then "refreshed" zero files and committed a snapshot that
            # looked successful while describing nothing.
            raise ManifestRefreshError(
                f"refresh_manifest: could not read the current manifest "
                f"{prev.manifest_list!r}; no snapshot was committed. Cause: {exc}"
            ) from exc

        total_files = 0
        total_size = 0
        total_data_size = 0
        total_records = 0
        failures: list = []

        for ent in prev_rows:
            if not isinstance(ent, dict):
                continue
            fp = ent.get("file_path")
            if not fp:
                continue
            try:
                inp = self.io.new_input(fp)
                with inp.open() as f:
                    data = f.read()
                # Full statistics including histograms and k-hashes
                file_size = len(data)
                manifest_entry = build_parquet_manifest_entry_from_bytes(
                    data, fp, file_size, field_id_by_name=self._field_id_by_name()
                )
                dent = manifest_entry.to_dict()
            except Exception as exc:
                # Collect and keep going: a bad batch write or bucket issue
                # usually affects many files at once, and surfacing them one
                # per re-run would take N runs to discover N bad files.
                failures.append((fp, exc))
                continue

            entries.append(dent)
            total_files += 1
            total_size += int(dent.get("file_size_in_bytes") or 0)
            total_data_size += int(dent.get("uncompressed_size_in_bytes") or 0)
            total_records += int(dent.get("record_count") or 0)

        if failures:
            detail = "; ".join(f"{fp}: {exc}" for fp, exc in failures)
            raise ManifestRefreshError(
                f"refresh_manifest: failed to recompute statistics for "
                f"{len(failures)} of {len(prev_rows)} file(s); no snapshot was "
                f"committed. Failures: {detail}"
            )

        # write new manifest
        manifest_path = self.catalog.write_parquet_manifest(
            snapshot_id, entries, self.metadata.location
        )

        # Build summary
        summary = {
            "added-data-files": 0,
            "added-files-size": 0,
            "added-data-size": 0,
            "added-records": 0,
            "deleted-data-files": 0,
            "deleted-files-size": 0,
            "deleted-data-size": 0,
            "deleted-records": 0,
            "total-data-files": total_files,
            "total-files-size": total_size,
            "total-data-size": total_data_size,
            "total-records": total_records,
        }

        # sequence number
        try:
            next_seq = self._next_sequence_number()
        except Exception:
            next_seq = 1

        parent_id = self.metadata.current_snapshot_id

        # Agent committer metadata
        agent_meta = {
            "timestamp": int(time.time() * 1000),
            "action": "statistics-refresh",
            "agent": agent,
        }

        snap = Snapshot(
            snapshot_id=snapshot_id,
            timestamp_ms=getattr(prev, "timestamp_ms", snapshot_id),
            author=use_author,
            sequence_number=next_seq,
            user_created=False,
            operation_type="statistics-refresh",
            parent_snapshot_id=parent_id,
            manifest_list=manifest_path,
            schema_id=self.metadata.current_schema_id,
            commit_message=getattr(prev, "commit_message", "statistics refresh"),
            summary=summary,
        )

        # attach agent metadata under summary
        if snap.summary is None:
            snap.summary = {}
        snap.summary["agent-committer"] = agent_meta

        # update in-memory metadata
        self.metadata.snapshots.append(snap)
        self.metadata.current_snapshot_id = snapshot_id

        # persist
        if self.catalog and hasattr(self.catalog, "save_snapshot"):
            self.catalog.save_snapshot(self.identifier, snap)
        if self.catalog and hasattr(self.catalog, "save_dataset_metadata"):
            self.catalog.save_dataset_metadata(self.identifier, self.metadata)

        return snapshot_id

    def truncate(
        self,
        author: str = None,
        commit_message: Optional[str] = None,
        commit_truncation: bool = False,
    ) -> None:
        """Delete all data files and manifests for this dataset.

        This attempts to delete every data file referenced by existing
        Parquet manifests and then delete the manifest files themselves.
        Finally it clears the in-memory snapshot list and persists the
        empty snapshot set via the attached `catalog` (if available).

        `commit_truncation` defaults to `False`, which preserves this
        method's existing behavior: the new snapshot is saved via
        `save_snapshot`, but the dataset document's `current-snapshot-id`
        pointer is NOT updated via `save_dataset_metadata`, so a fresh
        `load_dataset()` will still return the pre-truncate snapshot.
        This default is kept for backward compatibility with callers that
        rely on the current semantics (e.g. performing further in-memory
        mutations before a single final metadata save). Pass
        `commit_truncation=True` to also persist the updated dataset
        metadata immediately, so the truncation is visible to a subsequent
        `load_dataset()` call.
        """
        from .manifest import read_manifest_rows

        io = self.io
        # Collect files referenced by existing manifests but do NOT delete
        # them from storage. Instead we will write a new empty manifest and
        # create a truncate snapshot that records these files as deleted.
        snaps = list(self.metadata.snapshots)
        removed_files = []
        removed_total_size = 0
        removed_data_size = 0

        for snap in snaps:
            manifest_path = getattr(snap, "manifest_list", None)
            if not manifest_path:
                continue

            # Read manifest via FileIO if available
            rows = []
            try:
                inp = io.new_input(manifest_path)
                with inp.open() as f:
                    data = f.read()
                rows = read_manifest_rows(data)
            except Exception:
                rows = []

            for r in rows:
                fp = None
                fsize = 0
                data_size = 0
                if isinstance(r, dict):
                    fp = r.get("file_path")
                    fsize = int(r.get("file_size_in_bytes") or 0)
                    data_size = int(r.get("uncompressed_size_in_bytes") or 0)
                    if not fp and "data_file" in r and isinstance(r["data_file"], dict):
                        fp = r["data_file"].get("file_path") or r["data_file"].get("path")
                        fsize = int(r["data_file"].get("file_size_in_bytes") or 0)
                        data_size = int(r["data_file"].get("uncompressed_size_in_bytes") or 0)

                if fp:
                    removed_files.append(fp)
                    removed_total_size += fsize
                    removed_data_size += data_size

        # Create a new empty Parquet manifest (entries=[]) to represent the
        # truncated dataset for the new snapshot. Do not delete objects.
        snapshot_id = int(time.time() * 1000)

        # Do NOT write an empty Parquet manifest when there are no entries.
        # Per policy, create the snapshot without a manifest so older
        # snapshots remain readable and we avoid creating empty manifest files.
        manifest_path = None

        # Build summary reflecting deleted files (tracked, not removed)
        deleted_count = len(removed_files)
        deleted_size = removed_total_size

        summary = {
            "added-data-files": 0,
            "added-files-size": 0,
            "added-data-size": 0,
            "added-records": 0,
            "deleted-data-files": deleted_count,
            "deleted-files-size": deleted_size,
            "deleted-data-size": removed_data_size,
            "deleted-records": 0,
            "total-data-files": 0,
            "total-files-size": 0,
            "total-data-size": 0,
            "total-records": 0,
        }

        # Sequence number
        try:
            next_seq = self._next_sequence_number()
        except Exception:
            next_seq = 1

        if author is None:
            raise ValueError(
                "truncate() must be called with an explicit author; use truncate(author=...) in caller"
            )
        # update metadata author/timestamp for this truncate
        self.metadata.author = author
        self.metadata.timestamp_ms = snapshot_id
        # default commit message
        if commit_message is None:
            commit_message = f"commit by {author}"

        parent_id = self.metadata.current_snapshot_id

        snap = Snapshot(
            snapshot_id=snapshot_id,
            timestamp_ms=snapshot_id,
            author=author,
            sequence_number=next_seq,
            user_created=True,
            operation_type="truncate",
            parent_snapshot_id=parent_id,
            manifest_list=manifest_path,
            schema_id=self.metadata.current_schema_id,
            commit_message=commit_message,
            summary=summary,
        )

        # Append new snapshot and update current snapshot id
        self.metadata.snapshots.append(snap)
        self.metadata.current_snapshot_id = snapshot_id

        if self.catalog and hasattr(self.catalog, "save_snapshot"):
            self.catalog.save_snapshot(self.identifier, snap)
        if commit_truncation and self.catalog and hasattr(self.catalog, "save_dataset_metadata"):
            self.catalog.save_dataset_metadata(self.identifier, self.metadata)

        self._emit_audit(
            "truncate",
            author=author,
            snapshot_id=snapshot_id,
            files_removed=deleted_count,
            bytes_removed=removed_total_size,
        )
