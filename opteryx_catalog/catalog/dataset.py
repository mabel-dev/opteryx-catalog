from __future__ import annotations

import logging
import os
import time
import uuid
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from ..alerts import report as _alert
from ..audit import emit_audit
from ..exceptions import AddFilesReadError
from ..exceptions import ManifestReadError
from ..exceptions import SummaryInconsistencyError
from ..resource_types import ResourceType
from .manifest import ParquetManifestEntry
from .manifest import build_parquet_manifest_entry_from_bytes
from .manifest import build_parquet_manifest_entry_from_morsel
from .metadata import DatasetMetadata
from .metadata import Snapshot
from .metastore import Dataset

# Stable node identifier for this process (hex-mac-hex-pid)
_NODE = f"{uuid.getnode():x}-{os.getpid():x}"

logger = logging.getLogger(__name__)


def _as_number_or_text(text: str):
    """A numeric string as a number, otherwise the string itself."""
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        # Not a number, return the string itself for display
        return text


def _kmv_cardinality(hashes) -> tuple:
    """Distinct-value count from a min-k sketch: ``(count, is_exact)``.

    The sketch holds the 32 smallest distinct hashes seen. Fewer than 31 of
    them means the column had no more distinct values than that, so the count
    is EXACT; at 31 or more the sketch saturated and the KMV estimator takes
    over, whose relative standard error is roughly 1/sqrt(k-2) -- around 18% at
    k=32. Callers must keep the two apart: an estimate presented as a count
    gets quoted back as one.

    ``0`` with ``is_exact=False`` means "unknown", not "no values".

    Shared by the whole-column count and the ARRAY element count, which are the
    same sketch over different vectors -- rows in one case, the flat child in
    the other.
    """
    import heapq

    if not hashes:
        return 0, False
    try:
        smallest = heapq.nsmallest(32, hashes)
        k = len(smallest)
        if k < 31:
            return len(set(smallest)), True
        largest_of_smallest = max(smallest)
        if largest_of_smallest == 0:
            return len(set(smallest)), False
        return int((k - 1) * (1 << 64) / (largest_of_smallest + 1)), False
    except (TypeError, ValueError):
        # Members arrive through `_as_int`, so every one is an int and this
        # should be unreachable; it stays as a floor under the estimator rather
        # than letting one odd column fail a whole describe().
        return 0, False


def _decode_minmax(v):
    """A manifest min/max value as something comparable, or None.

    Min/max statistics reach us as numbers, as text, or as the UTF-8 bytes a
    writer stored for a string column - optionally with a trailing 0xFF, which
    marks a bound that was truncated to fit. Numeric-looking text decodes to a
    number so that bounds written by an older writer still compare against
    ones written today; anything genuinely undecodable is None, meaning "this
    file offers no bound for this column", not "the bound is zero".
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    # For strings stored as string values (not bytes), return as-is
    if isinstance(v, str):
        # Try to parse as number for backward compatibility
        return _as_number_or_text(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        b = bytes(v)
        if b and b[-1] == 0xFF:
            b = b[:-1]
        try:
            return _as_number_or_text(b.decode("utf-8"))
        except UnicodeDecodeError:
            # Binary or mid-codepoint-truncated bound: nothing to display.
            return None
    return None


def _at(values: Any, index: int) -> Any:
    """`values[index]`, or None when that is not something this can index.

    The per-column statistics on a manifest entry are parallel arrays that are
    supposed to line up with the schema, but an entry written by an older
    version - or by a writer that saw a different column set - can be short,
    scalar, or absent. A statistic that isn't there is not an error: it costs
    that column a pruning hint for that one file and nothing else.
    """
    try:
        return values[index]
    except (IndexError, KeyError, TypeError):
        return None


def _as_int(value: Any) -> int | None:
    """`value` as an int, or None if it will not convert.

    Snapshot rows and their summary counters come straight out of Firestore,
    so these fields arrive as ints, as numeric strings from an older writer,
    or missing entirely. Unconvertible is "unknown" - deliberately None rather
    than 0, so each caller chooses its own fallback: 0 is right for a
    reporting counter and wrong for `_next_sequence_number`, where it would
    restart the counter behind every snapshot already written.
    """
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def select_last_user_snapshot(
    snapshots: list[Snapshot], lookback: int | None = None
) -> Snapshot | None:
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
    ordered = sorted(snapshots, key=lambda s: (s.sequence_number or 0, s.snapshot_id or 0))
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
    element_type: str | None = None
    precision: int | None = None
    scale: int | None = None
    nullable: bool = True
    # Stable, catalog-assigned field-id (see DatasetMetadata.next_field_id) —
    # None for schemas persisted before field-ids existed.
    id: int | None = None


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
    def file_path(self) -> str | None:
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
    def lower_bounds(self) -> dict[int, Any] | None:
        """min_values keyed by field-id instead of position. None when this
        entry has no field-ids (pre-migration manifest row) — callers must
        fall back to positional indexing of min_values in that case."""
        field_ids, min_values = self.field_ids, self.min_values
        if not field_ids or len(field_ids) != len(min_values):
            return None
        return {fid: v for fid, v in zip(field_ids, min_values) if fid is not None}

    @property
    def upper_bounds(self) -> dict[int, Any] | None:
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

        Every path returns a number greater than one already on record, or 1
        only when nothing on record has one. Restarting the counter behind
        existing snapshots would silently reorder history: `sequence_number`
        is the primary sort key in `select_last_user_snapshot` and in
        expiration, so a snapshot numbered 1 sitting behind a hundred others
        is not a cosmetic wart - it moves which commit those read as current.

        Returns:
            The next sequence number (highest known sequence + 1, or 1 if none is known).
        """
        if not self.metadata.snapshots:
            # No snapshots yet - this is the first one
            return 1

        # Get the current (most recent) snapshot - should have the highest sequence number
        current = self.snapshot()
        seq = _as_int(getattr(current, "sequence_number", None))
        if seq is not None:
            return seq + 1

        # The current snapshot has no usable number: either it predates the
        # field, or `snapshot()` fell back to the in-memory list and found
        # nothing. Take the highest number actually on record rather than
        # restarting. Only a dataset whose snapshots ALL predate the field
        # reaches 1 here, which is the one case where 1 is honest.
        known = [
            number
            for number in (
                _as_int(getattr(snap, "sequence_number", None)) for snap in self.metadata.snapshots
            )
            if number is not None
        ]
        return max(known) + 1 if known else 1

    def snapshot(self, snapshot_id: int | None = None, user_only: bool = False) -> Snapshot | None:
        """Return a Snapshot.

        - If `snapshot_id` is None, return the in-memory current snapshot.
        - If a `snapshot_id` is provided, answer it from whatever copy is
          already in hand — `metadata.snapshots`, then the catalog's id-keyed
          `_snapshot_cache` — and only read the Firestore document when neither
          holds it, seeding the cache with what comes back.
        - If `user_only` is True (with no `snapshot_id`), return the most
          recent USER-created snapshot instead of the current one — see
          `last_user_snapshot`.

        Looking before fetching is safe for exactly the reason the catalog's
        caches exist (see the note on `_snapshot_cache` in
        `OpteryxCatalog.__init__`): a snapshot id addresses write-once content,
        so a copy already held IS the answer and the document read cannot
        return anything different. Only the dataset doc's `current-*-id`
        pointers are mutable, and those are re-read on every `get_relation()`.

        This used to try Firestore first and treat memory as the failure path,
        which cost a document read per call for a snapshot `load_dataset()` had
        just fetched. `get_dataset_metadata()` asks for the same id twice — once
        via `scan()`, once via `manifest_sketch_vectors()` — so planning a
        single-table query made three document reads of one document.
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

        # Already in memory: load_dataset() puts the current snapshot here, and
        # load_history=True puts every live one here.
        for s in self.metadata.snapshots:
            if s.snapshot_id == snapshot_id:
                return s

        # Try Firestore document lookup when catalog attached
        if self.catalog:
            try:
                collection, dataset_name = self.identifier.split(".")
                # The identifier split stays inside the try with the read it
                # keys: an identifier that is not `collection.dataset` raises
                # here, and that has always degraded to `None` rather than
                # propagating out of a read-only lookup.
                cache_key = (collection, dataset_name, snapshot_id)
                cached = self.catalog._snapshot_cache.get(cache_key)
                if cached is not None:
                    return cached
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
                    # Seed the id-keyed cache the same way load_dataset() does,
                    # so a historical id fetched once is not fetched again by
                    # the next Dataset built from this catalog.
                    self.catalog._snapshot_cache[cache_key] = snap
                    return snap
            except Exception as exc:  # noqa: BLE001 - Firestore client boundary
                # The google-cloud-firestore surface raises a wide, mostly
                # undocumented family here (transport, deadline, auth refresh)
                # on top of the KeyError/TypeError a drifted document shape
                # produces, so this catches broadly on purpose. It is a
                # read-only lookup with an in-memory answer available, so it
                # degrades rather than failing - but it says so.
                logger.debug(
                    "Snapshot %s of %s unreadable from Firestore (%s); using in-memory snapshots",
                    snapshot_id,
                    self.identifier,
                    exc,
                )

        # No in-memory copy, no cached copy, and either no catalog to ask or a
        # read that did not answer. There is nowhere left to look - the search
        # of `metadata.snapshots` that used to sit here has moved above the
        # remote read, so reaching this point already means it missed.
        return None

    def last_user_snapshot(self, lookback: int | None = None) -> Snapshot | None:
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
            except Exception as exc:  # noqa: BLE001 - Firestore client boundary, see snapshot()
                # Fall through to whatever is already in memory rather than
                # failing a read-only lookup outright.
                logger.debug(
                    "Snapshot stream for %s failed (%s); ranking in-memory snapshots instead",
                    self.identifier,
                    exc,
                )
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

    def schema(self, schema_id: str | None = None) -> RelationSchema | None:
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
            except Exception as exc:  # noqa: BLE001 - Firestore client boundary, see snapshot()
                logger.debug(
                    "Schema %s of %s unreadable from Firestore (%s); trying in-memory schemas",
                    sid,
                    self.identifier,
                    exc,
                )
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

    def _field_id_by_name(self) -> dict[str, int]:
        """Current schema's name->field_id mapping, for keying manifest stats.

        Returns an empty dict for schemas with no catalog-assigned ids (e.g.
        datasets created before field-ids existed and not yet backfilled) —
        callers must treat that as "no field-ids available", not an error.
        """
        schema = self.schema()
        if schema is None:
            return {}
        return {col.name: col.id for col in schema.columns if getattr(col, "id", None) is not None}

    def append(self, table: Any, author: str | None = None, commit_message: str | None = None):
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

        # Build the cumulative entry list for this snapshot: the previous
        # manifest's entries followed by the new one. A previous manifest that
        # cannot be read stops the commit — see `_parent_manifest_entries`.
        merged_entries = list(entries)
        prev_snap = self.snapshot(None)
        if prev_snap and getattr(prev_snap, "manifest_list", None):
            prev_rows = self._parent_manifest_entries(prev_snap)
            self._warn_if_summary_disagrees(prev_snap, prev_rows)
            merged_entries = prev_rows + merged_entries

        manifest_path = None
        if self.catalog and hasattr(self.catalog, "write_parquet_manifest"):
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

        # Totals describe the manifest just written, so they are computed from
        # it rather than accumulated from the parent's counters.
        summary = {
            "added-data-files": added_data_files,
            "added-files-size": added_files_size,
            "added-data-size": added_data_size,
            "added-records": added_records,
            "deleted-data-files": deleted_data_files,
            "deleted-files-size": deleted_files_size,
            "deleted-data-size": deleted_data_size,
            "deleted-records": deleted_records,
            **self._totals_from_entries(merged_entries),
        }

        # sequence number
        next_seq = self._next_sequence_number()

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

        self._after_commit(author, snap)

    def _parent_manifest_entries(self, snapshot) -> list[dict]:
        """Read the manifest entries a new commit must carry forward.

        Manifests are cumulative, so this list is the entire history of the
        dataset as far as the next snapshot is concerned. A read failure here
        used to be swallowed and treated as "no previous entries", which wrote
        a manifest containing only the newly added files — silently orphaning
        every file committed before it, with the totals in the snapshot summary
        left reading as though nothing had happened.

        Let the failure stop the commit. The previous snapshot keeps
        referencing its files, and the ingest can retry once the manifest is
        readable again.
        """
        manifest_path = snapshot.manifest_list
        try:
            from .manifest import read_manifest_rows

            inp = self.io.new_input(manifest_path)
            with inp.open() as f:
                data = f.read()
            return read_manifest_rows(data)
        except Exception as err:
            raise ManifestReadError(
                f"Cannot read parent manifest {manifest_path} for {self.identifier}: {err}. "
                "Refusing to commit a manifest that would drop its entries."
            ) from err

    def _totals_from_entries(self, entries: list[dict]) -> dict[str, int]:
        """Derive the snapshot summary totals from the manifest being written.

        These used to be carried forward as running counters (parent total +
        added - deleted), which made the summary an independent second source
        of truth that could — and did — drift arbitrarily far from the manifest
        it claimed to describe. Deriving them from the entries makes drift
        impossible by construction, and means a commit that follows a
        previously truncated manifest self-corrects the counters rather than
        inheriting the wrong ones. `DatasetCompactor` already computes its
        totals this way.
        """
        total_files_size = 0
        total_data_size = 0
        total_records = 0
        for entry in entries:
            total_files_size += int(entry.get("file_size_in_bytes") or 0)
            total_data_size += int(entry.get("uncompressed_size_in_bytes") or 0)
            total_records += int(entry.get("record_count") or 0)
        return {
            "total-data-files": len(entries),
            "total-files-size": total_files_size,
            "total-data-size": total_data_size,
            "total-records": total_records,
        }

    def _warn_if_summary_disagrees(self, snapshot, entries: list[dict]) -> None:
        """Log when a parent's recorded totals don't match its actual manifest.

        Evidence that an earlier commit wrote a manifest inconsistent with its
        summary. Deliberately not fatal: the totals for the snapshot being
        written are derived from the manifest, so proceeding repairs the
        counters, whereas refusing would strand the dataset in its corrupt
        state with no way to commit its way out.
        """
        summary = getattr(snapshot, "summary", None) or {}
        recorded = summary.get("total-data-files")
        if recorded is None or int(recorded) == len(entries):
            return
        import logging

        message = (
            f"Snapshot {snapshot.snapshot_id} of {self.identifier} records "
            f"total-data-files={recorded} but its manifest {snapshot.manifest_list} holds "
            f"{len(entries)} entries; totals for the new snapshot will be recomputed "
            "from the manifest."
        )
        logging.getLogger(__name__).error(message)
        # The only place in the package that compares recorded metadata against
        # reality, and it has only ever been a log line - which is why the
        # 2026-08-05 truncation kept reporting the pre-loss row count for hours.
        # Reported, not raised: see the docstring above.
        _alert(
            SummaryInconsistencyError(message),
            fingerprint=("summary-disagreement", self.identifier),
            context={
                "dataset": self.identifier,
                "snapshot_id": snapshot.snapshot_id,
                "manifest": snapshot.manifest_list,
                "recorded_total_data_files": recorded,
                "actual_manifest_entries": len(entries),
            },
        )

    def _emit_audit(self, action: str, *, author: str | None, **detail: Any) -> None:
        """Record a data-changing operation against this dataset."""
        collection, _, name = self.identifier.partition(".")
        emit_audit(
            action,
            resource_type=ResourceType.DATASET,
            workspace=getattr(self.catalog, "workspace", None),
            collection=collection,
            resource=name or None,
            author=author,
            **detail,
        )

    def _after_commit(self, author: str | None, snapshot: Snapshot) -> None:
        """Fire this dataset's triggers for a just-landed commit.

        Only user-created snapshots fire - `refresh_manifest`, compaction and
        expiration also land snapshots, and a housekeeping pass must not
        re-run every materialized view (`user_created` is authoritative, see
        `snapshot()`). Never raises into the commit path: `fire_triggers`
        alerts and audits its own failures, and this guard catches anything
        above it.
        """
        if getattr(snapshot, "user_created", None) is not True:
            return
        if self.catalog is None:
            return
        try:
            from ..trigger_firing import fire_triggers

            fire_triggers(
                self.catalog,
                self.identifier,
                author=author,
                snapshot_id=snapshot.snapshot_id,
            )
        except Exception as exc:  # noqa: BLE001 - the commit already landed
            _alert(
                exc,
                note="trigger firing failed after commit",
                fingerprint=("after-commit-firing", self.identifier),
                context={"dataset": self.identifier},
            )

    def _sort_for_write(self, table: Any):
        """Physically sort `table` by the dataset's configured sort order, if any.

        Compaction only clusters files once they've grown large enough for
        "sort-aware" merges (see compaction.py); left alone, freshly written
        files are never internally sorted. Since a single write's row count
        is small relative to compaction's thresholds, sorting here is cheap
        (draken's native sort is single-threaded, in-place-permutation, no
        Python object materialization) and gives every file - however small -
        real key locality plus honest `sorted_by` row-group metadata from the
        moment it's written.

        Returns (table, sort_column_name, sort_descending). On any resolution
        or sort failure, returns the input table unchanged with sort_column
        None - a write must never fail because sorting couldn't happen.
        """
        try:
            from .compaction import normalize_sort_order
            from .compaction import resolve_sort_column

            sort_order = normalize_sort_order(getattr(self.metadata, "sort_orders", None))
            if sort_order is None:
                return table, None, False

            schema = self.schema()
            columns = schema.columns if schema is not None else None
            if not columns:
                return table, None, False

            sort_column, _field_id, _index = resolve_sort_column(sort_order, columns)
            if sort_column is None:
                return table, None, False

            ascending = sort_order.get("ascending", True)

            from draken.morsels.sort import morsel_sort

            perm = morsel_sort(table, [sort_column], [ascending])
            table = table.take(list(perm))
            return table, sort_column, not ascending
        except Exception as exc:  # noqa: BLE001 - draken native sort, C-ABI boundary
            # Sorting is an optimisation: it buys key locality and honest
            # `sorted_by` row-group metadata, and the write is correct without
            # it. draken's sort is a native extension whose failures arrive as
            # anything from TypeError to SystemError, so this catches broadly -
            # but it must not be silent, or a dataset quietly stops being
            # clustered and only shows up as a query-performance regression.
            logger.warning(
                "Sort for write of %s failed (%s); writing unsorted", self.identifier, exc
            )
            return table, None, False

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

        table, sort_column, sort_descending = self._sort_for_write(table)

        write_options = dict(WRITE_PARQUET_OPTIONS)
        if sort_column is not None:
            write_options["sorted_by"] = sort_column
            write_options["sorted_descending"] = sort_descending

        pdata = write_parquet(table, **write_options)

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

    def alter_columns(
        self,
        add: list[dict] | None = None,
        drop: list[str] | None = None,
        rename: dict | None = None,
        retype: dict | None = None,
        author: str | None = None,
        commit_message: str | None = None,
    ):
        """Change the dataset's COLUMNS - ``ALTER TABLE ... ADD/DROP/RENAME/ALTER COLUMN``.

        Every current data file is rewritten to the new shape and committed as a
        new snapshot, so reads need to know nothing about schema generations:
        by the time this returns, every file the dataset points at matches its
        current schema.

        The rewrite does not decode the columns it is not changing. Parquet
        keeps the schema and the per-chunk byte offsets in a footer separate
        from the encoded pages, so each surviving column's pages are copied
        byte-for-byte and only a new footer is written (see
        ``rugo.parquet.patch_columns``). The cost tracks the dataset's SIZE, not
        the number of values in it - close to what moving the bytes costs.

        Files are written to NEW paths; the old ones are left alone for older
        snapshots to keep reading, and are reclaimed by the same sweep that
        reclaims dropped datasets. Nothing is ever mutated in place, so time
        travel keeps answering with the shape each snapshot was written under.

        Args:
            add: New columns, appended in order. Each is a stored-column dict
                (``{"name", "type", "element-type", "precision", "scale"}``)
                plus a ``"donor"``: a one-column, one-row parquet file carrying
                that column's type and the value existing rows are filled with
                (a null row means fill with NULL). Donors are built by the
                query engine, which owns the type vocabulary; the catalog only
                forwards them.
            drop: Column names to remove. Their pages are not carried over at
                all - the rewrite is a compaction that happens to drop a column.
            rename: ``{old_name: new_name}``. Touches no data whatsoever.
            retype: ``{name: {"type", ..., "donor": bytes}}``. The donor carries
                the TARGET type; its value is ignored.
            author: The identity making the change - None when unauthenticated,
                never substituted (see audit.emit_audit).
            commit_message: Optional message recorded on the snapshot.

        Raises:
            ValueError: If no author was given, or the requested change is not
                valid against the current schema (checked by the catalog before
                anything is written).
            AddFilesReadError: If a data file cannot be read to rewrite it.

        Note:
            The schema document and the snapshot are two writes, and the
            catalog has no transaction spanning them. Between them the dataset
            names the new columns while its current snapshot still lists the
            old files. The window is one Firestore round trip; it is called out
            here rather than papered over.
        """
        from rugo import parquet as _rugo_parquet

        # A rugo without the patcher cannot do this at all. Saying so beats the
        # bare ImportError the direct form would raise, which names a symbol
        # rather than the thing to upgrade.
        patch_columns = getattr(_rugo_parquet, "patch_columns", None)
        if patch_columns is None:
            raise RuntimeError(
                "Cannot alter columns: this deployment's rugo has no "
                "parquet.patch_columns. Upgrade rugo."
            )

        if author is None:
            raise ValueError("author must be provided when altering columns")
        add = [dict(c) for c in (add or [])]
        drop = list(drop or [])
        rename = dict(rename or {})
        retype = {k: dict(v) for k, v in (retype or {}).items()}
        if not (add or drop or rename or retype):
            raise ValueError("alter_columns was given no changes to make")
        if self.catalog is None:
            raise ValueError("alter_columns needs an attached catalog to record the schema")

        # Donors describe the FILES; everything else describes the SCHEMA. Split
        # them here so each half is handed only what it can act on.
        add_donors = [c.pop("donor", None) for c in add]
        retype_donors = {name: spec.pop("donor", None) for name, spec in retype.items()}
        for column, donor in zip(add, add_donors):
            if donor is None:
                raise ValueError(f"ADD COLUMN {column.get('name')!r} was given no donor file")
        for name, donor in retype_donors.items():
            if donor is None:
                raise ValueError(f"ALTER COLUMN {name!r} was given no donor file")

        # Check the change against the current schema BEFORE rewriting a single
        # file. The catalog validates it too, but only once the files are
        # written - so a typo'd column name would rewrite every file and then
        # fail in the parquet patcher, with a message about parquet rather than
        # about the column, leaving the rewritten files behind as orphans.
        schema = self.schema()
        known = {c.name for c in schema.columns} if schema is not None else set()
        if known:
            for name in list(drop) + list(rename) + list(retype):
                if name not in known:
                    raise ValueError(f"{self.identifier} has no column named '{name}'")
            surviving = {n for n in known if n not in drop}
            for old, new in rename.items():
                surviving.discard(old)
                if new in surviving:
                    raise ValueError(
                        f"renaming '{old}' to '{new}' would give {self.identifier} "
                        f"two columns called '{new}'"
                    )
                surviving.add(new)
            for column in add:
                if column.get("name") in surviving:
                    raise ValueError(
                        f"{self.identifier} already has a column called "
                        f"'{column.get('name')}'"
                    )
                surviving.add(column.get("name"))
            if not surviving:
                raise ValueError(
                    f"dropping every column of {self.identifier} would leave no relation"
                )

        prev = self.snapshot(None)
        prev_entries = []
        if prev and getattr(prev, "manifest_list", None):
            prev_entries = self._parent_manifest_entries(prev)
        current_files = [
            e.get("file_path") for e in prev_entries if isinstance(e, dict) and e.get("file_path")
        ]

        new_files = []
        for source_path in current_files:
            try:
                inp = self.io.new_input(source_path)
                with inp.open() as f:
                    data = f.read()
            except Exception as err:
                raise AddFilesReadError(
                    f"Cannot read {source_path} to alter {self.identifier}: {err}. "
                    "Refusing to commit a schema its own files do not match."
                ) from err

            patched = patch_columns(
                data,
                drop=drop or None,
                rename=rename or None,
                add=add_donors or None,
                retype=retype_donors or None,
            )

            fname = f"{time.time_ns():x}-{self._get_node()}.parquet"
            data_path = f"{self.metadata.location}/data/{fname}"
            out = self.io.new_output(data_path).create()
            out.write(patched)
            out.close()
            new_files.append(data_path)

        # Schema before snapshot: the manifest entries written below are keyed
        # by field id (`_field_id_by_name`), which has to resolve against the
        # schema the files now match, not the one they used to.
        schema_id = self.catalog.alter_dataset_schema(
            self.identifier,
            add=add or None,
            drop=drop or None,
            rename=rename or None,
            retype=retype or None,
            author=author,
        )
        self.metadata.current_schema_id = schema_id

        # truncate_and_add_files recomputes every statistic from the bytes it
        # just read, so nothing carries a stale bound from the old shape - the
        # positional remap a manifest-preserving commit would need does not
        # arise here at all.
        self.truncate_and_add_files(new_files, author=author, commit_message=commit_message)

        emit_audit(
            "alter_columns",
            resource_type=ResourceType.DATASET,
            workspace=getattr(self.catalog, "workspace", None),
            resource=self.identifier,
            author=author,
            added=[c.get("name") for c in add],
            dropped=drop,
            renamed=rename,
            retyped=sorted(retype),
            files_rewritten=len(new_files),
        )
        return schema_id

    def overwrite(self, table: Any, author: str | None = None, commit_message: str | None = None):
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
        added_files_size = sum(int(e.get("file_size_in_bytes") or 0) for e in new_entries)
        added_data_size = sum(int(e.get("uncompressed_size_in_bytes") or 0) for e in new_entries)
        added_records = sum(int(e.get("record_count") or 0) for e in new_entries)

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
        next_seq = self._next_sequence_number()

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

        self._after_commit(author, snap)

    def add_files(
        self,
        files: list[str],
        author: str | None = None,
        commit_message: str | None = None,
        footer_only: bool = False,
    ):
        """Add filenames to the dataset manifest without writing the files.

        - `files` is a list of file paths (strings). Files are assumed to
          already exist in storage; this method only updates the manifest.
        - Does not add files that already appear in the current manifest
          (deduplicates by `file_path`).
        - Creates a cumulative manifest for the new snapshot (previous
          entries + new unique entries).
        - `footer_only`, when set, builds each new entry's stats from the
          parquet footer alone instead of decoding every row group — see
          `build_parquet_manifest_entry_from_bytes`'s `footer_only` doc for
          what that trades away (no min/max/null-count/histogram pruning
          stats). This still fully downloads each file via `self.io` first
          (there's no ranged/partial-read path in `FileIO` yet) — it saves
          decode CPU, not network transfer.
        """
        if author is None:
            raise ValueError("author must be provided when adding files to a dataset")

        snapshot_id = int(time.time() * 1000)

        # Gather the previous manifest entries this commit must carry forward.
        # A previous manifest that cannot be read stops the commit — see
        # `_parent_manifest_entries`.
        prev = self.snapshot(None)
        prev_entries = []
        if prev and getattr(prev, "manifest_list", None):
            prev_entries = self._parent_manifest_entries(prev)
            self._warn_if_summary_disagrees(prev, prev_entries)

        existing = {
            e.get("file_path") for e in prev_entries if isinstance(e, dict) and e.get("file_path")
        }

        # Build new entries for files that don't already exist. Only accept
        # Parquet files; compute full statistics per file unless footer_only.
        new_entries = []
        seen = set()
        for fp in files:
            if not fp or fp in existing or fp in seen:
                continue
            if not fp.lower().endswith(".parquet"):
                # only accept parquet files
                continue
            seen.add(fp)

            # Read file and compute statistics (full, or footer-only — see docstring)
            try:
                inp = self.io.new_input(fp)
                with inp.open() as f:
                    data = f.read()

                if data:
                    file_size = len(data)
                    manifest_entry = build_parquet_manifest_entry_from_bytes(
                        data,
                        fp,
                        file_size,
                        field_id_by_name=self._field_id_by_name(),
                        footer_only=footer_only,
                    )
                else:
                    # A genuinely empty object is a real state, not a failure:
                    # it holds no rows, and a zero-row entry describes it
                    # honestly. Only an unreadable file goes to the handler.
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
                        min_lengths=[],
                        max_lengths=[],
                    )
            except Exception as err:
                raise AddFilesReadError(
                    f"Cannot read {fp} to add it to {self.identifier}: {err}. "
                    "Refusing to register a file whose statistics are unknown."
                ) from err
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
            added_files_size += int(entry.get("file_size_in_bytes") or 0)
            added_data_size += int(entry.get("uncompressed_size_in_bytes") or 0)
            added_records += int(entry.get("record_count") or 0)
        deleted_data_files = 0
        deleted_files_size = 0
        deleted_data_size = 0
        deleted_records = 0

        # Totals describe the manifest just written, so they are computed from
        # it rather than accumulated from the parent's counters.
        summary = {
            "added-data-files": added_data_files,
            "added-files-size": added_files_size,
            "added-data-size": added_data_size,
            "added-records": added_records,
            "deleted-data-files": deleted_data_files,
            "deleted-files-size": deleted_files_size,
            "deleted-data-size": deleted_data_size,
            "deleted-records": deleted_records,
            **self._totals_from_entries(merged_entries),
        }

        # Sequence number
        next_seq = self._next_sequence_number()

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

        self._after_commit(author, snap)

    def truncate_and_add_files(
        self, files: list[str], author: str | None = None, commit_message: str | None = None
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
            # Reporting-only: these feed the "deleted-*" counters in the new
            # snapshot's summary. An unparseable value is worth 0 rather than a
            # failed truncate, because the totals that matter are derived from
            # the manifest actually written (see `_totals_from_entries`).
            prev_total_files = _as_int(prev.summary.get("total-data-files")) or 0
            prev_total_size = _as_int(prev.summary.get("total-files-size")) or 0
            prev_total_records = _as_int(prev.summary.get("total-records")) or 0

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
                    # A genuinely empty object is a real state, not a failure:
                    # it holds no rows, and a zero-row entry describes it
                    # honestly. Only an unreadable file goes to the handler.
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
                        min_lengths=[],
                        max_lengths=[],
                    )
            except Exception as err:
                raise AddFilesReadError(
                    f"Cannot read {fp} to add it to {self.identifier}: {err}. "
                    "Refusing to register a file whose statistics are unknown."
                ) from err
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
            added_files_size += int(entry.get("file_size_in_bytes") or 0)
            added_data_size += int(entry.get("uncompressed_size_in_bytes") or 0)
            added_records += int(entry.get("record_count") or 0)

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
        next_seq = self._next_sequence_number()

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

        self._after_commit(author, snap)

    def scan(self, row_filter=None, snapshot_id: int | None = None) -> Iterable[Datafile]:
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

    def manifest_sketch_vectors(self, snapshot_id: int | None = None) -> dict:
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

    def describe(self, snapshot_id: int | None = None, bins: int = 10) -> dict:
        """Describe all schema columns for the given snapshot.

        Returns a dict mapping column name -> statistics (same shape as
        the previous `describe` per-column output).
        """
        snap = self.snapshot(snapshot_id)
        if snap is None or not getattr(snap, "manifest_list", None):
            raise ValueError("No manifest available for this dataset/snapshot")

        manifest_path = snap.manifest_list

        # Read manifest once using Arrow-native retrieval (30-50% faster)
        from .manifest_arrow import get_arrow_manifest

        manifest = get_arrow_manifest(self.io, manifest_path)
        entries = manifest.to_pylist()  # Convert to list only when needed
        if not entries:
            raise ValueError("Empty manifest data")

        # Resolve schema and describe all columns. A schema that cannot be
        # fetched used to be flattened into the same bare "Schema unavailable"
        # as a dataset that genuinely has none, which sent people looking for a
        # missing schema when the real answer was a Firestore permission or
        # transport error. Chain the cause instead.
        try:
            relation_schema = self.schema()
        except Exception as err:
            raise ValueError(
                f"Schema for {self.identifier} could not be read; cannot describe all columns"
            ) from err

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
                # ARRAY only: the same sketch and bounds every column gets,
                # computed over the flat child vector rather than the rows.
                "element_hashes": set(),
                "element_mins": [],
                "element_maxs": [],
            }

        total_rows = 0

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
            emv = ent.get("element_min_values") or []
            exv = ent.get("element_max_values") or []
            emks = ent.get("element_min_k_hashes") or []

            for cname, cidx in col_to_idx.items():
                # nulls
                null_count = _as_int(_at(ncounts, cidx))
                if null_count is not None:
                    stats[cname]["null_count"] += null_count

                # mins/maxs
                dmin = _decode_minmax(_at(mv, cidx))
                dmax = _decode_minmax(_at(xv, cidx))
                if dmin is not None:
                    stats[cname]["mins"].append(dmin)
                if dmax is not None:
                    stats[cname]["maxs"].append(dmax)

                # ARRAY element bounds and sketch
                element_min = _decode_minmax(_at(emv, cidx))
                element_max = _decode_minmax(_at(exv, cidx))
                if element_min is not None:
                    stats[cname]["element_mins"].append(element_min)
                if element_max is not None:
                    stats[cname]["element_maxs"].append(element_max)
                col_emk = _at(emks, cidx)
                if isinstance(col_emk, (int, float, bytes, bytearray, memoryview, str)):
                    col_emk = (col_emk,)
                elif col_emk is None:
                    col_emk = ()
                for h in col_emk:
                    hashed = _as_int(h)
                    if hashed is not None:
                        stats[cname]["element_hashes"].add(hashed)

                # min-k hashes (tolerant to scalar/list/tuple shapes)
                col_mk = _at(mks, cidx)
                if isinstance(col_mk, (int, float, bytes, bytearray, memoryview, str)):
                    col_mk = (col_mk,)
                elif col_mk is None:
                    col_mk = ()
                for h in col_mk:
                    hashed = _as_int(h)
                    if hashed is not None:
                        stats[cname]["hashes"].add(hashed)

                # histograms (tolerant to scalar/list/tuple)
                col_hist = _at(hists, cidx)
                if isinstance(col_hist, (int, float, str)):
                    col_hist = (col_hist,)
                elif col_hist is None:
                    col_hist = ()
                if col_hist and dmin is not None and dmax is not None and dmin != dmax:
                    # A string column reaches here with text min/max and has no
                    # numeric range to bucket; it keeps its mins/maxs and simply
                    # contributes no histogram.
                    with suppress(TypeError, ValueError):
                        stats[cname]["file_hist_infos"].append(
                            (float(dmin), float(dmax), list(col_hist))
                        )

                # uncompressed bytes for this column (sum across files)
                column_bytes = _as_int(_at(col_sizes, cidx))
                if column_bytes is not None:
                    stats[cname]["uncompressed_bytes"] += column_bytes

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
            cardinality, cardinality_is_exact = _kmv_cardinality(s["hashes"])

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
                            distribution[i - 1] = round(cum - prev)
                            prev = cum
                        diff = total - sum(distribution)
                        if diff != 0:
                            distribution[-1] += diff
                except Exception as exc:  # noqa: BLE001 - vendored numeric code, fallback below
                    # `maki_nage.distogram` is vendored third-party numeric
                    # code; a merge or quantile step can fail on degenerate
                    # inputs in ways not worth enumerating here. The block
                    # below recomputes the same distribution by linear
                    # interpolation, so this is a real fallback path, not a
                    # swallowed error.
                    logger.debug(
                        "Distogram merge failed for %s.%s (%s); interpolating instead",
                        self.identifier,
                        cname,
                        exc,
                    )
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
                            gi = max(gi, 0)
                            if gi >= effective_bins:
                                gi = effective_bins - 1
                            distribution[gi] += int(cnt)

            element_cardinality, element_cardinality_is_exact = _kmv_cardinality(
                s["element_hashes"]
            )
            element_mins = [v for v in s["element_mins"] if v is not None]
            element_maxs = [v for v in s["element_maxs"] if v is not None]

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
                # ARRAY only, 0/None elsewhere: statistics over the elements
                # pooled across every row's list, which is the only thing an
                # array column can be summarised or pruned by. The bounds are
                # the child's ORDINAL encoding, so they are a pruning key, not
                # a displayable value -- the same caveat every non-integer
                # column's min/max carries.
                "element_cardinality": element_cardinality,
                "element_cardinality_is_exact": element_cardinality_is_exact,
                "element_min": min(element_mins) if element_mins else None,
                "element_max": max(element_maxs) if element_maxs else None,
            }

            # If textual, attempt display prefixes like describe(). `_at`
            # absorbs a schema that is None or shorter than the stats arrays,
            # which is the only way any of this failed.
            column = _at(getattr(relation_schema, "columns", None), cidx)
            column_type = getattr(column, "type", None)
            spelling = "" if column_type is None else str(column_type).lower()
            is_text = "char" in spelling or "string" in spelling or "varchar" in spelling

            if is_text:
                # Use only textual display values collected from manifests.
                # Decode bytes and strip truncation marker (0xFF) if present.
                def _decode_display_raw(v):
                    if isinstance(v, (bytes, bytearray, memoryview)):
                        b = bytes(v)
                        if b and b[-1] == 0xFF:
                            b = b[:-1]
                        # errors="replace" means this cannot raise: a display
                        # prefix is cosmetic, so undecodable bytes become U+FFFD
                        # rather than costing the column its whole row.
                        return b.decode("utf-8", errors="replace")[:16]
                    if isinstance(v, str):
                        return v[:16]
                    return None

                min_disp = next(
                    (d for d in map(_decode_display_raw, s.get("min_displays") or []) if d), None
                )
                max_disp = next(
                    (d for d in map(_decode_display_raw, s.get("max_displays") or []) if d), None
                )

                if min_disp is not None or max_disp is not None:
                    res["min_display"] = min_disp
                    res["max_display"] = max_disp

            results[cname] = res

        return results

    def refresh_manifest(self, agent: str, author: str | None = None) -> int:
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
            except Exception as exc:  # noqa: BLE001 - collected, then raised below
                # Collect and keep going: a bad batch write or bucket issue
                # usually affects many files at once, and surfacing them one
                # per re-run would take N runs to discover N bad files. Nothing
                # is swallowed - `failures` is what decides whether this pass
                # commits at all.
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
        next_seq = self._next_sequence_number()

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
        author: str | None = None,
        commit_message: str | None = None,
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
            except Exception as exc:  # noqa: BLE001 - historical manifest, reporting only
                # This walks EVERY snapshot to tally what the truncate is
                # removing, and old snapshots legitimately outlive their
                # manifests once expiration has purged them - so one that will
                # not read cannot fail the truncate. It does undercount the
                # "deleted-*" figures in the resulting summary, which is worth
                # a line in the log rather than silence.
                logger.warning(
                    "Manifest %s of %s unreadable (%s); its files are missing from the "
                    "truncate summary",
                    manifest_path,
                    self.identifier,
                    exc,
                )
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
        next_seq = self._next_sequence_number()

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

        # An uncommitted truncation moved no version pointer - nothing for a
        # trigger to react to yet.
        if commit_truncation:
            self._after_commit(author, snap)
