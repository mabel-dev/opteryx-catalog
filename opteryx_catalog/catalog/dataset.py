from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any
from typing import Iterable
from typing import Optional

from .metadata import DatasetMetadata
from .metadata import Snapshot
from .metastore import Dataset


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


@dataclass
class SimpleDataset(Dataset):
    identifier: str
    _metadata: DatasetMetadata
    io: Any = None
    catalog: Any = None

    @property
    def metadata(self) -> DatasetMetadata:
        return self._metadata

    def snapshot(self, snapshot_id: Optional[int] = None) -> Optional[Snapshot]:
        """Return a Snapshot.

        - If `snapshot_id` is None, return the in-memory current snapshot.
        - If a `snapshot_id` is provided, prefer a Firestore lookup via the
          attached `catalog` (O(1) document get). Fall back to the in-memory
          `metadata.snapshots` list only when no catalog is attached or the
          remote lookup fails.
        """
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
                        snapshot_id=int(
                            sd.get("snapshot-id") or sd.get("snapshot_id") or snapshot_id
                        ),
                        timestamp_ms=int(sd.get("timestamp-ms") or sd.get("timestamp_ms") or 0),
                        author=sd.get("author"),
                        sequence_number=sd.get("sequence-number") or sd.get("sequence_number"),
                        user_created=sd.get("user-created") or sd.get("user_created"),
                        manifest_list=sd.get("manifest") or sd.get("manifest_list"),
                        schema_id=sd.get("schema-id") or sd.get("schema_id"),
                        summary=sd.get("summary", {}),
                        operation_type=sd.get("operation-type") or sd.get("operation_type"),
                        parent_snapshot_id=sd.get("parent-snapshot-id")
                        or sd.get("parent_snapshot_id"),
                        commit_message=sd.get("commit-message") or sd.get("commit_message"),
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

    def snapshots(self) -> Iterable[Snapshot]:
        return list(self.metadata.snapshots)

    def schema(self, schema_id: Optional[str] = None) -> Optional[dict]:
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

        # Try to construct an Orso RelationSchema
        from orso.schema import FlatColumn
        from orso.schema import RelationSchema

        # If metadata stored a raw schema
        raw = sdict.get("columns")

        columns = [
            FlatColumn(
                name=c.get("name"),
                type=c.get("type"),
                element_type=c.get("element-type"),
                precision=c.get("precision"),
                scale=c.get("scale"),
            )
            for c in raw
        ]
        orso_schema = RelationSchema(name=self.identifier, columns=columns)
        return orso_schema

    def append(self, table: Any, author: str = None, commit_message: Optional[str] = None):
        """Append a pyarrow.Table:

        - write a Parquet data file via `self.io`
        - create a simple Parquet manifest (one entry)
        - persist manifest and snapshot metadata using the attached `catalog`
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        snapshot_id = int(time.time() * 1000)

        if not hasattr(table, "schema"):
            raise TypeError("append() expects a pyarrow.Table-like object")

        # Write parquet file
        data_path = f"{self.metadata.location}/data/data-{snapshot_id}.parquet"
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf, compression="zstd")
        pdata = buf.getvalue().to_pybytes()

        out = self.io.new_output(data_path).create()
        out.write(pdata)
        out.close()

        # Prepare sketches/stats
        K = 32
        HBINS = 32
        min_k_hashes: list[list[int]] = []
        histograms: list[list[int]] = []
        min_values: list[int] = []
        max_values: list[int] = []

        # Use draken for efficient hashing and compression
        import heapq

        import opteryx.draken as draken  # type: ignore

        # canonical NULL flag for missing values
        NULL_FLAG = -(1 << 63)

        num_rows = int(table.num_rows)

        for col_idx, col in enumerate(table.columns):
            # hash column values to 64-bit via draken (new cpdef API)
            vec = draken.Vector.from_arrow(col)
            hashes = list(vec.hash())

            # Decide whether to compute min-k/histogram for this column based
            # on field type and, for strings, average length of values.
            field_type = table.schema.field(col_idx).type
            compute_min_k = False
            if (
                pa.types.is_integer(field_type)
                or pa.types.is_floating(field_type)
                or pa.types.is_decimal(field_type)
            ):
                compute_min_k = True
            elif (
                pa.types.is_timestamp(field_type)
                or pa.types.is_date(field_type)
                or pa.types.is_time(field_type)
            ):
                compute_min_k = True
            elif pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
                # compute average length from non-null values; only allow
                # min-k/histogram for short strings (avg <= 16)
                col_py = None
                try:
                    col_py = col.to_pylist()
                except Exception:
                    col_py = None

                if col_py is not None:
                    lens = [len(x) for x in col_py if x is not None]
                    if lens:
                        avg_len = sum(lens) / len(lens)
                        if avg_len <= 16:
                            compute_min_k = True

            # KMV: take K smallest hashes when allowed; otherwise store an
            # empty list for this column.
            if compute_min_k:
                smallest = heapq.nsmallest(K, hashes)
                col_min_k = sorted(smallest)
            else:
                col_min_k = []

            # For histogram decisions follow the same rule as min-k
            compute_hist = compute_min_k

            # Use draken.compress() to get canonical int64 per value
            mapped = list(vec.compress())
            non_nulls_mapped = [m for m in mapped if m != NULL_FLAG]
            if non_nulls_mapped:
                vmin = min(non_nulls_mapped)
                vmax = max(non_nulls_mapped)
                col_min = int(vmin)
                col_max = int(vmax)
                if compute_hist:
                    if vmin == vmax:
                        col_hist = [0] * HBINS
                        col_hist[-1] = len(non_nulls_mapped)
                    else:
                        col_hist = [0] * HBINS
                        span = float(vmax - vmin)
                        for m in non_nulls_mapped:
                            b = int(((float(m) - float(vmin)) / span) * (HBINS - 1))
                            if b < 0:
                                b = 0
                            if b >= HBINS:
                                b = HBINS - 1
                            col_hist[b] += 1
                else:
                    col_hist = [0] * HBINS
            else:
                # no non-null values; histogram via hash buckets
                col_min = NULL_FLAG
                col_max = NULL_FLAG
                if compute_hist:
                    col_hist = [0] * HBINS
                    for h in hashes:
                        b = (h >> (64 - 5)) & 0x1F
                        col_hist[b] += 1
                else:
                    col_hist = [0] * HBINS

            min_k_hashes.append(col_min_k)
            histograms.append(col_hist)
            min_values.append(col_min)
            max_values.append(col_max)

        entries = [
            {
                "file_path": data_path,
                "file_format": "parquet",
                "record_count": int(table.num_rows),
                "file_size_in_bytes": len(pdata),
                "min_k_hashes": min_k_hashes,
                "histogram_counts": histograms,
                "histogram_bins": HBINS,
                "min_values": min_values,
                "max_values": max_values,
            }
        ]

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
                    if self.io and hasattr(self.io, "new_input"):
                        inp = self.io.new_input(prev_manifest_path)
                        with inp.open() as f:
                            prev_data = f.read()
                        import pyarrow as pa
                        import pyarrow.parquet as pq

                        prev_table = pq.read_table(pa.BufferReader(prev_data))
                        prev_rows = prev_table.to_pylist()
                        merged_entries = prev_rows + merged_entries
                    else:
                        # Fall back to catalog storage client (GCS)
                        if (
                            self.catalog
                            and getattr(self.catalog, "_storage_client", None)
                            and getattr(self.catalog, "gcs_bucket", None)
                        ):
                            bucket = self.catalog._storage_client.bucket(self.catalog.gcs_bucket)
                            parsed = prev_manifest_path
                            if parsed.startswith("gs://"):
                                parsed = parsed[5 + len(self.catalog.gcs_bucket) + 1 :]
                            blob = bucket.blob(parsed)
                            prev_data = blob.download_as_bytes()
                            import pyarrow as pa
                            import pyarrow.parquet as pq

                            prev_table = pq.read_table(pa.BufferReader(prev_data))
                            prev_rows = prev_table.to_pylist()
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
        fsize = len(pdata)
        added_data_files = 1
        added_files_size = fsize
        added_records = recs
        deleted_data_files = 0
        deleted_files_size = 0
        deleted_records = 0

        prev = self.snapshot()
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
        else:
            prev_total_files = 0
            prev_total_size = 0
            prev_total_records = 0

        total_data_files = prev_total_files + added_data_files - deleted_data_files
        total_files_size = prev_total_size + added_files_size - deleted_files_size
        total_records = prev_total_records + added_records - deleted_records

        summary = {
            "added-data-files": added_data_files,
            "added-files-size": added_files_size,
            "added-records": added_records,
            "deleted-data-files": deleted_data_files,
            "deleted-files-size": deleted_files_size,
            "deleted-records": deleted_records,
            "total-data-files": total_data_files,
            "total-files-size": total_files_size,
            "total-records": total_records,
        }

        # sequence number
        try:
            max_seq = 0
            for s in self.metadata.snapshots:
                seq = getattr(s, "sequence_number", None)
                if seq is None:
                    continue
                try:
                    ival = int(seq)
                except Exception:
                    continue
                if ival > max_seq:
                    max_seq = ival
            next_seq = max_seq + 1
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

        if prev and getattr(prev, "manifest_list", None):
            # try to read prev manifest entries
            try:
                import pyarrow.parquet as pq
                import pyarrow as pa

                if self.io and hasattr(self.io, "new_input"):
                    inp = self.io.new_input(prev.manifest_list)
                    with inp.open() as f:
                        data = f.read()
                    table = pq.read_table(pa.BufferReader(data))
                    prev_entries = table.to_pylist()
                else:
                    if self.catalog and getattr(self.catalog, "_storage_client", None) and getattr(self.catalog, "gcs_bucket", None):
                        bucket = self.catalog._storage_client.bucket(self.catalog.gcs_bucket)
                        parsed = prev.manifest_list
                        if parsed.startswith("gs://"):
                            parsed = parsed[5 + len(self.catalog.gcs_bucket) + 1 :]
                        blob = bucket.blob(parsed)
                        data = blob.download_as_bytes()
                        table = pq.read_table(pa.BufferReader(data))
                        prev_entries = table.to_pylist()
            except Exception:
                prev_entries = []

        existing = {e.get("file_path") for e in prev_entries if isinstance(e, dict) and e.get("file_path")}

        # Build new entries for files that don't already exist
        new_entries = []
        seen = set()
        for fp in files:
            if not fp or fp in existing or fp in seen:
                continue
            seen.add(fp)
            fmt = "parquet" if fp.lower().endswith(".parquet") else None
            new_entries.append(
                {
                    "file_path": fp,
                    "file_format": fmt,
                    "record_count": 0,
                    "file_size_in_bytes": 0,
                    "min_k_hashes": [],
                    "histogram_counts": [],
                    "histogram_bins": 0,
                    "min_values": [],
                    "max_values": [],
                }
            )

        merged_entries = prev_entries + new_entries

        # write cumulative manifest
        manifest_path = None
        if self.catalog and hasattr(self.catalog, "write_parquet_manifest"):
            manifest_path = self.catalog.write_parquet_manifest(snapshot_id, merged_entries, self.metadata.location)

        # Build summary deltas
        added_data_files = len(new_entries)
        added_files_size = 0
        added_records = 0
        deleted_data_files = 0
        deleted_files_size = 0
        deleted_records = 0

        total_data_files = prev_total_files + added_data_files - deleted_data_files
        total_files_size = prev_total_size + added_files_size - deleted_files_size
        total_records = prev_total_records + added_records - deleted_records

        summary = {
            "added-data-files": added_data_files,
            "added-files-size": added_files_size,
            "added-records": added_records,
            "deleted-data-files": deleted_data_files,
            "deleted-files-size": deleted_files_size,
            "deleted-records": deleted_records,
            "total-data-files": total_data_files,
            "total-files-size": total_files_size,
            "total-records": total_records,
        }

        # Sequence number
        try:
            max_seq = 0
            for s in self.metadata.snapshots:
                seq = getattr(s, "sequence_number", None)
                if seq is None:
                    continue
                try:
                    ival = int(seq)
                except Exception:
                    continue
                if ival > max_seq:
                    max_seq = ival
            next_seq = max_seq + 1
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

    def truncate_and_add_files(self, files: list[str], author: str = None, commit_message: Optional[str] = None):
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

        # Build unique new entries (ignore duplicates in input)
        new_entries = []
        seen = set()
        for fp in files:
            if not fp or fp in seen:
                continue
            seen.add(fp)
            fmt = "parquet" if fp.lower().endswith(".parquet") else None
            new_entries.append(
                {
                    "file_path": fp,
                    "file_format": fmt,
                    "record_count": 0,
                    "file_size_in_bytes": 0,
                    "min_k_hashes": [],
                    "histogram_counts": [],
                    "histogram_bins": 0,
                    "min_values": [],
                    "max_values": [],
                }
            )

        manifest_path = None
        if self.catalog and hasattr(self.catalog, "write_parquet_manifest"):
            manifest_path = self.catalog.write_parquet_manifest(snapshot_id, new_entries, self.metadata.location)

        # Build summary: previous entries become deleted
        deleted_data_files = prev_total_files
        deleted_files_size = prev_total_size
        deleted_records = prev_total_records

        added_data_files = len(new_entries)
        added_files_size = 0
        added_records = 0

        total_data_files = added_data_files
        total_files_size = added_files_size
        total_records = added_records

        summary = {
            "added-data-files": added_data_files,
            "added-files-size": added_files_size,
            "added-records": added_records,
            "deleted-data-files": deleted_data_files,
            "deleted-files-size": deleted_files_size,
            "deleted-records": deleted_records,
            "total-data-files": total_data_files,
            "total-files-size": total_files_size,
            "total-records": total_records,
        }

        # Sequence number
        try:
            max_seq = 0
            for s in self.metadata.snapshots:
                seq = getattr(s, "sequence_number", None)
                if seq is None:
                    continue
                try:
                    ival = int(seq)
                except Exception:
                    continue
                if ival > max_seq:
                    max_seq = ival
            next_seq = max_seq + 1
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
        - Ignore `row_filter` for now and return all files listed in the
          snapshot's parquet manifest (if present).
        """
        # Determine snapshot to read using the dataset-level helper which
        # prefers the in-memory current snapshot and otherwise performs a
        # backend lookup for the requested id.
        snap = self.snapshot(snapshot_id)

        if snap is None or not getattr(snap, "manifest_list", None):
            return iter(())

        manifest_path = snap.manifest_list

        # Read manifest via FileIO if available
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            io = self.io
            inp = None
            data = None
            if io and hasattr(io, "new_input"):
                inp = io.new_input(manifest_path)
                with inp.open() as f:
                    data = f.read()
            else:
                # No FileIO available; attempt to use catalog storage client
                if (
                    self.catalog
                    and getattr(self.catalog, "_storage_client", None)
                    and getattr(self.catalog, "gcs_bucket", None)
                ):
                    bucket = self.catalog._storage_client.bucket(self.catalog.gcs_bucket)
                    # strip gs://<bucket>/ prefix if present
                    parsed = manifest_path
                    if parsed.startswith("gs://"):
                        parsed = parsed[5 + len(self.catalog.gcs_bucket) + 1 :]
                    blob = bucket.blob(parsed)
                    data = blob.download_as_bytes()

            if not data:
                return iter(())

            table = pq.read_table(pa.BufferReader(data))
            rows = table.to_pylist()
            for r in rows:
                yield Datafile(entry=r)
        except FileNotFoundError:
            return iter(())
        except Exception:
            return iter(())

    def truncate(self, author: str = None, commit_message: Optional[str] = None) -> None:
        """Delete all data files and manifests for this table.

        This attempts to delete every data file referenced by existing
        Parquet manifests and then delete the manifest files themselves.
        Finally it clears the in-memory snapshot list and persists the
        empty snapshot set via the attached `catalog` (if available).
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        io = self.io
        # Collect files referenced by existing manifests but do NOT delete
        # them from storage. Instead we will write a new empty manifest and
        # create a truncate snapshot that records these files as deleted.
        snaps = list(self.metadata.snapshots)
        removed_files = []
        removed_total_size = 0

        for snap in snaps:
            manifest_path = getattr(snap, "manifest_list", None)
            if not manifest_path:
                continue

            # Read manifest via FileIO if available
            rows = []
            try:
                if hasattr(io, "new_input"):
                    inp = io.new_input(manifest_path)
                    with inp.open() as f:
                        data = f.read()
                    table = pq.read_table(pa.BufferReader(data))
                    rows = table.to_pylist()
            except Exception:
                rows = []

            for r in rows:
                fp = None
                fsize = 0
                if isinstance(r, dict):
                    fp = r.get("file_path")
                    fsize = int(r.get("file_size_in_bytes") or 0)
                    if not fp and "data_file" in r and isinstance(r["data_file"], dict):
                        fp = r["data_file"].get("file_path") or r["data_file"].get("path")
                        fsize = int(r["data_file"].get("file_size_in_bytes") or 0)

                if fp:
                    removed_files.append(fp)
                    removed_total_size += fsize

        # Create a new empty Parquet manifest (entries=[]) to represent the
        # truncated table for the new snapshot. Do not delete objects.
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
            "added-records": 0,
            "deleted-data-files": deleted_count,
            "deleted-files-size": deleted_size,
            "deleted-records": 0,
            "total-data-files": 0,
            "total-files-size": 0,
            "total-records": 0,
        }

        # Sequence number
        try:
            max_seq = 0
            for s in self.metadata.snapshots:
                seq = getattr(s, "sequence_number", None)
                if seq is None:
                    continue
                try:
                    ival = int(seq)
                except Exception:
                    continue
                if ival > max_seq:
                    max_seq = ival
            next_seq = max_seq + 1
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
            try:
                self.catalog.save_snapshot(self.identifier, snap)
            except Exception:
                pass
