from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any
from typing import Iterable
from typing import Optional

from .metadata import Snapshot
from .metadata import TableMetadata
from .metastore import Table


@dataclass
class SimpleTable(Table):
    identifier: str
    _metadata: TableMetadata
    io: Any = None
    catalog: Any = None

    @property
    def metadata(self) -> TableMetadata:
        return self._metadata

    def current_snapshot(self) -> Optional[Snapshot]:
        return self.metadata.current_snapshot()

    def snapshots(self) -> Iterable[Snapshot]:
        return list(self.metadata.snapshots)

    def append(self, table: Any):
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

            # KMV: take K smallest hashes
            smallest = heapq.nsmallest(K, hashes)
            col_min_k = sorted(smallest)

            # Use draken.compress() to get canonical int64 per value
            mapped = list(vec.compress())
            non_nulls_mapped = [m for m in mapped if m != NULL_FLAG]
            if non_nulls_mapped:
                vmin = min(non_nulls_mapped)
                vmax = max(non_nulls_mapped)
                col_min = int(vmin)
                col_max = int(vmax)
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
                # no non-null values; histogram via hash buckets
                col_min = NULL_FLAG
                col_max = NULL_FLAG
                col_hist = [0] * HBINS
                for h in hashes:
                    b = (h >> (64 - 5)) & 0x1F
                    col_hist[b] += 1

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

        # persist manifest (let errors propagate)
        manifest_path = None
        if self.catalog and hasattr(self.catalog, "write_parquet_manifest"):
            manifest_path = self.catalog.write_parquet_manifest(
                snapshot_id, entries, self.metadata.location
            )

        # snapshot metadata
        author = (
            self.metadata.author
            or os.environ.get("USER")
            or os.environ.get("USERNAME")
            or "unknown"
        )

        recs = int(table.num_rows)
        fsize = len(pdata)
        added_data_files = 1
        added_files_size = fsize
        added_records = recs
        deleted_data_files = 0
        deleted_files_size = 0
        deleted_records = 0

        prev = self.metadata.current_snapshot()
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
            commit_message="",
            summary=summary,
        )

        self.metadata.snapshots.append(snap)
        self.metadata.current_snapshot_id = snapshot_id

        # persist metadata (let errors propagate)
        if self.catalog and hasattr(self.catalog, "save_snapshot"):
            self.catalog.save_snapshot(self.identifier, snap)
        if self.catalog and hasattr(self.catalog, "save_table_metadata"):
            self.catalog.save_table_metadata(self.identifier, self.metadata)

    def scan(self, **kwargs) -> Iterable[Any]:
        """Return an iterable/scan object. For now return empty iterator.
        A full DataScan implementation will be added later.
        """
        return iter(())

    def truncate(self) -> None:
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

        author = (
            self.metadata.author
            or os.environ.get("USER")
            or os.environ.get("USERNAME")
            or "unknown"
        )

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
            commit_message="",
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

        if self.catalog and hasattr(self.catalog, "save_table_metadata"):
            try:
                self.catalog.save_table_metadata(self.identifier, self.metadata)
            except Exception:
                pass

        return None
