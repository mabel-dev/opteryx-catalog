from __future__ import annotations

import os
import time
from typing import Any
from typing import Iterable
from typing import List
from typing import Optional

from google.cloud import firestore
from google.cloud import storage

from .catalog.metadata import Snapshot
from .catalog.metadata import TableMetadata
from .catalog.metastore import Metastore
from .catalog.table import SimpleTable
from .iops.base import FileIO


class FirestoreCatalog(Metastore):
    """Firestore-backed Metastore implementation.

    Stores table documents under: /<catalog>/<namespace>/tables/<table>
    Snapshots are stored in a `snapshots` subcollection.
    Parquet manifests are written to GCS under the table location's
    `metadata/manifest-<snapshot_id>.parquet` path.
    """

    def __init__(
        self,
        catalog_name: str,
        firestore_project: Optional[str] = None,
        firestore_database: Optional[str] = None,
        gcs_bucket: Optional[str] = None,
        io: Optional[FileIO] = None,
    ):
        self.catalog_name = catalog_name
        self.firestore_client = firestore.Client(
            project=firestore_project, database=firestore_database
        )
        self._catalog_ref = self.firestore_client.collection(catalog_name)
        self.gcs_bucket = gcs_bucket
        self._storage_client = storage.Client() if gcs_bucket else None
        # Default to a GCS-backed FileIO when a GCS bucket is configured and
        # no explicit `io` was provided.
        if io is not None:
            self.io = io
        else:
            if gcs_bucket:
                try:
                    from .iops.gcs import GcsFileIO

                    self.io = GcsFileIO()
                except Exception:
                    self.io = FileIO()
            else:
                self.io = FileIO()

    def _namespace_ref(self, namespace: str):
        return self._catalog_ref.document(namespace)

    def _tables_collection(self, namespace: str):
        return self._namespace_ref(namespace).collection("tables")

    def _table_doc_ref(self, namespace: str, table_name: str):
        return self._tables_collection(namespace).document(table_name)

    def _snapshots_collection(self, namespace: str, table_name: str):
        return self._table_doc_ref(namespace, table_name).collection("snapshots")

    def create_table(
        self, identifier: str, schema: Any, properties: dict | None = None
    ) -> SimpleTable:
        namespace, table_name = identifier.split(".")
        doc_ref = self._table_doc_ref(namespace, table_name)
        if doc_ref.get().exists:
            raise KeyError(f"Table already exists: {identifier}")

        # Build default table metadata
        location = f"gs://{self.gcs_bucket}/{self.catalog_name}/{namespace}/{table_name}"
        metadata = TableMetadata(
            table_identifier=identifier,
            schema=schema,
            location=location,
            properties=properties or {},
        )

        # Persist document with timestamp and author
        now_ms = int(time.time() * 1000)
        author = os.environ.get("USER") or os.environ.get("USERNAME") or "unknown"
        metadata.timestamp_ms = now_ms
        metadata.author = author
        doc_ref.set(
            {
                "name": table_name,
                "collection": namespace,
                "workspace": self.catalog_name,
                "location": location,
                "properties": metadata.properties,
                "format-version": metadata.format_version,
                "timestamp-ms": now_ms,
                "author": author,
                "maintenance-policy": metadata.maintenance_policy,
            }
        )

        # Persist initial schema into `schemas` subcollection if provided
        if schema is not None:
            schema_id = self._write_schema(namespace, table_name, schema)
            metadata.current_schema_id = schema_id
            # Read back the schema doc to capture timestamp-ms, author, sequence-number
            try:
                sdoc = doc_ref.collection("schemas").document(schema_id).get()
                sdata = sdoc.to_dict() or {}
                metadata.schemas = [
                    {
                        "schema_id": schema_id,
                        "columns": sdata.get("columns", self._schema_to_columns(schema)),
                        "timestamp-ms": sdata.get("timestamp-ms"),
                        "author": sdata.get("author"),
                        "sequence-number": sdata.get("sequence-number"),
                    }
                ]
            except Exception:
                metadata.schemas = [
                    {"schema_id": schema_id, "columns": self._schema_to_columns(schema)}
                ]
            # update table doc to reference current schema
            doc_ref.update({"current-schema-id": metadata.current_schema_id})

        # Return SimpleTable (attach this catalog so Table.append() can persist)
        return SimpleTable(identifier=identifier, _metadata=metadata, io=self.io, catalog=self)

    def load_table(self, identifier: str) -> SimpleTable:
        namespace, table_name = identifier.split(".")
        doc_ref = self._table_doc_ref(namespace, table_name)
        doc = doc_ref.get()
        if not doc.exists:
            raise KeyError(f"Table not found: {identifier}")

        data = doc.to_dict() or {}
        metadata = TableMetadata(
            table_identifier=identifier,
            location=data.get("location")
            or f"gs://{self.gcs_bucket}/{self.catalog_name}/{namespace}/{table_name}",
            schema=data.get("schema"),
            properties=data.get("properties") or {},
        )

        # Load table-level timestamp/author and collection/workspace
        metadata.timestamp_ms = data.get("timestamp-ms")
        metadata.author = data.get("author")
        # note: Firestore table doc stores the original collection and workspace
        # under keys `collection` and `workspace`.

        # Load snapshots
        snaps = []
        for snap_doc in self._snapshots_collection(namespace, table_name).stream():
            sd = snap_doc.to_dict() or {}
            snap = Snapshot(
                snapshot_id=sd.get("snapshot-id"),
                timestamp_ms=sd.get("timestamp-ms"),
                author=sd.get("author"),
                sequence_number=sd.get("sequence-number"),
                user_created=sd.get("user-created"),
                manifest_list=sd.get("manifest"),
                schema_id=sd.get("schema-id"),
                summary=sd.get("summary", {}),
                operation_type=sd.get("operation-type"),
                parent_snapshot_id=sd.get("parent-snapshot-id"),
            )
            snaps.append(snap)
        metadata.snapshots = snaps
        if snaps:
            metadata.current_snapshot_id = snaps[-1].snapshot_id

        # Load schemas subcollection
        try:
            schemas = []
            schemas_coll = doc_ref.collection("schemas")
            for sdoc in schemas_coll.stream():
                sd = sdoc.to_dict() or {}
                schemas.append(
                    {
                        "schema_id": sdoc.id,
                        "columns": sd.get("columns", []),
                        "timestamp-ms": sd.get("timestamp-ms"),
                        "author": sd.get("author"),
                        "sequence-number": sd.get("sequence-number"),
                    }
                )
            metadata.schemas = schemas
            metadata.current_schema_id = doc.to_dict().get("current-schema-id")
        except Exception:
            pass

        return SimpleTable(identifier=identifier, _metadata=metadata, io=self.io, catalog=self)

    def drop_table(self, identifier: str) -> None:
        namespace, table_name = identifier.split(".")
        # Delete snapshots
        snaps_coll = self._snapshots_collection(namespace, table_name)
        for doc in snaps_coll.stream():
            snaps_coll.document(doc.id).delete()
        # Delete table doc
        self._table_doc_ref(namespace, table_name).delete()

    def list_tables(self, namespace: str) -> Iterable[str]:
        coll = self._tables_collection(namespace)
        return [doc.id for doc in coll.stream()]

    def table_exists(self, identifier_or_namespace: str, table_name: Optional[str] = None) -> bool:
        """Return True if the table exists.

        Supports two call forms:
        - table_exists("namespace.table")
        - table_exists("namespace", "table")
        """
        # Normalize inputs
        if table_name is None:
            # Expect a single identifier like 'namespace.table'
            if "." not in identifier_or_namespace:
                raise ValueError("identifier must be 'namespace.table' or pass table_name separately")
            namespace, table_name = identifier_or_namespace.rsplit(".", 1)

        try:
            doc_ref = self._table_doc_ref(namespace, table_name)
            return doc_ref.get().exists
        except Exception:
            # On any error, be conservative and return False
            return False

    def write_parquet_manifest(
        self, snapshot_id: int, entries: List[dict], table_location: str
    ) -> Optional[str]:
        """Write a Parquet manifest for the given snapshot id and entries.

        Entries should be plain dicts convertible by pyarrow.Table.from_pylist.
        The manifest will be written to <table_location>/metadata/manifest-<snapshot_id>.parquet
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        # If entries is None we skip writing; if entries is empty list, write
        # an empty Parquet manifest (represents an empty table for this
        # snapshot). This preserves previous manifests so older snapshots
        # remain readable.
        if entries is None:
            return None

        # Print manifest entries so users can inspect the manifest when created
        try:
            import json

            print("[MANIFEST] Parquet manifest entries to write:")
            print(json.dumps(entries, indent=2, default=str))
        except Exception:
            print("[MANIFEST] Parquet manifest entries:", entries)

        parquet_path = f"{table_location}/metadata/manifest-{snapshot_id}.parquet"

        # Use provided FileIO if it supports writing; otherwise write to GCS
        try:
            # Use an explicit schema so PyArrow types (especially nested lists)
            # are correct and we avoid integer overflow / inference issues.
            schema = pa.schema(
                [
                    ("file_path", pa.string()),
                    ("file_format", pa.string()),
                    ("record_count", pa.int64()),
                    ("file_size_in_bytes", pa.int64()),
                    ("min_k_hashes", pa.list_(pa.list_(pa.uint64()))),
                    ("histogram_counts", pa.list_(pa.list_(pa.int64()))),
                    ("histogram_bins", pa.int32()),
                    ("min_values", pa.list_(pa.int64())),
                    ("max_values", pa.list_(pa.int64())),
                ]
            )

            table = pa.Table.from_pylist(entries, schema=schema)
            buf = pa.BufferOutputStream()
            pq.write_table(table, buf, compression="zstd")
            data = buf.getvalue().to_pybytes()

            if self.io:
                out = self.io.new_output(parquet_path).create()
                out.write(data)
                try:
                    # Some OutputFile implementations buffer and require close()
                    out.close()
                except Exception:
                    pass
            elif self._storage_client and self.gcs_bucket:
                # Write to GCS bucket
                bucket = self._storage_client.bucket(self.gcs_bucket)
                # object path: remove gs://bucket/ prefix
                parsed = parquet_path
                if parsed.startswith("gs://"):
                    parsed = parsed[5 + len(self.gcs_bucket) + 1 :]
                blob = bucket.blob(parsed)
                blob.upload_from_string(data)

            return parquet_path
        except Exception as e:
            # Log and return None on failure
            print(f"Failed to write Parquet manifest: {e}")
            raise e

    def save_snapshot(self, identifier: str, snapshot: Snapshot) -> None:
        """Persist a single snapshot document for a table."""
        namespace, table_name = identifier.split(".")
        snaps = self._snapshots_collection(namespace, table_name)
        doc_id = str(snapshot.snapshot_id)
        # Ensure summary contains all expected keys (zero defaults applied in dataclass)
        summary = snapshot.summary or {}
        # Provide explicit keys if missing
        for k in [
            "added-data-files",
            "added-files-size",
            "added-records",
            "deleted-data-files",
            "deleted-files-size",
            "deleted-records",
            "total-data-files",
            "total-files-size",
            "total-records",
        ]:
            summary.setdefault(k, 0)

        data = {
            "snapshot-id": snapshot.snapshot_id,
            "timestamp-ms": snapshot.timestamp_ms,
            "manifest": snapshot.manifest_list,
            "commit-message": getattr(snapshot, "commit_message", ""),
            "summary": summary,
            "author": getattr(snapshot, "author", None),
            "sequence-number": getattr(snapshot, "sequence_number", None),
            "operation-type": getattr(snapshot, "operation_type", None),
            "parent-snapshot-id": getattr(snapshot, "parent_snapshot_id", None),
        }
        if getattr(snapshot, "schema_id", None) is not None:
            data["schema-id"] = snapshot.schema_id
        snaps.document(doc_id).set(data)

    def save_table_metadata(self, identifier: str, metadata: TableMetadata) -> None:
        """Persist table-level metadata and snapshots to Firestore.

        This writes the table document and upserts snapshot documents.
        """
        namespace, table_name = identifier.split(".")
        doc_ref = self._table_doc_ref(namespace, table_name)
        doc_ref.set(
            {
                "name": table_name,
                "collection": namespace,
                "workspace": self.catalog_name,
                "location": metadata.location,
                "properties": metadata.properties,
                "format-version": metadata.format_version,
                "current-snapshot-id": metadata.current_snapshot_id,
                "current-schema-id": metadata.current_schema_id,
                "timestamp-ms": metadata.timestamp_ms,
                "author": metadata.author,
                "description": metadata.description,
                "describer": metadata.describer,
                "maintenance-policy": metadata.maintenance_policy,
                "sort-orders": metadata.sort_orders,
            }
        )

        snaps_coll = self._snapshots_collection(namespace, table_name)
        existing = {d.id for d in snaps_coll.stream()}
        new_ids = set()
        for snap in metadata.snapshots:
            new_ids.add(str(snap.snapshot_id))
            snaps_coll.document(str(snap.snapshot_id)).set(
                {
                    "snapshot-id": snap.snapshot_id,
                    "timestamp-ms": snap.timestamp_ms,
                    "manifest": snap.manifest_list,
                    "schema-id": snap.schema_id,
                    "summary": snap.summary or {},
                    "author": getattr(snap, "author", None),
                    "sequence-number": getattr(snap, "sequence_number", None),
                    "user-created": getattr(snap, "user_created", None),
                }
            )

        # Delete stale snapshots
        for stale in existing - new_ids:
            snaps_coll.document(stale).delete()

        # Persist schemas subcollection
        schemas_coll = doc_ref.collection("schemas")
        existing_schema_ids = {d.id for d in schemas_coll.stream()}
        new_schema_ids = set()
        for s in metadata.schemas:
            sid = s.get("schema_id")
            if not sid:
                continue
            new_schema_ids.add(sid)
            schemas_coll.document(sid).set(
                {
                    "columns": s.get("columns", []),
                    "timestamp-ms": s.get("timestamp-ms"),
                    "author": s.get("author"),
                    "sequence-number": s.get("sequence-number"),
                }
            )
        # Delete stale schema docs
        for stale in existing_schema_ids - new_schema_ids:
            schemas_coll.document(stale).delete()

    def _schema_to_columns(self, schema: Any) -> list:
        """Convert a pyarrow.Schema into a simple columns list for storage.

        Each column is a dict: {"id": index (1-based), "name": column_name, "type": str(type)}
        """
        # Support pyarrow.Schema and Orso RelationSchema. When Orso's
        # FlatColumn.from_arrow is available, use it to derive Orso types
        # (type, element-type, scale, precision). Fall back to simple
        # stringified types if Orso isn't installed.
        cols = []
        # Try Orso FlatColumn importer
        import orso
        import pyarrow as pa

        # If schema is an Orso RelationSchema, try to obtain a list of columns
        columns = None
        if isinstance(schema, orso.schema.RelationSchema):
            columns = schema.columns
        elif isinstance(schema, pa.Schema):
            orso_schema = orso.schema.convert_arrow_schema_to_orso_schema(schema)
            columns = orso_schema.columns
        else:
            print(f"[DEBUG] _schema_to_columns: unsupported schema type: {type(schema)}")
            raise ValueError(
                "Unsupported schema type, expected pyarrow.Schema or orso.RelationSchema"
            )

        print(f"[DEBUG] _schema_to_columns: processing {len(columns)} columns")

        for idx, column in enumerate(columns, start=1):
            # If f looks like a pyarrow.Field, use its name/type
            name = column.name

            # Extract expected attributes safely
            ctype = column.type
            element_type = column.element_type if column.element_type else None
            scale = column.scale
            precision = column.precision
            typed = {
                "id": idx,
                "name": name,
                "type": ctype,
                "element-type": element_type,
                "scale": scale,
                "precision": precision,
                "expectation-policies": [],
            }

            cols.append(typed)

        return cols

    def _write_schema(self, namespace: str, table_name: str, schema: Any) -> str:
        """Persist a schema document in the table's `schemas` subcollection and
        return the new schema id.
        """
        import uuid

        doc_ref = self._table_doc_ref(namespace, table_name)
        schemas_coll = doc_ref.collection("schemas")
        sid = str(uuid.uuid4())
        print(f"[DEBUG] _write_schema called for {namespace}/{table_name} sid={sid}")
        try:
            cols = self._schema_to_columns(schema)
        except Exception as e:
            print(
                f"[DEBUG] _write_schema: _schema_to_columns raised: {e}; falling back to empty columns list"
            )
            cols = []
        now_ms = int(time.time() * 1000)
        author = os.environ.get("USER") or os.environ.get("USERNAME") or "unknown"
        # Determine next sequence number by scanning existing schema docs
        try:
            max_seq = 0
            for d in schemas_coll.stream():
                sd = d.to_dict() or {}
                seq = sd.get("sequence-number") or 0
                if isinstance(seq, int) and seq > max_seq:
                    max_seq = seq
            new_seq = max_seq + 1
        except Exception:
            new_seq = 1

        try:
            print(
                f"[DEBUG] Writing schema doc {sid} for {namespace}/{table_name} (cols={len(cols)})"
            )
            schemas_coll.document(sid).set(
                {
                    "columns": cols,
                    "timestamp-ms": now_ms,
                    "author": author,
                    "sequence-number": new_seq,
                }
            )
            print(f"[DEBUG] Wrote schema doc {sid}")
        except Exception as e:
            print(f"[DEBUG] Failed to write schema doc {sid}: {e}")
        return sid
