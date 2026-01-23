from __future__ import annotations

import time
from typing import Any
from typing import Iterable
from typing import List
from typing import Optional

from google.cloud import firestore
from google.cloud import storage

from .catalog.dataset import SimpleDataset
from .catalog.metadata import DatasetMetadata
from .catalog.metadata import Snapshot
from .catalog.metastore import Metastore
from .catalog.view import View as CatalogView
from .exceptions import CollectionAlreadyExists
from .exceptions import DatasetAlreadyExists
from .exceptions import DatasetNotFound
from .exceptions import ViewAlreadyExists
from .exceptions import ViewNotFound
from .iops.base import FileIO
from .webhooks import send_webhook
from .webhooks.events import dataset_created_payload
from .webhooks.events import view_created_payload


class OpteryxCatalog(Metastore):
    """Firestore-backed Metastore implementation.

    Terminology: catalog -> workspace -> collection -> dataset|view

    Stores dataset documents under the configured workspace in Firestore.
    Snapshots are stored in a `snapshots` subcollection under each
    dataset's document. Parquet manifests are written to GCS under the
    dataset location's `metadata/manifest-<snapshot_id>.parquet` path.
    """

    def __init__(
        self,
        workspace: str,
        firestore_project: Optional[str] = None,
        firestore_database: Optional[str] = None,
        gcs_bucket: Optional[str] = None,
        io: Optional[FileIO] = None,
    ):
        # `workspace` is the configured catalog/workspace name
        self.workspace = workspace
        # Backwards-compatible alias: keep `catalog_name` for older code paths
        self.catalog_name = workspace
        self.firestore_client = firestore.Client(
            project=firestore_project, database=firestore_database
        )
        self._catalog_ref = self.firestore_client.collection(workspace)
        # Ensure workspace-level properties document exists in Firestore.
        # The $properties doc records metadata for the workspace such as
        # 'timestamp-ms', 'author', 'billing-account-id' and 'owner'.
        try:
            props_ref = self._catalog_ref.document("$properties")
            if not props_ref.get().exists:
                now_ms = int(time.time() * 1000)
                billing = None
                owner = None
                props_ref.set(
                    {
                        "timestamp-ms": now_ms,
                        "billing-account-id": billing,
                        "owner": owner,
                    }
                )
        except Exception:
            # Be conservative: don't fail catalog initialization on Firestore errors
            pass
        self.gcs_bucket = gcs_bucket
        self._storage_client = storage.Client() if gcs_bucket else None
        # Default to a GCS-backed FileIO when a GCS bucket is configured and
        # no explicit `io` was provided.
        if io is not None:
            self.io = io
        else:
            if gcs_bucket:
                from .iops.gcs import GcsFileIO

                self.io = GcsFileIO()
            else:
                self.io = FileIO()

    def _collection_ref(self, collection: str):
        """Alias for `_namespace_ref` using the preferred term `collection`.

        Do NOT change call signatures; this helper provides a clearer name
        for new code paths while remaining backwards-compatible.
        """
        return self._catalog_ref.document(collection)

    def _datasets_collection(self, collection: str):
        # Primary subcollection for datasets.
        return self._collection_ref(collection).collection("datasets")

    def _dataset_doc_ref(self, collection: str, dataset_name: str):
        return self._datasets_collection(collection).document(dataset_name)

    def _snapshots_collection(self, collection: str, dataset_name: str):
        return self._dataset_doc_ref(collection, dataset_name).collection("snapshots")

    def _views_collection(self, collection: str):
        return self._collection_ref(collection).collection("views")

    def _view_doc_ref(self, collection: str, view_name: str):
        return self._views_collection(collection).document(view_name)

    def create_dataset(
        self, identifier: str, schema: Any, properties: dict | None = None, author: str = None
    ) -> SimpleDataset:
        if author is None:
            raise ValueError("author must be provided when creating a dataset")
        collection, dataset_name = identifier.split(".")
        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        # Check primary `datasets` location
        if doc_ref.get().exists:
            raise DatasetAlreadyExists(f"Dataset already exists: {identifier}")

        # Build default dataset metadata
        location = f"gs://{self.gcs_bucket}/{self.workspace}/{collection}/{dataset_name}"
        metadata = DatasetMetadata(
            dataset_identifier=identifier,
            schema=schema,
            location=location,
            properties=properties or {},
        )

        # Persist document with timestamp and author
        now_ms = int(time.time() * 1000)
        metadata.timestamp_ms = now_ms
        metadata.author = author
        doc_ref.set(
            {
                "name": dataset_name,
                "collection": collection,
                "workspace": self.workspace,
                "location": location,
                "properties": metadata.properties,
                "format-version": metadata.format_version,
                "timestamp-ms": now_ms,
                "author": author,
                "maintenance-policy": metadata.maintenance_policy,
                "annotations": metadata.annotations,
            }
        )

        # Persist initial schema into `schemas` subcollection if provided
        if schema is not None:
            schema_id = self._write_schema(collection, dataset_name, schema, author=author)
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
            # update dataset doc to reference current schema
            doc_ref.update({"current-schema-id": metadata.current_schema_id})

        # Send webhook notification
        send_webhook(
            action="create",
            workspace=self.workspace,
            collection=collection,
            resource_type="dataset",
            resource_name=dataset_name,
            payload=dataset_created_payload(
                schema=schema,
                location=location,
                properties=properties,
            ),
        )

        # Return SimpleDataset (attach this catalog so append() can persist)
        return SimpleDataset(identifier=identifier, _metadata=metadata, io=self.io, catalog=self)

    def load_dataset(self, identifier: str, load_history: bool = False) -> SimpleDataset:
        """Load a dataset from Firestore.

        Args:
            identifier: Dataset identifier in format 'collection.dataset_name'
            load_history: If True, load all snapshots from Firestore (expensive for
                large histories). If False (default), only load the current snapshot,
                which is sufficient for most write operations.

        Returns:
            SimpleDataset instance with metadata loaded from Firestore.

        Raises:
            DatasetNotFound: If the dataset does not exist in Firestore.
        """
        collection, dataset_name = identifier.split(".")
        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        doc = doc_ref.get()
        if not doc.exists:
            raise DatasetNotFound(f"Dataset not found: {identifier}")

        data = doc.to_dict() or {}
        metadata = DatasetMetadata(
            dataset_identifier=identifier,
            location=data.get("location")
            or f"gs://{self.gcs_bucket}/{self.workspace}/{collection}/{dataset_name}",
            schema=data.get("schema"),
            properties=data.get("properties") or {},
        )

        # Load dataset-level timestamp/author and collection/workspace
        metadata.timestamp_ms = data.get("timestamp-ms")
        metadata.author = data.get("author")
        metadata.description = data.get("description")
        metadata.describer = data.get("describer")
        metadata.annotations = data.get("annotations") or []

        # Load snapshots based on load_history flag
        snaps = []
        if load_history:
            # Load all snapshots from Firestore (expensive for large histories)
            for snap_doc in self._snapshots_collection(collection, dataset_name).stream():
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
            if snaps:
                metadata.current_snapshot_id = snaps[-1].snapshot_id
        else:
            # Load only the current snapshot (efficient single read)
            current_snap_id = data.get("current-snapshot-id")
            if current_snap_id:
                try:
                    snap_doc = (
                        self._snapshots_collection(collection, dataset_name)
                        .document(str(current_snap_id))
                        .get()
                    )
                    if snap_doc.exists:
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
                        metadata.current_snapshot_id = current_snap_id
                except Exception:
                    pass
        metadata.snapshots = snaps

        # Load schemas subcollection
        schemas_coll = doc_ref.collection("schemas")
        # Load all schemas if requested; otherwise load only current schema
        if load_history:
            schemas = []
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
        else:
            # Only load the current schema document for efficiency
            current_schema_id = doc.to_dict().get("current-schema-id")
            if current_schema_id:
                sdoc = schemas_coll.document(str(current_schema_id)).get()
                if sdoc.exists:
                    sd = sdoc.to_dict() or {}
                    metadata.schemas = [
                        {
                            "schema_id": sdoc.id,
                            "columns": sd.get("columns", []),
                            "timestamp-ms": sd.get("timestamp-ms"),
                            "author": sd.get("author"),
                            "sequence-number": sd.get("sequence-number"),
                        }
                    ]
                    metadata.current_schema_id = current_schema_id
        return SimpleDataset(identifier=identifier, _metadata=metadata, io=self.io, catalog=self)

    def drop_dataset(self, identifier: str) -> None:
        collection, dataset_name = identifier.split(".")
        # Delete snapshots
        snaps_coll = self._snapshots_collection(collection, dataset_name)
        for doc in snaps_coll.stream():
            snaps_coll.document(doc.id).delete()
        # Delete dataset doc
        self._dataset_doc_ref(collection, dataset_name).delete()

    def list_datasets(self, collection: str) -> Iterable[str]:
        coll = self._datasets_collection(collection)
        return [doc.id for doc in coll.stream()]

    def create_collection(
        self,
        collection: str,
        properties: dict | None = None,
        exists_ok: bool = False,
        author: str = None,
    ) -> None:
        """Create a collection document under the catalog.

        If `exists_ok` is False and the collection already exists, a KeyError is raised.
        """
        doc_ref = self._collection_ref(collection)
        if doc_ref.get().exists:
            if exists_ok:
                return
            raise CollectionAlreadyExists(f"Collection already exists: {collection}")

        now_ms = int(time.time() * 1000)
        if author is None:
            raise ValueError("author must be provided when creating a collection")
        doc_ref.set(
            {
                "name": collection,
                "properties": properties or {},
                "timestamp-ms": now_ms,
                "author": author,
                "annotations": [],
            }
        )

    def create_collection_if_not_exists(
        self, collection: str, properties: dict | None = None, author: Optional[str] = None
    ) -> None:
        """Convenience wrapper that creates the collection only if missing."""
        self.create_collection(collection, properties=properties, exists_ok=True, author=author)

    def dataset_exists(
        self, identifier_or_collection: str, dataset_name: Optional[str] = None
    ) -> bool:
        """Return True if the dataset exists.

        Supports two call forms:
        - dataset_exists("collection.dataset")
        - dataset_exists("collection", "dataset")
        """
        # Normalize inputs
        if dataset_name is None:
            # Expect a single collection like 'collection.dataset'
            if "." not in identifier_or_collection:
                raise ValueError(
                    "collection must be 'collection.dataset' or pass dataset_name separately"
                )
            collection, dataset_name = identifier_or_collection.rsplit(".", 1)
        else:
            collection = identifier_or_collection

        try:
            doc_ref = self._dataset_doc_ref(collection, dataset_name)
            return doc_ref.get().exists
        except Exception:
            # On any error, be conservative and return False
            return False

    # Dataset API methods have been renamed to the preferred `dataset` terminology.

    # --- View support -------------------------------------------------
    def create_view(
        self,
        identifier: str | tuple,
        sql: str,
        schema: Any | None = None,
        author: str = None,
        description: Optional[str] = None,
        properties: dict | None = None,
        update_if_exists: bool = False,
    ) -> CatalogView:
        """Create a view document and a statement version in the `statement` subcollection.

        `identifier` may be a string like 'namespace.view' or a tuple ('namespace','view').
        """
        # Normalize identifier
        if isinstance(identifier, tuple) or isinstance(identifier, list):
            collection, view_name = identifier[0], identifier[1]
        else:
            collection, view_name = identifier.split(".")

        doc_ref = self._view_doc_ref(collection, view_name)
        if doc_ref.get().exists:
            if not update_if_exists:
                raise ViewAlreadyExists(f"View already exists: {collection}.{view_name}")
            # Update existing view - get current sequence number
            existing_doc = doc_ref.get().to_dict()
            current_statement_id = existing_doc.get("statement-id")
            if current_statement_id:
                stmt_ref = doc_ref.collection("statement").document(current_statement_id)
                stmt_doc = stmt_ref.get()
                if stmt_doc.exists:
                    sequence_number = stmt_doc.to_dict().get("sequence-number", 0) + 1
                else:
                    sequence_number = 1
            else:
                sequence_number = 1
        else:
            sequence_number = 1

        now_ms = int(time.time() * 1000)
        if author is None:
            raise ValueError("author must be provided when creating a view")

        # Write statement version
        statement_id = str(now_ms)
        stmt_coll = doc_ref.collection("statement")
        stmt_coll.document(statement_id).set(
            {
                "sql": sql,
                "timestamp-ms": now_ms,
                "author": author,
                "sequence-number": sequence_number,
            }
        )

        # Persist root view doc referencing the statement id
        doc_ref.set(
            {
                "name": view_name,
                "collection": collection,
                "workspace": self.workspace,
                "timestamp-ms": now_ms,
                "author": author,
                "description": description,
                "describer": author,
                "last-execution-ms": None,
                "last-execution-data-size": None,
                "last-execution-records": None,
                "statement-id": statement_id,
                "properties": properties or {},
            }
        )

        # Send webhook notification
        send_webhook(
            action="create" if not update_if_exists else "update",
            workspace=self.workspace,
            collection=collection,
            resource_type="view",
            resource_name=view_name,
            payload=view_created_payload(
                definition=sql,
                properties=properties,
            ),
        )

        # Return a simple CatalogView wrapper
        v = CatalogView(name=view_name, definition=sql, properties=properties or {})
        # provide convenient attributes used by docs/examples
        setattr(v, "sql", sql)
        setattr(v, "metadata", type("M", (), {})())
        v.metadata.schema = schema
        # Attach catalog and identifier for describe() method
        setattr(v, "_catalog", self)
        setattr(v, "_identifier", f"{collection}.{view_name}")
        return v

    def load_view(self, identifier: str | tuple) -> CatalogView:
        """Load a view by identifier. Returns a `CatalogView` with `.definition` and `.sql`.

        Raises `ViewNotFound` if the view doc is missing.
        """
        if isinstance(identifier, tuple) or isinstance(identifier, list):
            collection, view_name = identifier[0], identifier[1]
        else:
            collection, view_name = identifier.split(".")

        doc_ref = self._view_doc_ref(collection, view_name)
        doc = doc_ref.get()
        if not doc.exists:
            raise ViewNotFound(f"View not found: {collection}.{view_name}")

        data = doc.to_dict() or {}
        stmt_id = data.get("statement-id")
        sql = None
        schema = data.get("schema")

        sdoc = doc_ref.collection("statement").document(str(stmt_id)).get()
        sql = (sdoc.to_dict() or {}).get("sql")

        v = CatalogView(name=view_name, definition=sql or "", properties=data.get("properties", {}))
        setattr(v, "sql", sql or "")
        setattr(v, "metadata", type("M", (), {})())
        v.metadata.schema = schema
        # Populate metadata fields from the stored view document so callers
        # expecting attributes like `timestamp_ms` won't fail.
        v.metadata.author = data.get("author")
        v.metadata.description = data.get("description")
        v.metadata.timestamp_ms = data.get("timestamp-ms")
        # Execution/operational fields (may be None)
        v.metadata.last_execution_ms = data.get("last-execution-ms")
        v.metadata.last_execution_data_size = data.get("last-execution-data-size")
        v.metadata.last_execution_records = data.get("last-execution-records")
        # Optional describer (used to flag LLM-generated descriptions)
        v.metadata.describer = data.get("describer")
        # Attach catalog and identifier for describe() method
        setattr(v, "_catalog", self)
        setattr(v, "_identifier", f"{collection}.{view_name}")
        return v

    def drop_view(self, identifier: str | tuple) -> None:
        if isinstance(identifier, tuple) or isinstance(identifier, list):
            collection, view_name = identifier[0], identifier[1]
        else:
            collection, view_name = identifier.split(".")

        doc_ref = self._view_doc_ref(collection, view_name)
        # delete statement subcollection
        for d in doc_ref.collection("statement").stream():
            doc_ref.collection("statement").document(d.id).delete()

        doc_ref.delete()

    def list_views(self, collection: str) -> Iterable[str]:
        coll = self._views_collection(collection)
        return [doc.id for doc in coll.stream()]

    def view_exists(
        self, identifier_or_collection: str | tuple, view_name: Optional[str] = None
    ) -> bool:
        """Return True if the view exists.

        Supports two call forms:
        - view_exists("collection.view")
        - view_exists(("collection", "view"))
        - view_exists("collection", "view")
        """
        # Normalize inputs
        if view_name is None:
            if isinstance(identifier_or_collection, tuple) or isinstance(
                identifier_or_collection, list
            ):
                collection, view_name = identifier_or_collection[0], identifier_or_collection[1]
            else:
                if "." not in identifier_or_collection:
                    raise ValueError(
                        "identifier must be 'collection.view' or pass view_name separately"
                    )
                collection, view_name = identifier_or_collection.rsplit(".", 1)
        else:
            collection = identifier_or_collection

        try:
            doc_ref = self._view_doc_ref(collection, view_name)
            return doc_ref.get().exists
        except Exception:
            return False

    def update_view_execution_metadata(
        self,
        identifier: str | tuple,
        row_count: Optional[int] = None,
        execution_time: Optional[float] = None,
    ) -> None:
        if isinstance(identifier, tuple) or isinstance(identifier, list):
            collection, view_name = identifier[0], identifier[1]
        else:
            collection, view_name = identifier.split(".")

        doc_ref = self._view_doc_ref(collection, view_name)
        updates = {}
        now_ms = int(time.time() * 1000)
        if row_count is not None:
            updates["last-execution-records"] = row_count
        if execution_time is not None:
            updates["last-execution-time-ms"] = int(execution_time * 1000)
        updates["last-execution-ms"] = now_ms
        if updates:
            doc_ref.update(updates)

    def update_view_description(
        self,
        identifier: str | tuple,
        description: str,
        describer: Optional[str] = None,
    ) -> None:
        """Update the description for a view.

        Args:
            identifier: View identifier ('collection.view' or tuple)
            description: The new description text
            describer: Optional identifier for who/what created the description
        """
        if isinstance(identifier, tuple) or isinstance(identifier, list):
            collection, view_name = identifier[0], identifier[1]
        else:
            collection, view_name = identifier.split(".")

        doc_ref = self._view_doc_ref(collection, view_name)
        updates = {
            "description": description,
        }
        if describer is not None:
            updates["describer"] = describer
        doc_ref.update(updates)

    def update_dataset_description(
        self,
        identifier: str | tuple,
        description: str,
        describer: Optional[str] = None,
    ) -> None:
        """Update the description for a dataset.

        Args:
            identifier: Dataset identifier in format 'collection.dataset_name'
            description: The new description text
            describer: Optional identifier for who/what created the description
        """

        if isinstance(identifier, tuple) or isinstance(identifier, list):
            collection, dataset_name = identifier[0], identifier[1]
        else:
            collection, dataset_name = identifier.split(".")

        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        updates = {
            "description": description,
        }
        if describer is not None:
            updates["describer"] = describer
        doc_ref.update(updates)

    def write_parquet_manifest(
        self, snapshot_id: int, entries: List[dict], dataset_location: str
    ) -> Optional[str]:
        """Write a Parquet manifest for the given snapshot id and entries.

        Entries should be plain dicts convertible by pyarrow.Table.from_pylist.
        The manifest will be written to <dataset_location>/metadata/manifest-<snapshot_id>.parquet
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        # If entries is None we skip writing; if entries is empty list, write
        # an empty Parquet manifest (represents an empty dataset for this
        # snapshot). This preserves previous manifests so older snapshots
        # remain readable.
        if entries is None:
            return None

        parquet_path = f"{dataset_location}/metadata/manifest-{snapshot_id}.parquet"

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
                    ("uncompressed_size_in_bytes", pa.int64()),
                    ("column_uncompressed_sizes_in_bytes", pa.list_(pa.int64())),
                    ("null_counts", pa.list_(pa.int64())),
                    ("min_k_hashes", pa.list_(pa.list_(pa.uint64()))),
                    ("histogram_counts", pa.list_(pa.list_(pa.int64()))),
                    ("histogram_bins", pa.int32()),
                    ("min_values", pa.list_(pa.int64())),
                    ("max_values", pa.list_(pa.int64())),
                    ("min_values_display", pa.list_(pa.string())),
                    ("max_values_display", pa.list_(pa.string())),
                ]
            )

            # Normalize entries to match schema expectations:
            normalized = []
            for ent in entries:
                if not isinstance(ent, dict):
                    normalized.append(ent)
                    continue
                e = dict(ent)
                # Ensure list fields exist
                e.setdefault("min_k_hashes", [])
                e.setdefault("histogram_counts", [])
                e.setdefault("histogram_bins", 0)
                e.setdefault("column_uncompressed_sizes_in_bytes", [])
                e.setdefault("null_counts", [])
                e.setdefault("min_values_display", [])
                e.setdefault("max_values_display", [])

                # min/max values are stored as compressed int64 values
                # display values are string representations for human readability
                mv = e.get("min_values") or []
                xv = e.get("max_values") or []
                mv_disp = e.get("min_values_display") or []
                xv_disp = e.get("max_values_display") or []

                def truncate_display(v, max_len=32):
                    """Truncate display value to max_len characters, adding '...' if longer."""
                    if v is None:
                        return None
                    s = str(v)
                    if len(s) > max_len:
                        return s[:max_len] + "..."
                    return s

                # Ensure int64 values are properly typed for min/max
                e["min_values"] = [int(v) if v is not None else None for v in mv]
                e["max_values"] = [int(v) if v is not None else None for v in xv]
                # Display values truncated to 32 chars with '...' suffix if longer
                e["min_values_display"] = [truncate_display(v) for v in mv_disp]
                e["max_values_display"] = [truncate_display(v) for v in xv_disp]
                normalized.append(e)

            try:
                table = pa.Table.from_pylist(normalized, schema=schema)
            except Exception as exc:
                # Diagnostic output to help find malformed manifest entries

                print(
                    "[MANIFEST DEBUG] Failed to convert entries to Parquet manifest table. Dumping entries:"
                )
                for i, ent in enumerate(entries):
                    print(f" Entry {i}:")
                    if isinstance(ent, dict):
                        for k, v in ent.items():
                            tname = type(v).__name__
                            try:
                                s = repr(v)
                            except Exception:
                                s = "<unreprable>"
                            print(f"  - {k}: type={tname} repr={s[:200]}")
                    else:
                        print(
                            f"  - non-dict entry: type={type(ent).__name__} repr={repr(ent)[:200]}"
                        )
                raise exc

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

            return parquet_path
        except Exception as e:
            # Log and return None on failure
            # print(f"Failed to write Parquet manifest: {e}")
            raise e

    def save_snapshot(self, identifier: str, snapshot: Snapshot) -> None:
        """Persist a single snapshot document for a dataset."""
        namespace, dataset_name = identifier.split(".")
        snaps = self._snapshots_collection(namespace, dataset_name)
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

    def save_dataset_metadata(self, identifier: str, metadata: DatasetMetadata) -> None:
        """Persist dataset-level metadata and snapshots to Firestore.

        This writes the dataset document and upserts snapshot documents.
        """
        collection, dataset_name = identifier.split(".")
        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        doc_ref.set(
            {
                "name": dataset_name,
                "collection": collection,
                "workspace": self.workspace,
                "location": metadata.location,
                "properties": metadata.properties,
                "format-version": metadata.format_version,
                "annotations": metadata.annotations,
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

        # Metadata persisted in primary `datasets` collection only.

        snaps_coll = self._snapshots_collection(collection, dataset_name)
        # Upsert snapshot documents. Do NOT delete existing snapshot documents
        # here to avoid accidental removal of historical snapshots on save.
        for snap in metadata.snapshots:
            snaps_coll.document(str(snap.snapshot_id)).set(
                {
                    "snapshot-id": snap.snapshot_id,
                    "timestamp-ms": snap.timestamp_ms,
                    "manifest": snap.manifest_list,
                    "commit-message": getattr(snap, "commit_message", ""),
                    "schema-id": snap.schema_id,
                    "summary": snap.summary or {},
                    "author": getattr(snap, "author", None),
                    "sequence-number": getattr(snap, "sequence_number", None),
                    "user-created": getattr(snap, "user_created", None),
                }
            )

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
            # print(f"[DEBUG] _schema_to_columns: unsupported schema type: {type(schema)}")
            raise ValueError(
                "Unsupported schema type, expected pyarrow.Schema or orso.RelationSchema"
            )

        # print(f"[DEBUG] _schema_to_columns: processing {len(columns)} columns")

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
                "annotations": [],
            }

            cols.append(typed)

        return cols

    def _write_schema(self, namespace: str, dataset_name: str, schema: Any, author: str) -> str:
        """Persist a schema document in the dataset's `schemas` subcollection and
        return the new schema id.
        """
        import uuid

        doc_ref = self._dataset_doc_ref(namespace, dataset_name)
        schemas_coll = doc_ref.collection("schemas")
        sid = str(uuid.uuid4())
        # print(f"[DEBUG] _write_schema called for {namespace}/{dataset_name} sid={sid}")
        try:
            cols = self._schema_to_columns(schema)
        except Exception:
            # print(
            #     f"[DEBUG] _write_schema: _schema_to_columns raised: {e}; falling back to empty columns list"
            # )
            cols = []
        now_ms = int(time.time() * 1000)
        if author is None:
            raise ValueError("author must be provided when writing a schema")
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
            # print(
            #     f"[DEBUG] Writing schema doc {sid} for {namespace}/{dataset_name} (cols={len(cols)})"
            # )
            schemas_coll.document(sid).set(
                {
                    "columns": cols,
                    "timestamp-ms": now_ms,
                    "author": author,
                    "sequence-number": new_seq,
                }
            )
            # print(f"[DEBUG] Wrote schema doc {sid}")
        except Exception:
            # print(f"[DEBUG] Failed to write schema doc {sid}: {e}")
            pass
        return sid
