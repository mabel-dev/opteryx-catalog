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


def _core_type_to_stored(column_type: Any) -> tuple:
    """Map an Opteryx ``ColumnType`` to ``(type_name, element_type, precision, scale)``.

    ``type_name`` is the dispatch-category name (``INTEGER``, ``DECIMAL``,
    ``ARRAY``, ...) so it round-trips through ``parse_column_type`` on read.
    """
    if column_type is None:
        return ("VARCHAR", None, None, None)

    category = column_type.category.name
    if category == "DECIMAL":
        logical = column_type.logical
        precision = getattr(logical, "precision", None) if logical is not None else None
        scale = getattr(logical, "scale", None) if logical is not None else None
        return ("DECIMAL", None, precision, scale)
    if category == "ARRAY":
        element = column_type.element
        element_name = element.category.name if element is not None else None
        return ("ARRAY", element_name, None, None)
    return (category, None, None, None)


# draken physical type name (DrakenType.name, from Morsel.schema) -> the same
# category names _core_type_to_stored uses.
_DRAKEN_CATEGORY_OF = {
    "INT8": "INTEGER",
    "INT16": "INTEGER",
    "INT32": "INTEGER",
    "INT64": "INTEGER",
    "DECIMAL": "DECIMAL",
    "DECIMAL128": "DECIMAL",
    "FLOAT32": "FLOAT",
    "FLOAT64": "FLOAT",
    "DATE32": "DATE",
    "TIMESTAMP64": "TIMESTAMP",
    "TIME32": "TIME",
    "TIME64": "TIME",
    "INTERVAL": "INTERVAL",
    "BOOL": "BOOLEAN",
    "VARCHAR": "VARCHAR",
    "NVARCHAR": "NVARCHAR",
    "VARBINARY": "VARBINARY",
    "VARIANT": "VARIANT",
    "ARRAY": "ARRAY",
    "NULL": "NULL",
    "VECTOR_FP16": "VECTOR",
}


def _morsel_type_to_stored(dtype: Any) -> tuple:
    """Map a draken ``DrakenType`` (from ``Morsel.schema``) to
    ``(type_name, element_type, precision, scale)``.

    This is the write/create path (a Morsel's schema is being persisted), so
    draken's physical type tag is used directly — no opteryx-core or pyarrow
    involved. ``type_name`` uses the same category names as
    :func:`_core_type_to_stored`.

    ``Morsel.schema`` only exposes the physical type tag, not parameterization
    (DECIMAL precision/scale, ARRAY element type), so those fall back to the
    same defaults used on read (see ``dataset.py``'s ``_stored_type_display``).
    """
    name = getattr(dtype, "name", str(dtype))
    category = _DRAKEN_CATEGORY_OF.get(name, "VARCHAR")
    if category == "DECIMAL":
        return ("DECIMAL", None, 38, 9)
    if category == "ARRAY":
        return ("ARRAY", "VARIANT", None, None)
    return (category, None, None, None)


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
        # Caches for immutable, version-addressed artifacts. Snapshots, schemas
        # and data files are write-once (a new write mints a new id), so caching
        # them by id is always correct: only the dataset doc's current-*-id
        # pointers are mutable, and those are re-read on every get_relation().
        self._snapshot_cache = {}  # (collection, name, snapshot_id) -> Snapshot
        self._schema_cache = {}  # (collection, name, schema_id) -> schema dict
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

        # Allocate stable, never-reused field-ids for the initial schema's columns
        # up front, so they can be persisted alongside the dataset doc in the same
        # write as the rest of its metadata.
        field_ids = None
        if schema is not None:
            column_count = len(self._schema_to_columns(schema))
            field_ids = list(range(1, column_count + 1))
            metadata.next_field_id = column_count + 1

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
                "refresh-frequency-mins": None,
                "next-field-id": metadata.next_field_id,
            }
        )

        # Persist initial schema into `schemas` subcollection if provided
        if schema is not None:
            schema_id = self._write_schema(
                collection, dataset_name, schema, author=author, field_ids=field_ids
            )
            metadata.current_schema_id = schema_id
            # Read back the schema doc to capture timestamp-ms, author, sequence-number
            try:
                sdoc = doc_ref.collection("schemas").document(schema_id).get()
                sdata = sdoc.to_dict() or {}
                metadata.schemas = [
                    {
                        "schema_id": schema_id,
                        "columns": sdata.get(
                            "columns", self._schema_to_columns(schema, field_ids=field_ids)
                        ),
                        "timestamp-ms": sdata.get("timestamp-ms"),
                        "author": sdata.get("author"),
                        "sequence-number": sdata.get("sequence-number"),
                    }
                ]
            except Exception:
                metadata.schemas = [
                    {
                        "schema_id": schema_id,
                        "columns": self._schema_to_columns(schema, field_ids=field_ids),
                    }
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
                (dataset_name can contain dots, e.g. 'public.github.events')
            load_history: If True, load all snapshots from Firestore (expensive for
                large histories). If False (default), only load the current snapshot,
                which is sufficient for most write operations.

        Returns:
            SimpleDataset instance with metadata loaded from Firestore.

        Raises:
            DatasetNotFound: If the dataset does not exist in Firestore.
        """
        collection, dataset_name = identifier.split(".", 1)
        doc = self._dataset_doc_ref(collection, dataset_name).get()
        if not doc.exists:
            raise DatasetNotFound(f"Dataset not found: {identifier}")
        return self._build_dataset(identifier, collection, dataset_name, doc, load_history)

    def get_relation(self, identifier):
        """Catalog resolution step: resolve a relation without knowing whether
        it is a dataset or a view, in a single ``get_all([dataset, view])``
        round trip. Returns ``(kind, obj)`` with kind in
        ``{"dataset", "view", None}``.

        The mutable dataset doc is read here on every call (it is the version
        pointer), so there is no staleness. The immutable snapshot/schema docs
        reached while building a dataset are served from id-keyed caches.
        """
        if isinstance(identifier, (tuple, list)):
            ident = ".".join(str(p) for p in identifier)
        else:
            ident = identifier
        # Dataset names may contain dots (split on the first); views are
        # addressed name-last. The two coincide for the common two-part name.
        ds_collection, ds_name = ident.split(".", 1) if "." in ident else (ident, ident)
        parts = ident.split(".")
        vw_collection = ".".join(parts[:-1]) or ds_collection
        vw_name = parts[-1]

        ds_ref = self._dataset_doc_ref(ds_collection, ds_name)
        vw_ref = self._view_doc_ref(vw_collection, vw_name)

        docs_by_path = {}
        try:
            for d in self.firestore_client.get_all([ds_ref, vw_ref]):
                ref = getattr(d, "reference", None)
                if ref is not None:
                    docs_by_path[ref.path] = d
        except Exception:
            docs_by_path = {}

        ds_doc = docs_by_path.get(ds_ref.path)
        vw_doc = docs_by_path.get(vw_ref.path)

        if ds_doc is not None and ds_doc.exists:
            return "dataset", self._build_dataset(ident, ds_collection, ds_name, ds_doc)
        if vw_doc is not None and vw_doc.exists:
            return "view", self._build_view(vw_collection, vw_name, vw_doc)
        return None, None

    def _snapshot_from_dict(self, sd: dict) -> Snapshot:
        return Snapshot(
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

    def _schema_entry_from_doc(self, sdoc) -> dict:
        sd = sdoc.to_dict() or {}
        return {
            "schema_id": sdoc.id,
            "columns": sd.get("columns", []),
            "timestamp-ms": sd.get("timestamp-ms"),
            "author": sd.get("author"),
            "sequence-number": sd.get("sequence-number"),
        }

    def _build_dataset(
        self, identifier, collection, dataset_name, doc, load_history: bool = False
    ) -> SimpleDataset:
        """Build a SimpleDataset from an already-fetched dataset doc.

        Non-history path resolves the current snapshot + schema, preferring the
        id-keyed caches (immutable artifacts) and batching any misses into one
        get_all().
        """
        data = doc.to_dict() or {}
        metadata = DatasetMetadata(
            dataset_identifier=identifier,
            location=data.get("location")
            or f"gs://{self.gcs_bucket}/{self.workspace}/{collection}/{dataset_name}",
            schema=data.get("schema"),
            properties=data.get("properties") or {},
        )
        metadata.timestamp_ms = data.get("timestamp-ms")
        metadata.author = data.get("author")
        metadata.description = data.get("description")
        metadata.describer = data.get("describer")
        metadata.annotations = data.get("annotations") or []
        metadata.refresh_frequency_mins = data.get("refresh-frequency-mins")
        metadata.next_field_id = data.get("next-field-id", 1)
        # Load the configured sort order. Without this the value round-tripped
        # by save_dataset_metadata is silently dropped on read, so DatasetCompactor
        # always sees an empty sort_orders and falls back to the (non-locality-
        # preserving) brute strategy — i.e. order-aware compaction never runs.
        metadata.sort_orders = data.get("sort-orders") or []

        schemas_coll = self._dataset_doc_ref(collection, dataset_name).collection("schemas")

        if load_history:
            snaps = []
            for snap_doc in self._snapshots_collection(collection, dataset_name).stream():
                snaps.append(self._snapshot_from_dict(snap_doc.to_dict() or {}))
            if snaps:
                metadata.current_snapshot_id = snaps[-1].snapshot_id
            metadata.snapshots = snaps
            metadata.schemas = [self._schema_entry_from_doc(s) for s in schemas_coll.stream()]
            metadata.current_schema_id = data.get("current-schema-id")
            return SimpleDataset(identifier=identifier, _metadata=metadata, io=self.io, catalog=self)

        current_snap_id = data.get("current-snapshot-id")
        current_schema_id = data.get("current-schema-id")

        # Prefer the immutable id-keyed caches; batch any misses into one read.
        snap_obj = (
            self._snapshot_cache.get((collection, dataset_name, current_snap_id))
            if current_snap_id
            else None
        )
        schema_entry = (
            self._schema_cache.get((collection, dataset_name, current_schema_id))
            if current_schema_id
            else None
        )

        snap_ref = schema_ref = None
        refs = []
        if current_snap_id and snap_obj is None:
            snap_ref = self._snapshots_collection(collection, dataset_name).document(
                str(current_snap_id)
            )
            refs.append(snap_ref)
        if current_schema_id and schema_entry is None:
            schema_ref = schemas_coll.document(str(current_schema_id))
            refs.append(schema_ref)

        docs_by_path = {}
        if refs:
            try:
                for d in self.firestore_client.get_all(refs):
                    ref = getattr(d, "reference", None)
                    if ref is not None:
                        docs_by_path[ref.path] = d
            except Exception:
                docs_by_path = {}

        snaps = []
        if snap_obj is None and snap_ref is not None:
            sdoc = docs_by_path.get(snap_ref.path)
            if sdoc is not None and sdoc.exists:
                snap_obj = self._snapshot_from_dict(sdoc.to_dict() or {})
                self._snapshot_cache[(collection, dataset_name, current_snap_id)] = snap_obj
        if snap_obj is not None:
            snaps.append(snap_obj)
            metadata.current_snapshot_id = current_snap_id
        metadata.snapshots = snaps

        if schema_entry is None and schema_ref is not None:
            scdoc = docs_by_path.get(schema_ref.path)
            if scdoc is not None and scdoc.exists:
                schema_entry = self._schema_entry_from_doc(scdoc)
                self._schema_cache[(collection, dataset_name, current_schema_id)] = schema_entry
        if schema_entry is not None:
            metadata.schemas = [schema_entry]
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

    def list_collections(self) -> Iterable[str]:
        """List top-level collections (documents) in this workspace."""
        try:
            return [col.id for col in self._catalog_ref.list_documents() if col.id[0] != "$"]
        except (ValueError, KeyError, AttributeError):
            return []

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

        doc = self._view_doc_ref(collection, view_name).get()
        if not doc.exists:
            raise ViewNotFound(f"View not found: {collection}.{view_name}")
        return self._build_view(collection, view_name, doc)

    def _build_view(self, collection, view_name, doc) -> CatalogView:
        """Build a CatalogView from an already-fetched view doc."""
        data = doc.to_dict() or {}
        stmt_id = data.get("statement-id")
        schema = data.get("schema")

        sdoc = (
            self._view_doc_ref(collection, view_name)
            .collection("statement")
            .document(str(stmt_id))
            .get()
        )
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

        Entries should be plain dicts. The manifest will be written to
        <dataset_location>/metadata/manifest-<snapshot_id>.parquet
        """
        from draken.interop.vector_sequence import vector_from_sequence
        from draken.morsels.morsel import Morsel
        from rugo.parquet import write_parquet

        from .iops.fileio import WRITE_PARQUET_OPTIONS

        # If entries is None we skip writing; if entries is empty list, write
        # an empty Parquet manifest (represents an empty dataset for this
        # snapshot). This preserves previous manifests so older snapshots
        # remain readable.
        if entries is None:
            return None

        parquet_path = f"{dataset_location}/metadata/manifest-{snapshot_id}.parquet"

        # Use provided FileIO if it supports writing; otherwise write to GCS
        try:
            # Explicit dtype per column (especially the nested-list stats
            # columns) so rugo's writer gets a consistent shape regardless of
            # what individual entries happen to carry.
            columns = {
                "file_path": "VARCHAR",
                "file_format": "VARCHAR",
                "record_count": "INTEGER",
                "file_size_in_bytes": "INTEGER",
                "uncompressed_size_in_bytes": "INTEGER",
                "column_uncompressed_sizes_in_bytes": "ARRAY",
                "null_counts": "ARRAY",
                "min_k_hashes": "ARRAY",
                "histogram_counts": "ARRAY",
                "histogram_bins": "INTEGER",
                "min_values": "ARRAY",
                "max_values": "ARRAY",
                "min_values_display": "ARRAY",
                "max_values_display": "ARRAY",
                "min_lengths": "ARRAY",
                "max_lengths": "ARRAY",
                # Stable per-column field-id, same order/index as every other
                # per-column stats array above (min_values[i] is field_ids[i]'s
                # min, etc.) — lets readers key stats by a schema-stable id
                # instead of assuming today's array position equals a column's
                # position in some other schema snapshot. Empty for manifest
                # rows written before this existed; readers must fall back to
                # positional indexing in that case.
                "field_ids": "ARRAY",
            }

            # Normalize entries to match the column set above:
            normalized = []
            for ent in entries:
                if not isinstance(ent, dict):
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
                e.setdefault("min_lengths", [])
                e.setdefault("max_lengths", [])
                e.setdefault("field_ids", [])

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

                # min_k_hashes / histogram_counts are per-column lists of ints,
                # so each entry is list[list[int]] and the column is a native
                # nested ARRAY<ARRAY<...>> (rugo's writer emits the 2-level LIST
                # encoding; read_manifest_columns reads it straight back). No
                # string encoding — min_k_hashes are full-range xxhash uint64,
                # stored with an unsigned leaf so values above INT64_MAX survive.
                normalized.append(e)

            from draken import draken_native as _dn

            morsel = Morsel()
            for name, dtype in columns.items():
                values = [e.get(name) for e in normalized]
                if name == "min_k_hashes":
                    # UINT64 leaf: xxhash values span the full unsigned range; a
                    # signed leaf would read back negative above INT64_MAX and
                    # corrupt min-k ordering.
                    #
                    # Entries reach here in mixed forms during migration: freshly
                    # computed (int hashes), decoded from a legacy comma-joined
                    # manifest (int hashes, possibly NEGATIVE where a uint64 was
                    # stored signed), a per-hash decimal-string list, or a single
                    # comma-joined string per column. Normalize every hash to its
                    # unsigned 64-bit value: a hash is a 64-bit identifier, not an
                    # arithmetic quantity, so `int(h) & mask` recovers the true
                    # uint64 (no-op for correct values, fixes legacy negatives)
                    # and draken's UINT64 factory then accepts it.
                    def _norm_col(col):
                        if col is None:
                            return None
                        if isinstance(col, str):  # legacy comma-joined column
                            col = col.split(",") if col else []
                        return [
                            None if h is None else (int(h) & 0xFFFFFFFFFFFFFFFF) for h in col
                        ]

                    values = [
                        None if entry is None else [_norm_col(col) for col in entry]
                        for entry in values
                    ]
                    morsel.append_vector(
                        name,
                        _dn.vector_array_from_sequence(
                            values, element_type=_dn.DrakenType.UINT64.value, nesting_depth=2
                        ),
                    )
                else:
                    morsel.append_vector(name, vector_from_sequence(values, dtype=dtype))

            data = write_parquet(morsel, **WRITE_PARQUET_OPTIONS)

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
                "refresh-frequency-mins": metadata.refresh_frequency_mins,
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

    def _schema_to_columns(self, schema: Any, field_ids: list | None = None) -> list:
        """Convert a schema into a simple columns list for storage.

        Each column is a dict:
        ``{"id": field-id, "name", "type", "element-type", "scale",
        "precision", "expectation-policies", "annotations"}``.

        Accepts a draken ``Morsel`` (the schema of a table being written) or
        any relation-schema-like object exposing a ``.columns`` list of
        columns with ``.name``/``.column_type`` (duck-typed, so a real
        Opteryx ``RelationSchema`` works without importing opteryx-core
        here). The stored ``type`` is a category/type name (``INTEGER``,
        ``DECIMAL``, ``ARRAY``, ...) that round-trips through
        ``opteryx.types.logical_type.parse_column_type`` on the query-engine
        side.

        ``field_ids``, when provided, is a stable, catalog-allocated id per
        column (same order as ``entries``) used as ``"id"`` instead of a
        freshly-recomputed position — this is what lets manifest statistics
        stay keyed correctly across schema evolution. When omitted, falls
        back to the historical ``enumerate(..., start=1)`` behavior for
        callers that haven't been updated to pass allocated ids.
        """
        if hasattr(schema, "columns"):
            entries = [
                (col.name, _core_type_to_stored(col.column_type)) for col in schema.columns
            ]
        elif hasattr(schema, "num_rows") and hasattr(schema, "column_names"):
            # Duck-typed as a draken Morsel. Don't check for `.schema`
            # directly — older draken releases (e.g. 0.4.2) don't expose it.
            from .catalog.manifest import morsel_schema_dict

            entries = [
                (name, _morsel_type_to_stored(dtype))
                for name, dtype in morsel_schema_dict(schema).items()
            ]
        else:
            raise ValueError(
                "Unsupported schema type, expected a relation-schema-like object "
                "with a `.columns` attribute or a draken.morsels.morsel.Morsel"
            )

        if field_ids is not None and len(field_ids) != len(entries):
            raise ValueError(
                f"field_ids length ({len(field_ids)}) does not match column count "
                f"({len(entries)})"
            )
        ids = field_ids if field_ids is not None else range(1, len(entries) + 1)

        cols = []
        for col_id, (name, (type_name, element_type, precision, scale)) in zip(ids, entries):
            cols.append(
                {
                    "id": col_id,
                    "name": name,
                    "type": type_name,
                    "element-type": element_type,
                    "scale": scale,
                    "precision": precision,
                    "expectation-policies": [],
                    "annotations": [],
                }
            )

        return cols

    def _write_schema(
        self,
        namespace: str,
        dataset_name: str,
        schema: Any,
        author: str,
        field_ids: list | None = None,
    ) -> str:
        """Persist a schema document in the dataset's `schemas` subcollection and
        return the new schema id.
        """
        import uuid

        doc_ref = self._dataset_doc_ref(namespace, dataset_name)
        schemas_coll = doc_ref.collection("schemas")
        sid = str(uuid.uuid4())
        # print(f"[DEBUG] _write_schema called for {namespace}/{dataset_name} sid={sid}")
        try:
            cols = self._schema_to_columns(schema, field_ids=field_ids)
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
