"""A Firestore + GCS backed implementation of PyIceberg's catalog interface."""

from __future__ import annotations

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import orjson
import pyarrow as pa
from google.cloud import firestore
from orso.logging import get_logger
from pyiceberg.catalog import Identifier
from pyiceberg.catalog import MetastoreCatalog
from pyiceberg.catalog import PropertiesUpdateSummary
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.exceptions import NamespaceNotEmptyError
from pyiceberg.exceptions import NoSuchNamespaceError
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.exceptions import NoSuchViewError
from pyiceberg.exceptions import TableAlreadyExistsError
from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import CommitTableResponse
from pyiceberg.table import StaticTable
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.table.sorting import SortOrder
from pyiceberg.table.update import TableRequirement
from pyiceberg.table.update import TableUpdate
from pyiceberg.typedef import EMPTY_DICT
from pyiceberg.typedef import Properties

from .parquet_manifest import ManifestOptimizationConfig
from .parquet_manifest import OptimizedStaticTable
from .parquet_manifest import entry_to_dict
from .parquet_manifest import write_parquet_manifest
from .view import View
from .view import ViewAlreadyExistsError
from .view import ViewMetadata

logger = get_logger()
logger.setLevel(5)


def _get_firestore_client(
    project: Optional[str] = None, database: Optional[str] = None
) -> firestore.Client:
    if project:
        return firestore.Client(project=project, database=database)
    return firestore.Client(database=database)


class FirestoreCatalog(MetastoreCatalog):
    """PyIceberg catalog implementation backed by Firestore documents and GCS metadata."""

    TABLES_SUBCOLLECTION = "tables"
    VIEWS_SUBCOLLECTION = "views"

    def __init__(
        self,
        catalog_name: str,
        firestore_project: Optional[str] = None,
        firestore_database: Optional[str] = None,
        gcs_bucket: Optional[str] = None,
        **properties: str,
    ):
        # Ensure gcs bucket info is present in properties for FileIO resolution
        properties["gcs_bucket"] = gcs_bucket
        super().__init__(catalog_name, **properties)

        self.catalog_name = catalog_name
        self.bucket_name = gcs_bucket

        self.firestore_client = _get_firestore_client(firestore_project, firestore_database)
        self._catalog_ref = self.firestore_client.collection(catalog_name)
        self._properties = properties

    def _namespace_ref(self, namespace: str) -> firestore.DocumentReference:
        return self._catalog_ref.document(namespace)

    def _tables_collection(self, namespace: str) -> firestore.CollectionReference:
        return self._namespace_ref(namespace).collection(self.TABLES_SUBCOLLECTION)

    def _views_collection(self, namespace: str) -> firestore.CollectionReference:
        return self._namespace_ref(namespace).collection(self.VIEWS_SUBCOLLECTION)

    def _normalize_namespace(self, namespace: Union[str, Identifier]) -> str:
        tuple_identifier = self.identifier_to_tuple(namespace)
        if not tuple_identifier:
            raise ValueError("namespace must contain at least one segment")
        return ".".join(tuple_identifier)

    def _parse_identifier(self, identifier: Union[str, Identifier]) -> Tuple[str, str]:
        return self.identifier_to_database_and_table(identifier)

    def _require_namespace(self, namespace: Union[str, Identifier]) -> str:
        namespace_str = self._normalize_namespace(namespace)
        if not self._namespace_ref(namespace_str).get().exists:
            raise NoSuchNamespaceError(namespace_str)
        return namespace_str

    def _table_doc_ref(self, namespace: str, table_name: str) -> firestore.DocumentReference:
        return self._tables_collection(namespace).document(table_name)

    def _view_doc_ref(self, namespace: str, view_name: str) -> firestore.DocumentReference:
        return self._views_collection(namespace).document(view_name)

    def _metadata_doc_ref(self, namespace: str, table_name: str) -> firestore.DocumentReference:
        """Get the Firestore document reference for table metadata.

        Path: /<catalog>/<namespace>/tables/<table_name>
        """
        return self._table_doc_ref(namespace, table_name)

    def _snapshots_collection(
        self, namespace: str, table_name: str
    ) -> firestore.CollectionReference:
        """Get the Firestore collection reference for table snapshots.

        Path: /<catalog>/<namespace>/tables/<table_name>/snapshots
        """
        return self._table_doc_ref(namespace, table_name).collection("snapshots")

    def _snapshot_log_collection(
        self, namespace: str, table_name: str
    ) -> firestore.CollectionReference:
        """Get the Firestore collection reference for snapshot log.

        Path: /<catalog>/<namespace>/tables/<table_name>/snapshot_log
        """
        return self._table_doc_ref(namespace, table_name).collection("snapshot_log")

    def _load_metadata_from_firestore(
        self,
        namespace: str,
        table_name: str,
        include_full_history: bool = False,
    ) -> Optional[TableMetadataV2]:
        """Load metadata from Firestore.

        By default we only fetch the current snapshot to avoid fanning out over
        every historical snapshot document on each table open. Full history is
        fetched only when explicitly requested (e.g., time travel paths).
        """
        try:
            table_doc = self._table_doc_ref(namespace, table_name).get()
            if not table_doc.exists:
                return None

            data = table_doc.to_dict() or {}

            # Extract the metadata fields (exclude table management fields)
            metadata_fields = {
                k: v
                for k, v in data.items()
                if k
                not in (
                    "name",
                    "namespace",
                    "workspace",
                    "created_at",
                    "updated_at",
                    "metadata_location",
                )
            }

            current_snapshot_id = metadata_fields.get("current_snapshot_id") or metadata_fields.get(
                "current-snapshot-id"
            )

            snapshots_collection = self._snapshots_collection(namespace, table_name)
            snapshots = []

            if include_full_history:
                # Existing behavior: pull every snapshot doc (more reads)
                for snapshot_doc in snapshots_collection.stream():
                    snapshot_data = snapshot_doc.to_dict()
                    if snapshot_data:
                        snapshots.append(snapshot_data)
            elif current_snapshot_id is not None:
                # Fast path: only load the current snapshot document
                snap_doc = snapshots_collection.document(str(current_snapshot_id)).get()
                if snap_doc.exists:
                    snap_data = snap_doc.to_dict()
                    if snap_data:
                        snapshots.append(snap_data)
                else:
                    # Safety fallback: if the current snapshot doc is missing, fall back to full history
                    for snapshot_doc in snapshots_collection.stream():
                        snapshot_data = snapshot_doc.to_dict()
                        if snapshot_data:
                            snapshots.append(snapshot_data)
            else:
                # No current snapshot id present; fall back to full history to keep metadata consistent
                for snapshot_doc in snapshots_collection.stream():
                    snapshot_data = snapshot_doc.to_dict()
                    if snapshot_data:
                        snapshots.append(snapshot_data)

            if snapshots:
                metadata_fields["snapshots"] = snapshots

            snapshot_log_collection = self._snapshot_log_collection(namespace, table_name)

            if include_full_history:
                snapshot_log = [
                    log_doc.to_dict()
                    for log_doc in snapshot_log_collection.stream()
                    if log_doc.to_dict()
                ]
                if snapshot_log:
                    metadata_fields["snapshot-log"] = snapshot_log
            else:
                # For the fast path we omit snapshot-log to save reads; TableMetadataV2 tolerates its absence
                metadata_fields.setdefault("snapshot-log", [])

            if metadata_fields:
                logger.debug(
                    f"Loaded metadata for {namespace}.{table_name} from Firestore (full_history={include_full_history})"
                )
                return TableMetadataV2(**metadata_fields)
        except Exception as e:
            logger.warning(f"Failed to load metadata from Firestore: {e}")
        return None

    def _save_metadata_to_firestore(
        self, namespace: str, table_name: str, metadata: TableMetadataV2
    ) -> None:
        """Save metadata to Firestore, merging with existing table document."""
        try:
            metadata_dict = orjson.loads(metadata.model_dump_json(exclude_none=True))
            table_doc_ref = self._table_doc_ref(namespace, table_name)

            # Extract snapshots and snapshot-log to store in subcollections
            snapshots = metadata_dict.pop("snapshots", [])
            snapshot_log = metadata_dict.pop("snapshot-log", [])

            logger.debug(
                f"Saving metadata for {namespace}.{table_name}: {len(snapshots)} snapshots, {len(snapshot_log)} log entries"
            )

            # Get existing table document to preserve table management fields
            existing_doc = table_doc_ref.get()
            table_data = existing_doc.to_dict() if existing_doc.exists else {}

            # Merge metadata with existing table data, preserving table management fields
            table_data.update(metadata_dict)
            table_data["updated_at"] = firestore.SERVER_TIMESTAMP

            # Store combined data to table document
            table_doc_ref.set(table_data)

            # Store snapshots in subcollection without deleting everything first
            # Load existing docs so we can do a minimal diffed write/delete set
            snapshots_collection = self._snapshots_collection(namespace, table_name)
            existing_snapshots = {
                doc.id: doc.to_dict() or {} for doc in snapshots_collection.stream()
            }

            new_snapshots = {}
            for snapshot in snapshots:
                snapshot_id = snapshot.get("snapshot-id")
                if snapshot_id is None:
                    logger.warning(f"Snapshot missing snapshot-id: {snapshot}")
                    continue
                new_snapshots[str(snapshot_id)] = snapshot

            # Upsert changed or new snapshots
            for doc_id, snapshot in new_snapshots.items():
                if existing_snapshots.get(doc_id) != snapshot:
                    logger.debug(f"Writing snapshot {doc_id} to Firestore")
                    snapshots_collection.document(doc_id).set(snapshot)

            # Delete snapshots that no longer exist
            for doc_id in existing_snapshots.keys() - new_snapshots.keys():
                logger.debug(f"Deleting stale snapshot {doc_id} from Firestore")
                snapshots_collection.document(doc_id).delete()

            # Store snapshot log in subcollection without wholesale deletes
            snapshot_log_collection = self._snapshot_log_collection(namespace, table_name)
            existing_log = {doc.id: doc.to_dict() or {} for doc in snapshot_log_collection.stream()}

            new_log_entries = {}
            for idx, log_entry in enumerate(snapshot_log):
                snapshot_id = log_entry.get("snapshot-id")
                timestamp_ms = log_entry.get("timestamp-ms")
                if snapshot_id is None or timestamp_ms is None:
                    logger.warning(
                        f"Snapshot log entry missing required fields: {log_entry}, using index {idx}"
                    )
                    doc_id = f"entry_{idx}"
                else:
                    doc_id = f"{snapshot_id}_{timestamp_ms}"
                new_log_entries[doc_id] = log_entry

            # Upsert changed/new log entries
            for doc_id, log_entry in new_log_entries.items():
                if existing_log.get(doc_id) != log_entry:
                    logger.debug(f"Writing snapshot log entry {doc_id} to Firestore")
                    snapshot_log_collection.document(doc_id).set(log_entry)

            # Delete removed log entries
            for doc_id in existing_log.keys() - new_log_entries.keys():
                logger.debug(f"Deleting stale snapshot log entry {doc_id} from Firestore")
                snapshot_log_collection.document(doc_id).delete()

            logger.debug(f"Saved metadata for {namespace}.{table_name} to Firestore")
        except Exception as e:
            logger.warning(f"Failed to save metadata to Firestore: {e}")

    def _load_view_metadata_from_firestore(
        self, namespace: str, view_name: str
    ) -> Optional[ViewMetadata]:
        """Load view metadata from Firestore."""
        try:
            view_doc = self._view_doc_ref(namespace, view_name).get()
            if view_doc.exists:
                data = view_doc.to_dict() or {}
                logger.debug(f"Loaded view metadata for {namespace}.{view_name} from Firestore")
                return ViewMetadata.from_dict(data)
        except Exception as e:
            logger.warning(f"Failed to load view metadata from Firestore: {e}")
        return None

    def _save_view_metadata_to_firestore(
        self, namespace: str, view_name: str, metadata: ViewMetadata
    ) -> None:
        """Save view metadata to Firestore."""
        try:
            view_doc_ref = self._view_doc_ref(namespace, view_name)
            metadata_dict = metadata.to_dict()
            # Add tracking timestamps
            metadata_dict["updated_at"] = firestore.SERVER_TIMESTAMP
            if metadata.created_at is None:
                metadata_dict["created_at"] = firestore.SERVER_TIMESTAMP

            view_doc_ref.set(metadata_dict)
            logger.debug(f"Saved view metadata for {namespace}.{view_name} to Firestore")
        except Exception as e:
            logger.warning(f"Failed to save view metadata to Firestore: {e}")

    @staticmethod
    def _parse_metadata_version(metadata_location: str) -> int:
        return 0

    def create_namespace(
        self,
        namespace: Union[str, Identifier],
        properties: Properties = EMPTY_DICT,
        exists_ok: bool = False,
    ) -> Properties:
        namespace_str = self._normalize_namespace(namespace)
        doc_ref = self._namespace_ref(namespace_str)
        if doc_ref.get().exists:
            if exists_ok:
                return self.load_namespace_properties(namespace_str)
            raise NamespaceAlreadyExistsError(namespace_str)

        doc_data = {
            "name": namespace_str,
            "properties": dict(properties),
            "created_at": firestore.SERVER_TIMESTAMP,
            "updated_at": firestore.SERVER_TIMESTAMP,
        }
        doc_ref.set(doc_data)
        logger.debug(f"Created namespace {namespace_str} in catalog {self.catalog_name}")
        return properties

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        namespace_str = self._normalize_namespace(namespace)
        namespace_ref = self._namespace_ref(namespace_str)
        if not namespace_ref.get().exists:
            raise NoSuchNamespaceError(namespace_str)
        if any(True for _ in self._tables_collection(namespace_str).stream()):
            raise NamespaceNotEmptyError(namespace_str)
        if any(True for _ in self._views_collection(namespace_str).stream()):
            raise NamespaceNotEmptyError(namespace_str)
        namespace_ref.delete()
        logger.debug(f"Dropped namespace {namespace_str} from catalog {self.catalog_name}")

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        tuple_identifier = self.identifier_to_tuple(namespace)
        if tuple_identifier:
            namespace_str = ".".join(tuple_identifier)
            if not self._namespace_ref(namespace_str).get().exists:
                raise NoSuchNamespaceError(namespace_str)
            # For nested namespaces, you'd need to implement hierarchical structure
            return []
        return [(doc.id,) for doc in self._catalog_ref.stream()]

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        namespace_str = self._normalize_namespace(namespace)
        snapshot = self._namespace_ref(namespace_str).get()
        if not snapshot.exists:
            raise NoSuchNamespaceError(namespace_str)
        data = snapshot.to_dict() or {}
        return dict(data.get("properties", {}))

    def update_namespace_properties(
        self,
        namespace: Union[str, Identifier],
        removals: Optional[set[str]] = None,
        updates: Properties = EMPTY_DICT,
    ) -> PropertiesUpdateSummary:
        namespace_str = self._normalize_namespace(namespace)
        doc_ref = self._namespace_ref(namespace_str)
        snapshot = doc_ref.get()
        if not snapshot.exists:
            raise NoSuchNamespaceError(namespace_str)

        removals = removals or set()
        if removals and updates:
            overlap = set(removals) & set(updates)
            if overlap:
                raise ValueError(f"Updates and deletes overlap: {overlap}")

        properties: Dict[str, Any] = dict((snapshot.to_dict() or {}).get("properties", {}))
        removed: List[str] = []
        updated: List[str] = []
        missing: List[str] = []

        if removals:
            for key in removals:
                if key in properties:
                    properties.pop(key)
                    removed.append(key)
                else:
                    missing.append(key)
        if updates:
            for key, value in updates.items():
                properties[key] = value
                updated.append(key)

        doc_ref.set(
            {"properties": properties, "updated_at": firestore.SERVER_TIMESTAMP}, merge=True
        )
        return PropertiesUpdateSummary(removed=removed, updated=updated, missing=missing)

    def register_table(
        self,
        identifier: Union[str, Identifier],
        metadata_location: str,
    ) -> Table:
        namespace, table_name = self._parse_identifier(identifier)
        namespace_ref = self._namespace_ref(namespace)
        if not namespace_ref.get().exists:
            self.create_namespace(namespace)

        doc_ref = self._table_doc_ref(namespace, table_name)
        if doc_ref.get().exists:
            raise TableAlreadyExistsError(f"{namespace}.{table_name}")

        payload: Dict[str, Any] = {
            "workspace": self.catalog_name,
            "name": table_name,
            "namespace": namespace,
            "created_at": firestore.SERVER_TIMESTAMP,
            "updated_at": firestore.SERVER_TIMESTAMP,
        }

        doc_ref.set(payload)
        logger.debug(
            f"Registered table {namespace}.{table_name} in catalog {self.catalog_name}",
        )

        # Return a Table object
        return self.load_table(identifier)

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        namespace_str = self._require_namespace(namespace)
        return [(namespace_str, doc.id) for doc in self._tables_collection(namespace_str).stream()]

    def table_exists(self, identifier: Union[str, Identifier]) -> bool:
        namespace, table_name = self._parse_identifier(identifier)
        return self._table_doc_ref(namespace, table_name).get().exists

    def load_table(
        self, identifier: Union[str, Identifier], include_full_history: bool = False
    ) -> Table:
        namespace, table_name = self._parse_identifier(identifier)
        doc = self._table_doc_ref(namespace, table_name).get()
        if not doc.exists:
            raise NoSuchTableError(identifier)

        # metadata is stored in firestore
        metadata = self._load_metadata_from_firestore(
            namespace, table_name, include_full_history=include_full_history
        )
        if not metadata:
            raise NoSuchTableError(f"{self.catalog_name}.{namespace}.{table_name}")

        io = self._load_file_io({"type": "gcs", "bucket": self.bucket_name})

        # Return OptimizedStaticTable for fast query planning with Parquet manifests
        table = OptimizedStaticTable(
            identifier=(namespace, table_name),
            metadata=metadata,
            metadata_location=None,
            io=io,
            catalog=self,
        )

        # Track whether we loaded full snapshot history for downstream checks
        table.full_history_loaded = include_full_history

        return table

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        namespace, table_name = self._parse_identifier(identifier)
        doc_ref = self._table_doc_ref(namespace, table_name)
        if not doc_ref.get().exists:
            raise NoSuchTableError(f"{namespace}.{table_name}")
        doc_ref.delete()
        logger.debug(f"Dropped table {namespace}.{table_name}")

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        # For purge_table, you might want to also delete the metadata files from GCS
        # For now, just drop the table reference
        self.drop_table(identifier)

    def rename_table(
        self,
        from_identifier: Union[str, Identifier],
        to_identifier: Union[str, Identifier],
    ) -> Table:
        from_namespace, from_table = self._parse_identifier(from_identifier)
        to_namespace, to_table = self._parse_identifier(to_identifier)

        src_ref = self._table_doc_ref(from_namespace, from_table)
        src_doc = src_ref.get()
        if not src_doc.exists:
            raise NoSuchTableError(f"{from_namespace}.{from_table}")

        dst_ref = self._table_doc_ref(to_namespace, to_table)
        if dst_ref.get().exists:
            raise TableAlreadyExistsError(f"{to_namespace}.{to_table}")

        data = src_doc.to_dict() or {}
        data["name"] = to_table
        data["namespace"] = to_namespace
        data["updated_at"] = firestore.SERVER_TIMESTAMP

        dst_ref.set(data)
        src_ref.delete()

        logger.debug(
            f"Renamed table {from_namespace}.{from_table} to {to_namespace}.{to_table}",
        )

        # Return the updated table
        return self.load_table(to_identifier)

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        namespace, table_name = self._parse_identifier(identifier)

        if isinstance(schema, pa.Schema):
            schema = _pyarrow_to_schema_without_ids(schema)

        # Check if namespace exists, create if not
        namespace_ref = self._namespace_ref(namespace)
        if not namespace_ref.get().exists:
            self.create_namespace(namespace)

        # Check if table already exists
        if self.table_exists(identifier):
            raise TableAlreadyExistsError(f"{self.catalog_name}.{namespace}.{table_name}")

        # Create metadata
        io = self._load_file_io({})

        # Generate a metadata location
        if location is None:
            # Use a default location based on catalog properties or identifier
            location = f"gs://{self._properties.get('gcs_bucket')}/{self.catalog_name}/{namespace}/{table_name}"

        # Create new table metadata
        metadata = new_table_metadata(
            schema=schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
            location=location,
            properties=properties,
        )

        # Register the table in Firestore first with basic info
        payload: Dict[str, Any] = {
            "name": table_name,
            "namespace": namespace,
            "workspace": self.catalog_name,
            "created_at": firestore.SERVER_TIMESTAMP,
            "updated_at": firestore.SERVER_TIMESTAMP,
        }
        self._table_doc_ref(namespace, table_name).set(payload)

        # Now save metadata (which will merge with the existing document)
        self._save_metadata_to_firestore(namespace, table_name, metadata)

        metadata_location = None

        # Return the created table
        return StaticTable(
            identifier=(namespace, table_name),
            metadata=metadata,
            metadata_location=metadata_location,
            io=io,
            catalog=self,
        )

    def create_table_transaction(self, *args: Any, **kwargs: Any):
        raise NotImplementedError("FirestoreCatalog does not handle table transactions.")

    def commit_table(
        self,
        table: Table,
        requirements: tuple[TableRequirement, ...],
        updates: tuple[TableUpdate, ...],
    ) -> CommitTableResponse:
        """Commit updates to a table.

        Args:
            table (Table): The table to be updated.
            requirements: (Tuple[TableRequirement, ...]): Table requirements.
            updates: (Tuple[TableUpdate, ...]): Table updates.

        Returns:
            CommitTableResponse: The updated metadata.

        Raises:
            NoSuchTableError: If a table with the given identifier does not exist.
            CommitFailedException: Requirement not met, or a conflict with a concurrent commit.
        """
        # Get the identifier
        namespace, table_name = table.name()

        current_table: Table | None
        try:
            current_table = self.load_table((namespace, table_name))
        except NoSuchTableError:
            current_table = None

        updated_staged_table = self._update_and_stage_table(
            current_table, (namespace, table_name), requirements, updates
        )

        # Save metadata to Firestore
        self._save_metadata_to_firestore(namespace, table_name, updated_staged_table.metadata)

        # Write Parquet manifest for fast query planning
        io = self._load_file_io(
            updated_staged_table.metadata.properties, updated_staged_table.metadata.location
        )
        # Try to collect staged manifest entries from the staged table object
        staged_entries = None
        for attr in (
            "staged_entries",
            "staged_manifest_entries",
            "staged_data_files",
            "staged_files",
            "new_entries",
            "new_files",
        ):
            val = getattr(updated_staged_table, attr, None)
            if val:
                staged_entries = val
                break

        entries_to_write = None
        if staged_entries:
            # Convert ManifestEntry-like objects to dicts using entry_to_dict
            try:
                config = ManifestOptimizationConfig()
                schema = updated_staged_table.metadata.schema()
                converted = []
                for e in staged_entries:
                    if isinstance(e, dict):
                        converted.append(e)
                    else:
                        converted.append(entry_to_dict(e, schema, config))
                entries_to_write = converted
            except Exception:
                # If conversion fails, fall back to not writing a parquet manifest
                entries_to_write = None

        parquet_path = write_parquet_manifest(
            updated_staged_table.metadata,
            io,
            updated_staged_table.metadata.location,
            entries=entries_to_write,
        )

        if parquet_path:
            logger.info(
                f"Wrote Parquet manifest for {self.catalog_name}.{namespace}.{table_name} at {parquet_path}"
            )

            # Store Parquet manifest path in the current snapshot document
            current_snapshot_id = updated_staged_table.metadata.current_snapshot_id
            if current_snapshot_id is not None:
                snapshot_ref = self._snapshots_collection(namespace, table_name).document(
                    str(current_snapshot_id)
                )
                snapshot_ref.set(
                    {"parquet-manifest": parquet_path},
                    merge=True,
                )
                logger.debug(f"Added parquet-manifest reference to snapshot {current_snapshot_id}")

        # Update Firestore
        table_ref = self._table_doc_ref(namespace, table_name)
        table_ref.set(
            {
                "updated_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
            timeout=5,
        )
        logger.info(f"Committed table {self.catalog_name}.{namespace}.{table_name}")

        return CommitTableResponse(
            metadata=updated_staged_table.metadata,
            metadata_location=updated_staged_table.metadata_location,
        )

    def _load_file_io(self, properties: Dict[str, str], location: Optional[str] = None) -> FileIO:
        """Load a FileIO instance for GCS using optimized Opteryx FileIO."""
        from .fileio.gcs_fileio import GcsFileIO

        # Merge catalog properties with provided properties
        io_props = {**self._properties, **properties}

        # Always use our optimized GCS FileIO for better performance
        return GcsFileIO(properties=io_props)

    def initialize(self, catalog_properties: Properties) -> None:
        """Initialize the catalog."""
        # Store properties for later use
        self._properties.update(catalog_properties)

    def create_view(
        self,
        identifier: Union[str, Identifier],
        sql: str,
        schema: Optional[Schema] = None,
        author: Optional[str] = None,
        description: Optional[str] = None,
        properties: Properties = EMPTY_DICT,
    ) -> View:
        """Create a new SQL view in the catalog.

        Args:
            identifier: View identifier (namespace, view_name)
            sql: The SQL statement that defines the view
            schema: Optional schema of the view result set
            author: Optional username or identifier of the creator
            description: Optional human-readable description
            properties: Additional properties for the view

        Returns:
            View: The created view object

        Raises:
            TableAlreadyExistsError: If a view with this identifier already exists
            NoSuchNamespaceError: If the namespace doesn't exist
        """
        namespace, view_name = self._parse_identifier(identifier)

        # Convert pyarrow schema if needed
        if isinstance(schema, pa.Schema):
            schema = _pyarrow_to_schema_without_ids(schema)

        # Check if namespace exists, create if not
        namespace_ref = self._namespace_ref(namespace)
        if not namespace_ref.get().exists:
            self.create_namespace(namespace)

        # Check if view already exists
        if self.view_exists(identifier):
            raise ViewAlreadyExistsError(
                f"View {self.catalog_name}.{namespace}.{view_name} already exists"
            )

        # Create view metadata
        metadata = ViewMetadata(
            sql_text=sql,
            schema=schema,
            author=author,
            description=description,
            properties=dict(properties),
            workspace=self.catalog_name,
        )

        # Save view metadata to Firestore
        self._save_view_metadata_to_firestore(namespace, view_name, metadata)

        logger.debug(f"Created view {namespace}.{view_name} in catalog {self.catalog_name}")

        # Return the created view
        return View(
            identifier=(namespace, view_name),
            metadata=metadata,
            catalog_name=self.catalog_name,
        )

    def load_view(self, identifier: Union[str, Identifier]) -> View:
        """Load a view from the catalog.

        Args:
            identifier: View identifier (namespace, view_name)

        Returns:
            View: The loaded view object

        Raises:
            NoSuchViewError: If the view doesn't exist
        """
        namespace, view_name = self._parse_identifier(identifier)

        # Check if view exists
        if not self.view_exists(identifier):
            raise NoSuchViewError(f"View not found: {identifier}")

        # Load metadata from Firestore
        metadata = self._load_view_metadata_from_firestore(namespace, view_name)
        if not metadata:
            raise NoSuchViewError(
                f"View metadata not found: {self.catalog_name}.{namespace}.{view_name}"
            )

        # Ensure workspace is set on metadata for permissions
        if metadata.workspace is None:
            metadata.workspace = self.catalog_name

        # Return the view
        return View(
            identifier=(namespace, view_name),
            metadata=metadata,
            catalog_name=self.catalog_name,
        )

    def list_views(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        namespace_str = self._require_namespace(namespace)
        return [(namespace_str, doc.id) for doc in self._views_collection(namespace_str).stream()]

    def view_exists(self, identifier: Union[str, Identifier]) -> bool:
        namespace, view_name = self._parse_identifier(identifier)
        return self._view_doc_ref(namespace, view_name).get().exists

    def drop_view(self, identifier: Union[str, Identifier]) -> None:
        namespace, view_name = self._parse_identifier(identifier)
        doc_ref = self._view_doc_ref(namespace, view_name)
        if not doc_ref.get().exists:
            raise NoSuchViewError(f"View not found: {identifier}")
        doc_ref.delete()
        logger.debug(f"Dropped view {namespace}.{view_name}")

    def update_view_execution_metadata(
        self,
        identifier: Union[str, Identifier],
        row_count: Optional[int] = None,
        execution_time: Optional[float] = None,
    ) -> None:
        """Update view execution metadata after a view is run.

        This method updates the last_run_at timestamp and optionally the
        row count from the last execution. This is useful for tracking
        view usage and for query planning.

        Args:
            identifier: View identifier (namespace, view_name)
            row_count: Number of rows returned by the view
            execution_time: Execution time in seconds (stored in properties)

        Raises:
            NoSuchViewError: If the view doesn't exist
        """
        namespace, view_name = self._parse_identifier(identifier)
        doc_ref = self._view_doc_ref(namespace, view_name)

        if not doc_ref.get().exists:
            raise NoSuchViewError(f"View not found: {identifier}")

        # Build update dict
        update_data: Dict[str, Any] = {
            "last_run_at": firestore.SERVER_TIMESTAMP,
            "updated_at": firestore.SERVER_TIMESTAMP,
        }

        if row_count is not None:
            update_data["last_row_count"] = row_count

        if execution_time is not None:
            # Store execution time in properties using nested dict
            update_data["properties"] = {"last_execution_time_seconds": execution_time}

        doc_ref.set(update_data, merge=True)
        logger.debug(f"Updated execution metadata for view {namespace}.{view_name}")
