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
from .catalog.metadata import snapshot_is_tombstoned
from .catalog.metastore import Metastore
from .catalog.orphan_quarantine import MAINTENANCE_SUBCOLLECTION
from .catalog.view import View as CatalogView
from .alerts import report as _alert
from .exceptions import CollectionAlreadyExists
from .exceptions import CollectionLocked
from .exceptions import CollectionNotEmpty
from .exceptions import CollectionNotFound
from .exceptions import DatasetAlreadyExists
from .exceptions import DatasetLocked
from .exceptions import DatasetNotFound
from .exceptions import SnapshotMissingError
from .exceptions import ViewAlreadyExists
from .exceptions import ViewNotFound
from .exceptions import WorkspaceDeleteProtected
from .exceptions import WorkspaceDeleted
from .iops.base import FileIO
from .audit import emit_audit
from .webhooks import send_webhook
from .webhooks.events import dataset_created_payload
from .webhooks.events import dataset_deleted_payload
from .webhooks.events import dataset_renamed_payload
from .webhooks.events import view_created_payload
from .webhooks.events import view_deleted_payload
from .webhooks.events import workspace_deleted_payload
from .webhooks.events import workspace_locked_payload
from .webhooks.events import workspace_restored_payload
from .webhooks.events import workspace_unlocked_payload

# Workspace-level document holding drop tombstones. The `$` prefix keeps it out of
# `list_collections()`, which filters `$`-prefixed documents, so tombstones are
# invisible to normal catalog enumeration.
DROPPED_DOC = "$dropped"

# Root-level Firestore collection (a sibling to every workspace's own top-level
# collection, not nested under any single workspace) holding tombstones for
# workspaces that have been soft-deleted. Dataset tombstones live *inside* the
# workspace they were dropped from (`DROPPED_DOC` above) - that doesn't work
# for a workspace tombstoning itself, since a hard-deleted workspace's own
# top-level collection may no longer be a safe place to look. The 24h sweep
# reads this collection to find candidates without enumerating every
# workspace name blindly.
DROPPED_WORKSPACES_COLLECTION = "$dropped-workspaces"


def _core_type_to_stored(column_type: Any) -> tuple:
    """Map an Opteryx ``ColumnType`` to ``(type_name, element_type, precision, scale)``.

    ``type_name`` is the EXACT type string — ``str(ColumnType)`` — which is what
    ``parse_column_type`` reads back unchanged: ``IPV4``, ``UINT32``, ``INT8``,
    ``FLOAT32``, ``TIMESTAMP[ms]``, ``DECIMAL(10, 2)``, ``ARRAY<VARCHAR>``.

    It used to store the dispatch CATEGORY name, which is lossy in the direction
    that silently WIDENS and cannot be detected downstream: IPv4's category is
    INTEGER (deliberately — that is what makes ordering, grouping and joins run
    on the raw uint32), and so is every unsigned and narrow width's, so IPV4,
    UINT32, UINT64 and INT8 all persisted as ``INTEGER`` and came back as a
    signed INT64. A stored address then rendered as a number, and an unsigned
    column silently became signed.

    ``precision``/``scale``/``element_type`` stay populated alongside it. They are
    redundant with the parameterized name now, but they are separate stored
    columns other readers consume, and the engine's reader still falls back to
    them for the bare ``DECIMAL``/``ARRAY`` spellings written before this.
    """
    if column_type is None:
        return ("VARCHAR", None, None, None)

    type_name = str(column_type)
    category = column_type.category.name
    if category == "DECIMAL":
        logical = column_type.logical
        precision = getattr(logical, "precision", None) if logical is not None else None
        scale = getattr(logical, "scale", None) if logical is not None else None
        return (type_name, None, precision, scale)
    if category == "ARRAY":
        element = column_type.element
        element_name = element.category.name if element is not None else None
        return (type_name, element_name, None, None)
    return (type_name, None, None, None)


# draken physical type name (DrakenType.name, from Morsel.schema) -> the name to
# STORE. Every entry must be a spelling opteryx's parse_column_type reads back,
# which is why this is not simply `DrakenType.name`: DATE32, TIMESTAMP64, TIME32,
# TIME64 and VECTOR_FP16 do not parse, and a stored name that does not parse
# falls through the reader to its VARCHAR default — a timestamp column silently
# becoming a string, which is worse than the widening this change fixes.
#
# The EXACT widths map to themselves. That is the whole point: they used to
# collapse to INTEGER/FLOAT, so a UINT32 column read back signed and 64-bit.
#
# The rest keep the category spelling they always had, because a Morsel's schema
# exposes only the physical tag — no DECIMAL precision/scale, no VECTOR
# dimension, no TIMESTAMP unit — so there is nothing more exact to write. The
# reader's parameter-aware branches handle those bare names, as they always did.
_DRAKEN_STORED_NAME = {
    "INT8": "INT8",
    "INT16": "INT16",
    "INT32": "INT32",
    "INT64": "INT64",
    "UINT8": "UINT8",
    "UINT16": "UINT16",
    "UINT32": "UINT32",
    "UINT64": "UINT64",
    "FLOAT32": "FLOAT32",
    "FLOAT64": "FLOAT64",
    "DECIMAL": "DECIMAL",
    "DECIMAL128": "DECIMAL",
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
    stored = _DRAKEN_STORED_NAME.get(name, "VARCHAR")
    if stored == "DECIMAL":
        return ("DECIMAL", None, 38, 9)
    if stored == "ARRAY":
        return ("ARRAY", "VARIANT", None, None)
    return (stored, None, None, None)


_SNAPSHOT_SUMMARY_KEYS = (
    "added-data-files",
    "added-files-size",
    "added-records",
    "deleted-data-files",
    "deleted-files-size",
    "deleted-records",
    "total-data-files",
    "total-files-size",
    "total-records",
)


def _snapshot_to_document(snapshot: Snapshot) -> dict:
    """The canonical Firestore document for one snapshot.

    BOTH writers of a snapshot document — `save_snapshot` and
    `save_dataset_metadata`'s upsert loop — MUST serialize through here.
    They write the SAME document id with `.set()`, which REPLACES the
    document rather than merging it, so any field one writer omits is
    silently ERASED by the other.

    They previously carried different field sets, and `save_dataset_metadata`
    runs last in every write path (`SimpleDataset.append`/`overwrite`/
    `truncate`/`refresh_manifest` all call `save_snapshot` then
    `save_dataset_metadata`), so the two fields only `save_snapshot` wrote —
    `operation-type` and `parent-snapshot-id` — were written and then wiped
    on every commit. Every snapshot in the catalog therefore read back with
    `operation_type=None` and no parent link: an append, a compaction and a
    statistics refresh were indistinguishable after the fact, and the
    snapshot ancestry chain was never persisted at all. The asymmetry cut
    both ways — `user-created` existed only in `save_dataset_metadata`, so
    any path where `save_snapshot` ran last would erase that instead.
    `DatasetCompactor` calls only `save_dataset_metadata`, so its snapshots
    never recorded an operation type even once.

    `_snapshot_from_dict` is the reader; every key it looks for must be
    produced here. Keep the three in step.
    """
    summary = dict(snapshot.summary or {})
    for key in _SNAPSHOT_SUMMARY_KEYS:
        summary.setdefault(key, 0)

    return {
        "snapshot-id": snapshot.snapshot_id,
        "timestamp-ms": snapshot.timestamp_ms,
        "manifest": snapshot.manifest_list,
        "commit-message": getattr(snapshot, "commit_message", "") or "",
        "schema-id": getattr(snapshot, "schema_id", None),
        "summary": summary,
        "author": getattr(snapshot, "author", None),
        "sequence-number": getattr(snapshot, "sequence_number", None),
        "user-created": getattr(snapshot, "user_created", None),
        "operation-type": getattr(snapshot, "operation_type", None),
        "parent-snapshot-id": getattr(snapshot, "parent_snapshot_id", None),
    }


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
        include_deleted: bool = False,
    ):
        # `workspace` is the configured catalog/workspace name
        self.workspace = workspace
        # Backwards-compatible alias: keep `catalog_name` for older code paths
        self.catalog_name = workspace
        self.firestore_client = firestore.Client(
            project=firestore_project, database=firestore_database
        )
        self._catalog_ref = self.firestore_client.collection(workspace)
        # Ensure workspace-level properties document exists in Firestore, and
        # gate construction on workspace soft-delete state. The $properties doc
        # records metadata for the workspace such as 'timestamp-ms', 'author',
        # 'billing-account-id', 'owner', and the soft-delete/lock fields below.
        #
        # The existence-check read and the deleted-at-ms gate are deliberately
        # NOT under the same broad `except Exception: pass` - a Firestore read
        # failure here is tolerated (conservative: don't fail catalog init on
        # transient Firestore errors), but a WorkspaceDeleted raise is a real
        # business-logic decision and must always propagate, never be swallowed.
        props_doc = None
        try:
            props_ref = self._catalog_ref.document("$properties")
            props_doc = props_ref.get()
        except Exception:
            props_doc = None

        if props_doc is not None:
            if not props_doc.exists:
                try:
                    now_ms = int(time.time() * 1000)
                    props_ref.set(
                        {
                            "timestamp-ms": now_ms,
                            "billing-account-id": None,
                            "owner": None,
                            "deleted-at-ms": None,
                            "deleted-by": None,
                            "locked-by": None,
                            "locked-at-ms": None,
                        }
                    )
                except Exception:
                    # Be conservative: don't fail catalog initialization on Firestore errors
                    pass
            elif not include_deleted:
                data = props_doc.to_dict() or {}
                if data.get("deleted-at-ms") is not None:
                    raise WorkspaceDeleted(f"Workspace has been deleted: {workspace}")
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

    def _tombstones_collection(self):
        """Subcollection of drop tombstones for this workspace."""
        return self._catalog_ref.document(DROPPED_DOC).collection("datasets")

    def _dropped_workspaces_collection(self):
        """Root-level collection of workspace-drop tombstones.

        A sibling to every workspace's own top-level collection, in the same
        Firestore database - NOT nested under `self._catalog_ref`. See
        `DROPPED_WORKSPACES_COLLECTION` for why this can't live inside the
        workspace it tombstones.
        """
        return self.firestore_client.collection(DROPPED_WORKSPACES_COLLECTION)

    @staticmethod
    def _delete_subcollection(coll_ref) -> None:
        """Delete every document in a subcollection.

        Firestore does not cascade: deleting a document leaves its subcollections
        addressable but unreachable, so each one must be emptied explicitly.
        """
        for doc in coll_ref.stream():
            coll_ref.document(doc.id).delete()

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
                "locked-by": None,
                "locked-at-ms": None,
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

        emit_audit(
            "create_dataset",
            resource_type="dataset",
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            location=location,
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
            commit_message=sd.get("commit-message"),
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
        # Load the configured maintenance policy. Same failure mode as
        # sort-orders above: save_dataset_metadata writes 'maintenance-policy'
        # but nothing read it back, so every dataset presented the class
        # default of {'retained-snapshot-age-days': None} - which SnapshotExpiry
        # reads as "keep only the latest snapshot". Retention was therefore
        # ignored everywhere, and every expiration run condemned the entire
        # history regardless of what was configured.
        stored_maintenance_policy = data.get("maintenance-policy")
        if stored_maintenance_policy:
            metadata.maintenance_policy = stored_maintenance_policy

        schemas_coll = self._dataset_doc_ref(collection, dataset_name).collection("schemas")

        if load_history:
            snaps = []
            for snap_doc in self._snapshots_collection(collection, dataset_name).stream():
                snap_data = snap_doc.to_dict() or {}
                # Tombstoned snapshots stay out of every normal read. They are
                # records for the restore path, not history: if they appeared
                # here, expiration would re-condemn them each run, the orphan-
                # detection threshold would count them (a 15-minutely dataset
                # accrues ~2,880 tombstones per 30-day window - enough on its
                # own to trip MAX_SNAPSHOTS_FOR_ORPHAN_DETECTION and silently
                # disable orphan cleanup), and their manifests would count as
                # referenced, pinning the very files expiration just released.
                if snapshot_is_tombstoned(snap_data):
                    continue
                snaps.append(self._snapshot_from_dict(snap_data))
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
        elif current_snap_id:
            # The dataset names a current snapshot whose document we could not
            # resolve. The metastore-side analogue of a manifest 404: the
            # dataset loads as empty rather than failing, so every reader sees a
            # table with no data and no error, and garbage collection sees
            # nothing to protect.
            #
            # Reported, not raised: making this fatal would change what
            # `load_dataset` returns to callers who currently get an empty
            # dataset, which needs its own change.
            _alert(
                SnapshotMissingError(
                    f"Dataset {identifier} names current-snapshot-id {current_snap_id} "
                    "but that snapshot document could not be resolved; the dataset "
                    "will load as empty."
                ),
                fingerprint=("snapshot-missing", identifier),
                context={"dataset": identifier, "current_snapshot_id": current_snap_id},
            )
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

    def drop_dataset(self, identifier: str, author: str = None) -> None:
        """Drop a dataset, leaving a tombstone so its files can be reclaimed.

        Dropping removes the dataset from the catalog immediately, which also
        removes it from `list_datasets()` - and the expiration job only visits
        datasets it can still list. Without a record of the location, the files
        under it would be unreachable by any later sweep. The tombstone is that
        record; see `list_dropped_datasets()`.

        Raises `DatasetLocked` if the dataset's `locked-by` field is set -
        the two-person deniability lock takes precedence over the drop.

        The workspace's `delete_protection` does NOT apply here: it protects the
        workspace from being deleted, not the assets inside it. Per-asset
        protection is `locked-by`.
        """
        collection, dataset_name = identifier.split(".")
        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        doc = doc_ref.get()
        if not doc.exists:
            # Nothing to drop, so nothing to reclaim and nothing to announce.
            return

        data = doc.to_dict() or {}
        if data.get("locked-by") is not None:
            raise DatasetLocked(f"Dataset is locked: {identifier}")

        location = data.get("location")

        # Tombstone FIRST: a failure between here and the final delete leaves a
        # reclaimable record, whereas the reverse order would leak the location.
        self._write_tombstone(
            collection=collection,
            dataset_name=dataset_name,
            location=location,
            author=author,
        )

        self._delete_subcollection(self._snapshots_collection(collection, dataset_name))
        self._delete_subcollection(doc_ref.collection("schemas"))
        self._delete_subcollection(doc_ref.collection(MAINTENANCE_SUBCOLLECTION))
        doc_ref.delete()

        send_webhook(
            action="delete",
            workspace=self.workspace,
            collection=collection,
            resource_type="dataset",
            resource_name=dataset_name,
            payload=dataset_deleted_payload(location=location, dropped_by=author),
        )

        emit_audit(
            "drop_dataset",
            resource_type="dataset",
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            location=location,
        )

    def _blob_name(self, path: str) -> str:
        """Strip the `gs://<bucket>/` prefix off a path, leaving the blob name."""
        prefix = f"gs://{self.gcs_bucket}/"
        if path.startswith(prefix):
            return path[len(prefix) :]
        return path

    def _copy_object(self, source_path: str, target_path: str) -> None:
        """Copy one object, preferring a server-side copy.

        `copy_blob` is server-side: the bytes never travel through this process,
        which is what makes moving a large dataset merely slow rather than
        impossible. The FileIO read/write path is the fallback for a catalog
        with no GCS bucket configured (local/test FileIO).
        """
        if self._storage_client is not None and self.gcs_bucket:
            bucket = self._storage_client.bucket(self.gcs_bucket)
            bucket.copy_blob(
                bucket.blob(self._blob_name(source_path)),
                bucket,
                self._blob_name(target_path),
            )
            return

        with self.io.new_input(source_path).open() as handle:
            data = handle.read()
        out = self.io.new_output(target_path).create()
        out.write(data)
        out.close()

    def _read_bytes(self, path: str) -> bytes:
        with self.io.new_input(path).open() as handle:
            return handle.read()

    def _write_bytes(self, path: str, data: bytes) -> None:
        out = self.io.new_output(path).create()
        out.write(data)
        out.close()

    def rename_dataset(
        self, identifier: str, new_identifier: str, author: Optional[str] = None
    ) -> None:
        """Rename a dataset, moving its files, manifests and catalog entry.

        A dataset's `location` is assigned once at creation and never re-derived
        from its name, so a catalog-only rename would leave the files under the
        old prefix - and creating a new dataset under the vacated name would
        then derive that same prefix, putting two datasets on one location where
        dropping either reclaims the other's files. This moves everything so
        name and storage stay in step and no prefix is ever shared.

        All snapshots move, not just the current one: every historical manifest
        is rewritten at the new location with remapped file paths, so
        time-travel keeps working across a rename.

        Cost: this copies every data file the dataset references. It is
        server-side (see `_copy_object`) but still O(all bytes) and O(all
        objects) - a rename of a large dataset is a long-running job, not the
        instant metadata edit the SQL makes it look like.

        Ordering, since Firestore and GCS share no transaction:
        copy files -> rewrite manifests -> write new catalog entry -> delete old
        entry -> tombstone the old location. A failure before the new entry is
        written leaves the dataset untouched and some orphan copies at the new
        location (a re-run overwrites them). A failure after it leaves the old
        entry present alongside the new one, both readable, until re-run. No
        ordering here loses data; the old files are handed to the existing 24h
        reclamation sweep rather than deleted inline.

        Files the dataset references from *outside* its own location are left
        exactly where they are and referenced unchanged - they were never this
        dataset's to move.

        Args:
            identifier: Current identifier, 'collection.dataset'
            new_identifier: New identifier, 'collection.dataset'. May name a
                different collection; the workspace is always this catalog's.
            author: The identity making the change - None when unauthenticated,
                never substituted (see audit.emit_audit).

        Raises:
            DatasetNotFound: If the source does not exist.
            DatasetAlreadyExists: If the target already exists.
            DatasetLocked: If the source's `locked-by` field is set.
            ValueError: If source and target are the same.
        """
        if identifier == new_identifier:
            raise ValueError(f"rename source and target are the same: {identifier}")

        collection, dataset_name = identifier.split(".")
        new_collection, new_dataset_name = new_identifier.split(".")

        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        doc = doc_ref.get()
        if not doc.exists:
            raise DatasetNotFound(f"Dataset not found: {identifier}")

        data = doc.to_dict() or {}
        # Same precedence as drop_dataset - the two-person deniability lock
        # outranks any operation that would move the dataset out from under it.
        if data.get("locked-by") is not None:
            raise DatasetLocked(f"Dataset is locked: {identifier}")

        new_doc_ref = self._dataset_doc_ref(new_collection, new_dataset_name)
        if new_doc_ref.get().exists:
            raise DatasetAlreadyExists(f"Dataset already exists: {new_identifier}")

        old_location = data.get("location")
        new_location = (
            f"gs://{self.gcs_bucket}/{self.workspace}/{new_collection}/{new_dataset_name}"
        )

        def _remap(path: str) -> str:
            """Move a path from under old_location to under new_location.

            Anything not under old_location is returned unchanged - see the
            note about externally-referenced files above.
            """
            if old_location and path.startswith(old_location + "/"):
                return new_location + path[len(old_location) :]
            return path

        from .catalog.manifest import read_manifest_rows

        snapshot_docs = list(self._snapshots_collection(collection, dataset_name).stream())

        # 1. Copy data files, and 2. rewrite manifests. One pass per snapshot;
        # a file referenced by several snapshots is copied once (copied_paths).
        copied_paths = set()
        rewritten_manifests = {}
        for snapshot_doc in snapshot_docs:
            snapshot_data = snapshot_doc.to_dict() or {}
            manifest_path = snapshot_data.get("manifest")
            if not manifest_path:
                continue

            manifest_bytes = self._read_bytes(manifest_path)
            rows = read_manifest_rows(manifest_bytes)

            for row in rows:
                source_path = row.get("file_path")
                if not source_path:
                    continue
                target_path = _remap(source_path)
                if target_path == source_path or target_path in copied_paths:
                    continue
                self._copy_object(source_path, target_path)
                copied_paths.add(target_path)
                row["file_path"] = target_path

            snapshot_id = snapshot_data.get("snapshot-id")
            new_manifest_path = self.write_parquet_manifest(
                snapshot_id, rows, new_location
            )
            rewritten_manifests[snapshot_doc.id] = (snapshot_data, new_manifest_path)

        # 3. Write the new catalog entry: dataset doc, schemas, snapshots. This
        # is the point the rename becomes visible under the new name.
        new_doc_ref.set(
            {
                **data,
                "name": new_dataset_name,
                "collection": new_collection,
                "location": new_location,
                "timestamp-ms": int(time.time() * 1000),
            }
        )

        for schema_doc in doc_ref.collection("schemas").stream():
            new_doc_ref.collection("schemas").document(schema_doc.id).set(
                schema_doc.to_dict() or {}
            )

        new_snapshots = self._snapshots_collection(new_collection, new_dataset_name)
        for snapshot_doc in snapshot_docs:
            snapshot_data, new_manifest_path = rewritten_manifests.get(
                snapshot_doc.id, (snapshot_doc.to_dict() or {}, None)
            )
            payload = dict(snapshot_data)
            if new_manifest_path is not None:
                payload["manifest"] = new_manifest_path
            new_snapshots.document(snapshot_doc.id).set(payload)

        # 4. Remove the old catalog entry. The orphan quarantine is dropped
        # rather than carried over: its entries name paths under the old
        # location, which no longer exist, so the renamed dataset starts with a
        # clean record and its files each need two fresh sightings.
        self._delete_subcollection(self._snapshots_collection(collection, dataset_name))
        self._delete_subcollection(doc_ref.collection("schemas"))
        self._delete_subcollection(doc_ref.collection(MAINTENANCE_SUBCOLLECTION))
        doc_ref.delete()

        # 5. Hand the vacated prefix to the existing reclamation sweep rather
        # than deleting inline - same mechanism drop_dataset uses, so the files
        # are removed on the established 24h grace period instead of instantly.
        self._write_tombstone(
            collection=collection,
            dataset_name=dataset_name,
            location=old_location,
            author=author,
        )

        send_webhook(
            action="rename",
            workspace=self.workspace,
            collection=new_collection,
            resource_type="dataset",
            resource_name=new_dataset_name,
            payload=dataset_renamed_payload(
                old_identifier=identifier,
                new_identifier=new_identifier,
                old_location=old_location,
                new_location=new_location,
                renamed_by=author,
            ),
        )

        emit_audit(
            "rename_dataset",
            resource_type="dataset",
            workspace=self.workspace,
            collection=new_collection,
            resource=new_dataset_name,
            author=author,
            old_identifier=identifier,
            new_identifier=new_identifier,
            old_location=old_location,
            new_location=new_location,
            files_copied=len(copied_paths),
            snapshots_moved=len(snapshot_docs),
        )

    def _write_tombstone(
        self, collection: str, dataset_name: str, location: Optional[str], author: Optional[str]
    ) -> None:
        """Record a dropped dataset's storage location for later reclamation."""
        self._tombstones_collection().document(f"{collection}.{dataset_name}").set(
            {
                "name": dataset_name,
                "collection": collection,
                "workspace": self.workspace,
                "location": location,
                "dropped-at-ms": int(time.time() * 1000),
                "dropped-by": author,
            }
        )

    def list_dropped_datasets(self) -> List[dict]:
        """Tombstones for datasets dropped from this workspace.

        Each entry carries `location` (the storage prefix whose files are now
        unreferenced), `dropped-at-ms` and `dropped-by`. Consumed by the
        expiration job, which reclaims the files and then calls
        `delete_tombstone()`.
        """
        return [
            {**(doc.to_dict() or {}), "id": doc.id}
            for doc in self._tombstones_collection().stream()
        ]

    def delete_tombstone(self, tombstone_id: str) -> None:
        """Remove a tombstone once its storage location has been reclaimed."""
        self._tombstones_collection().document(tombstone_id).delete()

    # --- Workspace lifecycle (soft-delete / lock) ----------------------
    #
    # These methods execute the state change and record who asked - they do
    # NOT enforce identity rules (e.g. "a different owner must unlock",
    # "billing_admin required to delete"). That authorization decision lives
    # in the calling service (billing.opteryx), which originates both the
    # decision and the call with nothing in between.

    def soft_delete_workspace(self, author: str) -> None:
        """Mark this workspace deleted, and tombstone it for the 24h sweep.

        Sets `deleted-at-ms`/`deleted-by` on the `$properties` doc, which is
        what `__init__`'s construction-time gate checks - once this is set, no
        new `OpteryxCatalog` handle for this workspace can be obtained without
        `include_deleted=True`. Also writes an entry to the root-level
        `$dropped-workspaces` collection so the sweep can find this workspace
        without enumerating every workspace name blindly.

        Raises `WorkspaceDeleteProtected` if the workspace is delete-protected.
        This is the only operation that flag guards - it protects the workspace
        from deletion, not the assets inside it.
        """
        if author is None:
            raise ValueError("author must be provided when soft-deleting a workspace")

        self._assert_not_delete_protected()

        now_ms = int(time.time() * 1000)
        self._catalog_ref.document("$properties").update(
            {"deleted-at-ms": now_ms, "deleted-by": author}
        )

        self._dropped_workspaces_collection().document(self.workspace).set(
            {
                "workspace": self.workspace,
                "dropped-at-ms": now_ms,
                "dropped-by": author,
            }
        )

        send_webhook(
            action="delete",
            workspace=self.workspace,
            collection=None,
            resource_type="workspace",
            resource_name=self.workspace,
            payload=workspace_deleted_payload(dropped_by=author),
        )

        emit_audit(
            "soft_delete_workspace",
            resource_type="workspace",
            workspace=self.workspace,
            resource=self.workspace,
            author=author,
        )

    def restore_workspace(self, author: str) -> None:
        """Clear a workspace's soft-delete state.

        Clears `deleted-at-ms`/`deleted-by` on `$properties`, and removes the
        `$dropped-workspaces` tombstone written by `soft_delete_workspace` -
        without that, the workspace would still be a candidate for the 24h
        sweep despite having been restored.
        """
        if author is None:
            raise ValueError("author must be provided when restoring a workspace")

        self._catalog_ref.document("$properties").update(
            {"deleted-at-ms": None, "deleted-by": None}
        )

        self.delete_workspace_tombstone(self.workspace)

        send_webhook(
            action="restore",
            workspace=self.workspace,
            collection=None,
            resource_type="workspace",
            resource_name=self.workspace,
            payload=workspace_restored_payload(restored_by=author),
        )

        emit_audit(
            "restore_workspace",
            resource_type="workspace",
            workspace=self.workspace,
            resource=self.workspace,
            author=author,
        )

    def lock_workspace(self, author: str) -> None:
        """Set the two-person-deniability lock on this workspace."""
        if author is None:
            raise ValueError("author must be provided when locking a workspace")

        now_ms = int(time.time() * 1000)
        self._catalog_ref.document("$properties").update(
            {"locked-by": author, "locked-at-ms": now_ms}
        )

        send_webhook(
            action="lock",
            workspace=self.workspace,
            collection=None,
            resource_type="workspace",
            resource_name=self.workspace,
            payload=workspace_locked_payload(locked_by=author),
        )

        emit_audit(
            "lock_workspace",
            resource_type="workspace",
            workspace=self.workspace,
            resource=self.workspace,
            author=author,
        )

    def unlock_workspace(self, author: str) -> None:
        """Clear the lock set by `lock_workspace`."""
        if author is None:
            raise ValueError("author must be provided when unlocking a workspace")

        self._catalog_ref.document("$properties").update(
            {"locked-by": None, "locked-at-ms": None}
        )

        send_webhook(
            action="unlock",
            workspace=self.workspace,
            collection=None,
            resource_type="workspace",
            resource_name=self.workspace,
            payload=workspace_unlocked_payload(unlocked_by=author),
        )

        emit_audit(
            "unlock_workspace",
            resource_type="workspace",
            workspace=self.workspace,
            resource=self.workspace,
            author=author,
        )

    def _assert_not_delete_protected(self) -> None:
        """Refuse deletion of this workspace while it is delete-protected.

        Scope is the workspace itself and nothing else: `delete_protection`
        does not guard the datasets, collections or views inside it. Per-asset
        protection is the `locked-by` two-person lock, which is a separate
        mechanism with separate semantics.

        Read fresh from Firestore every time rather than cached on the handle: a
        cached flag would let a long-lived catalog object delete the workspace
        after protection was switched on elsewhere, which is exactly the case
        the setting exists to prevent.

        A Firestore read error propagates rather than being swallowed into
        "not protected".
        """
        if self.get_workspace_properties().get("delete_protection"):
            raise WorkspaceDeleteProtected(
                f"Cannot delete workspace '{self.workspace}': it is delete-protected. "
                f"Clear it with ALTER WORKSPACE {self.workspace} "
                "SET delete_protection TO OFF."
            )

    def get_workspace_properties(self) -> dict:
        """Return the workspace's `$properties` document as a plain dict.

        This is the whole document, lifecycle fields included, so callers can
        read `owner`, `billing-account-id`, `deleted-at-ms`, `locked-by` and any
        settable property in one go. Returns `{}` when the document does not
        exist - a workspace whose `$properties` never got written (the
        constructor's write is best-effort) reads as "no properties", not as an
        error, because every field it holds is optional.

        Unlike the constructor's read this does NOT gate on `deleted-at-ms`;
        reading a deleted workspace's properties is exactly how a caller finds
        out that it is deleted.
        """
        doc = self._catalog_ref.document("$properties").get()
        if not doc.exists:
            return {}
        return doc.to_dict() or {}

    # Fields on `$properties` that only their own dedicated methods may write.
    # Each one gates real control flow - `deleted-at-ms` makes the constructor
    # raise WorkspaceDeleted, `locked-by` makes drop_dataset/drop_collection
    # raise Locked - so letting a generic property setter touch them would let
    # a caller resurrect a deleted workspace or clear a lock while bypassing
    # drop_workspace/restore_workspace/lock_workspace/unlock_workspace and the
    # audit records and webhooks those emit.
    _RESERVED_WORKSPACE_PROPERTIES = frozenset(
        {
            "timestamp-ms",
            "deleted-at-ms",
            "deleted-by",
            "locked-by",
            "locked-at-ms",
        }
    )

    def set_workspace_properties(
        self, properties: dict, author: Optional[str] = None
    ) -> None:
        """Merge `properties` into the workspace's `$properties` document.

        A merge, not a replace: keys absent from `properties` are left as they
        are, so a caller setting one property cannot blank the rest by omission.
        To remove a property, set it to None explicitly.

        Args:
            properties: Property names to values. Must be non-empty.
            author: The identity making the change - None when unauthenticated,
                never substituted (see audit.emit_audit).

        Raises:
            ValueError: If `properties` is empty or names a reserved lifecycle
                field (see `_RESERVED_WORKSPACE_PROPERTIES`).
        """
        if not properties:
            raise ValueError("properties must be a non-empty mapping")

        reserved = sorted(set(properties) & self._RESERVED_WORKSPACE_PROPERTIES)
        if reserved:
            raise ValueError(
                f"Cannot set reserved workspace lifecycle field(s) {reserved} through "
                "set_workspace_properties; use the dedicated drop/restore/lock/unlock methods."
            )

        updates = dict(properties)
        updates["timestamp-ms"] = int(time.time() * 1000)

        # set(merge=True) rather than update() so a workspace whose $properties
        # doc was never written (the constructor's write is best-effort and
        # swallows Firestore errors) gets one here instead of raising NotFound.
        self._catalog_ref.document("$properties").set(updates, merge=True)

        emit_audit(
            "set_workspace_properties",
            resource_type="workspace",
            workspace=self.workspace,
            resource=self.workspace,
            author=author,
            properties=sorted(properties),
        )

    def list_dropped_workspaces(self) -> List[dict]:
        """Tombstones for workspaces soft-deleted anywhere in this Firestore
        database - not scoped to `self.workspace`. Root-level, mirroring
        `list_dropped_datasets()` one level up. Consumed by the 24h sweep,
        which walks each overdue workspace and then calls
        `delete_workspace_tombstone()`.
        """
        return [
            {**(doc.to_dict() or {}), "id": doc.id}
            for doc in self._dropped_workspaces_collection().stream()
        ]

    def delete_workspace_tombstone(self, workspace: str) -> None:
        """Remove a workspace's `$dropped-workspaces` tombstone.

        Takes an explicit `workspace` name (rather than always using
        `self.workspace`) since the sweep operates across workspaces from a
        single catalog handle.
        """
        self._dropped_workspaces_collection().document(workspace).delete()

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
                "locked-by": None,
                "locked-at-ms": None,
            }
        )

        emit_audit(
            "create_collection",
            resource_type="collection",
            workspace=self.workspace,
            resource=collection,
            author=author,
        )

    def create_collection_if_not_exists(
        self, collection: str, properties: dict | None = None, author: Optional[str] = None
    ) -> None:
        """Convenience wrapper that creates the collection only if missing."""
        self.create_collection(collection, properties=properties, exists_ok=True, author=author)

    def collection_exists(self, collection: str) -> bool:
        """Return True if the collection exists."""
        try:
            return self._collection_ref(collection).get().exists
        except Exception:
            # On any error, be conservative and return False
            return False

    def drop_collection(self, collection: str, author: Optional[str] = None) -> None:
        """Drop a collection.

        A collection owns no storage of its own - only its datasets and views
        do - so unlike `drop_dataset` this needs no tombstone/sweep; deleting
        the catalog document is the whole operation. Raises CollectionNotEmpty
        if any datasets or views remain, since deleting a non-empty collection
        would otherwise silently orphan them (still tombstoned/reclaimed
        individually, but no longer reachable through `list_collections()`).
        Raises `CollectionLocked` if the collection's `locked-by` field is
        set - the two-person deniability lock takes precedence over the drop.
        The workspace's `delete_protection` does not apply; it protects the
        workspace itself, not the assets inside it.
        """
        doc_ref = self._collection_ref(collection)
        doc = doc_ref.get()
        if not doc.exists:
            raise CollectionNotFound(f"Collection not found: {collection}")

        data = doc.to_dict() or {}
        if data.get("locked-by") is not None:
            raise CollectionLocked(f"Collection is locked: {collection}")

        if any(True for _ in self._datasets_collection(collection).stream()) or any(
            True for _ in self._views_collection(collection).stream()
        ):
            raise CollectionNotEmpty(f"Collection is not empty: {collection}")

        doc_ref.delete()

        emit_audit(
            "drop_collection",
            resource_type="collection",
            workspace=self.workspace,
            resource=collection,
            author=author,
        )

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

        emit_audit(
            "update_view" if update_if_exists else "create_view",
            resource_type="view",
            workspace=self.workspace,
            collection=collection,
            resource=view_name,
            author=author,
            statement_id=statement_id,
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

    def drop_view(self, identifier: str | tuple, author: str = None) -> None:
        """Drop a view.

        No tombstone: a view owns no storage, so dropping it leaves nothing to
        reclaim - unlike `drop_dataset`.

        The workspace's `delete_protection` does not apply; it protects the
        workspace itself, not the assets inside it.
        """
        if isinstance(identifier, tuple) or isinstance(identifier, list):
            collection, view_name = identifier[0], identifier[1]
        else:
            collection, view_name = identifier.split(".")

        doc_ref = self._view_doc_ref(collection, view_name)
        if not doc_ref.get().exists:
            return

        self._delete_subcollection(doc_ref.collection("statement"))
        doc_ref.delete()

        send_webhook(
            action="delete",
            workspace=self.workspace,
            collection=collection,
            resource_type="view",
            resource_name=view_name,
            payload=view_deleted_payload(dropped_by=author),
        )

        emit_audit(
            "drop_view",
            resource_type="view",
            workspace=self.workspace,
            collection=collection,
            resource=view_name,
            author=author,
        )

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

    def update_dataset_sort_order(
        self,
        identifier: str | tuple,
        columns: List[str],
        author: Optional[str] = None,
    ) -> None:
        """Set the clustering columns for a dataset (``ALTER TABLE ... CLUSTER BY``).

        Persists a single Iceberg-style sort-order entry naming ``columns`` in
        the given order, ascending. Replaces any previously configured sort
        order outright - CLUSTER BY re-declares the physical layout, it does
        not append to it. See ``catalog.compaction.normalize_sort_order`` for
        how this shape is consumed (only the first field is currently used as
        the primary sort key for compaction).

        Args:
            identifier: Dataset identifier in format 'collection.dataset_name'
            columns: Column names to cluster by, in priority order. Must be
                non-empty and must all exist in the dataset's current schema.
            author: The identity making the change - None when unauthenticated,
                never substituted (see audit.emit_audit).

        Raises:
            DatasetNotFound: If the dataset does not exist.
            ValueError: If ``columns`` is empty or names a column that is not
                in the dataset's current schema.
        """
        if not columns:
            raise ValueError("columns must be a non-empty list of column names")

        if isinstance(identifier, tuple) or isinstance(identifier, list):
            collection, dataset_name = identifier[0], identifier[1]
        else:
            collection, dataset_name = identifier.split(".")

        doc_ref = self._dataset_doc_ref(collection, dataset_name)
        doc = doc_ref.get()
        if not doc.exists:
            raise DatasetNotFound(f"Dataset not found: {collection}.{dataset_name}")

        data = doc.to_dict() or {}
        current_schema_id = data.get("current-schema-id")
        known_columns = set()
        if current_schema_id:
            schema_doc = doc_ref.collection("schemas").document(str(current_schema_id)).get()
            if schema_doc.exists:
                known_columns = {
                    c.get("name") for c in (schema_doc.to_dict() or {}).get("columns", [])
                }
        unknown = [c for c in columns if c not in known_columns]
        if unknown:
            raise ValueError(
                f"Unknown column(s) for CLUSTER BY on {collection}.{dataset_name}: {unknown}"
            )

        doc_ref.update(
            {
                "sort-orders": [
                    {
                        "order-id": 1,
                        "fields": [{"name": c, "direction": "asc"} for c in columns],
                    }
                ],
            }
        )

        emit_audit(
            "update_sort_order",
            resource_type="dataset",
            workspace=self.workspace,
            collection=collection,
            resource=dataset_name,
            author=author,
            columns=columns,
        )

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
                # Per-column byte-class histogram (8 fixed classes) and total
                # byte count, VARCHAR/NVARCHAR/VARBINARY columns only (empty
                # list / 0 elsewhere) — backs the LIKE '%needle%' selectivity
                # char-class estimator. See catalog/manifest.py's
                # _compute_column_stats / Vector.char_class_stats().
                "char_class_counts": "ARRAY",
                "char_total_bytes": "ARRAY",
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
                e.setdefault("min_lengths", [])
                e.setdefault("max_lengths", [])
                e.setdefault("field_ids", [])
                e.setdefault("char_class_counts", [])
                e.setdefault("char_total_bytes", [])

                # min/max values are stored as compressed int64 values
                mv = e.get("min_values") or []
                xv = e.get("max_values") or []

                # Ensure int64 values are properly typed for min/max
                e["min_values"] = [int(v) if v is not None else None for v in mv]
                e["max_values"] = [int(v) if v is not None else None for v in xv]

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
                # close() is where the upload actually happens - the GCS output
                # stream buffers into memory and flushes on close - so a failure
                # here means the manifest object does not exist. Swallowing it
                # returned this path to the caller, which committed a snapshot
                # pointing at a manifest that was never written; the next commit
                # then couldn't read its parent. Let it raise.
                out.close()

            return parquet_path
        except Exception as e:
            # Log and return None on failure
            # print(f"Failed to write Parquet manifest: {e}")
            raise e

    def save_snapshot(self, identifier: str, snapshot: Snapshot) -> None:
        """Persist a single snapshot document for a dataset."""
        namespace, dataset_name = identifier.split(".")
        snaps = self._snapshots_collection(namespace, dataset_name)
        snaps.document(str(snapshot.snapshot_id)).set(_snapshot_to_document(snapshot))

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
        # Serialized via _snapshot_to_document — the SAME writer save_snapshot
        # uses. These two both `.set()` the same document, so a field missing
        # from either one is destroyed by the other (see that function).
        for snap in metadata.snapshots:
            snaps_coll.document(str(snap.snapshot_id)).set(_snapshot_to_document(snap))

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
