from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import List
from typing import Optional


@dataclass
class Snapshot:
    snapshot_id: int
    timestamp_ms: int
    author: Optional[str] = None
    # Indicates whether this snapshot was created by a user (True) or internally (False)
    user_created: Optional[bool] = None
    # Monotonic sequence number for writes
    sequence_number: Optional[int] = None
    manifest_list: Optional[str] = None
    # Operation metadata
    operation_type: Optional[str] = None  # e.g., 'append', 'overwrite', 'compact'
    parent_snapshot_id: Optional[int] = None
    schema_id: Optional[str] = None
    # Commit message for the snapshot
    commit_message: Optional[str] = None
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


@dataclass
class DatasetMetadata:
    dataset_identifier: str
    format_version: int = 2
    location: str = ""
    schema: Any = None
    properties: dict = field(default_factory=dict)
    # Dataset-level created/updated metadata
    timestamp_ms: Optional[int] = None
    author: Optional[str] = None
    description: Optional[str] = None
    describer: Optional[str] = None
    sort_orders: List[int] = field(default_factory=list)
    # Maintenance policy: retention settings grouped under a single block
    maintenance_policy: dict = field(
        default_factory=lambda: {
            "retained-snapshot-count": None,
            "retained-snapshot-age-days": None,
            "compaction-policy": "performance",
        }
    )
    # Compaction policy lives under maintenance_policy as 'compaction-policy'
    snapshots: List[Snapshot] = field(default_factory=list)
    current_snapshot_id: Optional[int] = None
    # Schema management: schemas are stored in a subcollection in Firestore.
    # `schemas` contains dicts with keys: schema_id, columns (list of {id,name,type}).
    # Each schema dict may also include `timestamp-ms` and `author`.
    schemas: List[dict] = field(default_factory=list)
    current_schema_id: Optional[str] = None
    # Annotations: list of annotation objects attached to this dataset
    # Each annotation is a dict with keys like 'key' and 'value'.
    annotations: List[dict] = field(default_factory=list)

    def current_snapshot(self) -> Optional[Snapshot]:
        if self.current_snapshot_id is None:
            return self.snapshots[-1] if self.snapshots else None
        for s in self.snapshots:
            if s.snapshot_id == self.current_snapshot_id:
                return s
        return None


# Dataset terminology: TableMetadata renamed to DatasetMetadata
