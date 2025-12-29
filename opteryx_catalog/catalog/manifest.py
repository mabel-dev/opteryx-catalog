from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Dict


@dataclass
class DataFile:
    file_path: str
    file_format: str = "PARQUET"
    record_count: int = 0
    file_size_in_bytes: int = 0
    partition: Dict[str, object] = field(default_factory=dict)
    lower_bounds: Dict[int, bytes] | None = None
    upper_bounds: Dict[int, bytes] | None = None


@dataclass
class ManifestEntry:
    snapshot_id: int
    data_file: DataFile
    status: str = "added"  # 'added' | 'deleted'
