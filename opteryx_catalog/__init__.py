"""Opteryx lightweight Iceberg-inspired catalog library.

This package provides base classes and simple datatypes for a custom
catalog implementation that is compatible with Iceberg-style concepts.

Start here for building a Firestore+GCS backed catalog that writes
Parquet manifests and stores metadata/snapshots in Firestore.
"""

from .catalog.manifest import DataFile
from .catalog.manifest import ManifestEntry
from .catalog.metadata import Snapshot
from .catalog.metadata import TableMetadata
from .catalog.metastore import Metastore
from .catalog.metastore import Table
from .catalog.metastore import View
from .catalog.table import SimpleTable

__all__ = [
    "Metastore",
    "Table",
    "View",
    "SimpleTable",
    "TableMetadata",
    "Snapshot",
    "DataFile",
    "ManifestEntry",
]
