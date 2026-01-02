"""Opteryx lightweight catalog library.

This package provides base classes and simple datatypes for a custom
catalog implementation that stores dataset metadata in Firestore and
consolidated Parquet manifests in GCS.

Start here for building a Firestore+GCS backed catalog that writes
Parquet manifests and stores metadata/snapshots in Firestore.
"""

from .catalog.dataset import SimpleDataset
from .catalog.manifest import DataFile
from .catalog.manifest import ManifestEntry
from .catalog.metadata import DatasetMetadata
from .catalog.metadata import Snapshot
from .catalog.metastore import Dataset
from .catalog.metastore import Metastore
from .catalog.metastore import View
from .opteryx_catalog import OpteryxCatalog

__all__ = [
    "OpteryxCatalog",
    "Metastore",
    "Dataset",
    "View",
    "SimpleDataset",
    "DatasetMetadata",
    "Snapshot",
    "DataFile",
    "ManifestEntry",
]
