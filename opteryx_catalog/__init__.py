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
from .resource_types import ResourceType

__all__ = [
    "DataFile",
    "Dataset",
    "DatasetMetadata",
    "ManifestEntry",
    "Metastore",
    "OpteryxCatalog",
    "ResourceType",
    "SimpleDataset",
    "Snapshot",
    "View",
]


def __getattr__(name):
    # OpteryxCatalog pulls in google-cloud-firestore/google-cloud-storage at
    # import time (opteryx_catalog.py:10-11). Deferred here so importing the
    # backend-agnostic Metastore/Dataset/View ABCs above - e.g. for a non-
    # Firestore backend like opteryx-iceberg - doesn't require those SDKs to
    # be installed. `from opteryx_catalog import OpteryxCatalog` and
    # `opteryx_catalog.OpteryxCatalog` both still work; this only defers
    # *when* the import happens.
    if name == "OpteryxCatalog":
        from .opteryx_catalog import OpteryxCatalog

        return OpteryxCatalog
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
