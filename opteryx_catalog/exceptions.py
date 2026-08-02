"""Catalog-specific exceptions for opteryx_catalog.

Exceptions mirror previous behavior (they subclass KeyError where callers
may expect KeyError) but provide explicit types for datasets, views and
namespaces.
"""


class CatalogError(Exception):
    """Base class for catalog errors."""


class DatasetError(KeyError, CatalogError):
    pass


class DatasetAlreadyExists(DatasetError):
    pass


class DatasetNotFound(DatasetError):
    pass


class ViewError(KeyError, CatalogError):
    pass


class ViewAlreadyExists(ViewError):
    pass


class ViewNotFound(ViewError):
    pass


class CollectionAlreadyExists(KeyError, CatalogError):
    pass


class CollectionNotFound(KeyError, CatalogError):
    pass


class CollectionNotEmpty(CatalogError):
    pass


class CollectionLocked(CatalogError):
    """Raised by `drop_collection` when the collection's `locked-by` field is set."""


class DatasetLocked(CatalogError):
    """Raised by `drop_dataset` when the dataset's `locked-by` field is set."""


class WorkspaceDeleted(CatalogError):
    """Raised by `OpteryxCatalog.__init__` when the workspace's `$properties`
    document has `deleted-at-ms` set and `include_deleted` was not passed."""


class ManifestRefreshError(CatalogError):
    """A statistics refresh could not recompute every file's statistics.

    Raised by `SimpleDataset.refresh_manifest` when one or more data files
    could not be re-read or re-analyzed. No snapshot is committed when this
    is raised: a manifest where some files carry fresh statistics and others
    silently kept stale ones is indistinguishable, downstream, from one that
    fully succeeded — so the refresh fails whole rather than committing a
    partial result. The message names every file that failed.
    """
