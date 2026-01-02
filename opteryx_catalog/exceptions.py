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
