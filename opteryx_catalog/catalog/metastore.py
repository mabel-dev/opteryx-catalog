from __future__ import annotations

from collections.abc import Iterable
from typing import Any


class Metastore:
    """Abstract catalog interface.

    Implementations should provide methods to create, load and manage
    datasets and views. Terminology in this project follows the mapping:
    `catalog -> workspace -> collection -> dataset|view`.
    Signatures are intentionally simple and similar to other catalog
    implementations to ease future compatibility.
    """

    def load_dataset(self, identifier: str) -> Dataset:
        raise NotImplementedError()

    def create_dataset(
        self, identifier: str, schema: Any, properties: dict | None = None
    ) -> Dataset:
        raise NotImplementedError()

    def drop_dataset(self, identifier: str, author: str) -> None:
        """Drop a dataset. `author` is required - an unattributed drop is not
        something an implementation should silently accept."""
        raise NotImplementedError()

    def drop_view(self, identifier: str, author: str) -> None:
        """Drop a view. `author` is required - see `drop_dataset`."""
        raise NotImplementedError()

    def list_datasets(self, namespace: str) -> Iterable[str]:
        raise NotImplementedError()


class Dataset:
    """Abstract dataset interface.

    Minimal methods needed by the Opteryx engine and tests: access metadata,
    list snapshots, append data, and produce a data scan object.
    """

    @property
    def metadata(self) -> Any:
        raise NotImplementedError()

    def snapshots(self) -> Iterable[Any]:
        raise NotImplementedError()

    def snapshot(self, snapshot_id: int | None = None) -> Any | None:
        """Return a specific snapshot by id or the current snapshot when
        called with `snapshot_id=None`.
        """
        raise NotImplementedError()

    def append(self, table):
        """Append data (implementations can accept a draken Morsel or similar)."""
        raise NotImplementedError()

    def scan(
        self, row_filter=None, snapshot_id: int | None = None, row_limit: int | None = None
    ) -> Any:
        raise NotImplementedError()


class View:
    """Abstract view metadata representation."""

    @property
    def definition(self) -> str:
        raise NotImplementedError()
