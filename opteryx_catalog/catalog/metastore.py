from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Iterable
from typing import Optional


class Metastore(ABC):
    """Abstract catalog interface (Iceberg-like).

    Implementations should provide methods to create, load and manage tables
    and views. Signatures are intentionally simple and similar to PyIceberg's
    catalog API to ease future compatibility.
    """

    @abstractmethod
    def load_table(self, identifier: str) -> "Table":
        raise NotImplementedError()

    @abstractmethod
    def create_table(self, identifier: str, schema: Any, properties: dict | None = None) -> "Table":
        raise NotImplementedError()

    @abstractmethod
    def drop_table(self, identifier: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def list_tables(self, namespace: str) -> Iterable[str]:
        raise NotImplementedError()


class Table(ABC):
    """Abstract table interface.

    Minimal methods needed by the Opteryx engine and tests: access metadata,
    list snapshots, append data, and produce a data scan object.
    """

    @property
    @abstractmethod
    def metadata(self) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def current_snapshot(self) -> Optional[Any]:
        raise NotImplementedError()

    @abstractmethod
    def snapshots(self) -> Iterable[Any]:
        raise NotImplementedError()

    @abstractmethod
    def append(self, table):
        """Append data (implementations can accept pyarrow.Table or similar)."""
        raise NotImplementedError()

    @abstractmethod
    def scan(self, **kwargs) -> Any:
        raise NotImplementedError()


class View(ABC):
    """Abstract view metadata representation."""

    @property
    @abstractmethod
    def definition(self) -> str:
        raise NotImplementedError()
