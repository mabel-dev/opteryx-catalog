from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from typing import Optional


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

    # How `scan()` encodes the per-file `min_values`/`max_values` it yields.
    #
    #   True  - `Vector.ordinalize()` int64 ordinal keys (what this package's
    #           own stats builder writes; see catalog/manifest.py's
    #           compressible-categories note).
    #   False - real decoded values: a `str` for a VARCHAR column, a `float`
    #           for a DOUBLE, and so on (what an external catalog's manifest
    #           carries -- e.g. opteryx-iceberg decodes Iceberg's lower/upper
    #           bounds with `pyiceberg.conversions.from_bytes`).
    #
    # It is a property of WHOEVER PRODUCED THE BOUNDS, not of the connector
    # reading them, which is why it is declared here rather than assumed by
    # the reader. opteryx-core hands it straight to `Manifest(
    # bounds_are_ordinal=...)`, which decides whether to push predicate
    # literals through `ColumnType.ordinalize` before comparing them against
    # these bounds. Getting it wrong is a SILENT WRONG ANSWER, not a missed
    # optimisation: ordinalize is identity for signed ints, so int columns
    # look fine either way, while a FLOAT's ordinal key is an order-preserving
    # bit transform (0.5 -> 4602678819172646912). Comparing a real 0.5 against
    # ordinal-space bounds prunes every file that actually holds the matching
    # rows, and a real `str` bound meeting an ordinalized int literal raises
    # `'<' not supported between instances of 'str' and 'int'`.
    #
    # None means the implementation has not declared it. Readers must treat
    # that as an error rather than guessing a default -- either guess is
    # silently wrong for half the implementations.
    bounds_are_ordinal: Optional[bool] = None

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
