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

    # Whether a dataset held by this metastore can be FORKED - that is, whether
    # `CREATE TABLE ... CLONE` may create a new dataset whose first manifest
    # lists this one's files.
    #
    # True ONLY for the native Opteryx metastore. A fork borrows a snapshot's
    # manifest entries verbatim and pins that snapshot against expiration on
    # the upstream; both are Opteryx snapshot-store mechanics, and neither has
    # an equivalent in an external store. An Iceberg table's files are governed
    # by the Iceberg catalog's own expiry, which knows nothing of our forks; a
    # Postgres relation has no manifest to borrow at all. A clone of either
    # would be a fork resting on files nothing has promised to keep.
    #
    # False by default, so an implementation that has not thought about it -
    # or a duck-typed metastore that does not derive from this class at all,
    # which reads as False at the asking site - is refused rather than
    # silently forked. Opting in is a positive act by a store that really does
    # provide the snapshot mechanics.
    supports_forking: bool = False

    # Whether the workspace catalog's RECORD of a relation can be read as the
    # relation itself - i.e. whether the dataset object relation resolution
    # prefetched from the workspace's opteryx catalog entry (opteryx-core's
    # `resolve_relation`, handed on as `prefetched_table`) is something this
    # metastore's reads can be served from.
    #
    # True ONLY for the native Opteryx metastore, where that record IS the
    # dataset: its snapshot, schema and manifest. For an externally-bound
    # workspace the catalog entry is a LISTING - the relation's name and
    # columns, so it can be listed and described - with no snapshot and no
    # file list; the data lives in the external store. Reading that listing as
    # the relation binds an empty relation, a wrong answer reported as success
    # (2026-10-12: every Iceberg SELECT in production).
    #
    # False by default, the safe direction: the reader loads the relation from
    # this metastore, at the cost of one lookup, rather than trusting a record
    # it cannot vouch for.
    serves_catalog_records: bool = False

    def load_dataset(self, identifier: str) -> Dataset:
        raise NotImplementedError()

    def create_dataset(
        self, identifier: str, schema: Any, properties: dict | None = None
    ) -> Dataset:
        raise NotImplementedError()

    def create_dataset_with_files(
        self, identifier: str, schema: Any, *, author: str, **kwargs
    ) -> Dataset:
        """Create a dataset and its first snapshot as ONE atomic write: either it
        exists holding the files, or nothing was published (CREATE TABLE AS)."""
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

    # How `manifest_bytes()` and `scan()` encode the per-file
    # `min_values`/`max_values`.
    #
    #   True  - `Vector.ordinalize()` int64 ordinal keys (what this package's
    #           own stats builder writes, and what opteryx-iceberg converts
    #           Iceberg's lower/upper bounds to; see catalog/manifest.py's
    #           compressible-categories note).
    #   False - real decoded values. The manifest parquet holds a real value
    #           only for an integer, temporal or float column.
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

    def manifest_bytes(self, snapshot_id: int | None = None) -> bytes | None:
        """The bytes of a snapshot's manifest as the opteryx manifest parquet
        (`catalog.manifest.encode_parquet_manifest`), which opteryx-core decodes
        natively for planning - every backend serves this format, whatever its
        own manifests are. None when the snapshot carries no manifest - an empty
        dataset. A manifest that exists but cannot be read raises.

        opteryx-core caches the decoded manifest by the snapshot's
        `manifest_list`, so it must name exactly one payload for the life of the
        snapshot."""
        raise NotImplementedError()

    def delete_vectors_for(self, entries: Iterable[Any]) -> dict[str, list[int]]:
        """``{data_file_path: sorted deleted row ordinals}`` for manifest rows the
        caller already holds (each with file_path, delete_file_path and
        deleted_record_count), without re-reading the manifest. Raises when a
        referenced sidecar is unreadable or holds no vector for a file."""
        raise NotImplementedError()

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
