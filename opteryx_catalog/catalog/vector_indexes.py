"""
Vector indexes: definitions and per-data-file sidecars (opteryx-core
docs/VECTOR_INDEX_DESIGN.md §5).

An index is a property of its dataset, not an independent catalog object: its definition
lives in the dataset's `indexes` subcollection (never on the dataset document, whose
whole-document `set()` would erase it), keyed by the normalized index name.

The index data is one pair of immutable skene files PER DATA FILE, written by the engine:

    <location>/index/<index_id>/<data-file-stem>-<nonce>.vectors.skene
        (embedding VECTOR_FP16, ordinal UINT32) — each row group one IVF cluster's rows;
        a cluster may span several row groups
    <location>/index/<index_id>/<data-file-stem>-<nonce>.centroids.skene
        (centroid VECTOR_FP16, rows UINT32, row_groups ARRAY<INT32>)

The manifest references them per data file through six parallel ARRAY columns, ordered by
index id: the paths (`vector_index_ids`, `vector_index_vectors`, `vector_index_centroids`)
and the sizes the builder wrote (`vector_index_vectors_bytes`, `vector_index_centroids_bytes`
on disk, `vector_index_logical_bytes` decoded — the billed figure, design §5.5). Empty
(including on every manifest written before these columns existed) means "this file is not
indexed" — readers search such a file exactly. Because a sidecar is referenced from
the manifest row of the file it indexes, it lives and dies with that file: time travel,
rollback, tags and expiry need nothing extra, exactly as for delete vectors.

The index id (not the name) keys the sidecars, so DROP INDEX x; CREATE INDEX x creates a
new id and can never pick up the old index's files.
"""

from __future__ import annotations

import re
import secrets
import uuid
from typing import NamedTuple

INDEXES_SUBCOLLECTION = "indexes"

VECTOR_INDEX_IDS_KEY = "vector_index_ids"
VECTOR_INDEX_VECTORS_KEY = "vector_index_vectors"
VECTOR_INDEX_CENTROIDS_KEY = "vector_index_centroids"
VECTOR_INDEX_VECTORS_BYTES_KEY = "vector_index_vectors_bytes"
VECTOR_INDEX_CENTROIDS_BYTES_KEY = "vector_index_centroids_bytes"
VECTOR_INDEX_LOGICAL_BYTES_KEY = "vector_index_logical_bytes"
VECTOR_INDEX_ENTRY_KEYS = (
    VECTOR_INDEX_IDS_KEY,
    VECTOR_INDEX_VECTORS_KEY,
    VECTOR_INDEX_CENTROIDS_KEY,
    VECTOR_INDEX_VECTORS_BYTES_KEY,
    VECTOR_INDEX_CENTROIDS_BYTES_KEY,
    VECTOR_INDEX_LOGICAL_BYTES_KEY,
)

METHODS = frozenset({"ivf"})
METRICS = frozenset({"cosine"})
BUILD_MODES = frozenset({"sync", "async"})
DEFAULT_BUILD_MODE = "async"   # ruled 2026-10-02: a write never pays embedding cost unless asked
MAX_INDEX_NAME_LENGTH = 64
_INDEX_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

# <stem>-<nonce>.vectors.skene / .centroids.skene under index/<index_id>/. The nonce makes
# the name unique per write (two builders of the same file can never overwrite each other —
# the loser leaves an orphan for the sweeps), as for delete vectors.
VECTOR_INDEX_FILENAME_RE = re.compile(r"/index/([0-9a-f]{32})/[^/]+-[0-9a-f]+\.(vectors|centroids)\.skene$")


class IndexFiles(NamedTuple):
    """One index's two files for one data file, with the sizes the builder wrote.

    Sizes are handed to the commit with the paths and never read back from storage
    (design §5.3). `logical_bytes` is the decoded size of both files together - indexed
    rows x (4 + 2 x dim) plus clusters x (2 x dim + 8) - which is what
    `uncompressed_size_in_bytes` means for a data file, and what storage is billed on."""

    vectors: str
    centroids: str
    vectors_bytes: int
    centroids_bytes: int
    logical_bytes: int


class IndexBuildTask(NamedTuple):
    """One data file an index does not cover yet - what `REFRESH INDEX` builds (D-16).

    Planned from one snapshot: the file's size (a remote reader cannot ask for it), its
    deleted ordinals at that snapshot (rows deleted later stay in the index and are
    excluded at search, like any delete), and freshly minted paths for its two files."""

    data_file: str
    data_bytes: int
    deleted: tuple
    vectors: str
    centroids: str


def index_files(
    vectors: str, centroids: str, *, vectors_bytes: int, centroids_bytes: int, logical_bytes: int
) -> IndexFiles:
    """A validated IndexFiles. Refuses a missing path or a size that is not a positive
    integer: an index file never exists in a manifest without its recorded size."""
    for label, path in (("vectors", vectors), ("centroids", centroids)):
        if type(path) is not str or not path:
            raise ValueError(f"An index needs its {label} file path.")
    for label, size in (
        ("vectors_bytes", vectors_bytes),
        ("centroids_bytes", centroids_bytes),
        ("logical_bytes", logical_bytes),
    ):
        if type(size) is not int or size <= 0:
            raise ValueError(f"{label} must be a positive integer (the size the builder wrote), not {size!r}.")
    return IndexFiles(vectors, centroids, vectors_bytes, centroids_bytes, logical_bytes)


def normalize_index_name(name: str) -> str:
    """Validate an index name and return its canonical (lowercase) spelling.

    The normalized name is the Firestore document id, so name uniqueness and document-id
    uniqueness are one constraint."""
    if not isinstance(name, str) or not name:
        raise ValueError("An index name is required.")
    if len(name) > MAX_INDEX_NAME_LENGTH:
        raise ValueError(f"Index name is {len(name)} characters; the maximum is {MAX_INDEX_NAME_LENGTH}.")
    if not _INDEX_NAME_PATTERN.match(name):
        raise ValueError(
            f"'{name}' is not a valid index name. An index name starts with a letter and "
            "contains only letters, digits and underscores."
        )
    return name.lower()


def new_index_definition(
    *,
    name: str,
    column: str,
    method: str,
    metric: str,
    build: str | None,
    clusters: int,
    embedding_identity: str,
    dimensions: int,
    author: str,
    created_at_ms: int,
) -> dict:
    """The stored definition of a new index. Validates every field; refuses, never coerces."""
    if not author:
        raise ValueError("author must be provided when creating an index")
    if not isinstance(column, str) or not column:
        raise ValueError("An index needs the column it indexes.")
    if method not in METHODS:
        raise ValueError(f"Unknown index method '{method}'; supported: {sorted(METHODS)}.")
    if metric not in METRICS:
        raise ValueError(f"Unknown index metric '{metric}'; supported: {sorted(METRICS)}.")
    build = DEFAULT_BUILD_MODE if build is None else build
    if build not in BUILD_MODES:
        raise ValueError(f"Unknown build mode '{build}'; supported: {sorted(BUILD_MODES)}.")
    if type(clusters) is not int or clusters < 0:
        raise ValueError("clusters must be a non-negative integer (0 = sqrt of the file's rows).")
    if not embedding_identity:
        raise ValueError("An index records the embedding identity it was defined against.")
    if type(dimensions) is not int or not 1 <= dimensions <= 65535:
        raise ValueError("dimensions must be an integer between 1 and 65535.")
    return {
        "index-id": uuid.uuid4().hex,
        "name": normalize_index_name(name),
        "column": column,
        "method": method,
        "metric": metric,
        "build": build,
        "clusters": clusters,
        "embedding-identity": embedding_identity,
        "dimensions": dimensions,
        "created-by": author,
        "created-at-ms": created_at_ms,
    }


def vector_index_paths(dataset_location: str, index_id: str, data_file_path: str) -> tuple[str, str]:
    """Mint the (vectors, centroids) sidecar paths for one data file. New paths every call."""
    if not re.fullmatch(r"[0-9a-f]{32}", index_id or ""):
        raise ValueError(f"'{index_id}' is not an index id")
    stem = data_file_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    nonce = secrets.token_hex(4)
    base = f"{dataset_location.rstrip('/')}/index/{index_id}/{stem}-{nonce}"
    return f"{base}.vectors.skene", f"{base}.centroids.skene"


def is_vector_index_path(path: str) -> bool:
    return bool(VECTOR_INDEX_FILENAME_RE.search(path or ""))


def index_refs(entry: dict) -> dict[str, IndexFiles]:
    """{index_id: IndexFiles} for one manifest entry. Empty = not indexed.

    The six columns are parallel by construction; a row where they disagree is corrupt
    and is refused, never truncated to the shortest."""
    columns = [list(entry.get(key) or ()) for key in VECTOR_INDEX_ENTRY_KEYS]
    if len({len(column) for column in columns}) > 1:
        detail = ", ".join(f"{len(c)} {k}" for k, c in zip(VECTOR_INDEX_ENTRY_KEYS, columns))
        raise ValueError(
            f"manifest entry {entry.get('file_path')!r} has inconsistent vector index columns ({detail})"
        )
    ids, vectors, centroids, vectors_bytes, centroids_bytes, logical_bytes = columns
    return {
        i: index_files(v, c, vectors_bytes=int(vb), centroids_bytes=int(cb), logical_bytes=int(lb))
        for i, v, c, vb, cb, lb in zip(ids, vectors, centroids, vectors_bytes, centroids_bytes, logical_bytes)
    }


def with_index_refs(entry: dict, refs: dict[str, IndexFiles]) -> dict:
    """A copy of `entry` carrying exactly `refs`, ordered by index id."""
    out = dict(entry)
    ordered = sorted(refs.items())
    out[VECTOR_INDEX_IDS_KEY] = [i for i, _ in ordered]
    out[VECTOR_INDEX_VECTORS_KEY] = [f.vectors for _, f in ordered]
    out[VECTOR_INDEX_CENTROIDS_KEY] = [f.centroids for _, f in ordered]
    out[VECTOR_INDEX_VECTORS_BYTES_KEY] = [f.vectors_bytes for _, f in ordered]
    out[VECTOR_INDEX_CENTROIDS_BYTES_KEY] = [f.centroids_bytes for _, f in ordered]
    out[VECTOR_INDEX_LOGICAL_BYTES_KEY] = [f.logical_bytes for _, f in ordered]
    return out


def referenced_index_files(entry: dict) -> dict[str, int]:
    """Every index file a manifest entry references, with its recorded on-disk size —
    for the protection sets and expiry's reclaimed-bytes tally."""
    files: dict[str, int] = {}
    for f in index_refs(entry).values():
        files[f.vectors] = f.vectors_bytes
        files[f.centroids] = f.centroids_bytes
    return files


def index_totals(entries) -> dict[str, int]:
    """The snapshot summary's index counters, derived from the manifest being written
    (design §5.5), like the data totals. Kept apart from `total-files-size` and
    `total-data-size`, so creating an index never moves the data figures."""
    files = on_disk = logical = 0
    for entry in entries:
        for f in index_refs(entry).values():
            files += 2
            on_disk += f.vectors_bytes + f.centroids_bytes
            logical += f.logical_bytes
    return {
        "total-index-files": files,
        "total-index-size": on_disk,
        "total-index-data-size": logical,
    }
