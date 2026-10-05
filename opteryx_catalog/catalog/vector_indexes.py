"""
Vector indexes: definitions and per-data-file sidecars (opteryx-core
docs/VECTOR_INDEX_DESIGN.md §5).

An index is a property of its dataset, not an independent catalog object: its definition
lives ON the dataset document, in the `vector-indexes` map keyed by the normalized index
name (ruled 2026-10-04), so the dataset load every query already makes carries it - no
read per query. Only CREATE/ALTER/DROP INDEX change it, each in a transaction; the
dataset document's whole-document writes (a commit) carry the map from their OWN
transactional read, never from loaded metadata, so a definition created while a commit
was in flight cannot be overwritten by that commit's stale copy.

The index data is ONE immutable flat file PER DATA FILE, written by the engine (ruled
2026-10-04; opteryx-core src/cpp/engine/vector_index_file.hpp):

    <location>/index/<index_id>/<data-file-stem>-<nonce>.vidx
        cluster blocks (uint32 ordinals, fp16 vectors), then a footer holding the
        centroids and the block table, then a 24-byte tail

The manifest references it per data file through five parallel ARRAY columns, ordered by
index id: `vidx_ids`, `vidx_paths`, and the sizes the builder wrote — `vidx_bytes` (the file),
`vidx_footer_bytes` (its footer, so a search opens the file in ONE range read) and
`vidx_logical_bytes` (the billed figure, design §5.5; equal to the file for this format,
which is its own decoded form). Empty — including on every manifest written before these
columns existed, and on manifests carrying the retired two-file skene columns, which this
reader does not know — means "this file is not indexed": readers search such a file
exactly. Because a sidecar is referenced from the manifest row of the file it indexes, it
lives and dies with that file: time travel, rollback, tags and expiry need nothing extra,
exactly as for delete vectors.

The index id (not the name) keys the sidecars, so DROP INDEX x; CREATE INDEX x creates a
new id and can never pick up the old index's files.
"""

from __future__ import annotations

import re
import secrets
import uuid
from typing import NamedTuple

# The dataset-document field holding the definitions: {normalized name: definition}.
VECTOR_INDEXES_FIELD = "vector-indexes"

VECTOR_INDEX_IDS_KEY = "vidx_ids"
VECTOR_INDEX_PATHS_KEY = "vidx_paths"
VECTOR_INDEX_BYTES_KEY = "vidx_bytes"
VECTOR_INDEX_FOOTER_BYTES_KEY = "vidx_footer_bytes"
VECTOR_INDEX_LOGICAL_BYTES_KEY = "vidx_logical_bytes"
VECTOR_INDEX_ENTRY_KEYS = (
    VECTOR_INDEX_IDS_KEY,
    VECTOR_INDEX_PATHS_KEY,
    VECTOR_INDEX_BYTES_KEY,
    VECTOR_INDEX_FOOTER_BYTES_KEY,
    VECTOR_INDEX_LOGICAL_BYTES_KEY,
)

METHODS = frozenset({"ivf"})
METRICS = frozenset({"cosine"})
BUILD_MODES = frozenset({"sync", "async"})
DEFAULT_BUILD_MODE = "async"   # ruled 2026-10-02: a write never pays embedding cost unless asked
MAX_INDEX_NAME_LENGTH = 64
_INDEX_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

# <stem>-<nonce>.vidx under index/<index_id>/. The nonce makes the name unique per write
# (two builders of the same file can never overwrite each other — the loser leaves an
# orphan for the sweeps), as for delete vectors. The retired two-file skene names are still
# recognised as index files, so the sweeps reclaim them as the orphans they now are.
VECTOR_INDEX_FILENAME_RE = re.compile(
    r"/index/([0-9a-f]{32})/[^/]+-[0-9a-f]+\.(vidx|vectors\.skene|centroids\.skene)$"
)


class IndexFiles(NamedTuple):
    """One index's file for one data file, with the sizes the builder wrote.

    Sizes are handed to the commit with the path and never read back from storage (design
    §5.3). `footer_bytes` lets a search open the file in one range read. `logical_bytes` is
    what storage is billed on — the file's decoded size, which for this format is the file."""

    path: str
    file_bytes: int
    footer_bytes: int
    logical_bytes: int


class IndexBuildTask(NamedTuple):
    """One data file an index does not cover yet - what `REFRESH INDEX` builds (D-16).

    Planned from one snapshot: the file's size (a remote reader cannot ask for it), its
    deleted ordinals at that snapshot (rows deleted later stay in the index and are
    excluded at search, like any delete), and a freshly minted path for its index file."""

    data_file: str
    data_bytes: int
    deleted: tuple
    path: str


def index_files(path: str, *, file_bytes: int, footer_bytes: int, logical_bytes: int) -> IndexFiles:
    """A validated IndexFiles. Refuses a missing path or a size that is not a positive
    integer: an index file never exists in a manifest without its recorded sizes."""
    if type(path) is not str or not path:
        raise ValueError("An index needs its file path.")
    for label, size in (
        ("file_bytes", file_bytes),
        ("footer_bytes", footer_bytes),
        ("logical_bytes", logical_bytes),
    ):
        if type(size) is not int or size <= 0:
            raise ValueError(f"{label} must be a positive integer (the size the builder wrote), not {size!r}.")
    if footer_bytes >= file_bytes:
        raise ValueError("footer_bytes must be smaller than file_bytes.")
    return IndexFiles(path, file_bytes, footer_bytes, logical_bytes)


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


def vector_index_path(dataset_location: str, index_id: str, data_file_path: str) -> str:
    """Mint the index file path for one data file. A new path every call."""
    if not re.fullmatch(r"[0-9a-f]{32}", index_id or ""):
        raise ValueError(f"'{index_id}' is not an index id")
    stem = data_file_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    nonce = secrets.token_hex(4)
    return f"{dataset_location.rstrip('/')}/index/{index_id}/{stem}-{nonce}.vidx"


def is_vector_index_path(path: str) -> bool:
    return bool(VECTOR_INDEX_FILENAME_RE.search(path or ""))


def index_refs(entry: dict) -> dict[str, IndexFiles]:
    """{index_id: IndexFiles} for one manifest entry. Empty = not indexed.

    The five columns are parallel by construction; a row where they disagree is corrupt
    and is refused, never truncated to the shortest."""
    columns = [list(entry.get(key) or ()) for key in VECTOR_INDEX_ENTRY_KEYS]
    if len({len(column) for column in columns}) > 1:
        detail = ", ".join(f"{len(c)} {k}" for k, c in zip(VECTOR_INDEX_ENTRY_KEYS, columns))
        raise ValueError(
            f"manifest entry {entry.get('file_path')!r} has inconsistent vector index columns ({detail})"
        )
    ids, paths, file_bytes, footer_bytes, logical_bytes = columns
    return {
        i: index_files(p, file_bytes=int(fb), footer_bytes=int(tb), logical_bytes=int(lb))
        for i, p, fb, tb, lb in zip(ids, paths, file_bytes, footer_bytes, logical_bytes)
    }


def with_index_refs(entry: dict, refs: dict[str, IndexFiles]) -> dict:
    """A copy of `entry` carrying exactly `refs`, ordered by index id."""
    out = dict(entry)
    ordered = sorted(refs.items())
    out[VECTOR_INDEX_IDS_KEY] = [i for i, _ in ordered]
    out[VECTOR_INDEX_PATHS_KEY] = [f.path for _, f in ordered]
    out[VECTOR_INDEX_BYTES_KEY] = [f.file_bytes for _, f in ordered]
    out[VECTOR_INDEX_FOOTER_BYTES_KEY] = [f.footer_bytes for _, f in ordered]
    out[VECTOR_INDEX_LOGICAL_BYTES_KEY] = [f.logical_bytes for _, f in ordered]
    return out


def referenced_index_files(entry: dict) -> dict[str, int]:
    """Every index file a manifest entry references, with its recorded on-disk size —
    for the protection sets and expiry's reclaimed-bytes tally."""
    return {f.path: f.file_bytes for f in index_refs(entry).values()}


def index_totals(entries) -> dict[str, int]:
    """The snapshot summary's index counters, derived from the manifest being written
    (design §5.5), like the data totals. Kept apart from `total-files-size` and
    `total-data-size`, so creating an index never moves the data figures."""
    files = on_disk = logical = 0
    for entry in entries:
        for f in index_refs(entry).values():
            files += 1
            on_disk += f.file_bytes
            logical += f.logical_bytes
    return {
        "total-index-files": files,
        "total-index-size": on_disk,
        "total-index-data-size": logical,
    }
