"""Which dataset owns a file's bytes.

ONE RULE, AND EVERY PHYSICAL DELETER GOES THROUGH IT: a dataset may delete a
file only when that file sits under the dataset's own `location`. Dropping a
foreign path from a manifest is the whole of the delete; the bytes are someone
else's to reclaim.

This is not new behaviour for most of the catalog - it is behaviour three
deleters already had by accident and one did not:

* `DatasetDeepClean` and `drop_workspace` enumerate by LISTING the dataset's
  own location, so a foreign path is never even a candidate;
* reconciliation compares the bucket against the locations the catalog claims;
* `SnapshotExpiration` enumerates from MANIFEST ENTRIES, which may name any
  path `add_files` was given - and deleted them.

`rename_dataset` has always had the concept, from the other side: it remaps
paths under `old_location` and returns "externally-referenced files" unchanged.
`_is_own_path` is that same test, named, so the read side and the delete side
cannot drift apart.

Making it explicit everywhere means a deleter's safety is a property of the
code rather than of how it happened to enumerate - and it is what lets a
dataset's manifest reference another dataset's files at all (FORKS_DESIGN.md
S5.2).
"""

from __future__ import annotations


def _looks_absolute(path: str) -> bool:
    """Whether a path names a storage location rather than a bare file name.

    Every real path in this catalog is a URI - `gs://bucket/ws/coll/ds/...`,
    `file://...`, `mem://...`. Test doubles and fixtures use bare names
    (`manifest-3.parquet`) to mean "inside this dataset", and those cannot be
    compared against a location at all.
    """
    return "://" in path


def is_own_path(location: str | None, path: str | None) -> bool:
    """Whether `path` is a file `location`'s dataset owns, and may delete.

    True for a path under `location`. True for a relative path, which by
    construction belongs to the dataset that named it. False for an absolute
    path outside `location` - a file borrowed from another dataset.

    FAILS CLOSED on an unknown location: a dataset whose own location cannot
    be read must not delete anything absolute on the strength of not knowing
    where it lives. The one exception is the relative path above, which is not
    a location-scoped claim in the first place.

    The comparison is deliberately textual and deliberately requires the
    separator: `gs://b/ws/coll/events` must not own `gs://b/ws/coll/events_v2`.
    """
    if not path:
        return False
    if not _looks_absolute(path):
        return True
    if not location:
        return False
    return path.startswith(location.rstrip("/") + "/")


def is_admissible_path(location: str | None, path: str | None) -> bool:
    """Whether a commit may register `path` as one of `location`'s files.

    The read-side twin of `is_own_path`, and stricter. Scans read every file a
    manifest names with the engine's own storage credentials, so a manifest is
    a capability: an entry naming `gs://other-bucket/secret.parquet` would let
    a dataset read anything those credentials reach (confused deputy). A
    commit therefore admits only:

    * an absolute path under `location` - the dataset's own files, whether the
      location is a URI or (a local catalog) a local absolute path;
    * a bare file name with no directory part, the test-fixture spelling
      `_looks_absolute` describes. A relative path WITH a directory is
      refused: the GCS IO reads its first segment as a bucket name.

    Any `.` or `..` segment is refused outright, whatever the prefix: the
    comparison here is textual, and a reader that normalised the path would
    resolve it somewhere the comparison never looked.

    Files legitimately borrowed from another dataset (a fork's clone/resync)
    are not admitted by this test - the commit is handed them separately, as
    paths the catalog itself read from the pinned upstream manifest.
    """
    if not path:
        return False
    body = path.split("://", 1)[-1]
    if any(segment in (".", "..") for segment in body.split("/")):
        return False
    if path.startswith("/"):
        # A local absolute path (a local catalog's own files): admissible only under a
        # location spelled the same way. `_looks_absolute` sees only URIs, so without
        # this a local dataset could not register a single file of its own.
        return bool(location) and path.startswith(location.rstrip("/") + "/")
    if not _looks_absolute(path):
        return "/" not in path and "\\" not in path
    return is_own_path(location, path)
