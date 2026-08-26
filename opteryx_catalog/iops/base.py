from __future__ import annotations

import os
from collections import OrderedDict
from io import BytesIO
from typing import BinaryIO

# Why a content-less InputFile has no content. Both end as FileNotFoundError -
# callers rightly treat either as "no such object" - but they are diagnosed in
# completely different places, and for one evening they were indistinguishable
# in the logs: an unreadable manifest that named a path sitting in the bucket,
# with nothing to say whether anything had ever gone looking for it.
NOT_FETCHED = "no storage backend is configured for this catalog, so nothing was requested"
FETCH_404 = "storage returned HTTP 404"


class InputFile:
    def __init__(
        self, location: str, content: bytes | None = None, absent_reason: str = NOT_FETCHED
    ):
        self.location = location
        self._content = content
        self.absent_reason = absent_reason

    def open(self) -> BinaryIO:
        if self._content is None:
            raise FileNotFoundError(f"{self.location} ({self.absent_reason})")
        return BytesIO(self._content)


class OutputFile:
    def __init__(self, location: str):
        self.location = location

    def create(self):
        """Return a file-like object with a `write` method.

        Implementations may return a buffer or a writer that persists on write/close.
        """
        raise NotImplementedError()


class FileIO:
    """Minimal FileIO abstraction used by the `opteryx_catalog` layer.

    Concrete implementations should implement `new_input`, `new_output`, and
    optionally `delete`/`exists`. The abstraction intentionally keeps only the
    small surface needed by the catalog (read bytes, write bytes).
    """

    def new_input(self, location: str) -> InputFile:
        return InputFile(location)

    def new_output(self, location: str) -> OutputFile:
        return OutputFile(location)


# We keep a local cache of recently read files. Two limits, both enforced:
#
#   MAX_CACHE_SIZE   - entry count (the original bound).
#   MAX_CACHE_BYTES  - total held bytes. The count bound alone let 32 WHOLE
#                      OBJECTS sit on the heap with no size accounting: at the
#                      ~470 MB on-disk size of a compacted data file that is
#                      ~15 GB of RSS from a cache meant to save re-reads of
#                      small metadata. The byte bound makes the worst case a
#                      number that was chosen, not an accident of file sizes.
#
# Objects larger than MAX_CACHEABLE_OBJECT_BYTES are served but never cached:
# admitting one ~470 MB data file would evict every manifest to make room for
# an object that is read once. The caches this size guard excludes are exactly
# the reads compaction's own source cache (or the OS page cache, when spilling)
# already covers.
MAX_CACHE_SIZE: int = 32
MAX_CACHE_BYTES: int = int(os.environ.get("OPTERYX_GCS_CACHE_MB") or 256) * 1024 * 1024
MAX_CACHEABLE_OBJECT_BYTES: int = MAX_CACHE_BYTES // 4


class _ByteBudgetLRU:
    """An OrderedDict LRU of path -> bytes that tracks and bounds held bytes.

    Not thread-safe, matching the OrderedDict it replaces.
    """

    def __init__(
        self,
        max_entries: int = MAX_CACHE_SIZE,
        max_bytes: int = MAX_CACHE_BYTES,
        max_object_bytes: int = MAX_CACHEABLE_OBJECT_BYTES,
    ):
        self._data: OrderedDict[str, bytes] = OrderedDict()
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._max_object_bytes = max_object_bytes
        self._bytes = 0

    def get(self, location: str):
        data = self._data.get(location)
        if data is not None:
            self._data.move_to_end(location)
        return data

    def put(self, location: str, data: bytes) -> None:
        if len(data) > self._max_object_bytes:
            return
        old = self._data.pop(location, None)
        if old is not None:
            self._bytes -= len(old)
        self._data[location] = data
        self._bytes += len(data)
        while self._data and (len(self._data) > self._max_entries or self._bytes > self._max_bytes):
            _, evicted = self._data.popitem(last=False)
            self._bytes -= len(evicted)

    def pop(self, location: str, default=None):
        data = self._data.pop(location, default)
        if isinstance(data, (bytes, bytearray)):
            self._bytes -= len(data)
        return data

    def __contains__(self, location: str) -> bool:
        return location in self._data

    def __len__(self) -> int:
        return len(self._data)

    @property
    def held_bytes(self) -> int:
        return self._bytes
