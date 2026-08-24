from __future__ import annotations

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
