from __future__ import annotations

from io import BytesIO
from typing import BinaryIO


class InputFile:
    def __init__(self, location: str, content: bytes | None = None):
        self.location = location
        self._content = content

    def open(self) -> BinaryIO:
        if self._content is None:
            raise FileNotFoundError(self.location)
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
