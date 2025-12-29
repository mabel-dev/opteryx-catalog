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


class _GcsAdapterOutputFile(OutputFile):
    def __init__(self, location: str, gcs_fileio):
        super().__init__(location)
        self._location = location
        self._gcs_fileio = gcs_fileio

    def create(self):
        """Return a writer whose `write(data)` uploads the data via the wrapped GCS FileIO.

        We perform the upload on the first write and close the underlying stream
        afterwards so callers that simply call `out.write(data)` (common pattern
        in this codebase) will succeed.
        """

        class _Writer:
            def __init__(self, location: str, gcs_fileio):
                self._location = location
                self._gcs_fileio = gcs_fileio
                self._stream = None

            def write(self, data: bytes | bytearray):
                if self._stream is None:
                    # Create underlying output stream (may be a GcsOutputStream,
                    # DiscardOutputStream, or CaptureOutputStream depending on
                    # the wrapped FileIO behaviour).
                    out = self._gcs_fileio.new_output(self._location)
                    self._stream = out.create()
                # Underlying stream implements write/close semantics
                self._stream.write(data)

            def close(self):
                if self._stream is not None:
                    try:
                        self._stream.close()
                    except Exception:
                        pass

        return _Writer(self._location, self._gcs_fileio)


class GcsFileIO(FileIO):
    """GCS-backed FileIO adapter that wraps the existing GCS implementation.

    This adapter delegates to `pyiceberg_firestore_gcs.fileio.gcs_fileio.GcsFileIO`
    for actual network operations but exposes the small `opteryx_catalog.iops`
    `FileIO` interface used by the catalog layer.
    """

    def __init__(self, properties=None):
        # Lazy import to avoid pulling google libs unless used
        from pyiceberg_firestore_gcs.fileio.gcs_fileio import GcsFileIO as _GcsImpl

        self._impl = _GcsImpl(properties or {})

    def new_input(self, location: str) -> InputFile:
        # Read full bytes from the underlying InputFile and return an in-memory InputFile
        impl_input = self._impl.new_input(location)
        try:
            stream = impl_input.open()
            data = stream.read()
            return InputFile(location, data)
        except FileNotFoundError:
            return InputFile(location, None)

    def new_output(self, location: str) -> OutputFile:
        return _GcsAdapterOutputFile(location, self._impl)

    def delete(self, location: str) -> None:
        return self._impl.delete(location)

    def exists(self, location: str) -> bool:
        try:
            impl_in = self._impl.new_input(location)
            # Some implementations provide `exists()`
            if hasattr(impl_in, "exists"):
                return impl_in.exists()
            # Fallback: try to open
            _ = impl_in.open()
            return True
        except Exception:
            return False
