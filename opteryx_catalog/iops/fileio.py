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
    optionally `delete`/`exists`. Some modules also call `list_files`/`ls` —
    provide a safe default implementation on the base class so callers do not
    need to special-case missing methods.
    """

    def new_input(self, location: str) -> InputFile:
        return InputFile(location)

    def new_output(self, location: str) -> OutputFile:
        return OutputFile(location)

    def list_files(self, prefix: str) -> list:
        """Safe default: return empty list when listing is not supported.

        Implementations that can perform listing should override this method.
        """
        return []

    # alias commonly used by other FileIO implementations
    ls = list_files


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
                    # Underlying streams buffer and upload on close, so this is
                    # where a write actually succeeds or fails - for data files
                    # as well as manifests. Swallowing it let a write report
                    # success while the object was never created. Let it raise.
                    self._stream.close()

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

    def list_files(self, prefix: str) -> list:
        """List files under a storage prefix.

        Behavior:
        - If the underlying implementation provides `list_files`, delegate to it.
        - Otherwise, if `prefix` is a `gs://` URL, use google-cloud-storage to list objects.
        - Returns a list of fully-qualified paths (e.g. `gs://bucket/path/to/object`).
        """
        # Delegate to underlying implementation if available
        if hasattr(self._impl, "list_files"):
            try:
                return list(self._impl.list_files(prefix))
            except Exception:
                return []

        # Fallback: handle gs://<bucket>/<prefix> by using google-cloud-storage client
        try:
            if prefix.startswith("gs://"):
                from google.cloud import storage

                _, rest = prefix.split("://", 1)
                parts = rest.split("/", 1)
                bucket_name = parts[0]
                object_prefix = parts[1] if len(parts) > 1 else ""

                client = storage.Client()
                blobs = client.list_blobs(bucket_name, prefix=object_prefix)
                return [f"gs://{bucket_name}/{b.name}" for b in blobs]
        except Exception:
            # Silently return empty list on failure to avoid crashing callers
            return []

        # No supported listing available
        return []

    # alias
    ls = list_files


# Centralized Parquet write options used across the codebase when writing
# parquet files via rugo's native (no-pyarrow) writer. Exported here so all
# writers share the same configuration.
WRITE_PARQUET_OPTIONS = {
    "compression": "zstd",
    "bloom_filters": True,
    # No override: use rugo's own default (262,144 rows/row group).
}
