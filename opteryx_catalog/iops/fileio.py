from __future__ import annotations

import logging
from io import BytesIO
from typing import BinaryIO

from opteryx_catalog.exceptions import StorageReadError

from .base import FETCH_404
from .base import NOT_FETCHED

logger = logging.getLogger(__name__)


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

    This adapter delegates to `opteryx_catalog.iops.gcs.GcsFileIO`
    for actual network operations but exposes the small `opteryx_catalog.iops`
    `FileIO` interface used by the catalog layer.
    """

    def __init__(self, properties=None):
        # Lazy import to avoid pulling google libs unless used
        from opteryx_catalog.iops.gcs import GcsFileIO as _GcsImpl

        # `properties` is accepted for interface parity with other FileIO
        # implementations; the GCS impl reads its config from the ambient
        # credentials and takes no constructor arguments.
        self._impl = _GcsImpl()

    def new_input(self, location: str) -> InputFile:
        # Read full bytes from the underlying InputFile and return an in-memory InputFile
        impl_input = self._impl.new_input(location)
        try:
            stream = impl_input.open()
            data = stream.read()
            return InputFile(location, data)
        except FileNotFoundError:
            return InputFile(location, None, absent_reason=FETCH_404)

    def new_output(self, location: str) -> OutputFile:
        return _GcsAdapterOutputFile(location, self._impl)

    def delete(self, location: str) -> None:
        return self._impl.delete(location)

    def exists(self, location: str) -> bool:
        # Delegate to the impl's own exists() - a HEAD request. The previous
        # version probed for `exists` on the *InputFile* (which never has one)
        # and fell through to open(), i.e. it DOWNLOADED THE WHOLE OBJECT to
        # answer a boolean - per file, at data-file sizes, on exactly the GC
        # paths that ask this question most.
        try:
            return bool(self._impl.exists(location))
        except FileNotFoundError:
            return False
        except StorageReadError:
            # The storage layer could not answer. False would be a lie a caller
            # cannot detect, and the GC paths delete on absence - so this one
            # propagates rather than collapsing into the boolean.
            raise
        except Exception:  # noqa: BLE001 - storage boundary; the question is boolean
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
                logger.warning("Listing %s failed; reporting no files", prefix, exc_info=True)
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
            # Empty on failure so callers (deep-clean / expiration) continue.
            # Both already treat an empty listing as AMBIGUOUS rather than as
            # "nothing is orphaned" - see the comment in
            # `DatasetDeepClean.find_orphaned_files` - so this cannot cause a
            # deletion. It is logged because a silent empty listing otherwise
            # looks like a clean dataset.
            logger.warning("Listing %s failed; reporting no files", prefix, exc_info=True)
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
    # profile: rugo's default is "fast", which is what ingest and CTAS want —
    # the caller is waiting on the write. See COMPACTION_WRITE_PARQUET_OPTIONS
    # for the other side of that trade.
}

# Compaction rewrites bytes that are then read many times, so it can pay more
# at write time to make every subsequent read cheaper. rugo's "storage" profile
# raises the zstd level on BYTE_ARRAY columns ONLY — numeric columns come out
# byte-identical, because measurement showed they do not respond to level
# (high-cardinality integers are incompressible at any level). On ClickBench
# that is ~6% fewer bytes for ~40% more compress time, concentrated entirely in
# the string columns.
#
# The level itself is never passed in: it is rugo's policy, chosen per column.
COMPACTION_WRITE_PARQUET_OPTIONS = {
    **WRITE_PARQUET_OPTIONS,
    "profile": "storage",
}
