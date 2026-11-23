"""
S3 (and S3-compatible) FileIO for opteryx_catalog.iops
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from opteryx_catalog.exceptions import StorageReadError

from .base import FileIO
from .base import InputFile
from .base import OutputFile
from .base import _ByteBudgetLRU

# we keep a local cache of recently read files

logger = logging.getLogger(__name__)


def _split_bucket_key(location: str) -> tuple[str, str]:
    """Split ``s3://bucket/key`` into ``(bucket, key)``. Requires both parts."""
    path = location.removeprefix("s3://")
    bucket, _, key = path.partition("/")
    if not bucket or not key:
        raise ValueError(f"Not a valid s3://bucket/key location: {location!r}")
    return bucket, key


def _split_bucket_prefix(location: str) -> tuple[str, str]:
    """Split ``s3://bucket[/prefix]`` into ``(bucket, prefix)``. ``prefix`` may be empty."""
    path = location.removeprefix("s3://")
    bucket, _, prefix = path.partition("/")
    if not bucket:
        raise ValueError(f"Not a valid s3://bucket location: {location!r}")
    return bucket, prefix


class _S3InputFile(InputFile):
    def __init__(self, location: str, client, cache: _ByteBudgetLRU | None = None):
        # Fetch is LAZY (first open()), mirroring iops.gcs._GcsInputFile:
        # constructing an input must not itself transfer the whole object.
        super().__init__(location, None)
        self._client = client
        self._cache = cache
        self._fetched = False

    def _fetch(self) -> None:
        if self._fetched:
            return
        self._fetched = True

        if self._cache is not None:
            data = self._cache.get(self.location)
            if data is not None:
                self._content = data
                return

        from botocore.exceptions import ClientError

        try:
            bucket, key = _split_bucket_key(self.location)
            data = self._client.get_object(Bucket=bucket, Key=key)["Body"].read()
        except ClientError as err:
            code = err.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                return
            raise StorageReadError(f"Unable to read '{self.location}': {err}") from err

        if self._cache is not None:
            self._cache.put(self.location, data)
        self._content = data

    def open(self):
        self._fetch()
        return super().open()


# A part is handed to the upload pool once the buffer holds this much (S3's
# floor is 5 MiB for every part but the last; 10,000 parts caps an object at
# ~640 GiB, far past any data file we write).
MULTIPART_PART_BYTES = 64 * 1024 * 1024
# Parts of ONE stream in flight at once: bounds its memory to about this many
# parts while the writer keeps encoding.
MULTIPART_PARTS_IN_FLIGHT = 4
# Upload threads shared by every stream of one S3FileIO.
UPLOAD_THREADS = 8


class _S3OutputStream:
    """A write-then-close output that STREAMS large objects as a multipart upload.

    Writes accumulate in a buffer; once it holds `part_bytes` a multipart upload
    is opened and the buffer is handed, whole and without a copy, to the upload
    pool, so parts go over the wire while the caller keeps producing bytes. At
    most `parts_in_flight` parts of this stream are pending; a write that would
    exceed that waits for the oldest. An object that never reaches one part is
    uploaded in one PutObject on close, so manifests and metadata see no change
    in the requests made.

    A failed part surfaces on the next write or on close, which raise it. A
    `close` that raises has aborted the upload: no object is created.
    `abort()` waits out parts already sending, then aborts the upload.
    """

    def __init__(
        self,
        location: str,
        client,
        executor,
        cache: _ByteBudgetLRU | None = None,
        part_bytes: int = MULTIPART_PART_BYTES,
        parts_in_flight: int = MULTIPART_PARTS_IN_FLIGHT,
    ):
        self._location = location
        self._bucket, self._key = _split_bucket_key(location)
        self._client = client
        self._executor = executor
        self._cache = cache
        self._part_bytes = part_bytes
        self._parts_in_flight = parts_in_flight
        self._buffer = bytearray()
        self._upload_id: str | None = None
        self._pending: list = []  # futures of parts not yet collected, oldest first
        self._parts: list[dict] = []
        self._closed = False

    def write(self, data: bytes | bytearray | memoryview) -> int:
        if self._closed:
            raise ValueError(f"write to closed output '{self._location}'")
        self._buffer.extend(data)
        if len(self._buffer) >= self._part_bytes:
            self._send_buffer()
        return len(data)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._upload_id is None:
            self._client.put_object(Bucket=self._bucket, Key=self._key, Body=self._buffer)
        else:
            try:
                if self._buffer:
                    self._send_buffer()  # the last part may be under the 5 MiB floor
                while self._pending:
                    self._collect_oldest()
                self._client.complete_multipart_upload(
                    Bucket=self._bucket,
                    Key=self._key,
                    UploadId=self._upload_id,
                    MultipartUpload={"Parts": sorted(self._parts, key=lambda p: p["PartNumber"])},
                )
            except BaseException:
                self._abort_upload()
                raise
        self._buffer = bytearray()
        if self._cache is not None:
            self._cache.pop(self._location, None)

    def abort(self) -> None:
        """Discard everything: abort the multipart upload if one was opened.

        Best-effort on the wire - the caller is already handling a failure and
        an abort that fails changes nothing about that. An upload left open
        never becomes an object (a bucket lifecycle rule reclaims its parts).
        """
        if self._closed:
            return
        self._closed = True
        self._buffer = bytearray()
        if self._upload_id is not None:
            self._abort_upload()

    # -- multipart -------------------------------------------------------------

    def _send_buffer(self) -> None:
        if self._upload_id is None:
            self._upload_id = self._client.create_multipart_upload(
                Bucket=self._bucket, Key=self._key
            )["UploadId"]
        while len(self._pending) >= self._parts_in_flight:
            self._collect_oldest()
        part, self._buffer = self._buffer, bytearray()
        number = len(self._parts) + len(self._pending) + 1
        self._pending.append(self._executor.submit(self._upload_part, number, part))

    def _upload_part(self, number: int, body: bytearray) -> dict:
        response = self._client.upload_part(
            Bucket=self._bucket,
            Key=self._key,
            UploadId=self._upload_id,
            PartNumber=number,
            Body=body,
        )
        # Checksum* fields echo back: an upload created under a checksum
        # algorithm must name each part's checksum to complete.
        part = {
            k: v for k, v in response.items() if k.startswith("Checksum") and k != "ChecksumType"
        }
        part["ETag"] = response["ETag"]
        part["PartNumber"] = number
        return part

    def _collect_oldest(self) -> None:
        self._parts.append(self._pending.pop(0).result())

    def _abort_upload(self) -> None:
        from botocore.exceptions import BotoCoreError
        from botocore.exceptions import ClientError

        # A part still sending after the abort would leave storage behind it.
        for future in self._pending:
            future.cancel()
        for future in self._pending:
            if not future.cancelled():
                future.exception()
        self._pending = []
        try:
            self._client.abort_multipart_upload(
                Bucket=self._bucket, Key=self._key, UploadId=self._upload_id
            )
        except (BotoCoreError, ClientError):
            logger.warning("Could not abort multipart upload of '%s'", self._location)


class _S3OutputFile(OutputFile):
    def __init__(self, location: str, client, executor, cache: _ByteBudgetLRU | None = None):
        super().__init__(location)
        self._client = client
        self._executor = executor
        self._cache = cache

    def create(self):
        return _S3OutputStream(self.location, self._client, self._executor, self._cache)


class S3FileIO(FileIO):
    """boto3-backed FileIO for S3 and S3-compatible object storage.

    Implements the same `new_input`/`new_output`/`delete`/`exists`/
    `list_files` surface as `iops.gcs.GcsFileIO`, so an instance of this class
    is a drop-in `io=` for `OpteryxCatalog` (see its `io` constructor param) —
    including pointing at a bucket this process doesn't own, given the right
    credentials.

    `client_kwargs` is passed straight through to `boto3.client("s3", ...)`:
    pass `aws_access_key_id`/`aws_secret_access_key`/`aws_session_token` for a
    specific (e.g. customer-supplied) identity rather than this process's
    ambient one, and/or `endpoint_url`/`region_name` for a non-AWS S3-compatible
    store. With no kwargs, boto3 falls back to its normal credential chain
    (env vars, shared config/profile, instance/task role).
    """

    def __init__(self, **client_kwargs):
        import boto3

        self._client = boto3.client("s3", **client_kwargs)
        self._read_cache = _ByteBudgetLRU()
        self._upload_executor = ThreadPoolExecutor(
            max_workers=UPLOAD_THREADS, thread_name_prefix="s3-upload"
        )

    def new_input(self, location: str) -> InputFile:
        return _S3InputFile(location, self._client, self._read_cache)

    def new_output(self, location: str) -> OutputFile:
        logger.info(f"new_output -> {location}")
        self._read_cache.pop(location, None)
        return _S3OutputFile(location, self._client, self._upload_executor, self._read_cache)

    def delete(self, location: str | InputFile | OutputFile) -> None:
        if isinstance(location, (InputFile, OutputFile)):
            location = location.location

        self._read_cache.pop(location, None)

        from botocore.exceptions import ClientError

        bucket, key = _split_bucket_key(location)
        try:
            self._client.delete_object(Bucket=bucket, Key=key)
        except ClientError as err:
            raise OSError(f"Failed to delete '{location}': {err}") from err

    def exists(self, location: str) -> bool:
        from botocore.exceptions import ClientError

        bucket, key = _split_bucket_key(location)
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False

    def list_files(self, prefix: str) -> list:
        """List files under a storage prefix (s3://bucket/path).

        Returns a list of fully-qualified `s3://` object URIs. Mirrors
        `GcsFileIO.list_files`: on any failure, returns an empty list rather
        than raising, so callers (deep-clean/expiration) that treat an empty
        listing as ambiguous (never as "nothing is orphaned") continue safely.
        """
        try:
            if not prefix or not prefix.startswith("s3://"):
                return []
            bucket, key_prefix = _split_bucket_prefix(prefix)
            paginator = self._client.get_paginator("list_objects_v2")
            results = []
            for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
                for obj in page.get("Contents", []):
                    results.append(f"s3://{bucket}/{obj['Key']}")
            return results
        except Exception:
            logger.warning("Listing %s failed; reporting no files", prefix, exc_info=True)
            return []

    # alias
    ls = list_files

    def list_files_with_stats(self, prefix: str) -> dict:
        """List files under a prefix with each object's age in ms and size.

        Used to safety-gate destructive orphan cleanup — see
        `GcsFileIO.list_files_with_stats`. `list_objects_v2` already returns
        `LastModified` and `Size` per object, so this needs no extra request
        per file. Returns {uri: (age_ms, size_bytes)}; on any failure returns
        {} (every candidate then fails its age gate and is kept, the safe
        direction).
        """
        try:
            if not prefix or not prefix.startswith("s3://"):
                return {}
            import time as _time

            bucket, key_prefix = _split_bucket_prefix(prefix)
            paginator = self._client.get_paginator("list_objects_v2")
            now_ms = int(_time.time() * 1000)
            stats = {}
            for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
                for obj in page.get("Contents", []):
                    last_modified = obj.get("LastModified")
                    if last_modified is None:
                        continue
                    uri = f"s3://{bucket}/{obj['Key']}"
                    stats[uri] = (
                        now_ms - int(last_modified.timestamp() * 1000),
                        int(obj.get("Size") or 0),
                    )
            return stats
        except Exception:
            logger.warning("Could not read object stats under %s", prefix, exc_info=True)
            return {}

    def list_files_with_age_ms(self, prefix: str) -> dict:
        """Ages only, for callers that don't need sizes. See
        `list_files_with_stats`, which this reads from - one listing either
        way."""
        return {uri: age for uri, (age, _) in self.list_files_with_stats(prefix).items()}
