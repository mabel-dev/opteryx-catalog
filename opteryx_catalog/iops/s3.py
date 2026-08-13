"""
S3 (and S3-compatible) FileIO for opteryx_catalog.iops
"""

from __future__ import annotations

import logging
from collections import OrderedDict

from .base import FileIO
from .base import InputFile
from .base import OutputFile

# we keep a local cache of recently read files
MAX_CACHE_SIZE: int = 32

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
    def __init__(self, location: str, client, cache: OrderedDict | None = None):
        if cache is not None and location in cache:
            cache.move_to_end(location)
            super().__init__(location, cache[location])
            return

        from botocore.exceptions import ClientError

        try:
            bucket, key = _split_bucket_key(location)
            data = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        except ClientError as err:
            code = err.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                super().__init__(location, None)
                return
            raise OSError(f"Unable to read '{location}': {err}") from err

        if cache is not None:
            cache[location] = data
            if len(cache) > MAX_CACHE_SIZE:
                cache.popitem(last=False)

        super().__init__(location, data)


class _S3OutputStream:
    """Buffers writes in memory and uploads as a single PutObject on close.

    Mirrors `iops.gcs._GcsOutputStream`'s buffer-then-upload shape: callers
    that call `out.write(data)` and rely on `close()` to persist it (the
    common pattern in this codebase) work the same way against either
    backend.
    """

    def __init__(self, location: str, client, cache: OrderedDict | None = None):
        self._location = location
        self._client = client
        self._cache = cache
        self._buffer = bytearray()
        self._closed = False

    def write(self, data: bytes | bytearray) -> int:
        self._buffer.extend(data)
        return len(data)

    def close(self) -> None:
        if self._closed:
            return

        bucket, key = _split_bucket_key(self._location)
        self._client.put_object(Bucket=bucket, Key=key, Body=bytes(self._buffer))

        if self._cache is not None:
            self._cache.pop(self._location, None)

        self._closed = True


class _S3OutputFile(OutputFile):
    def __init__(self, location: str, client, cache: OrderedDict | None = None):
        super().__init__(location)
        self._client = client
        self._cache = cache

    def create(self):
        return _S3OutputStream(self._location, self._client, self._cache)


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
        self._read_cache: OrderedDict = OrderedDict()

    def new_input(self, location: str) -> InputFile:
        return _S3InputFile(location, self._client, self._read_cache)

    def new_output(self, location: str) -> OutputFile:
        logger.info(f"new_output -> {location}")
        self._read_cache.pop(location, None)
        return _S3OutputFile(location, self._client, self._read_cache)

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

    def list_files_with_age_ms(self, prefix: str) -> dict:
        """List files under a prefix along with each object's age in ms.

        Used to safety-gate destructive orphan cleanup — see
        `GcsFileIO.list_files_with_age_ms`. `list_objects_v2` already returns
        `LastModified` per object, so this needs no extra request per file.
        Returns {uri: age_ms}; on any failure returns {} (every candidate then
        fails its age gate and is kept, the safe direction).
        """
        try:
            if not prefix or not prefix.startswith("s3://"):
                return {}
            import time as _time

            bucket, key_prefix = _split_bucket_prefix(prefix)
            paginator = self._client.get_paginator("list_objects_v2")
            now_ms = int(_time.time() * 1000)
            ages = {}
            for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
                for obj in page.get("Contents", []):
                    last_modified = obj.get("LastModified")
                    if last_modified is None:
                        continue
                    uri = f"s3://{bucket}/{obj['Key']}"
                    ages[uri] = now_ms - int(last_modified.timestamp() * 1000)
            return ages
        except Exception:
            logger.warning("Could not read object ages under %s", prefix, exc_info=True)
            return {}
