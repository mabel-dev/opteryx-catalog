"""
Optimized GCS FileIO for opteryx_catalog.iops
"""

from __future__ import annotations

import io
import logging
import os
import random
import time
import urllib.parse
from collections.abc import Callable

import requests
from google.auth.transport.requests import Request
from requests.adapters import HTTPAdapter

from opteryx_catalog.exceptions import CredentialsUnavailable
from opteryx_catalog.exceptions import StorageReadError

from .base import FETCH_404
from .base import FileIO
from .base import InputFile
from .base import OutputFile
from .base import _ByteBudgetLRU

# Statuses where trying again is the right move: the request was rejected for a
# reason that has nothing to do with the request itself. Everything else - 401,
# 403, 404 - will answer identically however many times it is asked, so retrying
# only delays the error.
RETRYABLE_STATUSES: frozenset = frozenset({408, 429, 500, 502, 503, 504})
MAX_ATTEMPTS: int = 4
BACKOFF_BASE_SECONDS: float = 0.25
MAX_BACKOFF_SECONDS: float = 8.0

logger = logging.getLogger(__name__)


def _get_storage_credentials():
    from google.cloud import storage

    if os.environ.get("STORAGE_EMULATOR_HOST"):
        from google.auth.credentials import AnonymousCredentials

        storage_client = storage.Client(credentials=AnonymousCredentials())
    else:
        storage_client = storage.Client()
    return storage_client._credentials


def _backoff_seconds(response, attempt: int) -> float:
    """How long to wait before attempt `attempt` + 1.

    `Retry-After` is honoured when the service sends one - it knows more about
    when it will be ready than an exponential curve does. Otherwise the wait
    doubles per attempt, with jitter so a fleet of readers that all hit the same
    throttle do not come back in lockstep and reproduce it.
    """
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(float(retry_after), MAX_BACKOFF_SECONDS)
            except ValueError:
                pass
    delay = min(BACKOFF_BASE_SECONDS * (2**attempt), MAX_BACKOFF_SECONDS)
    return delay * (0.5 + random.random() / 2)


def _get_with_retry(
    url: str,
    location: str,
    session: requests.Session,
    access_token_getter: Callable[[], str],
) -> requests.Response:
    """GET an object, retrying only the failures that retrying can fix.

    The token is fetched inside the loop rather than once up front: a read that
    spans a token expiry otherwise spends every remaining attempt re-sending the
    same dead credential.

    Returns the 200 response. Raises `FileNotFoundError` for a 404 and
    `StorageReadError` for anything else, including a transport failure that
    outlived the retries.
    """
    last_response = None
    last_error = None

    for attempt in range(MAX_ATTEMPTS):
        if attempt:
            time.sleep(_backoff_seconds(last_response, attempt - 1))

        access_token = access_token_getter()
        headers = {"Accept-Encoding": "identity"}
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"

        try:
            last_error = None
            last_response = session.get(url, headers=headers, timeout=30)
        except requests.RequestException as err:
            # No response at all - a connection reset or a timeout. Worth
            # another attempt for the same reason a 503 is.
            last_response = None
            last_error = err
            logger.warning(
                "Read of '%s' failed on attempt %d/%d: %s",
                location,
                attempt + 1,
                MAX_ATTEMPTS,
                err,
            )
            continue

        if last_response.status_code == 200:
            return last_response

        if last_response.status_code not in RETRYABLE_STATUSES:
            break

        logger.warning(
            "Read of '%s' returned %d on attempt %d/%d",
            location,
            last_response.status_code,
            attempt + 1,
            MAX_ATTEMPTS,
        )

    if last_error is not None:
        raise StorageReadError(
            f"Unable to read '{location}' after {MAX_ATTEMPTS} attempts: {last_error}"
        ) from last_error

    status = last_response.status_code
    body = last_response.text[:500]

    if status == 404:
        # The one status that means what `FileNotFoundError` means. Message
        # discarded on purpose: callers catch this to mean "no such object", and
        # the path is the whole story.
        raise FileNotFoundError(location)

    raise StorageReadError(
        f"Unable to read '{location}' - status {status}: {body}",
        status=status,
        body=body,
    )


class _GcsInputStream(io.BytesIO):
    def __init__(
        self, path: str, session: requests.Session, access_token_getter: Callable[[], str]
    ):
        # Strip gs://
        path = path.removeprefix("gs://")
        bucket = path.split("/", 1)[0]
        object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
        url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

        response = _get_with_retry(url, path, session, access_token_getter)

        super().__init__(response.content)


# Resumable upload chunking. GCS requires every non-final chunk of a resumable
# session to be a multiple of 256 KiB; 32 MiB keeps a 4 GB data file to ~128
# PUTs while bounding what is held in memory between them.
RESUMABLE_CHUNK_BYTES: int = 32 * 1024 * 1024
_RESUMABLE_CHUNK_QUANTUM: int = 256 * 1024
# Per-request socket timeout for an upload PUT. `requests` applies this to
# connect and to each read, not to the whole transfer, so a 32 MiB chunk is
# not cut off mid-flight on a slow link - only a link that goes silent is.
UPLOAD_TIMEOUT_SECONDS: int = 120


def _split_gs(path: str) -> tuple[str, str]:
    path = path.removeprefix("gs://")
    bucket = path.split("/", 1)[0]
    return bucket, path[(len(bucket) + 1) :]


class _GcsOutputStream:
    """A write-then-close output that STREAMS large objects.

    Writes accumulate in a buffer; once it holds RESUMABLE_CHUNK_BYTES a
    resumable upload session is opened and full chunks are PUT as they arrive,
    so memory is bounded by the chunk size however large the object grows. An
    object that never reaches one chunk is uploaded in one shot on close,
    exactly as every write did before this class streamed - manifests,
    metadata and small data files see no change in the requests made.

    Bytes leave the buffer only once GCS has acknowledged them (308 with a
    `Range` covering them, or a 200/201 finalising the object). A chunk PUT
    that fails on a retryable status or a transport error is re-sent from the
    offset GCS reports it committed to, never from a guess.

    `abort()` cancels an open session. A stream that raised part-way is not
    left half-uploaded: the caller (see `Dataset.open_data_file_writer`) aborts
    it, and an unfinished resumable session never becomes an object.
    """

    def __init__(
        self,
        path: str,
        session: requests.Session,
        access_token_getter: Callable[[], str],
        chunk_bytes: int = RESUMABLE_CHUNK_BYTES,
    ):
        if chunk_bytes <= 0 or chunk_bytes % _RESUMABLE_CHUNK_QUANTUM != 0:
            raise ValueError(
                f"chunk_bytes must be a positive multiple of {_RESUMABLE_CHUNK_QUANTUM}, "
                f"got {chunk_bytes}"
            )
        self._path = path
        self._session = session
        self._access_token_getter = access_token_getter
        self._chunk_bytes = chunk_bytes
        self._buffer = bytearray()
        self._session_uri: str | None = None
        self._committed = 0  # bytes GCS has acknowledged
        self._closed = False

    # -- the file-like surface --------------------------------------------------

    def write(self, data: bytes | bytearray | memoryview) -> int:
        if self._closed:
            raise ValueError(f"write to closed output '{self._path}'")
        self._buffer.extend(data)
        while len(self._buffer) >= self._chunk_bytes:
            self._put_chunk(final=False)
        return len(data)

    def close(self) -> None:
        if self._closed:
            return
        if self._session_uri is None:
            self._single_shot_upload(bytes(self._buffer))
        else:
            self._put_chunk(final=True)
        self._closed = True
        self._buffer = bytearray()

    def abort(self) -> None:
        """Discard everything: cancel the session if one was opened.

        Best-effort on the wire - the caller is already handling a failure
        and a cancel that fails changes nothing about that. A session left
        uncancelled expires on its own and never becomes an object.
        """
        if self._closed:
            return
        self._closed = True
        self._buffer = bytearray()
        if self._session_uri is None:
            return
        try:
            self._session.delete(self._session_uri, timeout=10)
        except requests.RequestException:
            logger.warning("Could not cancel resumable upload of '%s'", self._path)

    # -- single-shot (small object) ----------------------------------------------

    def _single_shot_upload(self, data: bytes) -> None:
        bucket, object_name = _split_gs(self._path)
        url = f"https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o"

        token = self._access_token_getter()
        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(data)),
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        response = self._session.post(
            url,
            params={"uploadType": "media", "name": object_name},
            headers=headers,
            data=data,
            timeout=60,
        )

        if response.status_code not in (200, 201):
            raise OSError(
                f"Failed to write '{self._path}' - status {response.status_code}: {response.text}"
            )

    # -- resumable session -----------------------------------------------------

    def _auth_headers(self) -> dict:
        token = self._access_token_getter()
        return {"Authorization": f"Bearer {token}"} if token else {}

    def _start_session(self) -> None:
        bucket, object_name = _split_gs(self._path)
        url = f"https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o"
        last_response = None
        last_error = None
        for attempt in range(MAX_ATTEMPTS):
            if attempt:
                time.sleep(_backoff_seconds(last_response, attempt - 1))
            headers = self._auth_headers()
            headers["X-Upload-Content-Type"] = "application/octet-stream"
            headers["Content-Length"] = "0"
            try:
                last_error = None
                last_response = self._session.post(
                    url,
                    params={"uploadType": "resumable", "name": object_name},
                    headers=headers,
                    timeout=30,
                )
            except requests.RequestException as err:
                last_response = None
                last_error = err
                continue
            if last_response.status_code in (200, 201):
                location = last_response.headers.get("Location")
                if not location:
                    raise OSError(
                        f"Failed to start upload of '{self._path}': no session URI returned"
                    )
                self._session_uri = location
                return
            if last_response.status_code not in RETRYABLE_STATUSES:
                break
        if last_error is not None:
            raise OSError(
                f"Failed to start upload of '{self._path}' after {MAX_ATTEMPTS} attempts: "
                f"{last_error}"
            ) from last_error
        raise OSError(
            f"Failed to start upload of '{self._path}' - status "
            f"{last_response.status_code}: {last_response.text[:500]}"
        )

    def _query_committed(self) -> int | None:
        """Ask the session how much it holds. None means the object is finished."""
        headers = self._auth_headers()
        headers["Content-Range"] = "bytes */*"
        headers["Content-Length"] = "0"
        response = self._session.put(self._session_uri, headers=headers, timeout=30)
        if response.status_code in (200, 201):
            return None
        if response.status_code != 308:
            raise OSError(
                f"Failed to query upload of '{self._path}' - status "
                f"{response.status_code}: {response.text[:500]}"
            )
        header = response.headers.get("Range")
        if not header:
            return 0
        # "bytes=0-N": N is the last byte held, inclusive.
        return int(header.split("-", 1)[1]) + 1

    def _put_chunk(self, final: bool) -> None:
        if self._session_uri is None:
            self._start_session()

        chunk = bytes(self._buffer) if final else bytes(self._buffer[: self._chunk_bytes])
        start = self._committed
        end_exclusive = start + len(chunk)
        total = str(end_exclusive) if final else "*"

        last_response = None
        last_error = None
        for attempt in range(MAX_ATTEMPTS):
            if attempt:
                time.sleep(_backoff_seconds(last_response, attempt - 1))
                # Resend only what GCS has not got. It reports the offset; we
                # never assume the failed PUT landed nothing (or everything).
                held = self._query_committed()
                if held is None:
                    self._committed = end_exclusive
                    del self._buffer[: len(chunk)]
                    return
                if held < start or held > end_exclusive:
                    raise OSError(
                        f"Upload of '{self._path}' desynchronised: GCS holds {held} bytes, "
                        f"this chunk spans {start}-{end_exclusive}"
                    )
                start = held

            body = chunk[start - self._committed :]
            headers = self._auth_headers()
            headers["Content-Length"] = str(len(body))
            if len(body) == 0:
                # Only reachable on a final, empty flush (the object ended
                # exactly on a chunk boundary, or is empty): tell the session
                # the total and let it finalise.
                headers["Content-Range"] = f"bytes */{total}"
            else:
                headers["Content-Range"] = f"bytes {start}-{end_exclusive - 1}/{total}"
            try:
                last_error = None
                last_response = self._session.put(
                    self._session_uri, headers=headers, data=body, timeout=UPLOAD_TIMEOUT_SECONDS
                )
            except requests.RequestException as err:
                last_response = None
                last_error = err
                logger.warning(
                    "Upload chunk of '%s' failed on attempt %d/%d: %s",
                    self._path,
                    attempt + 1,
                    MAX_ATTEMPTS,
                    err,
                )
                continue

            status = last_response.status_code
            if (final and status in (200, 201)) or (not final and status == 308):
                self._committed = end_exclusive
                del self._buffer[: len(chunk)]
                return
            if status not in RETRYABLE_STATUSES and status != 308:
                break
            if status == 308 and final:
                # Finalising PUT answered "incomplete": the next attempt asks
                # for the committed offset and resends the remainder.
                logger.warning(
                    "Final upload chunk of '%s' answered 308 on attempt %d/%d",
                    self._path,
                    attempt + 1,
                    MAX_ATTEMPTS,
                )
                continue
            logger.warning(
                "Upload chunk of '%s' returned %d on attempt %d/%d",
                self._path,
                status,
                attempt + 1,
                MAX_ATTEMPTS,
            )

        if last_error is not None:
            raise OSError(
                f"Failed to write '{self._path}' after {MAX_ATTEMPTS} attempts: {last_error}"
            ) from last_error
        raise OSError(
            f"Failed to write '{self._path}' - status {last_response.status_code}: "
            f"{last_response.text[:500]}"
        )


class _GcsInputFile(InputFile):
    """An InputFile whose bytes are fetched LAZILY, on first ``open()``.

    Construction used to perform the download, which made ``new_input`` itself
    a full-object transfer - so code paths that constructed an input and never
    read it (or only wanted existence) paid for the whole object. Every caller
    that splits construction from open wraps both in one try block, so read
    errors surfacing at ``open()`` instead of ``new_input()`` reach the same
    handlers.
    """

    def __init__(
        self,
        location: str,
        session: requests.Session,
        access_token_getter: Callable[[], str],
        cache: _ByteBudgetLRU | None = None,
    ):
        super().__init__(location, None)
        self._session = session
        self._access_token_getter = access_token_getter
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

        try:
            stream = _GcsInputStream(self.location, self._session, self._access_token_getter)
            data = stream.read()
        except FileNotFoundError:
            # A genuinely absent object is represented as content-less, which is
            # what `InputFile.open()` turns back into a `FileNotFoundError` at
            # the point of read. A `StorageReadError` is deliberately NOT caught
            # here: swallowing it produced a content-less InputFile whose later
            # `open()` reported the object missing, discarding the status code
            # and body that said why the read actually failed.
            self.absent_reason = FETCH_404
            return

        # Add to cache (the cache itself declines oversized objects and
        # evicts to stay inside its entry and byte budgets)
        if self._cache is not None:
            self._cache.put(self.location, data)
        self._content = data

    def open(self):
        self._fetch()
        return super().open()


class _GcsOutputFile(OutputFile):
    def __init__(
        self, location: str, session: requests.Session, access_token_getter: Callable[[], str]
    ):
        super().__init__(location)
        self._location = location
        self._session = session
        self._access_token_getter = access_token_getter

    def create(self):
        return _GcsOutputStream(self._location, self._session, self._access_token_getter)


class GcsFileIO(FileIO):
    """Optimized HTTP-backed GCS FileIO.

    Implements a blackhole/capture pattern for manifest files and exposes
    `new_input`, `new_output`, `delete`, `exists`.
    """

    def __init__(self):
        # Track manifest paths and captured manifests
        self.manifest_paths: list[str] = []
        self.captured_manifests: list[tuple[str, bytes]] = []

        # LRU cache for read operations, bounded by entries AND bytes
        self._read_cache = _ByteBudgetLRU()

        # Prepare requests session and set up credential refresh helper (token may expire)
        self._credentials = _get_storage_credentials()
        self._access_token = None

        def _refresh_credentials():
            try:
                if not self._credentials.valid:
                    req = Request()
                    self._credentials.refresh(req)
                self._access_token = self._credentials.token
            except Exception as e:
                # Raised, not warned-and-nulled. A None token does not stop the
                # request: it goes out with no Authorization header, and a
                # private bucket answers 403 - which reads as a permissions
                # problem and sends whoever is holding the pager into the IAM
                # console, where they find the service account's grants are
                # perfectly correct. Failing here names the real cause once,
                # instead of disguising it as a different failure on every
                # subsequent read.
                self._access_token = None
                raise CredentialsUnavailable(f"Could not obtain GCS credentials: {e}") from e

        self._refresh_credentials = _refresh_credentials

        def get_access_token():
            # Refresh credentials on demand to avoid using expired tokens
            self._refresh_credentials()
            if not self._access_token and not os.environ.get("STORAGE_EMULATOR_HOST"):
                # A refresh that "succeeded" and produced nothing is the same
                # unauthenticated request by a quieter route. The emulator is
                # the one place an empty token is legitimate - it runs on
                # AnonymousCredentials by design (see _get_storage_credentials).
                raise CredentialsUnavailable(
                    "GCS credentials resolved to an empty access token; requests "
                    "would be sent unauthenticated"
                )
            return self._access_token

        self.get_access_token = get_access_token

        self._session = requests.session()
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self._session.mount("https://", adapter)

    def new_input(self, location: str) -> InputFile:
        return _GcsInputFile(location, self._session, self.get_access_token, self._read_cache)

    def new_output(self, location: str) -> OutputFile:
        logger.info(f"new_output -> {location}")

        # Invalidate cache entry if present
        self._read_cache.pop(location, None)

        return _GcsOutputFile(location, self._session, self.get_access_token)

    def delete(self, location: str | InputFile | OutputFile) -> None:
        if isinstance(location, (InputFile, OutputFile)):
            location = location.location

        # Invalidate cache entry if present
        self._read_cache.pop(location, None)

        path = location
        path = path.removeprefix("gs://")

        bucket = path.split("/", 1)[0]
        object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
        url = f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{object_full_path}"

        token = self.get_access_token()
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        response = self._session.delete(url, headers=headers, timeout=10)

        if response.status_code not in (204, 404):
            raise OSError(f"Failed to delete '{location}' - status {response.status_code}")

    def exists(self, location: str) -> bool:
        path = location
        path = path.removeprefix("gs://")

        bucket = path.split("/", 1)[0]
        object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
        url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

        token = self.get_access_token()
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        response = self._session.head(url, headers=headers, timeout=10)

        if response.status_code == 200:
            return True
        if response.status_code == 404:
            return False

        # Every other status is a failure to answer the question, not an answer
        # of "no". Returning False for a 403 tells a caller the object is absent
        # when it is sitting there unread, and the GC paths act on absence.
        raise StorageReadError(
            f"Unable to determine whether '{location}' exists - "
            f"status {response.status_code}: {response.text[:500]}",
            status=response.status_code,
            body=response.text[:500],
        )

    def copy(self, source: str, destination: str) -> None:
        """Copy one object to another location WITHOUT moving its bytes through here.

        GCS's rewrite endpoint does the copy inside the storage service, so the
        cost to this process is one request per chunk regardless of object size.
        Reading the source and writing it back through `new_input`/`new_output`
        would be correct but would pull every byte down and push it back up - at
        data-file sizes, on a path whose whole job is bulk copying, that is the
        difference between a copy that scales and one that does not.

        A rewrite is not always complete in one call: for a large object the
        service does as much as it wants to, returns `done: false` and a
        `rewriteToken`, and expects to be called again with it. The loop is the
        API contract, not a retry.
        """
        source_bucket, source_object = _split_gs(source.removeprefix("gs://"))
        destination_bucket, destination_object = _split_gs(destination.removeprefix("gs://"))

        # Invalidate any cached read of the destination - it is about to change.
        self._read_cache.pop(destination, None)

        url = (
            f"https://storage.googleapis.com/storage/v1/b/{source_bucket}/o/"
            f"{urllib.parse.quote(source_object, safe='')}/rewriteTo/b/{destination_bucket}/o/"
            f"{urllib.parse.quote(destination_object, safe='')}"
        )

        rewrite_token = None
        while True:
            params = {"rewriteToken": rewrite_token} if rewrite_token else {}
            response = None
            last_error = None
            for attempt in range(MAX_ATTEMPTS):
                token = self.get_access_token()
                headers = {"Content-Length": "0"}
                if token:
                    headers["Authorization"] = f"Bearer {token}"
                try:
                    response = self._session.post(url, headers=headers, params=params, timeout=60)
                except requests.RequestException as err:  # noqa: PERF203 - retry boundary
                    last_error = err
                    response = None
                if response is not None and response.status_code not in RETRYABLE_STATUSES:
                    break
                if attempt + 1 < MAX_ATTEMPTS:
                    time.sleep(_backoff_seconds(response, attempt))
            if response is None:
                raise OSError(f"Failed to copy '{source}' to '{destination}': {last_error}")
            if response.status_code != 200:
                raise OSError(
                    f"Failed to copy '{source}' to '{destination}' - "
                    f"status {response.status_code}: {response.text[:500]}"
                )

            payload = response.json()
            if payload.get("done"):
                return
            rewrite_token = payload.get("rewriteToken")
            if not rewrite_token:
                # Not done and nothing to continue with: the copy is incomplete
                # and there is no way to finish it. Returning here would leave a
                # truncated object that reads as a successful copy.
                raise OSError(
                    f"Copy of '{source}' to '{destination}' did not complete and "
                    "returned no continuation token"
                )

    def list_files(self, prefix: str) -> list:
        """List files under a storage prefix (gs://bucket/path).

        This uses the google-cloud-storage client as a fallback so callers that
        expect a `list_files`/`ls` API (used by deep-clean/expiration) will work
        regardless of which FileIO implementation is attached to the catalog.
        Returns a list of fully-qualified `gs://` object URIs.
        """
        try:
            if prefix and prefix.startswith("gs://"):
                from google.cloud import storage

                _, rest = prefix.split("://", 1)
                parts = rest.split("/", 1)
                bucket_name = parts[0]
                object_prefix = parts[1] if len(parts) > 1 else ""

                client = storage.Client()
                blobs = client.list_blobs(bucket_name, prefix=object_prefix)
                return [f"gs://{bucket_name}/{b.name}" for b in blobs]
        except Exception:
            # Be conservative: on any failure return empty list so callers
            # (deep-clean / expiration) can continue without crashing. Both
            # treat an empty listing as ambiguous, never as "nothing is
            # orphaned", so this cannot cause a deletion.
            logger.warning("Listing %s failed; reporting no files", prefix, exc_info=True)
            return []

        return []

    # alias
    ls = list_files

    def list_files_with_stats(self, prefix: str) -> dict:
        """List files under a prefix with each object's age in ms and size.

        Used to safety-gate destructive orphan cleanup: a data file can be
        uploaded to storage moments before its snapshot's manifest commit
        lands, so a file must be old enough that it can't still be
        mid-write before it's eligible for deletion (mirrors the age check
        already applied to orphaned manifest files).

        The size rides along because the listing already carries it - one
        `list_blobs` response holds both `time_created` and `size`, so the
        bytes an expiration run reclaims cost no request they weren't already
        making. Sizes taken here are the true on-disk size at deletion time,
        unlike a manifest's recorded `file_size_in_bytes`, which is absent for
        files found by physical reconciliation and can be 0 when unrecorded.

        Returns {uri: (age_ms, size_bytes)}. An object whose creation time
        can't be determined is omitted rather than guessed, so callers treat it
        as "not provably old enough" and leave it alone; a missing size is
        reported as 0, which understates a tally but never protects or condemns
        a file, since only the age gates deletion.
        """
        try:
            if prefix and prefix.startswith("gs://"):
                import time as _time

                from google.cloud import storage

                _, rest = prefix.split("://", 1)
                parts = rest.split("/", 1)
                bucket_name = parts[0]
                object_prefix = parts[1] if len(parts) > 1 else ""

                client = storage.Client()
                blobs = client.list_blobs(bucket_name, prefix=object_prefix)
                now_ms = int(_time.time() * 1000)
                stats = {}
                for b in blobs:
                    if b.time_created is None:
                        continue
                    uri = f"gs://{bucket_name}/{b.name}"
                    stats[uri] = (
                        now_ms - int(b.time_created.timestamp() * 1000),
                        int(b.size or 0),
                    )
                return stats
        except Exception:
            # No ages means every candidate fails its age gate and is KEPT,
            # so an empty map is the safe direction.
            logger.warning("Could not read object stats under %s", prefix, exc_info=True)
            return {}

    def list_files_with_age_ms(self, prefix: str) -> dict:
        """Ages only, for callers that don't need sizes. See
        `list_files_with_stats`, which this reads from - one listing either
        way."""
        return {uri: age for uri, (age, _) in self.list_files_with_stats(prefix).items()}

        return {}
