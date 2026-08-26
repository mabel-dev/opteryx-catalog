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


class _GcsOutputStream(io.BytesIO):
    def __init__(
        self, path: str, session: requests.Session, access_token_getter: Callable[[], str]
    ):
        super().__init__()
        self._path = path
        self._session = session
        self._access_token_getter = access_token_getter
        self._closed = False

    def close(self):
        if self._closed:
            return

        path = self._path
        path = path.removeprefix("gs://")

        bucket = path.split("/", 1)[0]
        url = f"https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o"

        data = self.getvalue()
        object_name = path[(len(bucket) + 1) :]

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

        self._closed = True
        super().close()


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

    def list_files_with_age_ms(self, prefix: str) -> dict:
        """List files under a prefix along with each object's age in ms.

        Used to safety-gate destructive orphan cleanup: a data file can be
        uploaded to storage moments before its snapshot's manifest commit
        lands, so a file must be old enough that it can't still be
        mid-write before it's eligible for deletion (mirrors the age check
        already applied to orphaned manifest files).

        Returns {uri: age_ms}. An object whose creation time can't be
        determined is omitted rather than guessed, so callers treat it as
        "not provably old enough" and leave it alone.
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
                ages = {}
                for b in blobs:
                    if b.time_created is None:
                        continue
                    uri = f"gs://{bucket_name}/{b.name}"
                    ages[uri] = now_ms - int(b.time_created.timestamp() * 1000)
                return ages
        except Exception:
            # No ages means every candidate fails its age gate and is KEPT,
            # so an empty map is the safe direction.
            logger.warning("Could not read object ages under %s", prefix, exc_info=True)
            return {}

        return {}
