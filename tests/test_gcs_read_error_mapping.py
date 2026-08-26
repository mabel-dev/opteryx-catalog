"""A failed object read must say why it failed.

Every case here is the same bug seen from a different angle: a non-404 status
was being reported as `FileNotFoundError`, naming a path that was present in the
bucket the whole time. The read path is exercised through `_get_with_retry` and
`_GcsInputFile`, which is where the collapsing happened.
"""

import pytest

from opteryx_catalog.exceptions import StorageReadError
from opteryx_catalog.iops import gcs


class _Response:
    def __init__(self, status_code, content=b"", headers=None):
        self.status_code = status_code
        self.content = content
        self.text = content.decode(errors="replace")
        self.headers = headers or {}


class _Session:
    """Answers each GET with the next scripted response."""

    def __init__(self, *responses):
        self._responses = list(responses)
        self.calls = 0

    def get(self, url, headers=None, timeout=None):
        self.calls += 1
        reply = self._responses.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


@pytest.fixture(autouse=True)
def _no_sleeping(monkeypatch):
    """Backoff is policy, not behaviour under test; don't wait for it."""
    monkeypatch.setattr(gcs.time, "sleep", lambda _seconds: None)


def _read(session):
    return gcs._get_with_retry(
        "https://storage.googleapis.com/bucket/object",
        "gs://bucket/object",
        session,
        lambda: "token",
    )


def test_403_is_not_reported_as_a_missing_file():
    session = _Session(_Response(403, b"does not have storage.objects.get access"))

    with pytest.raises(StorageReadError) as caught:
        _read(session)

    assert not isinstance(caught.value, FileNotFoundError)
    assert caught.value.status == 403
    assert "storage.objects.get" in caught.value.body
    # A permission error is settled on the first answer; asking again wastes a
    # request and delays a message the caller needs now.
    assert session.calls == 1


def test_404_is_a_missing_file():
    session = _Session(_Response(404, b"No such object"))

    with pytest.raises(FileNotFoundError) as caught:
        _read(session)

    assert str(caught.value) == "gs://bucket/object"


def test_throttling_is_retried_then_succeeds():
    session = _Session(_Response(429, b"slow down"), _Response(200, b"payload"))

    assert _read(session).content == b"payload"
    assert session.calls == 2


def test_retries_are_bounded_and_the_last_status_survives():
    session = _Session(*[_Response(503, b"unavailable")] * gcs.MAX_ATTEMPTS)

    with pytest.raises(StorageReadError) as caught:
        _read(session)

    assert caught.value.status == 503
    assert session.calls == gcs.MAX_ATTEMPTS


def test_connection_failure_is_retried_and_reported_without_a_status():
    import requests

    session = _Session(
        requests.ConnectionError("reset by peer"),
        _Response(200, b"payload"),
    )
    assert _read(session).content == b"payload"

    session = _Session(*[requests.ConnectionError("reset by peer")] * gcs.MAX_ATTEMPTS)
    with pytest.raises(StorageReadError) as caught:
        _read(session)
    assert caught.value.status is None


def test_retry_after_header_is_honoured(monkeypatch):
    waits = []
    monkeypatch.setattr(gcs.time, "sleep", waits.append)
    session = _Session(
        _Response(429, b"slow down", headers={"Retry-After": "2"}),
        _Response(200, b"payload"),
    )

    _read(session)
    assert waits == [2.0]


def test_token_is_refetched_for_each_attempt():
    """A read spanning a token expiry must not spend its retries on the dead one."""
    tokens = iter(["stale", "fresh"])
    handed_out = []

    def _token():
        value = next(tokens)
        handed_out.append(value)
        return value

    session = _Session(_Response(503, b"unavailable"), _Response(200, b"payload"))
    gcs._get_with_retry(
        "https://storage.googleapis.com/bucket/object", "gs://bucket/object", session, _token
    )

    assert handed_out == ["stale", "fresh"]


def test_input_file_propagates_a_403_instead_of_yielding_empty_content():
    """The swallow that produced the original misleading traceback.

    `_GcsInputFile` caught the failure and constructed itself content-less; the
    `open()` that followed then raised a bare `FileNotFoundError(location)` with
    the status code and body long gone.
    """
    session = _Session(_Response(403, b"forbidden"))

    # The fetch is lazy (construction is free; new_input no longer downloads),
    # so the failure surfaces at open() - with its status and body intact.
    handle = gcs._GcsInputFile("gs://bucket/object", session, lambda: "token", None)
    with pytest.raises(StorageReadError):
        handle.open()


def test_input_file_still_represents_a_real_404_as_absent():
    session = _Session(_Response(404, b"No such object"))

    handle = gcs._GcsInputFile("gs://bucket/object", session, lambda: "token", None)

    with pytest.raises(FileNotFoundError):
        handle.open()


def test_a_failed_read_is_not_cached():
    """A cached failure would outlive the permission change that caused it."""
    from opteryx_catalog.iops.base import _ByteBudgetLRU

    cache = _ByteBudgetLRU()
    session = _Session(_Response(403, b"forbidden"))

    handle = gcs._GcsInputFile("gs://bucket/object", session, lambda: "token", cache)
    with pytest.raises(StorageReadError):
        handle.open()

    assert len(cache) == 0


class _Credentials:
    """Stands in for google-auth credentials, refreshing however it's told to."""

    def __init__(self, *, valid=False, token="token", refresh_error=None):
        self.valid = valid
        self.token = token
        self._refresh_error = refresh_error

    def refresh(self, _request):
        if self._refresh_error is not None:
            raise self._refresh_error
        self.valid = True


def _file_io(credentials, monkeypatch):
    """A GcsFileIO wired to `credentials`, without touching google-auth."""
    monkeypatch.setattr(gcs, "_get_storage_credentials", lambda: credentials)
    return gcs.GcsFileIO()


def test_a_failed_refresh_is_raised_not_swallowed(monkeypatch):
    """The bug that made a working IAM policy look broken.

    The refresh failure was logged and the token set to None; the request then
    went out with no Authorization header and GCS answered 403. Every reader of
    that 403 - logs, alerts, the person on call - was pointed at permissions.
    """
    from opteryx_catalog.exceptions import CredentialsUnavailable

    io = _file_io(_Credentials(refresh_error=RuntimeError("metadata server timeout")), monkeypatch)

    with pytest.raises(CredentialsUnavailable) as caught:
        io.get_access_token()

    assert "metadata server timeout" in str(caught.value)


def test_an_empty_token_is_refused(monkeypatch):
    """A refresh that reports success and yields nothing is the same anonymous
    request arriving by a quieter route."""
    from opteryx_catalog.exceptions import CredentialsUnavailable

    io = _file_io(_Credentials(token=None), monkeypatch)

    with pytest.raises(CredentialsUnavailable):
        io.get_access_token()


def test_the_emulator_may_have_no_token(monkeypatch):
    """`_get_storage_credentials` hands back AnonymousCredentials against the
    emulator on purpose, so an empty token there is correct."""
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", "localhost:9023")
    io = _file_io(_Credentials(token=None), monkeypatch)

    assert io.get_access_token() is None


def test_a_valid_credential_is_returned(monkeypatch):
    io = _file_io(_Credentials(valid=True, token="a-real-token"), monkeypatch)

    assert io.get_access_token() == "a-real-token"


def test_a_404_and_a_missing_backend_do_not_look_alike():
    """Both are FileNotFoundError; only the message says which.

    They were identical for one evening, and the ambiguity cost hours: a
    catalog with no storage backend and a genuine 404 both raised
    `FileNotFoundError(location)` from the same line, so the logs could not say
    whether anything had gone looking for the object at all.
    """
    from opteryx_catalog.iops.base import FileIO

    with pytest.raises(FileNotFoundError) as never_fetched:
        FileIO().new_input("gs://bucket/object").open()

    session = _Session(_Response(404, b"No such object"))
    handle = gcs._GcsInputFile("gs://bucket/object", session, lambda: "token", None)
    with pytest.raises(FileNotFoundError) as fetched_404:
        handle.open()

    assert "nothing was requested" in str(never_fetched.value)
    assert "HTTP 404" in str(fetched_404.value)
    assert str(never_fetched.value) != str(fetched_404.value)
