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

    with pytest.raises(StorageReadError):
        gcs._GcsInputFile("gs://bucket/object", session, lambda: "token", None)


def test_input_file_still_represents_a_real_404_as_absent():
    session = _Session(_Response(404, b"No such object"))

    handle = gcs._GcsInputFile("gs://bucket/object", session, lambda: "token", None)

    with pytest.raises(FileNotFoundError):
        handle.open()


def test_a_failed_read_is_not_cached():
    """A cached failure would outlive the permission change that caused it."""
    cache = {}
    session = _Session(_Response(403, b"forbidden"))

    with pytest.raises(StorageReadError):
        gcs._GcsInputFile("gs://bucket/object", session, lambda: "token", cache)

    assert cache == {}
