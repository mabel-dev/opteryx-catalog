"""Resumable sessions opened for native writers, and compose.

A native writer (the vector index's vectors body, opteryx-core) streams to a session
URI it is handed; Python only opens the session, and afterwards composes the object
from its parts. What is pinned is the request shape and the refusals - the transport
is scripted.
"""

import pytest

from opteryx_catalog.iops import gcs


class _Response:
    def __init__(self, status_code, headers=None, text=""):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text


class _Session:
    def __init__(self, compose_replies=()):
        self.posts = []
        self.deletes = []
        self.compose_replies = list(compose_replies)

    def post(self, url, params=None, headers=None, data=None, json=None, timeout=None):
        self.posts.append({"url": url, "params": params, "headers": headers, "json": json})
        if params and params.get("uploadType") == "resumable":
            return _Response(200, {"Location": "https://upload.example/session-9"})
        if self.compose_replies:
            return self.compose_replies.pop(0)
        return _Response(200)

    def delete(self, url, timeout=None):
        self.deletes.append(url)
        return _Response(499)


@pytest.fixture(autouse=True)
def _no_sleeping(monkeypatch):
    monkeypatch.setattr(gcs.time, "sleep", lambda _seconds: None)


def _io(session):
    io = object.__new__(gcs.GcsFileIO)
    io._session = session
    io._read_cache = gcs._ByteBudgetLRU()
    io.get_access_token = lambda: "tok"
    return io


def test_open_upload_session_returns_the_session_uri():
    session = _Session()
    uri = _io(session).open_upload_session("gs://bucket/ds/index/abc/f-01.vectors.skene.body")
    assert uri == "https://upload.example/session-9"
    (post,) = session.posts
    assert post["params"] == {"uploadType": "resumable", "name": "ds/index/abc/f-01.vectors.skene.body"}
    assert post["headers"]["Authorization"] == "Bearer tok"


def test_cancel_deletes_the_session():
    session = _Session()
    _io(session).cancel_upload_session("https://upload.example/session-9")
    assert session.deletes == ["https://upload.example/session-9"]


def test_compose_names_the_sources_in_order():
    session = _Session()
    _io(session).compose(
        ["gs://bucket/ds/v.skene.prefix", "gs://bucket/ds/v.skene.body"], "gs://bucket/ds/v.skene"
    )
    (post,) = session.posts
    assert post["url"] == "https://storage.googleapis.com/storage/v1/b/bucket/o/ds%2Fv.skene/compose"
    assert [s["name"] for s in post["json"]["sourceObjects"]] == ["ds/v.skene.prefix", "ds/v.skene.body"]


def test_compose_retries_a_retryable_status_then_raises_on_a_refusal():
    session = _Session([_Response(503), _Response(200)])
    _io(session).compose(["gs://bucket/a"], "gs://bucket/b")
    assert len(session.posts) == 2

    session = _Session([_Response(403, text="denied")])
    with pytest.raises(OSError, match="403"):
        _io(session).compose(["gs://bucket/a"], "gs://bucket/b")


@pytest.mark.parametrize(
    "sources, message",
    [
        ([], "1 to 32"),
        ([f"gs://bucket/p{i}" for i in range(33)], "1 to 32"),
        (["gs://other/a"], "destination's bucket"),
    ],
)
def test_compose_refuses_what_gcs_cannot_do(sources, message):
    session = _Session()
    with pytest.raises(ValueError, match=message):
        _io(session).compose(sources, "gs://bucket/b")
    assert session.posts == []
