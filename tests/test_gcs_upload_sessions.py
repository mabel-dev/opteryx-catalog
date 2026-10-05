"""Resumable sessions opened for native writers.

A native writer (the vector index file, opteryx-core) streams to a session URI it is
handed and finishes the session itself; Python only opens it. What is pinned is the
request shape - the transport is scripted.
"""

import pytest

from opteryx_catalog.iops import gcs


class _Response:
    def __init__(self, status_code, headers=None, text=""):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text


class _Session:
    def __init__(self):
        self.posts = []
        self.deletes = []

    def post(self, url, params=None, headers=None, data=None, json=None, timeout=None):
        self.posts.append({"url": url, "params": params, "headers": headers, "json": json})
        if params and params.get("uploadType") == "resumable":
            return _Response(200, {"Location": "https://upload.example/session-9"})
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
    uri = _io(session).open_upload_session("gs://bucket/ds/index/abc/f-01.vidx")
    assert uri == "https://upload.example/session-9"
    (post,) = session.posts
    assert post["params"] == {"uploadType": "resumable", "name": "ds/index/abc/f-01.vidx"}
    assert post["headers"]["Authorization"] == "Bearer tok"


def test_cancel_deletes_the_session():
    session = _Session()
    _io(session).cancel_upload_session("https://upload.example/session-9")
    assert session.deletes == ["https://upload.example/session-9"]
