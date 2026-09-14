"""The GCS output stream uploads large objects in resumable chunks.

Before this, every write was buffered whole and sent as one media upload on
close, so a 4 GB data file would have been 4 GB of encoded bytes in memory and
one POST. Now an object that grows past one chunk goes up as it is written.
Small objects keep the single POST they always had - the requests made for a
manifest do not change.

The session is scripted: `post` answers the session start (or the single-shot
upload), `put` answers each chunk. What is pinned is the protocol - offsets,
Content-Range, when the buffer is released - not the transport.
"""

import pytest

from opteryx_catalog.iops import gcs

CHUNK = 256 * 1024  # the smallest legal chunk; keeps the test's payloads small


class _Response:
    def __init__(self, status_code, headers=None, text=""):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text


class _Session:
    def __init__(self):
        self.posts = []
        self.puts = []
        self.deletes = []
        self.put_replies = []  # scripted answers, consumed in order

    def post(self, url, params=None, headers=None, data=None, timeout=None):
        self.posts.append({"url": url, "params": params, "headers": headers, "data": data})
        if params and params.get("uploadType") == "resumable":
            return _Response(200, {"Location": "https://upload.example/session-1"})
        return _Response(200)

    def put(self, url, headers=None, data=None, timeout=None):
        self.puts.append({"url": url, "headers": headers, "data": bytes(data or b"")})
        if self.put_replies:
            reply = self.put_replies.pop(0)
            if isinstance(reply, Exception):
                raise reply
            return reply
        # Default protocol behaviour: 308 for a partial chunk, 200 to finalise.
        rng = headers.get("Content-Range", "")
        if rng.endswith("/*"):
            return _Response(308)
        return _Response(200)

    def delete(self, url, timeout=None):
        self.deletes.append(url)
        return _Response(499)


@pytest.fixture(autouse=True)
def _no_sleeping(monkeypatch):
    monkeypatch.setattr(gcs.time, "sleep", lambda _seconds: None)


def _stream(session, chunk=CHUNK):
    return gcs._GcsOutputStream("gs://bucket/dir/obj.parquet", session, lambda: "tok", chunk)


def test_small_object_is_uploaded_in_one_shot_on_close():
    session = _Session()
    out = _stream(session)
    out.write(b"abc")
    out.write(b"def")
    assert session.posts == []  # nothing leaves before close
    out.close()

    assert len(session.posts) == 1
    post = session.posts[0]
    assert post["params"] == {"uploadType": "media", "name": "dir/obj.parquet"}
    assert post["data"] == b"abcdef"
    assert session.puts == []


def test_large_object_streams_in_chunks_and_finalises_with_the_total():
    session = _Session()
    out = _stream(session)
    payload = bytes(range(256)) * (CHUNK // 256)  # exactly one chunk
    out.write(payload)  # fills one chunk: session starts, chunk 1 goes up
    out.write(payload[: CHUNK // 2])
    out.write(payload[CHUNK // 2 :])  # fills chunk 2
    out.write(b"tail")
    out.close()

    assert [p["params"]["uploadType"] for p in session.posts] == ["resumable"]
    assert session.posts[0]["headers"]["X-Upload-Content-Type"] == "application/octet-stream"

    ranges = [p["headers"]["Content-Range"] for p in session.puts]
    total = 2 * CHUNK + 4
    assert ranges == [
        f"bytes 0-{CHUNK - 1}/*",
        f"bytes {CHUNK}-{2 * CHUNK - 1}/*",
        f"bytes {2 * CHUNK}-{total - 1}/{total}",
    ]
    assert b"".join(p["data"] for p in session.puts) == payload + payload + b"tail"
    assert all(p["url"] == "https://upload.example/session-1" for p in session.puts)


def test_object_ending_on_a_chunk_boundary_finalises_with_an_empty_put():
    session = _Session()
    out = _stream(session)
    out.write(bytes(CHUNK))
    out.close()

    ranges = [p["headers"]["Content-Range"] for p in session.puts]
    assert ranges == [f"bytes 0-{CHUNK - 1}/*", f"bytes */{CHUNK}"]
    assert session.puts[-1]["data"] == b""


def test_a_failed_chunk_is_resent_from_the_offset_gcs_reports():
    session = _Session()
    # Chunk 1: transport error; status query says 64 KiB of it landed; the
    # resend carries the remainder and is accepted.
    session.put_replies = [
        gcs.requests.RequestException("reset"),
        _Response(308, {"Range": f"bytes=0-{64 * 1024 - 1}"}),
        _Response(308),
    ]
    out = _stream(session)
    out.write(bytes(CHUNK))
    out.write(b"x")
    out.close()

    ranges = [p["headers"]["Content-Range"] for p in session.puts]
    assert ranges[0] == f"bytes 0-{CHUNK - 1}/*"
    assert ranges[1] == "bytes */*"  # the status query
    assert ranges[2] == f"bytes {64 * 1024}-{CHUNK - 1}/*"
    assert len(session.puts[2]["data"]) == CHUNK - 64 * 1024
    assert ranges[3] == f"bytes {CHUNK}-{CHUNK}/{CHUNK + 1}"


def test_a_non_retryable_status_raises_and_names_the_object():
    session = _Session()
    session.put_replies = [_Response(403, text="forbidden")]
    out = _stream(session)
    with pytest.raises(OSError, match="gs://bucket/dir/obj.parquet.*403"):
        out.write(bytes(CHUNK))


def test_abort_cancels_an_open_session_and_discards_a_buffer():
    session = _Session()
    out = _stream(session)
    out.write(bytes(CHUNK))  # session open
    out.abort()
    assert session.deletes == ["https://upload.example/session-1"]
    with pytest.raises(ValueError, match="closed"):
        out.write(b"more")

    quiet = _Session()
    small = _stream(quiet)
    small.write(b"abc")
    small.abort()
    assert quiet.posts == [] and quiet.deletes == []  # never started, nothing to cancel


def test_chunk_size_must_be_a_multiple_of_256kib():
    with pytest.raises(ValueError, match="262144"):
        gcs._GcsOutputStream("gs://b/o", _Session(), lambda: "tok", 1000)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
