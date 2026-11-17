"""S3FileIO output streams against a real S3 API (multipart needs a server
that implements it; hadro is read-only). Point OPTERYX_TEST_S3_ENDPOINT at one,
e.g. SeaweedFS `weed mini` on http://127.0.0.1:8333, with AWS_ACCESS_KEY_ID /
AWS_SECRET_ACCESS_KEY set to its admin identity."""

import os
import uuid

import pytest

ENDPOINT = os.environ.get("OPTERYX_TEST_S3_ENDPOINT")
if not ENDPOINT:
    pytest.skip("OPTERYX_TEST_S3_ENDPOINT is not set", allow_module_level=True)

from botocore.config import Config

from opteryx_catalog.iops.s3 import S3FileIO
from opteryx_catalog.iops.s3 import _S3OutputStream

MiB = 1024 * 1024


@pytest.fixture(scope="module")
def io():
    io = S3FileIO(
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    bucket = f"mp-{uuid.uuid4().hex[:12]}"
    io._client.create_bucket(Bucket=bucket)
    io.bucket = bucket
    yield io
    for key in io.list_files(f"s3://{bucket}/"):
        io.delete(key)
    io._client.delete_bucket(Bucket=bucket)


def _stream(io, key, part_bytes, parts_in_flight=2):
    location = f"s3://{io.bucket}/{key}"
    return location, _S3OutputStream(
        location, io._client, io._upload_executor, part_bytes=part_bytes, parts_in_flight=parts_in_flight
    )


def _payload(size):
    return bytes((i * 31 + 7) & 0xFF for i in range(size))


def _read(io, location):
    return io.new_input(location).open().read()


def test_small_object_is_one_put(io):
    location = f"s3://{io.bucket}/small.bin"
    out = io.new_output(location).create()
    out.write(b"hello ")
    out.write(b"world")
    out.close()
    assert _read(io, location) == b"hello world"
    etag = io._client.head_object(Bucket=io.bucket, Key="small.bin")["ETag"]
    assert "-" not in etag  # not multipart


def test_large_object_streams_as_multipart(io):
    data = _payload(23 * MiB + 12345)
    location, out = _stream(io, "large.bin", part_bytes=5 * MiB)
    # odd-sized chunks, as rugo pushes row groups
    step = 3 * MiB + 77
    for start in range(0, len(data), step):
        out.write(data[start : start + step])
    out.close()
    assert _read(io, location) == data
    etag = io._client.head_object(Bucket=io.bucket, Key="large.bin")["ETag"]
    assert etag.strip('"').endswith("-4")  # 4 parts of >= 5 MiB, the last holds the rest


def test_abort_leaves_no_object_and_no_open_upload(io):
    location, out = _stream(io, "aborted.bin", part_bytes=5 * MiB)
    out.write(_payload(11 * MiB))  # two parts sent
    out.abort()
    assert not io.exists(location)
    uploads = io._client.list_multipart_uploads(Bucket=io.bucket).get("Uploads", [])
    assert [u for u in uploads if u["Key"] == "aborted.bin"] == []


def test_write_after_close_raises(io):
    _, out = _stream(io, "closed.bin", part_bytes=5 * MiB)
    out.write(b"x")
    out.close()
    with pytest.raises(ValueError, match="closed output"):
        out.write(b"y")
