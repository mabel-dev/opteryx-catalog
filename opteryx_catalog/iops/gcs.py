"""
Optimized GCS FileIO for opteryx_catalog.iops

Adapted from pyiceberg_firestore_gcs.fileio.gcs_fileio to provide a fast
HTTP-backed GCS implementation without depending on pyiceberg types.
"""

import io
import logging
import os
import urllib.parse
from typing import Union

import requests
from google.auth.transport.requests import Request
from requests.adapters import HTTPAdapter

from .base import FileIO
from .base import InputFile
from .base import OutputFile

logger = logging.getLogger(__name__)


def _get_storage_credentials():
    from google.cloud import storage

    if os.environ.get("STORAGE_EMULATOR_HOST"):
        from google.auth.credentials import AnonymousCredentials

        storage_client = storage.Client(credentials=AnonymousCredentials())
    else:
        storage_client = storage.Client()
    return storage_client._credentials


class _GcsInputStream(io.BytesIO):
    def __init__(self, path: str, session: requests.Session, access_token: str):
        # Strip gs://
        if path.startswith("gs://"):
            path = path[5:]
        bucket = path.split("/", 1)[0]
        object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
        url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

        response = session.get(
            url,
            headers={"Authorization": f"Bearer {access_token}", "Accept-Encoding": "identity"},
            timeout=30,
        )

        if response.status_code != 200:
            raise FileNotFoundError(f"Unable to read '{path}' - status {response.status_code}")

        super().__init__(response.content)


class _GcsOutputStream(io.BytesIO):
    def __init__(self, path: str, session: requests.Session, access_token: str):
        super().__init__()
        self._path = path
        self._session = session
        self._access_token = access_token
        self._closed = False

    def close(self):
        if self._closed:
            return

        path = self._path
        if path.startswith("gs://"):
            path = path[5:]

        bucket = path.split("/", 1)[0]
        url = f"https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o"

        data = self.getvalue()
        object_name = path[(len(bucket) + 1) :]

        response = self._session.post(
            url,
            params={"uploadType": "media", "name": object_name},
            headers={
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(data)),
            },
            data=data,
            timeout=60,
        )

        if response.status_code not in (200, 201):
            raise IOError(
                f"Failed to write '{self._path}' - status {response.status_code}: {response.text}"
            )

        self._closed = True
        super().close()


class _GcsInputFile(InputFile):
    def __init__(self, location: str, session: requests.Session, access_token: str):
        # read entire bytes via optimized session
        try:
            stream = _GcsInputStream(location, session, access_token)
            data = stream.read()
            super().__init__(location, data)
        except FileNotFoundError:
            super().__init__(location, None)


class _GcsOutputFile(OutputFile):
    def __init__(self, location: str, session: requests.Session, access_token: str):
        super().__init__(location)
        self._location = location
        self._session = session
        self._access_token = access_token

    def create(self):
        return _GcsOutputStream(self._location, self._session, self._access_token)


class GcsFileIO(FileIO):
    """Optimized HTTP-backed GCS FileIO.

    Implements a blackhole/capture pattern for manifest files and exposes
    `new_input`, `new_output`, `delete`, `exists`.
    """

    def __init__(self):
        # Track manifest paths and captured manifests
        self.manifest_paths: list[str] = []
        self.captured_manifests: list[tuple[str, bytes]] = []

        # Prepare requests session and access token
        self._credentials = _get_storage_credentials()
        if not self._credentials.valid:
            req = Request()
            self._credentials.refresh(req)
        self._access_token = self._credentials.token

        self._session = requests.session()
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self._session.mount("https://", adapter)

    def new_input(self, location: str) -> InputFile:
        return _GcsInputFile(location, self._session, self._access_token)

    def new_output(self, location: str) -> OutputFile:
        logger.info(f"new_output -> {location}")

        return _GcsOutputFile(location, self._session, self._access_token)

    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        if isinstance(location, (InputFile, OutputFile)):
            location = location.location

        path = location
        if path.startswith("gs://"):
            path = path[5:]

        bucket = path.split("/", 1)[0]
        object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
        url = f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{object_full_path}"

        response = self._session.delete(
            url, headers={"Authorization": f"Bearer {self._access_token}"}, timeout=10
        )

        if response.status_code not in (204, 404):
            raise IOError(f"Failed to delete '{location}' - status {response.status_code}")

    def exists(self, location: str) -> bool:
        path = location
        if path.startswith("gs://"):
            path = path[5:]

        bucket = path.split("/", 1)[0]
        object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
        url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

        response = self._session.head(
            url, headers={"Authorization": f"Bearer {self._access_token}"}, timeout=10
        )
        return response.status_code == 200
