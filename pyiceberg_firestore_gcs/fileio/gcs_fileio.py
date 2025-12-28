"""
Google Cloud Storage FileIO implementation using Opteryx's optimized I/O.

This implements PyIceberg's FileIO interface but uses Opteryx's optimized
HTTP-based GCS access for 10% better performance than the SDK, with
connection pooling for efficiency.
"""

import io
import os
import urllib.parse
from typing import Union

from pyiceberg.io import FileIO
from pyiceberg.io import InputFile
from pyiceberg.io import InputStream
from pyiceberg.io import OutputFile
from pyiceberg.io import OutputStream
from pyiceberg.typedef import EMPTY_DICT
from pyiceberg.typedef import Properties


def get_storage_credentials():
    """Get GCS credentials."""
    from google.cloud import storage

    if os.environ.get("STORAGE_EMULATOR_HOST"):  # pragma: no cover
        from google.auth.credentials import AnonymousCredentials

        storage_client = storage.Client(credentials=AnonymousCredentials())
    else:  # pragma: no cover
        storage_client = storage.Client()
    return storage_client._credentials


class GcsInputStream(io.BytesIO):
    """
    File-like wrapper for GCS input streams.

    Reads the entire object into memory on open for maximum performance.
    """

    def __init__(self, path: str, session, access_token):
        """Initialize GCS file by reading entire object."""
        # Strip gs:// prefix
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

        # Initialize BytesIO with the content
        super().__init__(response.content)

    @property
    def memoryview(self):
        """Return a memoryview of the file content."""
        return memoryview(self.getbuffer())


class GcsOutputStream(io.BytesIO):
    """
    Output stream for GCS that buffers writes and uploads on close.
    """

    def __init__(self, path: str, session, access_token):
        """Initialize output stream."""
        super().__init__()
        self._path = path
        self._session = session
        self._access_token = access_token
        self._closed = False

    def close(self):
        """Upload buffer to GCS on close."""
        if self._closed:
            return

        # Strip gs:// prefix
        path = self._path
        if path.startswith("gs://"):
            path = path[5:]

        bucket = path.split("/", 1)[0]
        url = f"https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o"

        # Get buffer content
        data = self.getvalue()

        # Extract object name for upload
        object_name = path[(len(bucket) + 1) :]

        # Upload using GCS JSON API
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


class GcsInputFile(InputFile):
    """InputFile implementation for GCS."""

    def __init__(self, location: str, session, access_token):
        super().__init__(location=location)
        self._session = session
        self._access_token = access_token
        self._length = None

    def __len__(self) -> int:
        """Return the total length of the file, in bytes."""
        if self._length is None:
            # Use HEAD request to get size
            path = self.location
            if path.startswith("gs://"):
                path = path[5:]

            bucket = path.split("/", 1)[0]
            object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
            url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

            response = self._session.head(
                url,
                headers={"Authorization": f"Bearer {self._access_token}"},
                timeout=10,
            )

            if response.status_code == 200:
                self._length = int(response.headers.get("content-length", 0))
            else:
                raise FileNotFoundError(f"File not found: {self.location}")

        return self._length

    def exists(self) -> bool:
        """Check whether the file exists."""
        try:
            path = self.location
            if path.startswith("gs://"):
                path = path[5:]

            bucket = path.split("/", 1)[0]
            object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
            url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

            response = self._session.head(
                url,
                headers={"Authorization": f"Bearer {self._access_token}"},
                timeout=10,
            )

            return response.status_code == 200
        except Exception:
            return False

    def open(self, seekable: bool = True) -> InputStream:
        """Open the file for reading."""
        return GcsInputStream(self.location, self._session, self._access_token)


class GcsOutputFile(OutputFile):
    """OutputFile implementation for GCS."""

    def __init__(self, location: str, session, access_token):
        super().__init__(location=location)
        self._session = session
        self._access_token = access_token

    def __len__(self) -> int:
        """Return the total length of the file, in bytes."""
        # For output files that exist, return their size
        try:
            path = self.location
            if path.startswith("gs://"):
                path = path[5:]

            bucket = path.split("/", 1)[0]
            object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
            url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

            response = self._session.head(
                url,
                headers={"Authorization": f"Bearer {self._access_token}"},
                timeout=10,
            )

            if response.status_code == 200:
                return int(response.headers.get("content-length", 0))
        except Exception:
            pass
        return 0

    def exists(self) -> bool:
        """Check whether the file exists."""
        try:
            path = self.location
            if path.startswith("gs://"):
                path = path[5:]

            bucket = path.split("/", 1)[0]
            object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
            url = f"https://storage.googleapis.com/{bucket}/{object_full_path}"

            response = self._session.head(
                url,
                headers={"Authorization": f"Bearer {self._access_token}"},
                timeout=10,
            )

            return response.status_code == 200
        except Exception:
            return False

    def to_input_file(self) -> InputFile:
        """Return an InputFile for this location."""
        return GcsInputFile(self.location, self._session, self._access_token)

    def create(self, overwrite: bool = False) -> OutputStream:
        """Create output stream for writing."""
        if not overwrite and self.exists():
            raise FileExistsError(f"File already exists: {self.location}")

        return GcsOutputStream(self.location, self._session, self._access_token)


class GcsFileIO(FileIO):
    """
    FileIO implementation for GCS using optimized HTTP access.

    Uses direct GCS JSON API calls for 10% better performance than SDK,
    with connection pooling for efficiency.
    """

    def __init__(self, properties: Properties = EMPTY_DICT):
        super().__init__(properties=properties)

        import requests
        from google.auth.transport.requests import Request
        from requests.adapters import HTTPAdapter

        # Get GCS credentials
        self.client_credentials = get_storage_credentials()

        # Cache access tokens for accessing GCS
        if not self.client_credentials.valid:
            request = Request()
            self.client_credentials.refresh(request)
        self.access_token = self.client_credentials.token

        # Create a HTTP connection session to reduce effort for each fetch
        self.session = requests.session()
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self.session.mount("https://", adapter)

    def new_input(self, location: str) -> InputFile:
        """Get an InputFile instance to read bytes from the file at the given location."""
        return GcsInputFile(location, self.session, self.access_token)

    def new_output(self, location: str) -> OutputFile:
        """Get an OutputFile instance to write bytes to the file at the given location."""
        return GcsOutputFile(location, self.session, self.access_token)

    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        """Delete the file at the given path."""
        if isinstance(location, (InputFile, OutputFile)):
            location = location.location

        # Strip gs:// prefix
        path = location
        if path.startswith("gs://"):
            path = path[5:]

        bucket = path.split("/", 1)[0]
        object_full_path = urllib.parse.quote(path[(len(bucket) + 1) :], safe="")
        url = f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{object_full_path}"

        response = self.session.delete(
            url,
            headers={"Authorization": f"Bearer {self.access_token}"},
            timeout=10,
        )

        if response.status_code not in (204, 404):
            raise IOError(f"Failed to delete '{location}' - status {response.status_code}")
