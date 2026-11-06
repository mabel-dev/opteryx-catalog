"""Process-wide Google Cloud clients, shared by every catalog handle.

Services build an `OpteryxCatalog` per request (often several), and each one
used to construct its own `firestore.Client` and `storage.Client`. A new client
means credential discovery, a new access-token fetch, and a new gRPC/TLS
channel on its first call - a fixed cost paid again on every request, and the
largest part of a cheap request's latency. These clients are thread-safe and
hold no workspace state, so one per process is correct.

What is shared is the CONNECTION, never a catalog handle. A handle's
constructor reads `$properties` to decide whether its workspace still exists;
caching handles would let a dropped workspace keep answering until the process
restarted. That read stays per handle.

The cache key carries three things besides the obvious arguments:

- the process id: a gRPC channel opened before `fork()` is unusable in the
  child, so a forked worker (gunicorn `--preload`, multiprocessing) builds its
  own instead of inheriting a dead one;
- the client class object itself: tests here and in the service repos patch
  `firestore.Client` / `storage.Client` per test, and each patch is a new
  object, so a patched factory never receives a client another test cached;
- for storage, whether the emulator is configured, which changes credentials.
"""

from __future__ import annotations

import os
import threading
from typing import Any

from google.cloud import firestore
from google.cloud import storage

_lock = threading.Lock()
_clients: dict[tuple, Any] = {}


def _shared(key: tuple, build):
    client = _clients.get(key)
    if client is not None:
        return client
    with _lock:
        client = _clients.get(key)
        if client is None:
            client = build()
            _clients[key] = client
        return client


def shared_firestore_client(project: str | None = None, database: str | None = None):
    """The process's Firestore client for (`project`, `database`)."""
    factory = firestore.Client
    key = ("firestore", os.getpid(), factory, project, database)
    return _shared(key, lambda: factory(project=project, database=database))


def shared_storage_client():
    """The process's GCS client, anonymous when the storage emulator is set."""
    factory = storage.Client
    emulator = os.environ.get("STORAGE_EMULATOR_HOST")
    key = ("storage", os.getpid(), factory, emulator)

    def build():
        if emulator:
            from google.auth.credentials import AnonymousCredentials

            return factory(credentials=AnonymousCredentials())
        return factory()

    return _shared(key, build)


def clear_shared_clients() -> None:
    """Forget every cached client. For tests; services never need this."""
    with _lock:
        _clients.clear()
