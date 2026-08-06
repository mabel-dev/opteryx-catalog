"""Resolving a credential: environment first, then Secret Manager.

Extracted from `github.py` when a second sink needed the same thing. Both the
GitHub token and the Discord webhook URL follow the identical pattern - an
environment variable for local development, a Secret Manager secret when
deployed - and both want the same sticky negative cache, because retrying a
missing secret on a path that only runs when something is already broken is not
worth the latency.

Nothing here raises. A credential that cannot be resolved disables its sink and
logs; the stdout sink is unaffected and remains the delivery guarantee.
"""

from __future__ import annotations

import logging
import os
import threading

import requests

logger = logging.getLogger(__name__)

_lock = threading.Lock()
# cache key -> resolved value; a key present with None means "looked and failed"
_cache: dict = {}


def reset_cache() -> None:
    """Forget every cached lookup, successful or failed.

    Called by `configure()` and `reset()`. Without it, changing which secret to
    read after one failed lookup was a no-op for the lifetime of the process.
    """
    with _lock:
        _cache.clear()


def _project() -> str | None:
    project = (
        os.environ.get("GCP_PROJECT_ID")
        or os.environ.get("GCP_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
    )
    if project:
        return project
    try:
        response = requests.get(
            "http://metadata.google.internal/computeMetadata/v1/project/project-id",
            headers={"Metadata-Flavor": "Google"},
            timeout=1.0,
        )
        if response.status_code == 200:
            return response.text.strip()
    except Exception:
        return None
    return None


def access_secret(secret_name: str):
    """Read the latest version of a Secret Manager secret in the ambient project."""
    project = _project()
    if not project:
        logger.warning("alerts: no GCP project, cannot read secret '%s'", secret_name)
        return None

    try:
        from google.cloud import secretmanager  # type: ignore
    except ImportError:
        logger.warning(
            "alerts: google-cloud-secret-manager is not installed; "
            "install the 'alerts' extra to use a remote sink"
        )
        return None

    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project}/secrets/{secret_name}/versions/latest"
        return client.access_secret_version(request={"name": name}).payload.data.decode().strip()
    except Exception as exc:
        logger.warning("alerts: could not read secret '%s': %s", secret_name, exc)
        return None


def resolve(env_var: str, secret_name: str):
    """The credential, from `env_var` if set, else Secret Manager `secret_name`.

    Both outcomes are cached, including failure, keyed on the pair - so two sinks
    reading different secrets don't share a verdict.
    """
    key = (env_var, secret_name)
    with _lock:
        if key in _cache:
            return _cache[key]

    value = (os.environ.get(env_var) or "").strip()
    if not value and secret_name:
        value = access_secret(secret_name) or ""

    with _lock:
        _cache[key] = value or None
        return _cache[key]
