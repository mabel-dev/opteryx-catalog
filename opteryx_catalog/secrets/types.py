"""Secret types: option schemas, validation at creation, and `SCOPE` matching.

jobs.opteryx docs/design/secrets.md §4 and §4.1. Three types:

- `http_endpoint` - a URL plus optional headers (Slack, PagerDuty, a customer's
  own hook). Outbound delivery is SEC-9 and not built here; the type exists so a
  secret can be created and validated now.
- `gcs_service_account` - a service-account JSON key, for `READ_*` over `gs://`.
- `aws_access_key` - an access key pair, for `READ_*` over `s3://`.

Unknown option keys are an error, never ignored: a typo'd key that is silently
dropped is a secret that does not work and a customer who thinks it does.

`SCOPE` is required on both object-store types and is not a credential - it is
stored in the clear and `SHOW SECRETS` returns it. `scope_admits` is the one
implementation of the bucket-boundary match; the worker's resolver calls it for
the literal path and for every file a glob expands to.

Nothing here ever puts an option VALUE into an exception message. Keys, types
and the scheme of a scope are safe to name; values are not.
"""

from __future__ import annotations

import ipaddress
import json
import re
import socket
from dataclasses import dataclass
from typing import Callable
from urllib.parse import urlsplit

from .errors import SecretInvalid

HTTP_ENDPOINT = "http_endpoint"
GCS_SERVICE_ACCOUNT = "gcs_service_account"
AWS_ACCESS_KEY = "aws_access_key"

SECRET_TYPES = (HTTP_ENDPOINT, GCS_SERVICE_ACCOUNT, AWS_ACCESS_KEY)

# The scheme each object-store type may be pointed at.
OBJECT_STORE_SCHEMES = {GCS_SERVICE_ACCOUNT: "gs", AWS_ACCESS_KEY: "s3"}

# §4: the header allow-list for http_endpoint. Spelled as option keys
# (`HEADER_AUTHORIZATION`), mapped to the header they set.
_HTTP_HEADERS = {
    "HEADER_AUTHORIZATION": "Authorization",
    "HEADER_X_API_KEY": "X-Api-Key",
    "HEADER_X_AUTH_TOKEN": "X-Auth-Token",
}

_MAX_HTTP_PAYLOAD = 8 * 1024
# Object-store payloads carry a service-account key (~2.4 KiB) or a session
# token (up to ~2 KiB for STS); 16 KiB bounds both with room to spare.
_MAX_OBJECT_STORE_PAYLOAD = 16 * 1024

_AWS_KEY_ID = re.compile(r"^(AKIA|ASIA)[A-Z0-9]{16}$")
_AWS_SECRET = re.compile(r"^[A-Za-z0-9/+=]{40}$")
_AWS_REGION = re.compile(r"^[a-z]{2}(-[a-z]+)+-\d+$")
# GCS bucket names; S3's are a subset of this shape for our purposes.
_BUCKET = re.compile(r"^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$")
_GLOB_CHARS = frozenset("*?[]{}")


@dataclass(frozen=True)
class ValidatedSecret:
    """What a CREATE SECRET reduces to once its options are checked.

    `payload` is the credential-bearing part and is what gets sealed; `scope`
    is stored in the clear. `__repr__` names neither the payload's values nor
    its keys' values, so a stray log of this object leaks nothing.
    """

    secret_type: str
    payload: dict
    scope: str | None

    def __repr__(self) -> str:  # pragma: no cover - exercised indirectly
        return f"ValidatedSecret(type={self.secret_type!r}, keys={sorted(self.payload)}, scope={self.scope!r})"


def validate_secret(
    secret_type: str,
    options: dict,
    *,
    resolve_host: Callable[[str], list] | None = None,
) -> ValidatedSecret:
    """Check `options` against `secret_type`'s schema.

    `options` maps option keys (case-insensitive) to string values, `TYPE`
    excluded. `resolve_host` returns the addresses a hostname resolves to; it
    defaults to the system resolver and is injectable for tests.
    """
    if not isinstance(secret_type, str) or secret_type.lower() not in SECRET_TYPES:
        raise SecretInvalid(
            f"unknown secret TYPE {secret_type!r}; expected one of {', '.join(SECRET_TYPES)}"
        )
    secret_type = secret_type.lower()

    normalised: dict[str, str] = {}
    for key, value in (options or {}).items():
        upper = str(key).upper()
        if upper == "TYPE":
            raise SecretInvalid("TYPE is given once, as the secret's type")
        if upper in normalised:
            raise SecretInvalid(f"option {upper} is given more than once")
        if not isinstance(value, str):
            raise SecretInvalid(f"option {upper} must be a string")
        normalised[upper] = value

    if secret_type == HTTP_ENDPOINT:
        return _validate_http_endpoint(normalised, resolve_host or _system_resolve)
    if secret_type == GCS_SERVICE_ACCOUNT:
        return _validate_gcs_service_account(normalised)
    return _validate_aws_access_key(normalised)


def _reject_unknown(secret_type: str, options: dict, allowed) -> None:
    unknown = sorted(set(options) - set(allowed))
    if unknown:
        raise SecretInvalid(
            f"{secret_type} does not take option(s) {', '.join(unknown)}; "
            f"expected {', '.join(sorted(allowed))}"
        )


def _require(secret_type: str, options: dict, key: str) -> str:
    value = options.get(key)
    if value is None or value == "":
        raise SecretInvalid(f"{secret_type} requires {key}")
    return value


def _payload_size(payload: dict) -> int:
    return len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))


# -- http_endpoint ---------------------------------------------------------


def _system_resolve(host: str) -> list:
    return [info[4][0] for info in socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)]


def _validate_http_endpoint(options: dict, resolve_host) -> ValidatedSecret:
    _reject_unknown(HTTP_ENDPOINT, options, ("URL", *_HTTP_HEADERS))
    url = _require(HTTP_ENDPOINT, options, "URL")

    parts = urlsplit(url)
    if parts.scheme != "https":
        raise SecretInvalid("http_endpoint URL must be https")
    if parts.username is not None or parts.password is not None or "@" in parts.netloc:
        raise SecretInvalid("http_endpoint URL must not carry credentials in its userinfo")
    host = parts.hostname
    if not host:
        raise SecretInvalid("http_endpoint URL has no host")

    try:
        addresses = [host] if _is_ip(host) else resolve_host(host)
    except OSError as err:
        raise SecretInvalid("http_endpoint URL's host does not resolve") from err
    if not addresses:
        raise SecretInvalid("http_endpoint URL's host does not resolve")
    for address in addresses:
        ip = ipaddress.ip_address(address)
        if not ip.is_global:
            raise SecretInvalid(
                "http_endpoint URL resolves to a private, loopback or link-local address"
            )

    payload = {"url": url}
    headers = {
        _HTTP_HEADERS[key]: value for key, value in options.items() if key in _HTTP_HEADERS
    }
    if headers:
        payload["headers"] = headers
    if _payload_size(payload) > _MAX_HTTP_PAYLOAD:
        raise SecretInvalid("http_endpoint secret exceeds 8 KiB")
    return ValidatedSecret(HTTP_ENDPOINT, payload, None)


def _is_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    return True


# -- object stores ---------------------------------------------------------


def normalise_scope(secret_type: str, scope: str) -> str:
    """Validate a SCOPE and return its canonical spelling.

    A scope is a prefix, not a glob: `<scheme>://<bucket>/[<prefix>]`. A bare
    bucket is canonicalised with a trailing `/`, which is what makes the match
    respect the bucket boundary - `gs://mabel_logs/` never admits
    `gs://mabel_logs_private/...`.
    """
    expected = OBJECT_STORE_SCHEMES[secret_type]
    if "://" not in scope:
        raise SecretInvalid(f"SCOPE must be a {expected}:// prefix")
    scheme, _, rest = scope.partition("://")
    if scheme.lower() != expected:
        raise SecretInvalid(f"{secret_type} SCOPE must be a {expected}:// prefix")
    if any(ch in _GLOB_CHARS for ch in scope):
        raise SecretInvalid("SCOPE is a prefix, not a glob")
    bucket, slash, prefix = rest.partition("/")
    if not _BUCKET.match(bucket):
        raise SecretInvalid("SCOPE must name a bucket")
    if _has_dot_segment(prefix):
        raise SecretInvalid("SCOPE must not contain '.' or '..' segments")
    if "//" in prefix:
        raise SecretInvalid("SCOPE must not contain empty path segments")
    return f"{expected}://{bucket}/{prefix}"


def _has_dot_segment(path: str) -> bool:
    return any(segment in (".", "..") for segment in path.split("/"))


def scope_admits(scope: str, path: str) -> bool:
    """Whether `path` falls under `scope`, on a bucket boundary.

    `scope` is a stored, already-canonical scope. The path's scheme and bucket
    must match exactly; its object key must start with the scope's prefix. A
    path with `.`/`..` segments or empty segments is refused outright rather
    than normalised: object stores do not resolve them, but anything between
    here and the store that does would turn a prefix check into a suggestion.
    """
    if not scope or not path or "://" not in path:
        return False
    scope_scheme, _, scope_rest = scope.partition("://")
    path_scheme, _, path_rest = path.partition("://")
    if path_scheme.lower() != scope_scheme:
        return False
    scope_bucket, _, scope_prefix = scope_rest.partition("/")
    path_bucket, slash, path_key = path_rest.partition("/")
    if path_bucket != scope_bucket or not slash:
        return False
    if _has_dot_segment(path_key) or "//" in path_key:
        return False
    return path_key.startswith(scope_prefix)


def _validate_scope(secret_type: str, options: dict) -> str:
    return normalise_scope(secret_type, _require(secret_type, options, "SCOPE"))


def _validate_gcs_service_account(options: dict) -> ValidatedSecret:
    _reject_unknown(GCS_SERVICE_ACCOUNT, options, ("KEY", "SCOPE"))
    scope = _validate_scope(GCS_SERVICE_ACCOUNT, options)
    raw = _require(GCS_SERVICE_ACCOUNT, options, "KEY")
    try:
        key = json.loads(raw)
    except ValueError as err:
        raise SecretInvalid("gcs_service_account KEY is not valid JSON") from None
    if not isinstance(key, dict) or key.get("type") != "service_account":
        raise SecretInvalid('gcs_service_account KEY must be a JSON key with "type": "service_account"')
    for field in ("client_email", "private_key", "token_uri"):
        if not isinstance(key.get(field), str) or not key[field]:
            raise SecretInvalid(f"gcs_service_account KEY is missing {field}")
    payload = {"key": key}
    if _payload_size(payload) > _MAX_OBJECT_STORE_PAYLOAD:
        raise SecretInvalid("gcs_service_account secret exceeds 16 KiB")
    return ValidatedSecret(GCS_SERVICE_ACCOUNT, payload, scope)


def _validate_aws_access_key(options: dict) -> ValidatedSecret:
    _reject_unknown(
        AWS_ACCESS_KEY,
        options,
        ("ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "SESSION_TOKEN", "REGION", "SCOPE"),
    )
    scope = _validate_scope(AWS_ACCESS_KEY, options)
    key_id = _require(AWS_ACCESS_KEY, options, "ACCESS_KEY_ID")
    secret = _require(AWS_ACCESS_KEY, options, "SECRET_ACCESS_KEY")
    if not _AWS_KEY_ID.match(key_id):
        raise SecretInvalid("aws_access_key ACCESS_KEY_ID does not have the shape of an AWS key id")
    if not _AWS_SECRET.match(secret):
        raise SecretInvalid("aws_access_key SECRET_ACCESS_KEY does not have the shape of an AWS secret key")
    payload = {"access_key_id": key_id, "secret_access_key": secret}
    token = options.get("SESSION_TOKEN")
    if token:
        payload["session_token"] = token
    elif key_id.startswith("ASIA"):
        raise SecretInvalid("aws_access_key with a temporary (ASIA) key id requires SESSION_TOKEN")
    region = options.get("REGION")
    if region:
        if not _AWS_REGION.match(region):
            raise SecretInvalid("aws_access_key REGION is not an AWS region name")
        payload["region"] = region
    if _payload_size(payload) > _MAX_OBJECT_STORE_PAYLOAD:
        raise SecretInvalid("aws_access_key secret exceeds 16 KiB")
    return ValidatedSecret(AWS_ACCESS_KEY, payload, scope)
