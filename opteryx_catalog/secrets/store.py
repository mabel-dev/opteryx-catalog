"""Customer secrets on the catalog: `catalogs/{workspace}/$secrets/items/{name}`.

jobs.opteryx docs/design/secrets.md §5-§9. Mixed into `OpteryxCatalog`, so a
secret is addressed the way a task is - through the workspace's own catalog
handle - and there is no query shape that reaches another workspace's secrets.

The four methods are the whole surface:

- `create_secret` - validate, seal under the billing account's KEK, write.
- `drop_secret`   - delete.
- `list_secrets` / `get_secret` - every field EXCEPT the key material.
- `use_secret`    - decrypt for one use, stamp `last-used-at-ms` / `use-count`,
                    write an audit record. Returns a `SecretMaterial`, which
                    refuses to be printed or pickled.

There is no method that returns a plaintext value to a caller that has not
said what it is using it for, and nothing here logs a value.

Authorisation is NOT checked here. The workspace-level manage right (§9) is
the caller's to establish - jobs.opteryx at submission for CREATE, the engine's
binder for DROP and SHOW, the worker's resolver for use - exactly as the
catalog's task methods leave AUTOMATE to their callers.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

from ..audit import emit_audit
from ..resource_types import ResourceType
from ..security import kms
from .errors import SecretAlreadyExists
from .errors import SecretInvalid
from .errors import SecretKeyUnavailable
from .errors import SecretNotFound
from .errors import SecretRefused
from .types import OBJECT_STORE_SCHEMES
from .types import scope_admits
from .types import validate_secret

logger = logging.getLogger(__name__)

SECRETS_DOCUMENT = "$secrets"
SECRETS_SUBCOLLECTION = "items"

# The key ring holding one KEK per billing account (§6), as a full resource
# name: projects/{p}/locations/{l}/keyRings/customer-secrets. Configuration,
# not code, because it names a production KMS object.
KEY_RING_ENV = "OPTERYX_SECRETS_KEY_RING"

_NAME = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
# KMS CryptoKey ids are [a-zA-Z0-9_-]{1,63}; `ba-` takes three.
_ACCOUNT_FOR_KEY = re.compile(r"^[A-Za-z0-9_-]{1,60}$")

# Overridable for tests and for a deployment that names keys differently.
# Called with the billing account id, returns the CryptoKey resource name.
_kek_namer = None


def set_kek_namer(namer) -> None:
    """Install `namer(account_id) -> CryptoKey resource name`. None restores the default."""
    global _kek_namer
    _kek_namer = namer


def kek_for_account(account_id: str) -> str:
    """The KEK for a billing account: `{key ring}/cryptoKeys/ba-{account_id}`.

    The key must already exist. Lazy creation on an account's first secret
    (SEC-2) is deliberately not done here: creating KMS keys is an IAM-bearing
    act against production and is provisioned separately.
    """
    if _kek_namer is not None:
        return _kek_namer(account_id)
    ring = os.environ.get(KEY_RING_ENV, "").strip().rstrip("/")
    if not ring:
        raise SecretKeyUnavailable(
            f"secrets are not configured in this deployment ({KEY_RING_ENV} is not set)"
        )
    if not _ACCOUNT_FOR_KEY.match(account_id):
        raise SecretKeyUnavailable("billing account id cannot name a key")
    return f"{ring}/cryptoKeys/ba-{account_id}"


def normalise_secret_name(name: str) -> str:
    if not isinstance(name, str) or not _NAME.match(name.lower()):
        raise SecretInvalid(
            "secret names are 1-63 characters of letters, digits and underscores, "
            "not starting with a digit"
        )
    return name.lower()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _public_fields(name: str, data: dict) -> dict:
    """A secret document with the key material removed (§5, SHOW SECRETS)."""
    record = {key: value for key, value in data.items() if key not in kms.SEALED_FIELDS}
    record["name"] = data.get("name", name)
    return record


class SecretMaterial:
    """A decrypted secret, held for one use.

    Deliberately awkward to leak: no repr of its values, not picklable (a plan
    or a cache that tries to serialise it fails loudly), and no `__dict__` to
    dump.
    """

    __slots__ = ("workspace", "name", "secret_type", "scope", "_payload")

    def __init__(self, workspace: str, name: str, secret_type: str, scope, payload: dict):
        self.workspace = workspace
        self.name = name
        self.secret_type = secret_type
        self.scope = scope
        self._payload = payload

    @property
    def reference(self) -> str:
        return f"{self.workspace}.{self.name}"

    @property
    def payload(self) -> dict:
        return self._payload

    def admits(self, path: str) -> bool:
        """Whether this secret may be pointed at `path` (§4.1). False without a scope."""
        return self.scope is not None and scope_admits(self.scope, path)

    def __repr__(self) -> str:
        return f"<SecretMaterial {self.reference} type={self.secret_type}>"

    __str__ = __repr__

    def __reduce__(self):
        raise TypeError("a decrypted secret cannot be serialised")


class SecretsMixin:
    """The secret methods of `OpteryxCatalog`. Expects `workspace` and `_catalog_ref`."""

    # -- addressing --------------------------------------------------------

    def _secrets_collection(self):
        return self._catalog_ref.document(SECRETS_DOCUMENT).collection(SECRETS_SUBCOLLECTION)

    def _secret_doc_ref(self, name: str):
        return self._secrets_collection().document(name)

    def _secret_context(self, name: str, secret_type: str) -> str:
        # Bound into the AEAD so a sealed record only opens at its own address.
        return f"{self.workspace}/{SECRETS_DOCUMENT}/{name}/{secret_type}"

    def _secrets_kek(self) -> str:
        properties = self._catalog_ref.document("$properties").get()
        data = (properties.to_dict() or {}) if properties.exists else {}
        account = data.get("billing-account-id")
        if not account:
            raise SecretKeyUnavailable(
                f"workspace {self.workspace} has no billing account, so it has no key to "
                "encrypt secrets under"
            )
        return kek_for_account(str(account))

    # -- DDL -----------------------------------------------------------------

    def create_secret(
        self,
        name: str,
        secret_type: str,
        options: dict,
        *,
        author: str,
        replace: bool = False,
        if_not_exists: bool = False,
        resolve_host=None,
    ) -> dict:
        """CREATE [OR REPLACE] SECRET [IF NOT EXISTS] (§2, §5).

        Returns the stored record without key material, plus `created: bool`
        (False when IF NOT EXISTS found one already there).
        """
        if not author:
            raise ValueError("author must be provided when creating a secret")
        if replace and if_not_exists:
            raise SecretInvalid("CREATE OR REPLACE ... IF NOT EXISTS is a contradiction")
        name = normalise_secret_name(name)
        validated = validate_secret(secret_type, options, resolve_host=resolve_host)

        doc_ref = self._secret_doc_ref(name)
        if if_not_exists:
            existing = doc_ref.get()
            if existing.exists:
                return {**_public_fields(name, existing.to_dict() or {}), "created": False}

        sealed = kms.seal(
            json.dumps(validated.payload, separators=(",", ":")),
            self._secrets_kek(),
            context=self._secret_context(name, validated.secret_type),
        )
        now_ms = _now_ms()
        record: dict[str, Any] = {
            "name": name,
            "type": validated.secret_type,
            "scope": validated.scope,
            **sealed,
            "created-at-ms": now_ms,
            "created-by": author,
            "updated-at-ms": now_ms,
            "updated-by": author,
            "last-used-at-ms": None,
            "use-count": 0,
        }

        if replace:
            # One whole-document write: the value is replaced atomically, with
            # no moment at which the name exists without one. created-* is
            # carried over so the record still says when the NAME was made.
            existing = doc_ref.get()
            if existing.exists:
                prior = existing.to_dict() or {}
                record["created-at-ms"] = prior.get("created-at-ms", now_ms)
                record["created-by"] = prior.get("created-by", author)
            doc_ref.set(record)
            action = "replace_secret" if existing.exists else "create_secret"
        else:
            try:
                doc_ref.create(record)
            except Exception as err:
                if _is_already_exists(err):
                    if if_not_exists:
                        current = doc_ref.get()
                        return {
                            **_public_fields(name, current.to_dict() or {}),
                            "created": False,
                        }
                    raise SecretAlreadyExists(
                        f"secret {self.workspace}.{name} already exists "
                        "(use CREATE OR REPLACE SECRET to replace its value)"
                    ) from None
                raise
            action = "create_secret"

        emit_audit(
            action,
            resource_type=ResourceType.SECRET,
            workspace=self.workspace,
            resource=name,
            author=author,
            secret_type=validated.secret_type,
            scope=validated.scope,
        )
        return {**_public_fields(name, record), "created": True}

    def drop_secret(self, name: str, *, author: str, if_exists: bool = False) -> bool:
        """DROP SECRET [IF EXISTS]. Returns whether a secret was removed."""
        if not author:
            raise ValueError("author must be provided when dropping a secret")
        name = normalise_secret_name(name)
        doc_ref = self._secret_doc_ref(name)
        if not doc_ref.get().exists:
            if if_exists:
                return False
            raise SecretNotFound(
                f"secret {self.workspace}.{name} does not exist "
                "(use DROP SECRET IF EXISTS to make this quiet)"
            )
        doc_ref.delete()
        emit_audit(
            "drop_secret",
            resource_type=ResourceType.SECRET,
            workspace=self.workspace,
            resource=name,
            author=author,
        )
        return True

    def get_secret(self, name: str) -> dict:
        """One secret's record, without key material. Raises `SecretNotFound`."""
        name = normalise_secret_name(name)
        snapshot = self._secret_doc_ref(name).get()
        if not snapshot.exists:
            raise SecretNotFound(f"secret {self.workspace}.{name} does not exist")
        return _public_fields(name, snapshot.to_dict() or {})

    def list_secrets(self) -> list[dict]:
        """Every secret in the workspace, without key material, by name (SHOW SECRETS)."""
        records = [
            _public_fields(doc.id, doc.to_dict() or {}) for doc in self._secrets_collection().stream()
        ]
        return sorted(records, key=lambda record: record["name"])

    # -- use -------------------------------------------------------------------

    def use_secret(
        self,
        name: str,
        *,
        used_by: str,
        purpose: str,
        path: str | None = None,
        query_id: str | None = None,
    ) -> SecretMaterial:
        """Decrypt a secret for one use (§8.2, §9.1).

        When `path` is given the secret must be an object-store type whose
        scheme matches the path's, and the path must be under its SCOPE; the
        refusal says which, naming nothing from the secret but its name.

        Stamps `last-used-at-ms` / `use-count` - last ATTEMPTED, per §5 - and
        writes an audit record naming the query, the reference and the path.
        """
        name = normalise_secret_name(name)
        reference = f"{self.workspace}.{name}"
        doc_ref = self._secret_doc_ref(name)
        snapshot = doc_ref.get()
        if not snapshot.exists:
            raise SecretNotFound(f"secret {reference} does not exist")
        data = snapshot.to_dict() or {}
        secret_type = data.get("type")
        scope = data.get("scope")

        if path is not None:
            expected = OBJECT_STORE_SCHEMES.get(secret_type)
            scheme = path.partition("://")[0].lower() if "://" in path else ""
            if expected is None or scheme != expected:
                raise SecretRefused(
                    f"secret {reference} is of type {secret_type}, which cannot read "
                    f"{scheme + '://' if scheme else 'local'} paths"
                )
            if not scope or not scope_admits(scope, path):
                raise SecretRefused(f"the path is outside secret {reference}'s SCOPE ({scope})")

        try:
            self._stamp_secret_use(doc_ref)
        except Exception as err:  # the audit record below is the durable trail
            logger.warning("could not stamp use of secret %s: %s", reference, type(err).__name__)

        emit_audit(
            "use_secret",
            resource_type=ResourceType.SECRET,
            workspace=self.workspace,
            resource=name,
            author=used_by,
            purpose=purpose,
            path=path,
            query_id=query_id,
        )

        plaintext = kms.unseal(data, context=self._secret_context(name, secret_type))
        return SecretMaterial(self.workspace, name, secret_type, scope, json.loads(plaintext))

    def _stamp_secret_use(self, doc_ref) -> None:
        from google.cloud import firestore

        doc_ref.update({"last-used-at-ms": _now_ms(), "use-count": firestore.Increment(1)})


def _is_already_exists(err: Exception) -> bool:
    try:
        from google.api_core.exceptions import AlreadyExists
        from google.api_core.exceptions import Conflict
    except ImportError:  # pragma: no cover
        return type(err).__name__ in ("AlreadyExists", "Conflict")
    return isinstance(err, (AlreadyExists, Conflict))
