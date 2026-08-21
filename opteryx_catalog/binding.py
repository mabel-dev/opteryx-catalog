"""Workspace catalog bindings: which catalog service backs a workspace.

The binding is a `catalog` block on the workspace's `$properties` document -
the same doc that already gates workspace existence and carries lifecycle
state - so binding lifecycle rides along with the workspace's own: created
with it, deleted by DROP WORKSPACE, reachable only by whoever already knows
the workspace's name. A workspace with no block is backed by the native
catalog; every pre-existing workspace is therefore grandfathered with zero
writes. See WORKSPACE_CATALOG_RESOLUTION.md for the full design.

These are deliberately plain functions over a Firestore client, NOT methods
on `OpteryxCatalog`: the query-time resolver must not pay full handle
construction (storage client, parquet-engine check, existence gating) for one
doc read, and a workspace bound to an external catalog has no data plane for
that handle to construct anyway.

Block schema (kebab-case field names, matching `$properties` convention):

    catalog:
      kind: "iceberg"                # allowlisted NAME - never a class path;
                                     # only the engine maps names to code
      config: {...}                  # arbitrary nested dict, passed through
                                     # verbatim; must not contain the keys the
                                     # engine injects/strips itself
      auth:
        mode: "ambient" | "stored"   # ambient = the engine's own identity
                                     # (ADC); a first-class mode, not an empty
                                     # credential
        ciphertext: str | None       # stored only: KMS-envelope blob
                                     # (see security/kms.py)
        kms-key: str | None          # stored only: wrapping key resource name
        inject-as: str | None        # stored only: dotted config path that
                                     # receives the decrypted plaintext
      preserve-sql-case: bool
      version: int                   # monotonic; the engine folds it into its
                                     # connector-cache validity check, so any
                                     # write here rotates cached connectors on
                                     # the next lookup, in every process
      updated-at-ms: int
      updated-by: str
      listing-synced-at-ms: int      # written by stub_projection.py, NOT here:
      listing-count: int             # when the workspace's dataset listing was
                                     # last projected into Firestore, and how
                                     # many names it held. Read back so a UI can
                                     # state the age of the list beside its
                                     # refresh control - see stub_projection's
                                     # module docstring for why that matters.
                                     # Absent means "never refreshed".

Writing a binding REPLACES the whole block, so the two listing fields do not
survive it. That is the honest direction: a rebind can point the workspace at
a different catalog entirely, and carrying an old stamp forward would report
freshness for a listing that was taken from somewhere else. The workspace
reads as "never refreshed" until someone runs a sync, which is what section
6.5 of the UI design asks for after a settings change anyway.

The version is `max(now_ms, previous + 1, floor + 1)`, where `floor` is a
`catalog-version-floor` field `clear_catalog_binding` leaves on the doc
recording the removed block's version. That keeps versions monotonic across
overwrite AND across clear-then-rebind - including both happening inside one
millisecond, where wall clock alone regresses - which is what keeps a rebound
workspace from ever re-presenting a version some process may still hold a
cached connector for.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from dataclasses import field

from opteryx_catalog.exceptions import InvalidCatalogBinding
from opteryx_catalog.exceptions import WorkspaceNotFound

PROPERTIES_DOC = "$properties"

# Keys the engine injects (workspace) or strips (connector, prefix) itself; a
# config carrying them would be silently fought over, so it is rejected loudly.
RESERVED_CONFIG_KEYS = ("workspace", "connector", "prefix")

AUTH_MODE_AMBIENT = "ambient"
AUTH_MODE_STORED = "stored"
_AUTH_MODES = (AUTH_MODE_AMBIENT, AUTH_MODE_STORED)

# Kind names are identifiers the engine looks up in its code-side allowlist.
_KIND = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass
class CatalogBinding:
    """One workspace's catalog binding, as read back from `$properties`."""

    kind: str
    config: dict = field(default_factory=dict)
    auth_mode: str = AUTH_MODE_AMBIENT
    ciphertext: str | None = None
    kms_key: str | None = None
    inject_as: str | None = None
    preserve_sql_case: bool = False
    version: int = 0
    updated_at_ms: int | None = None
    updated_by: str | None = None
    listing_synced_at_ms: int | None = None
    listing_count: int | None = None


def _properties_ref(firestore_client, workspace: str):
    return firestore_client.collection(workspace).document(PROPERTIES_DOC)


def _validate(kind: str, config: dict, auth_mode: str, ciphertext, kms_key, inject_as) -> None:
    if not isinstance(kind, str) or not _KIND.match(kind):
        raise InvalidCatalogBinding(
            f"catalog binding kind {kind!r} is not a usable name - lowercase letters, "
            "digits and underscores, starting with a letter"
        )
    if not isinstance(config, dict):
        raise InvalidCatalogBinding(
            f"catalog binding config must be a dict, got {type(config).__name__}"
        )
    reserved = [key for key in RESERVED_CONFIG_KEYS if key in config]
    if reserved:
        raise InvalidCatalogBinding(
            f"catalog binding config must not contain {reserved} - the engine injects or "
            "strips those keys itself, so a stored value would be silently fought over"
        )
    if auth_mode not in _AUTH_MODES:
        raise InvalidCatalogBinding(
            f"catalog binding auth mode {auth_mode!r} is not one of {_AUTH_MODES}"
        )
    if auth_mode == AUTH_MODE_STORED:
        missing = [
            name
            for name, value in (
                ("ciphertext", ciphertext),
                ("kms_key", kms_key),
                ("inject_as", inject_as),
            )
            if not value
        ]
        if missing:
            raise InvalidCatalogBinding(
                f"catalog binding with auth mode 'stored' is missing {missing}"
            )
    else:  # ambient
        carried = [
            name
            for name, value in (("ciphertext", ciphertext), ("kms_key", kms_key))
            if value
        ]
        if carried:
            raise InvalidCatalogBinding(
                f"catalog binding with auth mode 'ambient' must not carry {carried} - "
                "ambient means the engine's own identity, not an empty stored credential"
            )


def read_catalog_binding(firestore_client, workspace: str) -> CatalogBinding | None:
    """The workspace's binding, or None.

    None covers both "the `$properties` doc has no `catalog` block" (a native
    workspace - the overwhelmingly common case) and "the doc does not exist"
    (an unknown workspace). Callers resolve both the same way - fall through
    to the native default, whose own existence gate raises `WorkspaceNotFound`
    for the second case - so the distinction is deliberately not surfaced
    here. Firestore read failures propagate: the caller must fail its query
    loudly, never treat "couldn't look" as "native".
    """
    snapshot = _properties_ref(firestore_client, workspace).get()
    if not snapshot.exists:
        return None
    block = (snapshot.to_dict() or {}).get("catalog")
    if not block:
        return None
    auth = block.get("auth") or {}
    return CatalogBinding(
        kind=block.get("kind"),
        config=dict(block.get("config") or {}),
        auth_mode=auth.get("mode", AUTH_MODE_AMBIENT),
        ciphertext=auth.get("ciphertext"),
        kms_key=auth.get("kms-key"),
        inject_as=auth.get("inject-as"),
        preserve_sql_case=bool(block.get("preserve-sql-case", False)),
        version=int(block.get("version", 0)),
        updated_at_ms=block.get("updated-at-ms"),
        updated_by=block.get("updated-by"),
        listing_synced_at_ms=block.get("listing-synced-at-ms"),
        listing_count=block.get("listing-count"),
    )


def write_catalog_binding(
    firestore_client,
    workspace: str,
    *,
    kind: str,
    config: dict | None = None,
    auth_mode: str = AUTH_MODE_AMBIENT,
    ciphertext: str | None = None,
    kms_key: str | None = None,
    inject_as: str | None = None,
    preserve_sql_case: bool = False,
    updated_by: str,
) -> int:
    """Create or replace the workspace's binding; returns the new version.

    Validation happens here, before any write, so a malformed binding fails
    where its author can see it (raising `InvalidCatalogBinding`) rather than
    at some later query's resolution. Only the ciphertext ever reaches this
    function - encrypting the raw secret is the CALLER's job, done before the
    value gets anywhere near a `.set()`.

    When the `$properties` doc does not exist this provisions a SHELL
    workspace: the standard lifecycle fields (the same set
    `OpteryxCatalog.__init__` seeds with `create_if_missing=True`) plus the
    block - so locking and DROP WORKSPACE work uniformly on bound workspaces.
    Identity rules (who may bind a workspace) are deliberately NOT enforced
    here - per this library's convention, authorization belongs to the
    governance service calling it.

    Plain read-then-write, not a transaction: binding writes are rare,
    human-driven, and serialized behind the governance service; the
    time-based version keeps even a lost race producing distinct, monotonic
    versions rather than a silent replay.
    """
    if not updated_by:
        raise ValueError("updated_by must be provided when writing a catalog binding")
    _validate(kind, config or {}, auth_mode, ciphertext, kms_key, inject_as)

    reference = _properties_ref(firestore_client, workspace)
    snapshot = reference.get()
    now_ms = int(time.time() * 1000)

    previous_version = 0
    version_floor = 0
    if snapshot.exists:
        document = snapshot.to_dict() or {}
        previous_block = document.get("catalog") or {}
        previous_version = int(previous_block.get("version", 0))
        version_floor = int(document.get("catalog-version-floor", 0))
    version = max(now_ms, previous_version + 1, version_floor + 1)

    block = {
        "kind": kind,
        "config": dict(config or {}),
        "auth": {
            "mode": auth_mode,
            "ciphertext": ciphertext,
            "kms-key": kms_key,
            "inject-as": inject_as,
        },
        "preserve-sql-case": bool(preserve_sql_case),
        "version": version,
        "updated-at-ms": now_ms,
        "updated-by": updated_by,
    }

    if snapshot.exists:
        reference.update({"catalog": block})
    else:
        reference.set(
            {
                # The same lifecycle seed OpteryxCatalog.__init__ writes for
                # create_if_missing=True - keep the two in step.
                "timestamp-ms": now_ms,
                "billing-account-id": None,
                "owner": None,
                "deleted-at-ms": None,
                "deleted-by": None,
                "locked-by": None,
                "locked-at-ms": None,
                "catalog": block,
            }
        )
    return version


def clear_catalog_binding(firestore_client, workspace: str) -> bool:
    """Remove the binding, reverting the workspace to the native catalog.

    Returns True if a binding was removed, False if there was none. Raises
    `WorkspaceNotFound` for a workspace with no `$properties` doc at all -
    "revert to native" presupposes a workspace to revert. The doc itself is
    left in place: a formerly bound (shell) workspace keeps its lifecycle
    fields, and DROP WORKSPACE remains the only thing that deletes it.

    The removed block's version is preserved as `catalog-version-floor`, so a
    later rebind resumes above it rather than restarting - see the module
    docstring for why a version must never be re-presented.
    """
    from google.cloud import firestore

    reference = _properties_ref(firestore_client, workspace)
    snapshot = reference.get()
    if not snapshot.exists:
        raise WorkspaceNotFound(f"Workspace does not exist: {workspace}")
    block = (snapshot.to_dict() or {}).get("catalog")
    if not block:
        return False
    reference.update(
        {
            "catalog": firestore.DELETE_FIELD,
            "catalog-version-floor": int(block.get("version", 0)),
        }
    )
    return True
