"""Exceptions for customer secrets (jobs.opteryx docs/design/secrets.md).

Kept beside the feature rather than in `opteryx_catalog.exceptions` so the
whole surface is one package. Every message names at most the secret's name,
type, option KEYS and the scheme of a scope - never an option value.
"""

from __future__ import annotations

from ..exceptions import CatalogError


class SecretError(KeyError, CatalogError):
    """A secret operation that cannot proceed.

    KeyError for parity with `TaskError`, so a caller treating a missing
    catalog object generically catches this too. `__str__` is overridden
    because KeyError's quotes the message.
    """

    def __str__(self) -> str:
        return str(self.args[0]) if self.args else ""


class SecretInvalid(SecretError, ValueError):
    """The statement's options do not describe a valid secret of its type."""


class SecretAlreadyExists(SecretError):
    pass


class SecretNotFound(SecretError):
    pass


class SecretKeyUnavailable(SecretError):
    """No key-encryption key can be named for the workspace's billing account.

    Raised before anything is encrypted or written: a workspace with no billing
    account, or a deployment with no key ring configured.
    """


class SecretRefused(SecretError):
    """A secret exists but may not be used for what was asked of it.

    The wrong type for a path's scheme, or a path outside the secret's SCOPE.
    """
