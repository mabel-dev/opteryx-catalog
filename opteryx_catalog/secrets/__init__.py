"""Customer secrets - credentials held on a customer's behalf.

The design is jobs.opteryx docs/design/secrets.md. Not to be confused with
`opteryx_catalog.alerts._secrets`, which resolves the platform's OWN
credentials from the environment.
"""

from .errors import SecretAlreadyExists
from .errors import SecretError
from .errors import SecretInvalid
from .errors import SecretKeyUnavailable
from .errors import SecretNotFound
from .errors import SecretRefused
from .store import SecretMaterial
from .store import SecretsMixin
from .store import kek_for_account
from .store import normalise_secret_name
from .store import set_kek_namer
from .types import AWS_ACCESS_KEY
from .types import GCS_SERVICE_ACCOUNT
from .types import HTTP_ENDPOINT
from .types import SECRET_TYPES
from .types import scope_admits
from .types import validate_secret

__all__ = [
    "AWS_ACCESS_KEY",
    "GCS_SERVICE_ACCOUNT",
    "HTTP_ENDPOINT",
    "SECRET_TYPES",
    "SecretAlreadyExists",
    "SecretError",
    "SecretInvalid",
    "SecretKeyUnavailable",
    "SecretMaterial",
    "SecretNotFound",
    "SecretRefused",
    "SecretsMixin",
    "kek_for_account",
    "normalise_secret_name",
    "scope_admits",
    "set_kek_namer",
    "validate_secret",
]
