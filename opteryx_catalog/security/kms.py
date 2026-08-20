"""KMS envelope encryption for catalog-held secrets.

Ciphertext lives ON the Firestore document (a workspace binding's auth block,
an external table's credential field) - deliberately NOT in Secret Manager,
whose per-secret cost and count explode at one-secret-per-tenant scale. The
scheme is standard enveloping:

- a fresh 256-bit DEK is generated per secret and encrypts the payload
  locally with AES-256-GCM;
- Cloud KMS encrypts (wraps) only the DEK, so the payload never leaves the
  process and KMS never sees it;
- what is stored is one opaque string: base64 over a JSON envelope
  `{"v": 1, "kek": <key name>, "dek": ..., "iv": ..., "ct": ...}`.

The GCM call binds the KMS key resource name as associated data, so an
envelope pasted under a different key name fails authentication instead of
decrypting: the ciphertext is usable only with the key it names.

Decryption needs `cloudkms.cryptoKeyVersions.useToDecrypt` on the named key -
that IAM grant, not anything in this module, is what decides which identities
can recover a secret.

Both dependencies (`google-cloud-kms`, `cryptography`) are optional - install
`opteryx-catalog[kms]` - and imported lazily inside the functions, so a
catalog that never touches a stored credential works without them.
"""

from __future__ import annotations

import base64
import json
import os

_KMS_HELP = (
    "opteryx-catalog needs `google-cloud-kms` and `cryptography` to encrypt or "
    "decrypt stored secrets. Install them with:\n"
    "    pip install 'opteryx-catalog[kms]'"
)

_ENVELOPE_VERSION = 1

# Module-level singleton, matching the client-caching shape used elsewhere in
# this codebase. None until first use; tests may inject a fake.
_kms_client = None


class SecretDecryptionError(Exception):
    """An envelope could not be decrypted.

    Covers a malformed envelope, a KMS unwrap refusal, and a GCM
    authentication failure (tampered payload, or an envelope presented under
    a different key name than it was sealed for). The message never includes
    the envelope or any key material.
    """


def _require_crypto():
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError as err:
        raise ImportError(f"{_KMS_HELP}\n\nImport failed with: {err!r}") from err
    return AESGCM


def _get_kms_client():
    global _kms_client
    if _kms_client is None:
        try:
            from google.cloud import kms
        except ImportError as err:
            raise ImportError(f"{_KMS_HELP}\n\nImport failed with: {err!r}") from err
        _kms_client = kms.KeyManagementServiceClient()
    return _kms_client


def _wrap_dek(dek: bytes, kms_key: str) -> bytes:
    """KMS-encrypt the DEK. Isolated so tests can stub the KMS round-trip."""
    response = _get_kms_client().encrypt(request={"name": kms_key, "plaintext": dek})
    return response.ciphertext


def _unwrap_dek(wrapped_dek: bytes, kms_key: str) -> bytes:
    """KMS-decrypt the DEK. Isolated so tests can stub the KMS round-trip."""
    response = _get_kms_client().decrypt(request={"name": kms_key, "ciphertext": wrapped_dek})
    return response.plaintext


def encrypt_secret(plaintext: bytes | str, kms_key: str) -> str:
    """Seal `plaintext` under `kms_key`, returning the storable envelope string.

    `kms_key` is the full CryptoKey resource name
    (projects/.../locations/.../keyRings/.../cryptoKeys/...). The caller holds
    the plaintext only up to this call - nothing here logs or stores it.
    """
    if not kms_key:
        raise ValueError("kms_key must be the full CryptoKey resource name")
    if isinstance(plaintext, str):
        plaintext = plaintext.encode("utf-8")

    AESGCM = _require_crypto()
    dek = AESGCM.generate_key(bit_length=256)
    iv = os.urandom(12)
    ct = AESGCM(dek).encrypt(iv, plaintext, kms_key.encode("utf-8"))
    wrapped = _wrap_dek(dek, kms_key)

    envelope = {
        "v": _ENVELOPE_VERSION,
        "kek": kms_key,
        "dek": base64.b64encode(wrapped).decode("ascii"),
        "iv": base64.b64encode(iv).decode("ascii"),
        "ct": base64.b64encode(ct).decode("ascii"),
    }
    return base64.b64encode(json.dumps(envelope).encode("ascii")).decode("ascii")


def decrypt_secret(ciphertext: str, kms_key: str) -> bytes:
    """Open an envelope produced by `encrypt_secret`.

    Raises `SecretDecryptionError` on any failure - malformed envelope, KMS
    refusal, tampering, or a `kms_key` that differs from the one the envelope
    was sealed under. Error messages carry no envelope contents and no key
    material; callers must preserve that when re-raising.
    """
    if not kms_key:
        raise ValueError("kms_key must be the full CryptoKey resource name")

    AESGCM = _require_crypto()
    try:
        envelope = json.loads(base64.b64decode(ciphertext))
        if envelope.get("v") != _ENVELOPE_VERSION:
            raise SecretDecryptionError(
                f"unsupported envelope version {envelope.get('v')!r}"
            )
        wrapped = base64.b64decode(envelope["dek"])
        iv = base64.b64decode(envelope["iv"])
        ct = base64.b64decode(envelope["ct"])
    except SecretDecryptionError:
        raise
    except Exception as err:
        raise SecretDecryptionError("malformed secret envelope") from err

    try:
        dek = _unwrap_dek(wrapped, kms_key)
        return AESGCM(dek).decrypt(iv, ct, kms_key.encode("utf-8"))
    except Exception as err:
        raise SecretDecryptionError(
            "secret could not be decrypted (wrong key, tampered envelope, or KMS refusal)"
        ) from err
