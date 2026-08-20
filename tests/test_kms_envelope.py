"""Envelope encryption round-trip (security/kms.py).

The KMS wrap/unwrap round-trip is stubbed - these tests exercise the envelope
format, the local AES-GCM layer, and the key-name binding, none of which need
a live KMS. The stub is deliberately reversible-but-not-identity so a bug
that skips the unwrap can't pass.
"""

from __future__ import annotations

import base64
import json

import pytest

from opteryx_catalog.security import kms as kms_module
from opteryx_catalog.security.kms import SecretDecryptionError
from opteryx_catalog.security.kms import decrypt_secret
from opteryx_catalog.security.kms import encrypt_secret

KEY_A = "projects/p/locations/l/keyRings/r/cryptoKeys/key-a"
KEY_B = "projects/p/locations/l/keyRings/r/cryptoKeys/key-b"


@pytest.fixture(autouse=True)
def stub_kms(monkeypatch):
    """Reversible fake wrap: XOR with a keystream derived from the key name."""

    def _stream(kms_key: str, length: int) -> bytes:
        seed = kms_key.encode("utf-8")
        return bytes(seed[i % len(seed)] ^ (i & 0xFF) for i in range(length))

    monkeypatch.setattr(
        kms_module, "_wrap_dek", lambda dek, key: bytes(a ^ b for a, b in zip(dek, _stream(key, len(dek))))
    )
    monkeypatch.setattr(
        kms_module, "_unwrap_dek", lambda wrapped, key: bytes(a ^ b for a, b in zip(wrapped, _stream(key, len(wrapped))))
    )


def test_round_trip_str_and_bytes():
    for secret in ("hunter2", b"\x00\x01binary\xff"):
        envelope = encrypt_secret(secret, KEY_A)
        expected = secret.encode("utf-8") if isinstance(secret, str) else secret
        assert decrypt_secret(envelope, KEY_A) == expected


def test_envelopes_are_unique_per_call():
    # fresh DEK + fresh IV every call: identical plaintexts must not produce
    # identical ciphertexts (that equality would leak "same credential").
    assert encrypt_secret("same", KEY_A) != encrypt_secret("same", KEY_A)


def test_wrong_key_name_fails_authentication():
    # the key name is bound as GCM associated data - an envelope presented
    # under a different key must fail, not decrypt.
    envelope = encrypt_secret("hunter2", KEY_A)
    with pytest.raises(SecretDecryptionError):
        decrypt_secret(envelope, KEY_B)


def test_tampered_payload_fails():
    envelope = encrypt_secret("hunter2", KEY_A)
    raw = json.loads(base64.b64decode(envelope))
    ct = bytearray(base64.b64decode(raw["ct"]))
    ct[0] ^= 0xFF
    raw["ct"] = base64.b64encode(bytes(ct)).decode("ascii")
    tampered = base64.b64encode(json.dumps(raw).encode("ascii")).decode("ascii")
    with pytest.raises(SecretDecryptionError):
        decrypt_secret(tampered, KEY_A)


def test_malformed_envelope_fails_cleanly():
    with pytest.raises(SecretDecryptionError, match="malformed"):
        decrypt_secret("not-even-base64-json", KEY_A)


def test_unsupported_version_fails_cleanly():
    envelope = json.loads(base64.b64decode(encrypt_secret("s", KEY_A)))
    envelope["v"] = 99
    blob = base64.b64encode(json.dumps(envelope).encode("ascii")).decode("ascii")
    with pytest.raises(SecretDecryptionError, match="version"):
        decrypt_secret(blob, KEY_A)


def test_failure_messages_carry_no_secret_material():
    envelope = encrypt_secret("super-secret-value", KEY_A)
    try:
        decrypt_secret(envelope, KEY_B)
    except SecretDecryptionError as err:
        text = str(err)
        assert "super-secret-value" not in text
        assert envelope not in text
        assert KEY_A not in text and KEY_B not in text
    else:  # pragma: no cover
        raise AssertionError("expected SecretDecryptionError")
