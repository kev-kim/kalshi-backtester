"""Tests for RSA-PSS signing correctness.

Verifies against a locally generated test key pair — no Kalshi API required.
The known-vector test signs a fixed input and verifies the signature can be
validated by the corresponding public key using the same PSS parameters.
"""

from __future__ import annotations

import base64
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from kalshi_collector.auth import build_auth_headers, load_private_key, sign_request

FIXTURE_KEY = Path(__file__).parent / "fixtures" / "test_private_key.pem"

# Fixed-input known vector: signing this exact message must produce a signature
# that verifies cleanly against the public key extracted from FIXTURE_KEY.
_KNOWN_TIMESTAMP = "1700000000000"
_KNOWN_METHOD = "GET"
_KNOWN_PATH = "/trade-api/v2/markets"
_KNOWN_MESSAGE = (_KNOWN_TIMESTAMP + _KNOWN_METHOD + _KNOWN_PATH).encode()


@pytest.fixture(scope="module")
def private_key() -> object:
    return load_private_key(FIXTURE_KEY)


@pytest.fixture(scope="module")
def public_key(private_key: object) -> object:  # type: ignore[override]
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
    assert isinstance(private_key, RSAPrivateKey)
    return private_key.public_key()


def test_load_private_key_succeeds() -> None:
    key = load_private_key(FIXTURE_KEY)
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
    assert isinstance(key, RSAPrivateKey)


def test_load_wrong_key_type_raises(tmp_path: Path) -> None:
    """A non-RSA key should raise TypeError."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    ed_key = Ed25519PrivateKey.generate()
    pem = ed_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    bad_key_path = tmp_path / "ed25519.pem"
    bad_key_path.write_bytes(pem)
    with pytest.raises(TypeError, match="RSA"):
        load_private_key(bad_key_path)


def test_sign_request_returns_base64(private_key: object) -> None:
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
    assert isinstance(private_key, RSAPrivateKey)
    ts_str, sig = sign_request(private_key, _KNOWN_METHOD, _KNOWN_PATH, int(_KNOWN_TIMESTAMP))
    assert ts_str == _KNOWN_TIMESTAMP
    # Must be valid base64
    decoded = base64.b64decode(sig)
    assert len(decoded) == 256  # 2048-bit key → 256-byte signature


def test_known_vector_signature_verifies(private_key: object, public_key: object) -> None:
    """Core signing correctness: a signature produced by sign_request must
    verify against the public key using the same PSS/SHA-256/DIGEST_LENGTH params."""
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey
    assert isinstance(private_key, RSAPrivateKey)
    assert isinstance(public_key, RSAPublicKey)

    ts_str, sig_b64 = sign_request(private_key, _KNOWN_METHOD, _KNOWN_PATH, int(_KNOWN_TIMESTAMP))
    raw_sig = base64.b64decode(sig_b64)

    # Verifying with the wrong padding or hash should fail;
    # verifying with the correct params must succeed (no exception = pass).
    public_key.verify(
        raw_sig,
        _KNOWN_MESSAGE,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )


def test_query_string_stripped_from_path(private_key: object, public_key: object) -> None:
    """The signature path must NOT include the query string."""
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey
    assert isinstance(private_key, RSAPrivateKey)
    assert isinstance(public_key, RSAPublicKey)

    path_with_qs = "/trade-api/v2/markets?status=open&limit=100"
    path_clean = "/trade-api/v2/markets"

    # sign_request should sign only the path portion
    ts_str, sig_b64 = sign_request(private_key, "GET", path_with_qs, 1700000000000)
    raw_sig = base64.b64decode(sig_b64)
    expected_message = ("1700000000000GET" + path_clean).encode()

    public_key.verify(
        raw_sig,
        expected_message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )


def test_build_auth_headers_keys(private_key: object) -> None:
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
    assert isinstance(private_key, RSAPrivateKey)

    headers = build_auth_headers("my-key-id", private_key, "GET", "/trade-api/v2/markets")
    assert set(headers.keys()) == {
        "KALSHI-ACCESS-KEY",
        "KALSHI-ACCESS-TIMESTAMP",
        "KALSHI-ACCESS-SIGNATURE",
    }
    assert headers["KALSHI-ACCESS-KEY"] == "my-key-id"


def test_different_timestamps_produce_different_signatures(private_key: object) -> None:
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
    assert isinstance(private_key, RSAPrivateKey)

    _, sig1 = sign_request(private_key, "GET", "/trade-api/v2/markets", 1700000000000)
    _, sig2 = sign_request(private_key, "GET", "/trade-api/v2/markets", 1700000000001)
    assert sig1 != sig2
