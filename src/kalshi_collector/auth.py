"""RSA-PSS request signing for Kalshi API authentication.

Signature covers: timestamp_ms_str + HTTP_method.upper() + path_without_query
Algorithm: RSA-PSS, SHA-256, salt length = digest length (32 bytes).
Headers required on every signed request:
    KALSHI-ACCESS-KEY        — the Key ID string
    KALSHI-ACCESS-TIMESTAMP  — milliseconds since epoch (string)
    KALSHI-ACCESS-SIGNATURE  — base64-encoded PSS signature
"""

from __future__ import annotations

import base64
import time
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey


def load_private_key(path: Path) -> RSAPrivateKey:
    """Load an RSA private key from a PEM file."""
    pem_bytes = path.read_bytes()
    key = serialization.load_pem_private_key(pem_bytes, password=None)
    if not isinstance(key, RSAPrivateKey):
        raise TypeError(f"Expected RSA private key, got {type(key).__name__}")
    return key


def sign_request(
    private_key: RSAPrivateKey,
    method: str,
    path: str,
    timestamp_ms: int | None = None,
) -> tuple[str, str]:
    """Return (timestamp_ms_str, base64_signature) for a Kalshi API request.

    Args:
        private_key:  Loaded RSA private key.
        method:       HTTP method, e.g. 'GET' or 'POST'.
        path:         URL path WITHOUT query string, e.g. '/trade-api/v2/markets'.
        timestamp_ms: Override timestamp (milliseconds). Defaults to now().

    Returns:
        (timestamp_str, signature_b64) — values to set in auth headers.
    """
    if timestamp_ms is None:
        timestamp_ms = int(time.time() * 1000)

    ts_str = str(timestamp_ms)
    message = (ts_str + method.upper() + path).encode("utf-8")

    raw_sig = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return ts_str, base64.b64encode(raw_sig).decode("ascii")


def build_auth_headers(
    key_id: str,
    private_key: RSAPrivateKey,
    method: str,
    path: str,
    timestamp_ms: int | None = None,
) -> dict[str, str]:
    """Return the three Kalshi auth headers for an HTTP or WebSocket request."""
    ts_str, sig = sign_request(private_key, method, path, timestamp_ms)
    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-TIMESTAMP": ts_str,
        "KALSHI-ACCESS-SIGNATURE": sig,
    }
