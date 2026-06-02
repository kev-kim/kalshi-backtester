"""Signed async HTTP client with two-bucket rate limiting and tenacity retry.

Rate-limit model (Kalshi Basic tier defaults):
  Read  bucket: 200 tokens/s, burst = 400 (2s headroom)
  Write bucket: 100 tokens/s, burst = 100 (1s headroom)

Every GET costs 10 read tokens; this collector never writes, so the write
bucket is initialised but unused. Buckets refill on a real-time token basis
(not a fixed window).
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any
from urllib.parse import urlparse

import httpx
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from kalshi_collector.auth import build_auth_headers
from kalshi_collector.logging import get_logger

log = get_logger(__name__)

_DEFAULT_READ_RATE = 200.0   # tokens/second (Basic tier)
_DEFAULT_READ_BURST = 400.0
_DEFAULT_READ_COST = 10.0

_MAX_RETRIES = 6
_BASE_WAIT = 1.0
_MAX_WAIT = 60.0


class TokenBucket:
    """Async token bucket for rate limiting."""

    def __init__(self, rate: float, burst: float) -> None:
        self._rate = rate
        self._burst = burst
        self._tokens = burst
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, cost: float = 1.0) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
            self._last_refill = now

            if self._tokens < cost:
                wait = (cost - self._tokens) / self._rate
                await asyncio.sleep(wait)
                self._tokens = 0.0
            else:
                self._tokens -= cost


class _RateLimitError(Exception):
    """Raised internally when a 429 is received, to trigger tenacity retry."""


class KalshiHttpClient:
    """Signed httpx AsyncClient with per-bucket rate limiting."""

    def __init__(
        self,
        base_url: str,
        key_id: str,
        private_key: RSAPrivateKey,
        read_rate: float = _DEFAULT_READ_RATE,
        read_burst: float = _DEFAULT_READ_BURST,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._key_id = key_id
        self._private_key = private_key
        self._read_bucket = TokenBucket(read_rate, read_burst)
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "KalshiHttpClient":
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=httpx.Timeout(30.0),
            headers={"Content-Type": "application/json"},
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._client:
            await self._client.aclose()

    def _path_from_url(self, url: str) -> str:
        """Extract path (no query) from a relative or absolute URL."""
        if url.startswith("/"):
            return url.split("?")[0]
        parsed = urlparse(url)
        return parsed.path

    async def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        cost: float = _DEFAULT_READ_COST,
    ) -> httpx.Response:
        """Signed GET with rate-limit enforcement and retry."""
        await self._read_bucket.acquire(cost)
        return await self._get_with_retry(path, params)

    @retry(
        retry=retry_if_exception_type(_RateLimitError),
        wait=wait_exponential_jitter(initial=_BASE_WAIT, max=_MAX_WAIT, jitter=2.0),
        stop=stop_after_attempt(_MAX_RETRIES),
        reraise=True,
    )
    async def _get_with_retry(
        self,
        path: str,
        params: dict[str, Any] | None,
    ) -> httpx.Response:
        assert self._client is not None, "Client not started — use as async context manager"

        signed_path = self._path_from_url(path)
        headers = build_auth_headers(self._key_id, self._private_key, "GET", signed_path)

        t0 = time.monotonic()
        resp = await self._client.get(path, params=params, headers=headers)
        latency_ms = int((time.monotonic() - t0) * 1000)

        rate_limited = resp.status_code == 429
        log.debug(
            "api_request",
            method="GET",
            path=signed_path,
            status=resp.status_code,
            latency_ms=latency_ms,
            rate_limited=rate_limited,
        )

        if rate_limited:
            jitter = random.uniform(0.5, 1.5)
            log.warning("rate_limited", path=signed_path, backoff_hint=f"{jitter:.1f}s")
            await asyncio.sleep(jitter)
            raise _RateLimitError(f"429 on {signed_path}")

        resp.raise_for_status()
        return resp

    async def get_paginated(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        response_key: str = "items",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Paginate through a cursor-based endpoint, returning all items."""
        params = dict(params or {})
        params["limit"] = limit
        results: list[dict[str, Any]] = []

        while True:
            resp = await self.get(path, params)
            data = resp.json()
            items = data.get(response_key) or []
            results.extend(items)

            cursor = data.get("cursor")
            if not cursor or not items:
                break
            params["cursor"] = cursor

        return results
