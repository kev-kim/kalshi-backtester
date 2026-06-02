"""Signed WebSocket client for Kalshi.

Auth: three headers on the upgrade request (same RSA-PSS scheme as REST),
signing over timestamp_ms + "GET" + "/trade-api/ws/v2".

Handles reconnection with exponential backoff. Dispatches incoming JSON
messages to a registered async callback. Supports subscribe / add_markets /
delete_markets commands.
"""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any

import websockets
import websockets.exceptions
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from kalshi_collector.auth import build_auth_headers
from kalshi_collector.logging import get_logger

log = get_logger(__name__)

_WS_SIGN_PATH = "/trade-api/ws/v2"
_RECONNECT_BASE = 1.0
_RECONNECT_MAX = 60.0
MessageCallback = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]


class KalshiWsClient:
    """Signed WebSocket client with automatic reconnection."""

    def __init__(
        self,
        ws_url: str,
        key_id: str,
        private_key: RSAPrivateKey,
        on_message: MessageCallback,
    ) -> None:
        self._ws_url = ws_url
        self._key_id = key_id
        self._private_key = private_key
        self._on_message = on_message
        self._subscriptions: list[dict[str, Any]] = []
        self._cmd_id = 0
        self._ws: Any = None
        self._running = False

    def _next_id(self) -> int:
        self._cmd_id += 1
        return self._cmd_id

    def _auth_headers(self) -> dict[str, str]:
        return build_auth_headers(
            self._key_id, self._private_key, "GET", _WS_SIGN_PATH
        )

    async def start(self) -> None:
        """Connect and run the receive loop with reconnection."""
        self._running = True
        backoff = _RECONNECT_BASE

        while self._running:
            try:
                async with websockets.connect(
                    self._ws_url,
                    additional_headers=self._auth_headers(),
                    ping_interval=20,
                    ping_timeout=30,
                ) as ws:
                    self._ws = ws
                    log.info("ws_connected", url=self._ws_url)
                    backoff = _RECONNECT_BASE  # reset on successful connect

                    # Re-subscribe to all channels after reconnect
                    for sub in self._subscriptions:
                        await self._send(sub)

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            log.warning("ws_bad_json", raw=raw[:200])
                            continue
                        try:
                            await self._on_message(msg)
                        except Exception:
                            log.exception("ws_callback_error", msg_type=msg.get("type"))

            except websockets.exceptions.ConnectionClosed as exc:
                log.warning("ws_disconnected", code=exc.code, reason=exc.reason)
            except OSError as exc:
                log.warning("ws_os_error", error=str(exc))
            except Exception:
                log.exception("ws_unexpected_error")

            if not self._running:
                break

            jitter = random.uniform(0, backoff * 0.3)
            wait = backoff + jitter
            log.info("ws_reconnecting", wait_s=round(wait, 1))
            await asyncio.sleep(wait)
            backoff = min(backoff * 2, _RECONNECT_MAX)

    async def stop(self) -> None:
        self._running = False
        if self._ws is not None:
            await self._ws.close()

    async def _send(self, payload: dict[str, Any]) -> None:
        if self._ws is None:
            log.warning("ws_send_before_connect")
            return
        try:
            await self._ws.send(json.dumps(payload))
        except websockets.exceptions.ConnectionClosed:
            log.warning("ws_send_on_closed_connection")

    async def subscribe(
        self,
        channels: list[str],
        market_tickers: list[str] | None = None,
    ) -> None:
        """Subscribe to one or more channels, optionally for specific markets."""
        params: dict[str, Any] = {"channels": channels}
        if market_tickers:
            params["market_tickers"] = market_tickers

        cmd = {"id": self._next_id(), "cmd": "subscribe", "params": params}
        self._subscriptions.append(cmd)
        await self._send(cmd)

    async def add_markets(self, sid: int, market_tickers: list[str]) -> None:
        """Add markets to an existing subscription."""
        await self._send({
            "id": self._next_id(),
            "cmd": "update_subscription",
            "params": {
                "sid": sid,
                "action": "add_markets",
                "market_tickers": market_tickers,
            },
        })

    async def delete_markets(self, sid: int, market_tickers: list[str]) -> None:
        """Remove markets from an existing subscription."""
        await self._send({
            "id": self._next_id(),
            "cmd": "update_subscription",
            "params": {
                "sid": sid,
                "action": "delete_markets",
                "market_tickers": market_tickers,
            },
        })

    async def messages(self) -> AsyncIterator[dict[str, Any]]:
        """Async generator yielding parsed messages. Alternative to callback API."""
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

        async def _enqueue(msg: dict[str, Any]) -> None:
            await queue.put(msg)

        original = self._on_message
        self._on_message = _enqueue
        try:
            while True:
                yield await queue.get()
        finally:
            self._on_message = original
