"""Market metadata collector.

Periodically re-fetches market metadata for all active markets and upserts
any changes. Also handles market_lifecycle_v2 WebSocket events to capture
status changes in near-real-time.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import asyncpg

from kalshi_collector.http_client import KalshiHttpClient
from kalshi_collector.logging import get_logger
from kalshi_collector.models import MarketLifecycleMsg, MarketModel, WsBaseMessage
from kalshi_collector.storage.writers import (
    append_market_update,
    upsert_market,
    upsert_settlement,
)

log = get_logger(__name__)

_POLL_INTERVAL_SEC = 300   # re-poll metadata every 5 minutes


class MetadataCollector:
    """Maintains up-to-date market metadata and appends change-log rows."""

    def __init__(
        self,
        client: KalshiHttpClient,
        pool: asyncpg.Pool,
        market_id_map: dict[str, int],  # ticker -> DB id
    ) -> None:
        self._client = client
        self._pool = pool
        self._market_id_map = market_id_map
        self._running = False

    async def poll_once(self, tickers: list[str]) -> None:
        """Fetch and upsert current metadata for the given market tickers."""
        if not tickers:
            return

        # Batch into chunks of 100 to stay within query param limits
        for i in range(0, len(tickers), 100):
            chunk = tickers[i : i + 100]
            try:
                resp = await self._client.get(
                    "/markets",
                    {"tickers": ",".join(chunk), "limit": len(chunk)},
                )
                data = resp.json()
            except Exception:
                log.exception("metadata_poll_failed", chunk_start=i)
                continue

            for raw in data.get("markets", []):
                market = MarketModel.model_validate(raw)
                market_id = self._market_id_map.get(market.ticker)
                if market_id is None:
                    continue

                async with self._pool.acquire() as conn:
                    await upsert_market(conn, market)
                    await append_market_update(
                        conn,
                        market_id,
                        market,
                        kalshi_ts=datetime.now(tz=timezone.utc),
                    )

    async def handle_lifecycle_event(self, msg: WsBaseMessage) -> None:
        """Process a market_lifecycle_v2 WebSocket message."""
        try:
            lifecycle = MarketLifecycleMsg.model_validate(msg.msg)
        except Exception:
            log.warning("lifecycle_parse_failed", raw=msg.msg)
            return

        market_id = self._market_id_map.get(lifecycle.market_ticker)
        if market_id is None:
            log.debug("lifecycle_unknown_ticker", ticker=lifecycle.market_ticker)
            return

        log.info(
            "lifecycle_event",
            ticker=lifecycle.market_ticker,
            event_type=lifecycle.event_type,
        )

        if lifecycle.event_type == "settled":
            async with self._pool.acquire() as conn:
                await upsert_settlement(conn, market_id, lifecycle)

    async def run(self, tickers: list[str]) -> None:
        self._running = True
        while self._running:
            await self.poll_once(tickers)
            await asyncio.sleep(_POLL_INTERVAL_SEC)

    def stop(self) -> None:
        self._running = False
