"""Settlement collector.

Captures settlement outcomes two ways:
  1. Real-time: market_lifecycle_v2 WS events with event_type='settled'
     are handled by MetadataCollector (which calls upsert_settlement).
  2. Periodic REST check: polls /markets for 'finalized' status on all
     known markets and writes settlements that may have been missed
     (e.g. during collector downtime).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import asyncpg

from kalshi_collector.http_client import KalshiHttpClient
from kalshi_collector.logging import get_logger
from kalshi_collector.models import MarketLifecycleMsg, MarketModel
from kalshi_collector.storage.writers import upsert_settlement

log = get_logger(__name__)

_POLL_INTERVAL_SEC = 60


class SettlementsCollector:
    """Polls for finalized markets and records settlement outcomes."""

    def __init__(
        self,
        client: KalshiHttpClient,
        pool: asyncpg.Pool,
        market_id_map: dict[str, int],
    ) -> None:
        self._client = client
        self._pool = pool
        self._market_id_map = market_id_map
        self._running = False

    async def poll_once(self, tickers: list[str]) -> None:
        """Check for newly finalized markets among the watched tickers."""
        if not tickers:
            return

        for i in range(0, len(tickers), 100):
            chunk = tickers[i : i + 100]
            try:
                resp = await self._client.get(
                    "/markets",
                    {"tickers": ",".join(chunk), "status": "finalized", "limit": len(chunk)},
                )
                data = resp.json()
            except Exception:
                log.exception("settlements_poll_failed", chunk_start=i)
                continue

            for raw in data.get("markets", []):
                market = MarketModel.model_validate(raw)
                if market.result is None:
                    continue

                market_id = self._market_id_map.get(market.ticker)
                if market_id is None:
                    continue

                # Build a synthetic MarketLifecycleMsg from the REST response
                settled_ts_epoch: int | None = None
                if hasattr(market, "close_time") and market.close_time:
                    settled_ts_epoch = int(market.close_time.timestamp())

                msg = MarketLifecycleMsg(
                    event_type="settled",
                    market_ticker=market.ticker,
                    result=market.result,
                    settled_ts=settled_ts_epoch,
                    settlement_value=market.last_price_dollars,
                )
                async with self._pool.acquire() as conn:
                    await upsert_settlement(conn, market_id, msg)

                log.info(
                    "settlement_recorded",
                    ticker=market.ticker,
                    result=market.result,
                )

    async def run(self, tickers: list[str]) -> None:
        self._running = True
        while self._running:
            await self.poll_once(tickers)
            await asyncio.sleep(_POLL_INTERVAL_SEC)

    def stop(self) -> None:
        self._running = False
