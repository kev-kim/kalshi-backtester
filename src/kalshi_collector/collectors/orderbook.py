"""Orderbook collector.

Primary: WebSocket orderbook_delta channel (initial snapshot + incremental deltas).
Fallback: REST polling at ORDERBOOK_POLL_INTERVAL_MS when WS is unavailable.

The collector maintains an in-memory OrderbookState per market, applies deltas,
and calls maybe_append_orderbook() — which only writes if the reconstructed
snapshot differs from the previous one (change-only semantics).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import asyncpg

from kalshi_collector.config import Settings
from kalshi_collector.http_client import KalshiHttpClient
from kalshi_collector.logging import get_logger
from kalshi_collector.models import (
    GetOrderbookResponse,
    OrderbookDeltaMsg,
    OrderbookSnapshotMsg,
    OrderbookState,
    WsBaseMessage,
)
from kalshi_collector.storage.checkpoints import write_checkpoint
from kalshi_collector.storage.writers import maybe_append_orderbook

log = get_logger(__name__)


class OrderbookCollector:
    """Manages live orderbook state and persistence for a set of markets."""

    def __init__(
        self,
        client: KalshiHttpClient,
        pool: asyncpg.Pool,
        settings: Settings,
        market_id_map: dict[str, int],  # ticker -> DB id
    ) -> None:
        self._client = client
        self._pool = pool
        self._poll_interval = settings.orderbook_poll_interval_ms / 1000.0
        self._market_id_map = market_id_map
        self._states: dict[str, OrderbookState] = {}
        self._running = False
        self._ws_active = False

    def add_market(self, ticker: str) -> None:
        if ticker not in self._states:
            self._states[ticker] = OrderbookState(market_ticker=ticker)

    def remove_market(self, ticker: str) -> None:
        self._states.pop(ticker, None)

    # ------------------------------------------------------------------
    # WebSocket message handling
    # ------------------------------------------------------------------

    async def handle_ws_message(self, msg: WsBaseMessage) -> None:
        """Route orderbook WS messages to the correct handler."""
        if msg.type == "orderbook_snapshot":
            await self._handle_snapshot(msg)
        elif msg.type == "orderbook_delta":
            await self._handle_delta(msg)

    async def _handle_snapshot(self, msg: WsBaseMessage) -> None:
        try:
            snap = OrderbookSnapshotMsg.model_validate(msg.msg)
        except Exception:
            log.warning("ob_snapshot_parse_failed", raw=str(msg.msg)[:200])
            return

        ticker = snap.market_ticker
        state = self._states.setdefault(ticker, OrderbookState(market_ticker=ticker))
        state.yes_bids = {Decimal(p): Decimal(q) for p, q in snap.yes_dollars_fp}
        state.no_bids = {Decimal(p): Decimal(q) for p, q in snap.no_dollars_fp}
        self._ws_active = True
        await self._persist(ticker, state, datetime.now(tz=timezone.utc))

        if msg.seq is not None:
            market_id = self._market_id_map.get(ticker)
            if market_id:
                async with self._pool.acquire() as conn:
                    await write_checkpoint(
                        conn, "orderbook_delta", market_id, last_seq=msg.seq
                    )

    async def _handle_delta(self, msg: WsBaseMessage) -> None:
        try:
            delta = OrderbookDeltaMsg.model_validate(msg.msg)
        except Exception:
            log.warning("ob_delta_parse_failed", raw=str(msg.msg)[:200])
            return

        ticker = delta.market_ticker
        state = self._states.get(ticker)
        if state is None:
            log.debug("ob_delta_unknown_ticker", ticker=ticker)
            return

        state.apply_delta(delta)

        ts = (
            datetime.fromtimestamp(delta.ts_ms / 1000, tz=timezone.utc)
            if delta.ts_ms
            else datetime.now(tz=timezone.utc)
        )
        await self._persist(ticker, state, ts)

        if msg.seq is not None:
            market_id = self._market_id_map.get(ticker)
            if market_id:
                async with self._pool.acquire() as conn:
                    await write_checkpoint(
                        conn, "orderbook_delta", market_id, last_seq=msg.seq
                    )

    # ------------------------------------------------------------------
    # REST polling fallback
    # ------------------------------------------------------------------

    async def poll_once(self, ticker: str) -> None:
        """Fetch orderbook via REST and persist if changed."""
        try:
            resp = await self._client.get(f"/markets/{ticker}/orderbook", {"depth": 0})
            data = GetOrderbookResponse.model_validate(resp.json())
        except Exception:
            log.exception("ob_poll_failed", ticker=ticker)
            return

        state = self._states.setdefault(ticker, OrderbookState(market_ticker=ticker))
        from decimal import Decimal

        state.yes_bids = {Decimal(p): Decimal(q) for p, q in data.orderbook_fp.yes_dollars}
        state.no_bids = {Decimal(p): Decimal(q) for p, q in data.orderbook_fp.no_dollars}

        await self._persist(ticker, state, datetime.now(tz=timezone.utc))

    async def run_polling(self, tickers: list[str]) -> None:
        """Polling loop — used when WS is unavailable for a market class."""
        log.info("ob_polling_fallback_started", market_count=len(tickers))
        self._running = True
        while self._running:
            tasks = [self.poll_once(t) for t in tickers]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(self._poll_interval)

    def stop(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    async def _persist(
        self,
        ticker: str,
        state: OrderbookState,
        kalshi_ts: datetime,
    ) -> None:
        market_id = self._market_id_map.get(ticker)
        if market_id is None:
            return

        async with self._pool.acquire() as conn:
            wrote = await maybe_append_orderbook(conn, market_id, state, kalshi_ts)

        if wrote:
            log.debug("ob_snapshot_written", ticker=ticker, ts=kalshi_ts.isoformat())
