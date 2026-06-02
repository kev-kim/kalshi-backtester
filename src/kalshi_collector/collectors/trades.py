"""Trades collector.

Primary: WebSocket 'trade' channel — one subscription per watched market.
Fallback: REST polling GET /markets/trades?ticker=<t>&min_ts=<last_seen>.

WS trade messages are immediately persisted. REST polling uses the last-seen
trade timestamp from checkpoints to avoid re-inserting duplicates (ON CONFLICT
DO NOTHING handles any overlap).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import asyncpg

from kalshi_collector.config import Settings
from kalshi_collector.http_client import KalshiHttpClient
from kalshi_collector.logging import get_logger
from kalshi_collector.models import GetTradesResponse, TradeModel, WsBaseMessage, WsTradeMsg
from kalshi_collector.storage.checkpoints import read_checkpoint, write_checkpoint
from kalshi_collector.storage.writers import append_trade

log = get_logger(__name__)

_POLL_INTERVAL_SEC = 5.0


class TradesCollector:
    """Persists trades from WebSocket or REST polling."""

    def __init__(
        self,
        client: KalshiHttpClient,
        pool: asyncpg.Pool,
        settings: Settings,
        market_id_map: dict[str, int],
    ) -> None:
        self._client = client
        self._pool = pool
        self._market_id_map = market_id_map
        self._running = False

    # ------------------------------------------------------------------
    # WebSocket handler
    # ------------------------------------------------------------------

    async def handle_ws_message(self, msg: WsBaseMessage) -> None:
        if msg.type != "trade":
            return

        try:
            ws_trade = WsTradeMsg.model_validate(msg.msg)
        except Exception:
            log.warning("trade_ws_parse_failed", raw=str(msg.msg)[:200])
            return

        ticker = ws_trade.market_ticker
        market_id = self._market_id_map.get(ticker)
        if market_id is None:
            log.debug("trade_unknown_ticker", ticker=ticker)
            return

        trade = TradeModel(
            trade_id=ws_trade.trade_id,
            ticker=ticker,
            count_fp=ws_trade.count_fp,
            yes_price_dollars=ws_trade.yes_price_dollars,
            no_price_dollars=ws_trade.no_price_dollars,
            taker_outcome_side=ws_trade.taker_outcome_side,
            taker_side=ws_trade.taker_side,
            taker_book_side=ws_trade.taker_book_side,
            ts_ms=ws_trade.ts_ms,
            is_block_trade=ws_trade.is_block_trade,
        )

        async with self._pool.acquire() as conn:
            await append_trade(conn, market_id, trade)
            if msg.seq is not None:
                await write_checkpoint(conn, "trade", market_id, last_seq=msg.seq)

        log.debug("trade_written", ticker=ticker, trade_id=ws_trade.trade_id)

    # ------------------------------------------------------------------
    # REST polling fallback
    # ------------------------------------------------------------------

    async def poll_once(self, ticker: str) -> None:
        market_id = self._market_id_map.get(ticker)
        if market_id is None:
            return

        async with self._pool.acquire() as conn:
            _, last_ts = await read_checkpoint(conn, "poll_trades", market_id)

        params: dict[str, object] = {"ticker": ticker, "limit": 1000}
        if last_ts is not None:
            params["min_ts"] = int(last_ts.timestamp())

        try:
            resp = await self._client.get("/markets/trades", params)
            data = GetTradesResponse.model_validate(resp.json())
        except Exception:
            log.exception("trades_poll_failed", ticker=ticker)
            return

        if not data.trades:
            return

        newest_ts: datetime | None = None
        async with self._pool.acquire() as conn:
            for trade in data.trades:
                await append_trade(conn, market_id, trade)
                ts = trade.kalshi_ts
                if newest_ts is None or ts > newest_ts:
                    newest_ts = ts

            if newest_ts is not None:
                await write_checkpoint(conn, "poll_trades", market_id, last_ts=newest_ts)

        log.debug("trades_polled", ticker=ticker, count=len(data.trades))

    async def run_polling(self, tickers: list[str]) -> None:
        """Polling loop — used when WS is unavailable."""
        log.info("trades_polling_fallback_started", market_count=len(tickers))
        self._running = True
        while self._running:
            tasks = [self.poll_once(t) for t in tickers]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(_POLL_INTERVAL_SEC)

    def stop(self) -> None:
        self._running = False
