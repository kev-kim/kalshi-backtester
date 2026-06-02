#!/usr/bin/env python3
"""Backfill settlements and metadata for already-closed esports markets.

This is the only historical data retrievable from Kalshi. It walks:
  - GET /historical/markets?series_ticker=<t>  (markets older than ~90d cutoff)
  - GET /markets?series_ticker=<t>&status=finalized  (recent finalized markets)

For each discovered market it upserts the market record and settlement row.
Historical trade data is also fetched where available.

Usage:
    python scripts/backfill_settlements.py [--env demo|prod] [--series TICKER ...]

If --series is omitted, all esports series are discovered automatically.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# Allow running from repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import asyncpg

from kalshi_collector.auth import load_private_key
from kalshi_collector.config import KalshiEnv, Settings
from kalshi_collector.discovery import _ESPORTS_TAGS, _fetch_esports_series
from kalshi_collector.http_client import KalshiHttpClient
from kalshi_collector.logging import configure_logging, get_logger
from kalshi_collector.models import MarketLifecycleMsg, MarketModel
from kalshi_collector.storage.db import create_pool, run_migrations
from kalshi_collector.storage.writers import upsert_event, upsert_market, upsert_series, upsert_settlement

log = get_logger(__name__)


async def _fetch_historical_markets(
    client: KalshiHttpClient,
    series_ticker: str,
) -> list[MarketModel]:
    """Fetch settled markets from both historical and live endpoints."""
    markets: dict[str, MarketModel] = {}

    # Historical endpoint (>90 days old)
    raw = await client.get_paginated(
        "/historical/markets",
        {"series_ticker": series_ticker, "limit": 1000},
        "markets",
    )
    for item in raw:
        m = MarketModel.model_validate(item)
        markets[m.ticker] = m

    # Recent finalized markets (<90 days, already closed)
    raw = await client.get_paginated(
        "/markets",
        {"series_ticker": series_ticker, "status": "finalized", "limit": 1000},
        "markets",
    )
    for item in raw:
        m = MarketModel.model_validate(item)
        markets[m.ticker] = m

    return list(markets.values())


async def backfill_series(
    client: KalshiHttpClient,
    pool: asyncpg.Pool,
    series_ticker: str,
) -> int:
    """Backfill all settled markets for one series. Returns count written."""
    log.info("backfill_series_start", series=series_ticker)
    markets = await _fetch_historical_markets(client, series_ticker)
    log.info("backfill_markets_found", series=series_ticker, count=len(markets))

    written = 0
    for market in markets:
        if not market.result:
            continue  # not settled, skip

        # Ensure series_ticker is populated
        if not market.series_ticker:
            market = market.model_copy(update={"series_ticker": series_ticker})

        # Fetch the event so we can upsert it first (FK requirement)
        try:
            resp = await client.get(f"/events/{market.event_ticker}")
            event_data = resp.json().get("event", {})
            if event_data:
                from kalshi_collector.models import EventModel
                event = EventModel.model_validate(event_data)
                async with pool.acquire() as conn:
                    await upsert_event(conn, event)
        except Exception:
            log.warning("backfill_event_fetch_failed", event_ticker=market.event_ticker)

        async with pool.acquire() as conn:
            market_id = await upsert_market(conn, market)

            msg = MarketLifecycleMsg(
                event_type="settled",
                market_ticker=market.ticker,
                result=market.result,
                settlement_value=market.last_price_dollars,
                settled_ts=None,
            )
            await upsert_settlement(conn, market_id, msg)

        written += 1
        log.info("backfill_market_written", ticker=market.ticker, result=market.result)

    return written


async def run_backfill(settings: Settings, series_tickers: list[str] | None) -> None:
    pool = await create_pool(settings)
    await run_migrations(pool)

    private_key = load_private_key(settings.kalshi_private_key_path)

    async with KalshiHttpClient(
        settings.rest_base_url, settings.kalshi_api_key_id, private_key
    ) as client:
        if not series_tickers:
            log.info("backfill_discovering_series")
            series_list = await _fetch_esports_series(client)
            series_tickers = [s.ticker for s in series_list]
            # Upsert series records while we have them
            for s in series_list:
                async with pool.acquire() as conn:
                    await upsert_series(conn, s)
            log.info("backfill_series_discovered", count=len(series_tickers))

        total = 0
        for ticker in series_tickers:
            try:
                count = await backfill_series(client, pool, ticker)
                total += count
            except Exception:
                log.exception("backfill_series_failed", series=ticker)

    await pool.close()
    log.info("backfill_complete", total_markets=total)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Kalshi esports settlement history")
    parser.add_argument("--env", choices=["demo", "prod"], default=None)
    parser.add_argument(
        "--series",
        nargs="*",
        metavar="TICKER",
        help="Series tickers to backfill. Omit to discover all esports series.",
    )
    args = parser.parse_args()

    settings = Settings()  # type: ignore[call-arg]
    if args.env:
        import os
        os.environ["KALSHI_ENV"] = args.env
        settings = Settings()  # type: ignore[call-arg]

    configure_logging(settings.log_level)
    asyncio.run(run_backfill(settings, args.series or None))


if __name__ == "__main__":
    main()
