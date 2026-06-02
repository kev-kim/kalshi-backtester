"""Esports market discovery loop.

Strategy (per KNOWN_LIMITATIONS.md §7):
  1. GET /series?category=esports → collect all esports series tickers.
  2. GET /search/tags_by_categories → enumerate esports-related tags for
     supplemental series queries (e.g. 'valorant', 'counter-strike').
  3. For each series ticker: GET /events?series_ticker=<t>&status=open
  4. For each event: GET /markets?event_ticker=<t>&status=open
  5. Upsert series/events/markets. Yield newly seen market tickers to callers.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import asyncpg

from kalshi_collector.config import Settings
from kalshi_collector.http_client import KalshiHttpClient
from kalshi_collector.logging import get_logger
from kalshi_collector.models import (
    EventModel,
    GetEventsResponse,
    GetMarketsResponse,
    GetSeriesListResponse,
    MarketModel,
    SeriesModel,
)
from kalshi_collector.storage.writers import upsert_event, upsert_market, upsert_series

log = get_logger(__name__)

# Kalshi uses category="Sports" for esports; the Esports tag is the real filter.
# "Video games" catches some additional series (e.g. LoL Worlds).
_ESPORTS_TAGS = ["Esports", "Video games"]


async def _fetch_esports_series(client: KalshiHttpClient) -> list[SeriesModel]:
    """Fetch all series with esports-related tags."""
    seen: dict[str, SeriesModel] = {}

    for tag in _ESPORTS_TAGS:
        raw = await client.get_paginated("/series", {"tags": tag}, "series")
        for item in raw:
            s = SeriesModel.model_validate(item)
            seen[s.ticker] = s

    return list(seen.values())


async def _fetch_events_for_series(
    client: KalshiHttpClient,
    series_ticker: str,
) -> list[EventModel]:
    raw = await client.get_paginated(
        "/events",
        {"series_ticker": series_ticker, "status": "open"},
        "events",
        limit=200,
    )
    return [EventModel.model_validate(e) for e in raw]


async def _fetch_markets_for_event(
    client: KalshiHttpClient,
    event_ticker: str,
) -> list[MarketModel]:
    raw = await client.get_paginated(
        "/markets",
        {"event_ticker": event_ticker, "status": "open"},
        "markets",
        limit=1000,
    )
    return [MarketModel.model_validate(m) for m in raw]


class DiscoveryLoop:
    """Continuously discovers new esports markets and upserts them to the DB."""

    def __init__(
        self,
        client: KalshiHttpClient,
        pool: asyncpg.Pool,
        settings: Settings,
    ) -> None:
        self._client = client
        self._pool = pool
        self._interval = settings.discovery_interval_sec
        self._known_tickers: set[str] = set()
        self._running = False

    async def run_once(self) -> list[MarketModel]:
        """Run one discovery pass. Returns list of newly discovered markets."""
        new_markets: list[MarketModel] = []

        try:
            series_list = await _fetch_esports_series(self._client)
        except Exception:
            log.exception("discovery_series_fetch_failed")
            return []

        log.info("discovery_series_found", count=len(series_list))

        for series in series_list:
            async with self._pool.acquire() as conn:
                await upsert_series(conn, series)

            try:
                events = await _fetch_events_for_series(self._client, series.ticker)
            except Exception:
                log.exception("discovery_events_fetch_failed", series=series.ticker)
                continue

            for event in events:
                # Ensure series_ticker is set (may be missing if event came
                # with_nested_markets but series_ticker not populated)
                if not event.series_ticker:
                    event = event.model_copy(update={"series_ticker": series.ticker})

                async with self._pool.acquire() as conn:
                    await upsert_event(conn, event)

                try:
                    markets = await _fetch_markets_for_event(self._client, event.event_ticker)
                except Exception:
                    log.exception("discovery_markets_fetch_failed", event=event.event_ticker)
                    continue

                for market in markets:
                    if not market.series_ticker:
                        market = market.model_copy(update={"series_ticker": series.ticker})

                    async with self._pool.acquire() as conn:
                        await upsert_market(conn, market)

                    if market.ticker not in self._known_tickers:
                        self._known_tickers.add(market.ticker)
                        new_markets.append(market)
                        log.info("discovery_new_market", ticker=market.ticker)

        return new_markets

    async def run(
        self,
        on_new_markets: "asyncio.Queue[list[MarketModel]] | None" = None,
    ) -> None:
        """Run discovery loop continuously at the configured interval."""
        self._running = True
        while self._running:
            new = await self.run_once()
            if new and on_new_markets is not None:
                await on_new_markets.put(new)
            await asyncio.sleep(self._interval)

    def stop(self) -> None:
        self._running = False

    @property
    def known_tickers(self) -> frozenset[str]:
        return frozenset(self._known_tickers)
