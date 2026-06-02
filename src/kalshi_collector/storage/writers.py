"""Append-only and upsert writers for all Kalshi data tables.

Design rules (from spec):
  - Append-only: market_updates, orderbook_snapshots, trades
  - Idempotent upsert: series, events, markets, settlements (keyed on natural IDs)
  - Change-only orderbook: skip write if new snapshot == previous checksum
  - ON CONFLICT DO NOTHING for observations
  - ON CONFLICT DO UPDATE for entities
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import asyncpg

from kalshi_collector.logging import get_logger
from kalshi_collector.models import (
    EventModel,
    MarketLifecycleMsg,
    MarketModel,
    OrderbookState,
    SeriesModel,
    TradeModel,
)

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def _orderbook_checksum(yes_levels: list[Any], no_levels: list[Any]) -> str:
    raw = json.dumps({"y": yes_levels, "n": no_levels}, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Entity upserts (idempotent)
# ---------------------------------------------------------------------------

async def upsert_series(conn: asyncpg.Connection, s: SeriesModel) -> None:
    await conn.execute(
        """
        INSERT INTO series
            (ticker, title, category, tags, frequency,
             settlement_sources, additional_metadata, ingest_ts, last_seen_ts)
        VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())
        ON CONFLICT (ticker) DO UPDATE SET
            title                = EXCLUDED.title,
            category             = EXCLUDED.category,
            tags                 = EXCLUDED.tags,
            frequency            = EXCLUDED.frequency,
            settlement_sources   = EXCLUDED.settlement_sources,
            additional_metadata  = EXCLUDED.additional_metadata,
            last_seen_ts         = now()
        """,
        s.ticker,
        s.title,
        s.category,
        s.tags,
        s.frequency,
        json.dumps(s.settlement_sources) if s.settlement_sources else None,
        json.dumps(s.additional_metadata) if s.additional_metadata else None,
    )


async def upsert_event(conn: asyncpg.Connection, e: EventModel) -> None:
    await conn.execute(
        """
        INSERT INTO events
            (ticker, series_ticker, title, category, status,
             open_time, close_time, additional_metadata, ingest_ts, last_seen_ts)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), now())
        ON CONFLICT (ticker) DO UPDATE SET
            title               = EXCLUDED.title,
            category            = EXCLUDED.category,
            status              = EXCLUDED.status,
            open_time           = EXCLUDED.open_time,
            close_time          = EXCLUDED.close_time,
            additional_metadata = EXCLUDED.additional_metadata,
            last_seen_ts        = now()
        """,
        e.event_ticker,
        e.series_ticker,
        e.title,
        e.category,
        e.status,
        e.open_time,
        e.close_time,
        json.dumps(e.additional_metadata) if e.additional_metadata else None,
    )


async def upsert_market(conn: asyncpg.Connection, m: MarketModel) -> int:
    """Upsert market and return its internal DB id."""
    row = await conn.fetchrow(
        """
        INSERT INTO markets
            (ticker, event_ticker, series_ticker, market_type, title, subtitle,
             yes_sub_title, no_sub_title, open_time, close_time, status, result,
             rules_primary, rules_secondary, price_level_structure,
             additional_metadata, ingest_ts, last_seen_ts)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,now(),now())
        ON CONFLICT (ticker) DO UPDATE SET
            series_ticker       = EXCLUDED.series_ticker,
            market_type         = EXCLUDED.market_type,
            title               = EXCLUDED.title,
            subtitle            = EXCLUDED.subtitle,
            yes_sub_title       = EXCLUDED.yes_sub_title,
            no_sub_title        = EXCLUDED.no_sub_title,
            open_time           = EXCLUDED.open_time,
            close_time          = EXCLUDED.close_time,
            status              = EXCLUDED.status,
            result              = EXCLUDED.result,
            rules_primary       = EXCLUDED.rules_primary,
            rules_secondary     = EXCLUDED.rules_secondary,
            price_level_structure = EXCLUDED.price_level_structure,
            additional_metadata = EXCLUDED.additional_metadata,
            last_seen_ts        = now()
        RETURNING id
        """,
        m.ticker,
        m.event_ticker,
        m.series_ticker or "",
        m.market_type,
        m.title,
        m.subtitle,
        m.yes_sub_title,
        m.no_sub_title,
        m.open_time,
        m.close_time,
        m.status,
        m.result,
        m.rules_primary,
        m.rules_secondary,
        m.price_level_structure,
        json.dumps(m.additional_metadata) if m.additional_metadata else None,
    )
    return int(row["id"])  # type: ignore[index]


# ---------------------------------------------------------------------------
# Append-only observations
# ---------------------------------------------------------------------------

async def append_market_update(
    conn: asyncpg.Connection,
    market_id: int,
    market: MarketModel,
    kalshi_ts: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO market_updates
            (market_id, kalshi_ts, ingest_ts, status, result,
             yes_bid, yes_ask, no_bid, no_ask, last_price,
             volume, volume_24h, open_interest, additional_metadata)
        VALUES ($1,$2,now(),$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
        """,
        market_id,
        kalshi_ts,
        market.status,
        market.result,
        market.yes_bid,
        market.yes_ask,
        market.no_bid,
        market.no_ask,
        market.last_price,
        market.volume,
        market.volume_24h,
        market.open_interest,
        json.dumps(market.additional_metadata) if market.additional_metadata else None,
    )


async def maybe_append_orderbook(
    conn: asyncpg.Connection,
    market_id: int,
    state: OrderbookState,
    kalshi_ts: datetime,
) -> bool:
    """Write orderbook snapshot iff it differs from the previous one.

    Returns True if a row was written, False if skipped (no change).
    """
    yes_levels = state.to_sorted_levels("yes")
    no_levels = state.to_sorted_levels("no")
    checksum = _orderbook_checksum(yes_levels, no_levels)

    prev_checksum: str | None = await conn.fetchval(
        """
        SELECT checksum
        FROM orderbook_snapshots
        WHERE market_id = $1
        ORDER BY kalshi_ts DESC
        LIMIT 1
        """,
        market_id,
    )

    if prev_checksum == checksum:
        return False

    await conn.execute(
        """
        INSERT INTO orderbook_snapshots
            (market_id, kalshi_ts, ingest_ts, yes_bids, no_bids, checksum)
        VALUES ($1, $2, now(), $3, $4, $5)
        """,
        market_id,
        kalshi_ts,
        json.dumps(yes_levels),
        json.dumps(no_levels),
        checksum,
    )
    return True


async def append_trade(
    conn: asyncpg.Connection,
    market_id: int,
    trade: TradeModel,
) -> None:
    """Insert a trade; silently skip if already present (ON CONFLICT DO NOTHING)."""
    kalshi_ts = trade.kalshi_ts
    await conn.execute(
        """
        INSERT INTO trades
            (trade_id, market_id, ticker, kalshi_ts, ingest_ts,
             yes_price, no_price, count, taker_side, taker_book_side, is_block_trade)
        VALUES ($1,$2,$3,$4,now(),$5,$6,$7,$8,$9,$10)
        ON CONFLICT (trade_id, kalshi_ts) DO NOTHING
        """,
        trade.trade_id,
        market_id,
        trade.ticker,
        kalshi_ts,
        trade.yes_price,
        trade.no_price,
        trade.count,
        trade.taker_outcome_side or trade.taker_side or "",
        trade.taker_book_side or "",
        trade.is_block_trade,
    )


async def upsert_settlement(
    conn: asyncpg.Connection,
    market_id: int,
    msg: MarketLifecycleMsg,
) -> None:
    """Record settlement outcome for a market."""
    from datetime import timezone as _tz

    settled_ts: datetime
    if msg.settled_ts is not None:
        settled_ts = datetime.fromtimestamp(msg.settled_ts, tz=_tz.utc)
    else:
        settled_ts = _now_utc()

    settlement_value: Decimal | None = (
        Decimal(msg.settlement_value) if msg.settlement_value else None
    )

    await conn.execute(
        """
        INSERT INTO settlements
            (market_id, settled_ts, ingest_ts, result, settlement_value, additional_metadata)
        VALUES ($1, $2, now(), $3, $4, $5)
        ON CONFLICT (market_id) DO UPDATE SET
            settled_ts        = EXCLUDED.settled_ts,
            result            = EXCLUDED.result,
            settlement_value  = EXCLUDED.settlement_value,
            additional_metadata = EXCLUDED.additional_metadata
        """,
        market_id,
        settled_ts,
        msg.result or "unknown",
        settlement_value,
        None,
    )


# ---------------------------------------------------------------------------
# Operational logging
# ---------------------------------------------------------------------------

async def log_api_request(
    conn: asyncpg.Connection,
    *,
    run_id: int | None,
    endpoint: str,
    method: str = "GET",
    status_code: int | None,
    latency_ms: int | None,
    retry_count: int = 0,
    rate_limited: bool = False,
    error_message: str | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO api_requests
            (run_id, endpoint, method, status_code, latency_ms,
             retry_count, rate_limited, error_message)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        """,
        run_id,
        endpoint,
        method,
        status_code,
        latency_ms,
        retry_count,
        rate_limited,
        error_message,
    )


async def log_data_quality_event(
    conn: asyncpg.Connection,
    *,
    event_type: str,
    severity: str,
    message: str,
    market_id: int | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO data_quality_events
            (market_id, event_type, severity, message, payload)
        VALUES ($1, $2, $3, $4, $5)
        """,
        market_id,
        event_type,
        severity,
        message,
        json.dumps(payload) if payload else None,
    )
