"""Tests for DB writer idempotency and upsert correctness.

Requires Postgres (set via POSTGRES_* env vars). Skipped otherwise.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

import pytest
import pytest_asyncio

from kalshi_collector.models import EventModel, MarketLifecycleMsg, MarketModel, SeriesModel


def _db_available() -> bool:
    return bool(os.environ.get("POSTGRES_PASSWORD"))


@pytest.fixture(scope="module")
def event_loop():  # type: ignore[override]
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="module")
async def pool():  # type: ignore[return]
    if not _db_available():
        pytest.skip("No Postgres configured")

    from kalshi_collector.config import Settings
    from kalshi_collector.storage.db import create_pool, run_migrations

    settings = Settings()  # type: ignore[call-arg]
    p = await create_pool(settings)
    await run_migrations(p)
    yield p
    await p.close()


# ---------------------------------------------------------------------------
# Series upsert
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upsert_series_idempotent(pool: object) -> None:
    import asyncpg
    assert isinstance(pool, asyncpg.Pool)

    from kalshi_collector.storage.writers import upsert_series

    series = SeriesModel(ticker="TEST-SERIES-W", title="Writer Test Series", category="esports")

    async with pool.acquire() as conn:
        await upsert_series(conn, series)
        await upsert_series(conn, series)  # second call must not raise
        count: int = await conn.fetchval(
            "SELECT COUNT(*) FROM series WHERE ticker = 'TEST-SERIES-W'"
        )
    assert count == 1


@pytest.mark.asyncio
async def test_upsert_series_updates_fields(pool: object) -> None:
    import asyncpg
    assert isinstance(pool, asyncpg.Pool)

    from kalshi_collector.storage.writers import upsert_series

    series = SeriesModel(ticker="TEST-SERIES-W", title="Original Title", category="esports")
    async with pool.acquire() as conn:
        await upsert_series(conn, series)

    updated = SeriesModel(ticker="TEST-SERIES-W", title="Updated Title", category="esports")
    async with pool.acquire() as conn:
        await upsert_series(conn, updated)
        title: str = await conn.fetchval(
            "SELECT title FROM series WHERE ticker = 'TEST-SERIES-W'"
        )
    assert title == "Updated Title"


# ---------------------------------------------------------------------------
# Market upsert + settlement
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upsert_market_returns_id(pool: object) -> None:
    import asyncpg
    assert isinstance(pool, asyncpg.Pool)

    from kalshi_collector.storage.writers import upsert_event, upsert_market, upsert_series

    series = SeriesModel(ticker="TEST-SERIES-W")
    event = EventModel(event_ticker="TEST-EVENT-W", series_ticker="TEST-SERIES-W")
    market = MarketModel(
        ticker="TEST-MKT-W",
        event_ticker="TEST-EVENT-W",
        series_ticker="TEST-SERIES-W",
        status="active",
    )

    async with pool.acquire() as conn:
        await upsert_series(conn, series)
        await upsert_event(conn, event)
        market_id = await upsert_market(conn, market)

    assert isinstance(market_id, int)
    assert market_id > 0


@pytest.mark.asyncio
async def test_upsert_market_idempotent(pool: object) -> None:
    import asyncpg
    assert isinstance(pool, asyncpg.Pool)

    from kalshi_collector.storage.writers import upsert_market

    market = MarketModel(
        ticker="TEST-MKT-W",
        event_ticker="TEST-EVENT-W",
        series_ticker="TEST-SERIES-W",
        status="active",
    )

    async with pool.acquire() as conn:
        id1 = await upsert_market(conn, market)
        id2 = await upsert_market(conn, market)

    assert id1 == id2


@pytest.mark.asyncio
async def test_upsert_settlement_idempotent(pool: object) -> None:
    import asyncpg
    assert isinstance(pool, asyncpg.Pool)

    from kalshi_collector.storage.writers import upsert_market, upsert_settlement

    market = MarketModel(
        ticker="TEST-MKT-W",
        event_ticker="TEST-EVENT-W",
        series_ticker="TEST-SERIES-W",
        status="finalized",
        result="yes",
    )
    msg = MarketLifecycleMsg(
        event_type="settled",
        market_ticker="TEST-MKT-W",
        result="yes",
        settlement_value="1.0000",
        settled_ts=1700000000,
    )

    async with pool.acquire() as conn:
        market_id = await upsert_market(conn, market)
        await upsert_settlement(conn, market_id, msg)
        await upsert_settlement(conn, market_id, msg)  # second call must not raise
        count: int = await conn.fetchval(
            "SELECT COUNT(*) FROM settlements WHERE market_id = $1", market_id
        )
    assert count == 1


# ---------------------------------------------------------------------------
# Checkpoint round-trip
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_checkpoint_write_and_read(pool: object) -> None:
    import asyncpg
    assert isinstance(pool, asyncpg.Pool)

    from kalshi_collector.storage.checkpoints import read_checkpoint, write_checkpoint

    async with pool.acquire() as conn:
        await write_checkpoint(conn, "discovery", market_id=None, last_seq=42)
        seq, ts = await read_checkpoint(conn, "discovery", market_id=None)

    assert seq == 42
    assert ts is None


@pytest.mark.asyncio
async def test_checkpoint_upsert(pool: object) -> None:
    import asyncpg
    assert isinstance(pool, asyncpg.Pool)

    from kalshi_collector.storage.checkpoints import read_checkpoint, write_checkpoint

    now = datetime.now(tz=timezone.utc)
    async with pool.acquire() as conn:
        await write_checkpoint(conn, "discovery", market_id=None, last_seq=1, last_ts=now)
        await write_checkpoint(conn, "discovery", market_id=None, last_seq=2, last_ts=now)
        seq, _ = await read_checkpoint(conn, "discovery", market_id=None)

    assert seq == 2
