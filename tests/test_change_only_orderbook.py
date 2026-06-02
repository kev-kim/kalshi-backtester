"""Tests for change-only orderbook write logic.

Verifies that maybe_append_orderbook() skips writes when the snapshot is
byte-identical to the previous one, and writes when it differs.

Uses a real (ephemeral) Postgres via the POSTGRES_* env vars set by 'make test'.
Falls back to in-memory logic tests if no DB is available.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import pytest
import pytest_asyncio

from kalshi_collector.models import OrderbookState
from kalshi_collector.storage.writers import _orderbook_checksum, maybe_append_orderbook


# ---------------------------------------------------------------------------
# Pure logic tests (no DB required)
# ---------------------------------------------------------------------------

def test_checksum_identical_snapshots() -> None:
    yes = [["0.6500", "100.00"], ["0.6000", "50.00"]]
    no = [["0.3500", "75.00"]]
    c1 = _orderbook_checksum(yes, no)
    c2 = _orderbook_checksum(yes, no)
    assert c1 == c2


def test_checksum_different_price() -> None:
    yes = [["0.6500", "100.00"]]
    no = [["0.3500", "75.00"]]
    yes2 = [["0.6600", "100.00"]]
    assert _orderbook_checksum(yes, no) != _orderbook_checksum(yes2, no)


def test_checksum_different_qty() -> None:
    yes = [["0.6500", "100.00"]]
    no = [["0.3500", "75.00"]]
    yes2 = [["0.6500", "101.00"]]
    assert _orderbook_checksum(yes, no) != _orderbook_checksum(yes2, no)


def test_orderbook_state_apply_delta_add() -> None:
    state = OrderbookState(market_ticker="TEST-1")
    from kalshi_collector.models import OrderbookDeltaMsg
    delta = OrderbookDeltaMsg(
        market_ticker="TEST-1",
        price_dollars="0.6500",
        delta_fp="100.00",
        side="yes",
    )
    state.apply_delta(delta)
    assert state.yes_bids[Decimal("0.6500")] == Decimal("100.00")


def test_orderbook_state_apply_delta_remove() -> None:
    state = OrderbookState(market_ticker="TEST-1")
    state.yes_bids[Decimal("0.6500")] = Decimal("100.00")
    from kalshi_collector.models import OrderbookDeltaMsg
    delta = OrderbookDeltaMsg(
        market_ticker="TEST-1",
        price_dollars="0.6500",
        delta_fp="0",
        side="yes",
    )
    state.apply_delta(delta)
    assert Decimal("0.6500") not in state.yes_bids


def test_to_sorted_levels_descending() -> None:
    state = OrderbookState(market_ticker="TEST-1")
    state.yes_bids = {
        Decimal("0.50"): Decimal("100"),
        Decimal("0.65"): Decimal("200"),
        Decimal("0.60"): Decimal("150"),
    }
    levels = state.to_sorted_levels("yes")
    prices = [Decimal(p) for p, _ in levels]
    assert prices == sorted(prices, reverse=True)


# ---------------------------------------------------------------------------
# DB integration tests (skipped if no Postgres configured)
# ---------------------------------------------------------------------------

def _db_available() -> bool:
    return bool(os.environ.get("POSTGRES_PASSWORD"))


@pytest.fixture(scope="module")
def event_loop():  # type: ignore[override]
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="module")
async def db_pool():  # type: ignore[return]
    if not _db_available():
        pytest.skip("No Postgres configured — set POSTGRES_PASSWORD to enable DB tests")

    import asyncpg
    from kalshi_collector.config import Settings
    from kalshi_collector.storage.db import create_pool, run_migrations

    settings = Settings()  # type: ignore[call-arg]
    pool = await create_pool(settings)
    await run_migrations(pool)

    # Minimal seed data so FK constraints are satisfied
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO series (ticker) VALUES ('TEST-SERIES')
            ON CONFLICT DO NOTHING
            """
        )
        await conn.execute(
            """
            INSERT INTO events (ticker, series_ticker) VALUES ('TEST-EVENT', 'TEST-SERIES')
            ON CONFLICT DO NOTHING
            """
        )
        await conn.execute(
            """
            INSERT INTO markets (ticker, event_ticker, series_ticker)
            VALUES ('TEST-MKT-1', 'TEST-EVENT', 'TEST-SERIES')
            ON CONFLICT DO NOTHING
            """
        )

    yield pool
    await pool.close()


@pytest.mark.asyncio
async def test_first_snapshot_always_written(db_pool: object) -> None:
    import asyncpg
    assert isinstance(db_pool, asyncpg.Pool)

    async with db_pool.acquire() as conn:
        market_id: int = await conn.fetchval(
            "SELECT id FROM markets WHERE ticker = 'TEST-MKT-1'"
        )
        # Clean slate
        await conn.execute(
            "DELETE FROM orderbook_snapshots WHERE market_id = $1", market_id
        )

    state = OrderbookState(market_ticker="TEST-MKT-1")
    state.yes_bids = {Decimal("0.65"): Decimal("100")}
    state.no_bids = {Decimal("0.35"): Decimal("75")}

    async with db_pool.acquire() as conn:
        wrote = await maybe_append_orderbook(
            conn, market_id, state, datetime.now(tz=timezone.utc)
        )
    assert wrote is True


@pytest.mark.asyncio
async def test_identical_snapshot_skipped(db_pool: object) -> None:
    import asyncpg
    assert isinstance(db_pool, asyncpg.Pool)

    async with db_pool.acquire() as conn:
        market_id: int = await conn.fetchval(
            "SELECT id FROM markets WHERE ticker = 'TEST-MKT-1'"
        )

    state = OrderbookState(market_ticker="TEST-MKT-1")
    state.yes_bids = {Decimal("0.65"): Decimal("100")}
    state.no_bids = {Decimal("0.35"): Decimal("75")}

    # Write once (idempotent with previous test, but ensure at least one row)
    async with db_pool.acquire() as conn:
        await maybe_append_orderbook(conn, market_id, state, datetime.now(tz=timezone.utc))
        count_before: int = await conn.fetchval(
            "SELECT COUNT(*) FROM orderbook_snapshots WHERE market_id = $1", market_id
        )
        # Write same state again — must be skipped
        wrote = await maybe_append_orderbook(
            conn, market_id, state, datetime.now(tz=timezone.utc)
        )
        count_after: int = await conn.fetchval(
            "SELECT COUNT(*) FROM orderbook_snapshots WHERE market_id = $1", market_id
        )

    assert wrote is False
    assert count_after == count_before


@pytest.mark.asyncio
async def test_changed_snapshot_written(db_pool: object) -> None:
    import asyncpg
    assert isinstance(db_pool, asyncpg.Pool)

    async with db_pool.acquire() as conn:
        market_id: int = await conn.fetchval(
            "SELECT id FROM markets WHERE ticker = 'TEST-MKT-1'"
        )

    state = OrderbookState(market_ticker="TEST-MKT-1")
    state.yes_bids = {Decimal("0.65"): Decimal("100")}
    state.no_bids = {Decimal("0.35"): Decimal("75")}

    async with db_pool.acquire() as conn:
        count_before: int = await conn.fetchval(
            "SELECT COUNT(*) FROM orderbook_snapshots WHERE market_id = $1", market_id
        )

    # Change the state
    state.yes_bids[Decimal("0.70")] = Decimal("200")

    async with db_pool.acquire() as conn:
        wrote = await maybe_append_orderbook(
            conn, market_id, state, datetime.now(tz=timezone.utc)
        )
        count_after: int = await conn.fetchval(
            "SELECT COUNT(*) FROM orderbook_snapshots WHERE market_id = $1", market_id
        )

    assert wrote is True
    assert count_after == count_before + 1
