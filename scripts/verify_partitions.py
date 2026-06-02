#!/usr/bin/env python3
"""Verify that weekly partitions exist for all partitioned tables.

Creates missing partitions for the current week + 4 future weeks and
optionally calls pg_partman's run_maintenance_proc() to let partman
manage the rest going forward.

Usage:
    python scripts/verify_partitions.py [--run-maintenance]
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from kalshi_collector.config import Settings
from kalshi_collector.logging import configure_logging, get_logger
from kalshi_collector.storage.db import create_pool

log = get_logger(__name__)

_PARTITIONED_TABLES = ["market_updates", "orderbook_snapshots", "trades"]
_WEEKS_AHEAD = 4


def _week_bounds(monday: date) -> tuple[str, str]:
    return monday.isoformat(), (monday + timedelta(days=7)).isoformat()


def _partition_name(table: str, monday: date) -> str:
    iso = monday.isocalendar()
    return f"{table}_{iso[0]}_w{iso[1]:02d}"


async def ensure_partitions(pool: object, weeks_ahead: int = _WEEKS_AHEAD) -> None:
    import asyncpg

    assert isinstance(pool, asyncpg.Pool)

    today = date.today()
    # Align to Monday of current ISO week
    monday = today - timedelta(days=today.weekday())

    async with pool.acquire() as conn:
        for i in range(weeks_ahead + 1):
            week_monday = monday + timedelta(weeks=i)
            start, end = _week_bounds(week_monday)

            for table in _PARTITIONED_TABLES:
                pname = _partition_name(table, week_monday)

                exists: bool = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = $1 AND n.nspname = 'public'
                    )
                    """,
                    pname,
                )

                if exists:
                    log.debug("partition_exists", name=pname)
                    continue

                await conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {pname}
                    PARTITION OF {table}
                    FOR VALUES FROM ('{start}') TO ('{end}')
                    """
                )
                log.info("partition_created", name=pname, start=start, end=end)


async def run_partman_maintenance(pool: object) -> None:
    import asyncpg

    assert isinstance(pool, asyncpg.Pool)
    async with pool.acquire() as conn:
        try:
            await conn.execute("SELECT public.run_maintenance_proc()")
            log.info("partman_maintenance_run")
        except asyncpg.UndefinedFunctionError:
            log.warning("partman_not_available", hint="pg_partman may not be installed")


async def main_async(run_maintenance: bool) -> None:
    settings = Settings()  # type: ignore[call-arg]
    configure_logging(settings.log_level)

    pool = await create_pool(settings)
    try:
        await ensure_partitions(pool)
        if run_maintenance:
            await run_partman_maintenance(pool)
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify and create weekly partitions")
    parser.add_argument(
        "--run-maintenance",
        action="store_true",
        help="Also call partman.run_maintenance_proc()",
    )
    args = parser.parse_args()
    asyncio.run(main_async(args.run_maintenance))


if __name__ == "__main__":
    main()
