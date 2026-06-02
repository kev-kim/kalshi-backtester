"""asyncpg connection pool management."""

from __future__ import annotations

import asyncpg

from kalshi_collector.config import Settings
from kalshi_collector.logging import get_logger

log = get_logger(__name__)


async def create_pool(settings: Settings) -> asyncpg.Pool:
    """Create and return an asyncpg connection pool."""
    pool = await asyncpg.create_pool(
        dsn=settings.postgres_dsn,
        min_size=settings.postgres_pool_min,
        max_size=settings.postgres_pool_max,
        command_timeout=60,
    )
    if pool is None:
        raise RuntimeError("asyncpg.create_pool returned None")
    log.info(
        "db_pool_created",
        host=settings.postgres_host,
        db=settings.postgres_db,
        min=settings.postgres_pool_min,
        max=settings.postgres_pool_max,
    )
    return pool


async def run_migrations(pool: asyncpg.Pool) -> None:
    """Apply SQL migration files in order, skipping already-applied ones."""
    import re
    from pathlib import Path

    sql_dir = Path(__file__).parent.parent.parent.parent / "sql"

    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _migrations (
                filename TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )

        for sql_file in sorted(sql_dir.glob("*.sql")):
            already_applied = await conn.fetchval(
                "SELECT 1 FROM _migrations WHERE filename = $1", sql_file.name
            )
            if already_applied:
                continue

            log.info("applying_migration", file=sql_file.name)
            sql = sql_file.read_text()
            await conn.execute(sql)
            await conn.execute(
                "INSERT INTO _migrations (filename) VALUES ($1)", sql_file.name
            )
            log.info("migration_applied", file=sql_file.name)
