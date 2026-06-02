"""Data quality and system health checks for the monitor service.

All six required checks:
  1. API failure rate per endpoint class
  2. Rate-limit hits in the last hour
  3. Stale markets (active market, no updates > threshold during open window)
  4. Schema drift (unexpected fields logged to data_quality_events)
  5. Ingestion lag (now() - max(ingest_ts) per active market)
  6. Missing partitions for the upcoming week
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import asyncpg

from kalshi_collector.config import Settings
from kalshi_collector.logging import get_logger

log = get_logger(__name__)


@dataclass
class CheckResult:
    name: str
    ok: bool
    message: str
    level: str = "warning"   # 'info' | 'warning' | 'error'


async def check_api_failure_rate(
    conn: asyncpg.Connection, window_minutes: int = 60
) -> CheckResult:
    """Alert if >5% of requests in the last hour returned non-2xx."""
    row = await conn.fetchrow(
        """
        SELECT
            COUNT(*)                                             AS total,
            COUNT(*) FILTER (WHERE status_code >= 400)          AS failures,
            COUNT(*) FILTER (WHERE rate_limited = TRUE)         AS rate_limited
        FROM api_requests
        WHERE ts >= now() - ($1 * INTERVAL '1 minute')
        """,
        window_minutes,
    )
    if row is None or row["total"] == 0:
        return CheckResult("api_failure_rate", True, "No API requests in window")

    total: int = row["total"]
    failures: int = row["failures"]
    rate = failures / total
    msg = f"{failures}/{total} requests failed ({rate:.1%}) in last {window_minutes}m"

    if rate > 0.10:
        return CheckResult("api_failure_rate", False, msg, "error")
    if rate > 0.05:
        return CheckResult("api_failure_rate", False, msg, "warning")
    return CheckResult("api_failure_rate", True, msg)


async def check_rate_limit_hits(
    conn: asyncpg.Connection, window_minutes: int = 60
) -> CheckResult:
    """Alert if any rate-limit hits in the last hour."""
    count: int = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM api_requests
        WHERE rate_limited = TRUE
          AND ts >= now() - ($1 * INTERVAL '1 minute')
        """,
        window_minutes,
    )
    msg = f"{count} rate-limit hits in last {window_minutes}m"
    if count > 50:
        return CheckResult("rate_limit_hits", False, msg, "error")
    if count > 0:
        return CheckResult("rate_limit_hits", False, msg, "warning")
    return CheckResult("rate_limit_hits", True, msg)


async def check_stale_markets(
    conn: asyncpg.Connection, settings: Settings
) -> list[CheckResult]:
    """Alert for each active market with no orderbook updates during its open window."""
    threshold = settings.stale_market_threshold_sec

    rows = await conn.fetch(
        """
        SELECT
            m.ticker,
            m.open_time,
            m.close_time,
            MAX(os.ingest_ts) AS last_snap
        FROM markets m
        LEFT JOIN orderbook_snapshots os ON os.market_id = m.id
        WHERE m.status IN ('active', 'initialized')
          AND m.open_time  <= now()
          AND m.close_time >= now()
        GROUP BY m.ticker, m.open_time, m.close_time
        HAVING MAX(os.ingest_ts) IS NULL
            OR MAX(os.ingest_ts) < now() - ($1 * INTERVAL '1 second')
        """,
        threshold,
    )

    results: list[CheckResult] = []
    for row in rows:
        ticker: str = row["ticker"]
        last_snap: datetime | None = row["last_snap"]
        lag = "never" if last_snap is None else f"{(datetime.now(tz=timezone.utc) - last_snap).seconds}s ago"
        msg = f"Market {ticker} stale: last snapshot {lag} (threshold {threshold}s)"
        results.append(CheckResult("stale_market", False, msg, "warning"))

    if not results:
        results.append(CheckResult("stale_markets", True, "All active markets have fresh data"))
    return results


async def check_ingestion_lag(conn: asyncpg.Connection) -> list[CheckResult]:
    """Alert for active markets with high ingest lag."""
    rows = await conn.fetch(
        """
        SELECT
            m.ticker,
            now() - MAX(os.ingest_ts) AS lag
        FROM markets m
        JOIN orderbook_snapshots os ON os.market_id = m.id
        WHERE m.status IN ('active', 'initialized')
          AND m.open_time  <= now()
          AND m.close_time >= now()
        GROUP BY m.ticker
        HAVING now() - MAX(os.ingest_ts) > INTERVAL '5 minutes'
        """
    )

    results: list[CheckResult] = []
    for row in rows:
        lag: timedelta = row["lag"]
        ticker: str = row["ticker"]
        msg = f"Market {ticker} ingest lag: {int(lag.total_seconds())}s"
        level = "error" if lag.total_seconds() > 900 else "warning"
        results.append(CheckResult("ingestion_lag", False, msg, level))

    if not results:
        results.append(CheckResult("ingestion_lag", True, "Ingest lag within bounds"))
    return results


async def check_missing_partitions(conn: asyncpg.Connection) -> CheckResult:
    """Verify that partitions exist for the upcoming week."""
    from datetime import date

    next_monday = date.today() + timedelta(days=(7 - date.today().weekday()))
    week_str = f"W{next_monday.isocalendar()[1]:02d}"
    year_str = str(next_monday.year)

    tables = ["market_updates", "orderbook_snapshots", "trades"]
    missing: list[str] = []

    for table in tables:
        partition_name = f"{table}_{year_str}_{week_str.lower()}"
        exists: bool = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = $1 AND n.nspname = 'public'
            )
            """,
            partition_name,
        )
        if not exists:
            missing.append(partition_name)

    if missing:
        msg = f"Missing partitions for upcoming week: {', '.join(missing)}"
        return CheckResult("missing_partitions", False, msg, "error")
    return CheckResult("missing_partitions", True, f"Partitions ready for {year_str}-{week_str}")


async def check_schema_drift(conn: asyncpg.Connection, window_minutes: int = 60) -> CheckResult:
    """Alert if schema_drift events were logged recently."""
    count: int = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM data_quality_events
        WHERE event_type = 'schema_drift'
          AND ts >= now() - ($1 * INTERVAL '1 minute')
        """,
        window_minutes,
    )
    msg = f"{count} schema_drift events in last {window_minutes}m"
    if count > 0:
        return CheckResult("schema_drift", False, msg, "warning")
    return CheckResult("schema_drift", True, msg)


async def run_all_checks(
    conn: asyncpg.Connection, settings: Settings
) -> list[CheckResult]:
    results: list[CheckResult] = []
    results.append(await check_api_failure_rate(conn))
    results.append(await check_rate_limit_hits(conn))
    results.extend(await check_stale_markets(conn, settings))
    results.extend(await check_ingestion_lag(conn))
    results.append(await check_missing_partitions(conn))
    results.append(await check_schema_drift(conn))
    return results
