"""Monitor service — runs health checks and fires Discord alerts."""

from __future__ import annotations

import asyncio

import asyncpg

from kalshi_collector.config import Settings
from kalshi_collector.logging import get_logger
from kalshi_collector.monitor.checks import CheckResult, run_all_checks
from kalshi_collector.monitor.discord import DiscordAlerter

log = get_logger(__name__)


class MonitorService:
    def __init__(self, pool: asyncpg.Pool, settings: Settings) -> None:
        self._pool = pool
        self._settings = settings
        self._running = False

    async def run(self) -> None:
        interval = self._settings.monitor_check_interval_sec
        self._running = True

        async with DiscordAlerter(self._settings.discord_webhook_url) as alerter:
            while self._running:
                try:
                    async with self._pool.acquire() as conn:
                        results = await run_all_checks(conn, self._settings)
                except Exception:
                    log.exception("monitor_check_error")
                    await asyncio.sleep(interval)
                    continue

                for r in results:
                    log.info(
                        "monitor_check",
                        check=r.name,
                        ok=r.ok,
                        message=r.message,
                        level=r.level,
                    )
                    if not r.ok:
                        await alerter.send(
                            title=f"[{r.level.upper()}] {r.name}",
                            description=r.message,
                            level=r.level,
                        )

                await asyncio.sleep(interval)

    def stop(self) -> None:
        self._running = False
