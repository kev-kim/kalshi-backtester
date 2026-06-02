"""Entrypoint for both the collector and monitor services.

Usage:
    python -m main collector   # start the data collector
    python -m main monitor     # start the monitor/alerting service
"""

from __future__ import annotations

import argparse
import asyncio
import platform
import signal
import socket
import sys
from datetime import datetime, timezone
from typing import Any

import asyncpg

from kalshi_collector.auth import load_private_key
from kalshi_collector.collectors.metadata import MetadataCollector
from kalshi_collector.collectors.orderbook import OrderbookCollector
from kalshi_collector.collectors.settlements import SettlementsCollector
from kalshi_collector.collectors.trades import TradesCollector
from kalshi_collector.config import Settings
from kalshi_collector.discovery import DiscoveryLoop
from kalshi_collector.http_client import KalshiHttpClient
from kalshi_collector.logging import configure_logging, get_logger
from kalshi_collector.models import MarketModel, WsBaseMessage
from kalshi_collector.monitor import MonitorService
from kalshi_collector.storage.db import create_pool, run_migrations
from kalshi_collector.ws_client import KalshiWsClient

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

def _install_signal_handler(shutdown_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def _handler() -> None:
        log.info("shutdown_signal_received")
        shutdown_event.set()

    if platform.system() != "Windows":
        loop.add_signal_handler(signal.SIGTERM, _handler)
        loop.add_signal_handler(signal.SIGINT, _handler)


# ---------------------------------------------------------------------------
# Ingestion run registration
# ---------------------------------------------------------------------------

async def _register_run(pool: asyncpg.Pool, settings: Settings) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO ingestion_runs (env, version, host, pid)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            """,
            settings.kalshi_env.value,
            settings.version,
            socket.gethostname(),
            __import__("os").getpid(),
        )
    return int(row["id"])  # type: ignore[index]


async def _close_run(pool: asyncpg.Pool, run_id: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE ingestion_runs SET end_ts = now() WHERE id = $1", run_id
        )


# ---------------------------------------------------------------------------
# Collector entrypoint
# ---------------------------------------------------------------------------

async def run_collector(settings: Settings) -> None:
    pool = await create_pool(settings)
    await run_migrations(pool)
    run_id = await _register_run(pool, settings)
    log.info("collector_started", run_id=run_id, env=settings.kalshi_env.value)

    shutdown = asyncio.Event()
    _install_signal_handler(shutdown)

    private_key = load_private_key(settings.kalshi_private_key_path)

    # Shared state: ticker -> DB id, populated as markets are discovered
    market_id_map: dict[str, int] = {}

    async with KalshiHttpClient(
        settings.rest_base_url, settings.kalshi_api_key_id, private_key
    ) as client:
        # ---- Initial discovery pass (blocking — we need markets before WS) ----
        discovery = DiscoveryLoop(client, pool, settings)
        new_markets_queue: asyncio.Queue[list[MarketModel]] = asyncio.Queue()

        initial_markets = await discovery.run_once()
        for m in initial_markets:
            async with pool.acquire() as conn:
                from kalshi_collector.storage.writers import upsert_market
                db_id = await upsert_market(conn, m)
            market_id_map[m.ticker] = db_id

        tickers = list(market_id_map.keys())
        log.info("initial_discovery_complete", market_count=len(tickers))

        # ---- Collectors ----
        ob_collector = OrderbookCollector(client, pool, settings, market_id_map)
        trades_collector = TradesCollector(client, pool, settings, market_id_map)
        meta_collector = MetadataCollector(client, pool, market_id_map)
        settlements_collector = SettlementsCollector(client, pool, market_id_map)

        for t in tickers:
            ob_collector.add_market(t)

        # ---- WebSocket dispatch ----
        async def on_ws_message(msg: dict[str, Any]) -> None:
            parsed = WsBaseMessage.model_validate(msg)
            if parsed.type in ("orderbook_snapshot", "orderbook_delta"):
                await ob_collector.handle_ws_message(parsed)
            elif parsed.type == "trade":
                await trades_collector.handle_ws_message(parsed)
            elif parsed.type == "market_lifecycle_v2":
                await meta_collector.handle_lifecycle_event(parsed)

        ws_client = KalshiWsClient(
            settings.ws_url, settings.kalshi_api_key_id, private_key, on_ws_message
        )

        # ---- Subscribe to WS channels ----
        async def start_ws() -> None:
            ws_task = asyncio.create_task(ws_client.start())
            # Brief wait for connection to establish before subscribing
            await asyncio.sleep(2)
            if tickers:
                await ws_client.subscribe(["orderbook_delta", "trade", "market_lifecycle_v2"], tickers)
            await ws_task

        # ---- Discovery updates new markets into WS subscriptions ----
        async def discovery_loop() -> None:
            while not shutdown.is_set():
                await asyncio.sleep(settings.discovery_interval_sec)
                new = await discovery.run_once()
                for m in new:
                    async with pool.acquire() as conn:
                        from kalshi_collector.storage.writers import upsert_market
                        db_id = await upsert_market(conn, m)
                    market_id_map[m.ticker] = db_id
                    ob_collector.add_market(m.ticker)
                    log.info("subscribed_new_market", ticker=m.ticker)
                    # WS add_markets requires the orderbook_delta sid; use sid=1 (first sub)
                    await ws_client.add_markets(1, [m.ticker])

        # ---- Run all tasks ----
        tasks = [
            asyncio.create_task(start_ws(), name="ws"),
            asyncio.create_task(discovery_loop(), name="discovery"),
            asyncio.create_task(meta_collector.run(tickers), name="metadata"),
            asyncio.create_task(settlements_collector.run(tickers), name="settlements"),
        ]

        await shutdown.wait()

        log.info("collector_shutting_down")
        await ws_client.stop()
        ob_collector.stop()
        trades_collector.stop()
        meta_collector.stop()
        settlements_collector.stop()

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    await _close_run(pool, run_id)
    await pool.close()
    log.info("collector_stopped", run_id=run_id)


# ---------------------------------------------------------------------------
# Monitor entrypoint
# ---------------------------------------------------------------------------

async def run_monitor(settings: Settings) -> None:
    pool = await create_pool(settings)
    log.info("monitor_started", env=settings.kalshi_env.value)

    shutdown = asyncio.Event()
    _install_signal_handler(shutdown)

    monitor = MonitorService(pool, settings)
    monitor_task = asyncio.create_task(monitor.run())

    await shutdown.wait()
    monitor.stop()
    monitor_task.cancel()
    await asyncio.gather(monitor_task, return_exceptions=True)
    await pool.close()
    log.info("monitor_stopped")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Kalshi esports data collector")
    parser.add_argument("mode", choices=["collector", "monitor"])
    args = parser.parse_args()

    settings = Settings()  # type: ignore[call-arg]
    configure_logging(settings.log_level)

    if args.mode == "collector":
        asyncio.run(run_collector(settings))
    else:
        asyncio.run(run_monitor(settings))


if __name__ == "__main__":
    main()
