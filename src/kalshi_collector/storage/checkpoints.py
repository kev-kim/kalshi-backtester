"""Read and write collector checkpoints.

A checkpoint records the last-seen sequence number or timestamp for a
(market_id, channel) pair so the collector can resume cleanly after restart.

channel values:
  'orderbook_delta'  — WS orderbook subscription sequence number
  'trade'            — WS trade subscription sequence number
  'poll_orderbook'   — last poll timestamp for polling fallback
  'poll_trades'      — last trade timestamp from REST polling
  'discovery'        — last discovery run timestamp
"""

from __future__ import annotations

from datetime import datetime

import asyncpg

from kalshi_collector.logging import get_logger

log = get_logger(__name__)


async def read_checkpoint(
    conn: asyncpg.Connection,
    channel: str,
    market_id: int | None = None,
) -> tuple[int | None, datetime | None]:
    """Return (last_seq, last_ts) for the given channel, or (None, None)."""
    row = await conn.fetchrow(
        """
        SELECT last_seq, last_ts
        FROM checkpoints
        WHERE channel = $1
          AND (market_id = $2 OR ($2 IS NULL AND market_id IS NULL))
        """,
        channel,
        market_id,
    )
    if row is None:
        return None, None
    return row["last_seq"], row["last_ts"]


async def write_checkpoint(
    conn: asyncpg.Connection,
    channel: str,
    market_id: int | None = None,
    last_seq: int | None = None,
    last_ts: datetime | None = None,
) -> None:
    """Upsert a checkpoint record."""
    await conn.execute(
        """
        INSERT INTO checkpoints (market_id, channel, last_seq, last_ts, updated_ts)
        VALUES ($1, $2, $3, $4, now())
        ON CONFLICT (market_id, channel) DO UPDATE SET
            last_seq   = EXCLUDED.last_seq,
            last_ts    = EXCLUDED.last_ts,
            updated_ts = now()
        """,
        market_id,
        channel,
        last_seq,
        last_ts,
    )
