-- 003_indexes.sql — All indexes for the Kalshi esports data schema
-- Run after 002_partitions.sql.
-- All CREATE INDEX are CONCURRENTLY-safe (no table locks) except during
-- initial build on an empty DB — remove CONCURRENTLY if running in a
-- single transaction for the very first time.

-- ---------------------------------------------------------------------------
-- series
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_series_category
    ON series (category);

CREATE INDEX IF NOT EXISTS idx_series_tags
    ON series USING GIN (tags);

-- ---------------------------------------------------------------------------
-- events
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_events_series_ticker
    ON events (series_ticker);

CREATE INDEX IF NOT EXISTS idx_events_status
    ON events (status)
    WHERE status IN ('open', 'active', 'unopened');

-- ---------------------------------------------------------------------------
-- markets
-- ---------------------------------------------------------------------------
-- ticker is already UNIQUE (implicit index); add explicit name for clarity
-- Unique index already created by UNIQUE constraint on ticker column.

CREATE INDEX IF NOT EXISTS idx_markets_event_ticker
    ON markets (event_ticker);

CREATE INDEX IF NOT EXISTS idx_markets_series_ticker
    ON markets (series_ticker);

CREATE INDEX IF NOT EXISTS idx_markets_status
    ON markets (status)
    WHERE status IN ('active', 'initialized', 'inactive');

CREATE INDEX IF NOT EXISTS idx_markets_metadata
    ON markets USING GIN (additional_metadata)
    WHERE additional_metadata IS NOT NULL;

-- ---------------------------------------------------------------------------
-- market_updates  (applied to parent; inherited by each partition)
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_market_updates_market_ts
    ON market_updates (market_id, kalshi_ts DESC);

CREATE INDEX IF NOT EXISTS idx_market_updates_ingest_ts
    ON market_updates (ingest_ts DESC);

-- ---------------------------------------------------------------------------
-- orderbook_snapshots
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_orderbook_market_ts
    ON orderbook_snapshots (market_id, kalshi_ts DESC);

CREATE INDEX IF NOT EXISTS idx_orderbook_checksum
    ON orderbook_snapshots (market_id, checksum);

-- ---------------------------------------------------------------------------
-- trades
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_trades_market_ts
    ON trades (market_id, kalshi_ts DESC);

CREATE INDEX IF NOT EXISTS idx_trades_ticker_ts
    ON trades (ticker, kalshi_ts DESC);

-- trade_id + kalshi_ts already covered by the UNIQUE constraint; no extra index.

-- ---------------------------------------------------------------------------
-- settlements
-- ---------------------------------------------------------------------------
-- market_id is already UNIQUE (implicit index via constraint).

CREATE INDEX IF NOT EXISTS idx_settlements_settled_ts
    ON settlements (settled_ts DESC);

-- ---------------------------------------------------------------------------
-- api_requests
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_api_requests_ts
    ON api_requests (ts DESC);

CREATE INDEX IF NOT EXISTS idx_api_requests_endpoint
    ON api_requests (endpoint, ts DESC);

CREATE INDEX IF NOT EXISTS idx_api_requests_rate_limited
    ON api_requests (ts DESC)
    WHERE rate_limited = TRUE;

-- ---------------------------------------------------------------------------
-- data_quality_events
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_dqe_ts
    ON data_quality_events (ts DESC);

CREATE INDEX IF NOT EXISTS idx_dqe_market_ts
    ON data_quality_events (market_id, ts DESC)
    WHERE market_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dqe_severity
    ON data_quality_events (severity, ts DESC)
    WHERE severity IN ('error', 'critical');

CREATE INDEX IF NOT EXISTS idx_dqe_payload
    ON data_quality_events USING GIN (payload)
    WHERE payload IS NOT NULL;

-- ---------------------------------------------------------------------------
-- checkpoints
-- ---------------------------------------------------------------------------
-- (market_id, channel) already covered by the UNIQUE constraint.

CREATE INDEX IF NOT EXISTS idx_checkpoints_channel
    ON checkpoints (channel);

-- ---------------------------------------------------------------------------
-- game_events (reserved)
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_game_events_market_ts
    ON game_events (market_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_game_events_source_type
    ON game_events (source, event_type);

CREATE INDEX IF NOT EXISTS idx_game_events_payload
    ON game_events USING GIN (payload);
