-- 001_init.sql — Kalshi esports market data schema
-- PostgreSQL 16+  |  all timestamps TIMESTAMPTZ UTC  |  all prices NUMERIC
-- Run once on a fresh database. Idempotent via IF NOT EXISTS.

-- ---------------------------------------------------------------------------
-- Extensions
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS pg_partman;

-- ---------------------------------------------------------------------------
-- series — top-level grouping (e.g. "Valorant Champions")
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS series (
    ticker                  TEXT        PRIMARY KEY,
    title                   TEXT,
    category                TEXT,
    tags                    TEXT[]      NOT NULL DEFAULT '{}',
    frequency               TEXT,
    settlement_sources      JSONB,
    additional_metadata     JSONB,
    ingest_ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_ts            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- events — match / tournament level (child of series)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events (
    ticker                  TEXT        PRIMARY KEY,
    series_ticker           TEXT        NOT NULL REFERENCES series(ticker),
    title                   TEXT,
    category                TEXT,
    status                  TEXT,
    open_time               TIMESTAMPTZ,
    close_time              TIMESTAMPTZ,
    additional_metadata     JSONB,
    ingest_ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_ts            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- markets — one row per Kalshi market ticker
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS markets (
    id                      BIGSERIAL   PRIMARY KEY,
    ticker                  TEXT        NOT NULL UNIQUE,
    event_ticker            TEXT        NOT NULL REFERENCES events(ticker),
    series_ticker           TEXT        NOT NULL REFERENCES series(ticker),
    market_type             TEXT,
    title                   TEXT,
    subtitle                TEXT,
    yes_sub_title           TEXT,
    no_sub_title            TEXT,
    open_time               TIMESTAMPTZ,
    close_time              TIMESTAMPTZ,
    status                  TEXT,
    result                  TEXT,
    rules_primary           TEXT,
    rules_secondary         TEXT,
    price_level_structure   TEXT,
    additional_metadata     JSONB,
    ingest_ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_ts            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- market_updates — append-only log of mutable field changes
-- Partitioned weekly by kalshi_ts.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_updates (
    id                      BIGSERIAL,
    market_id               BIGINT      NOT NULL,   -- FK enforced per-partition
    kalshi_ts               TIMESTAMPTZ NOT NULL,
    ingest_ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
    status                  TEXT,
    result                  TEXT,
    yes_bid                 NUMERIC(10,4),
    yes_ask                 NUMERIC(10,4),
    no_bid                  NUMERIC(10,4),
    no_ask                  NUMERIC(10,4),
    last_price              NUMERIC(10,4),
    volume                  NUMERIC(16,2),
    volume_24h              NUMERIC(16,2),
    open_interest           NUMERIC(16,2),
    additional_metadata     JSONB,
    PRIMARY KEY (id, kalshi_ts)
) PARTITION BY RANGE (kalshi_ts);

-- ---------------------------------------------------------------------------
-- orderbook_snapshots — change-only; YES bids and NO bids stored separately
-- Partitioned weekly by kalshi_ts.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    id                      BIGSERIAL,
    market_id               BIGINT      NOT NULL,
    kalshi_ts               TIMESTAMPTZ NOT NULL,
    ingest_ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
    yes_bids                JSONB       NOT NULL,   -- [[price_str, qty_str], ...]
    no_bids                 JSONB       NOT NULL,
    checksum                TEXT        NOT NULL,   -- SHA-256 of yes_bids||no_bids; used for change-detection
    PRIMARY KEY (id, kalshi_ts)
) PARTITION BY RANGE (kalshi_ts);

-- ---------------------------------------------------------------------------
-- trades — every observed trade
-- Partitioned weekly by kalshi_ts.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trades (
    id                      BIGSERIAL,
    trade_id                TEXT        NOT NULL,   -- Kalshi UUID
    market_id               BIGINT      NOT NULL,
    ticker                  TEXT        NOT NULL,
    kalshi_ts               TIMESTAMPTZ NOT NULL,
    ingest_ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
    yes_price               NUMERIC(10,4) NOT NULL,
    no_price                NUMERIC(10,4) NOT NULL,
    count                   NUMERIC(12,2) NOT NULL,
    taker_side              TEXT        NOT NULL,   -- 'yes' | 'no'
    taker_book_side         TEXT        NOT NULL,   -- 'bid' | 'ask'
    is_block_trade          BOOLEAN     NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id, kalshi_ts),
    UNIQUE (trade_id, kalshi_ts)
) PARTITION BY RANGE (kalshi_ts);

-- ---------------------------------------------------------------------------
-- settlements — one row per settled market
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS settlements (
    id                      BIGSERIAL   PRIMARY KEY,
    market_id               BIGINT      NOT NULL UNIQUE REFERENCES markets(id),
    settled_ts              TIMESTAMPTZ NOT NULL,
    ingest_ts               TIMESTAMPTZ NOT NULL DEFAULT now(),
    result                  TEXT        NOT NULL,   -- 'yes' | 'no' | 'scalar'
    settlement_value        NUMERIC(10,4),
    additional_metadata     JSONB
);

-- ---------------------------------------------------------------------------
-- ingestion_runs — per-process-start record
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingestion_runs (
    id                      BIGSERIAL   PRIMARY KEY,
    start_ts                TIMESTAMPTZ NOT NULL DEFAULT now(),
    end_ts                  TIMESTAMPTZ,
    env                     TEXT        NOT NULL,   -- 'demo' | 'prod'
    version                 TEXT        NOT NULL,
    host                    TEXT        NOT NULL,
    pid                     INTEGER     NOT NULL
);

-- ---------------------------------------------------------------------------
-- api_requests — every outbound API call
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS api_requests (
    id                      BIGSERIAL   PRIMARY KEY,
    run_id                  BIGINT      REFERENCES ingestion_runs(id),
    ts                      TIMESTAMPTZ NOT NULL DEFAULT now(),
    endpoint                TEXT        NOT NULL,
    method                  TEXT        NOT NULL DEFAULT 'GET',
    status_code             INTEGER,
    latency_ms              INTEGER,
    retry_count             INTEGER     NOT NULL DEFAULT 0,
    rate_limited            BOOLEAN     NOT NULL DEFAULT FALSE,
    error_message           TEXT
);

-- ---------------------------------------------------------------------------
-- data_quality_events — gaps, stale markets, schema drift, validation failures
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_quality_events (
    id                      BIGSERIAL   PRIMARY KEY,
    ts                      TIMESTAMPTZ NOT NULL DEFAULT now(),
    market_id               BIGINT      REFERENCES markets(id),
    event_type              TEXT        NOT NULL,   -- 'gap' | 'stale' | 'schema_drift' | 'validation'
    severity                TEXT        NOT NULL,   -- 'warning' | 'error' | 'critical'
    message                 TEXT        NOT NULL,
    payload                 JSONB
);

-- ---------------------------------------------------------------------------
-- checkpoints — last-seen sequence numbers for WS and polling resumption
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS checkpoints (
    id                      BIGSERIAL   PRIMARY KEY,
    market_id               BIGINT      REFERENCES markets(id),
    channel                 TEXT        NOT NULL,   -- 'orderbook_delta' | 'trade' | 'poll' | 'discovery'
    last_seq                BIGINT,
    last_ts                 TIMESTAMPTZ,
    updated_ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (market_id, channel)
);

-- ---------------------------------------------------------------------------
-- game_events — reserved; future HLTV / Bayes / GRID game-state ingestion
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS game_events (
    id                      BIGSERIAL   PRIMARY KEY,
    market_id               BIGINT      NOT NULL REFERENCES markets(id),
    ts                      TIMESTAMPTZ NOT NULL,
    source                  TEXT        NOT NULL,   -- 'hltv' | 'bayes' | 'grid'
    event_type              TEXT        NOT NULL,
    payload                 JSONB       NOT NULL
);
