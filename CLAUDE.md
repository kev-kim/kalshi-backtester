# Claude Code Prompt — Kalshi Esports Market Data Collection System

You are a senior data engineer, quantitative researcher, and market data infrastructure engineer.

Build a COMPLETE, runnable Kalshi esports market data collection system in this repository. Historical Kalshi tick data is not publicly available, so this system exists to build a proprietary historical database going forward.

**Do NOT build:** trading strategies, backtesters, execution logic, or signal generation.
**DO build:** data collection, storage, schema, quality monitoring, and deployment.

Produce production-quality code only. No pseudocode, no placeholders, no "TODO" stubs, no conceptual explanations in code comments. If you encounter an API limitation, document it in `KNOWN_LIMITATIONS.md` and implement the closest correct alternative — do not fabricate endpoints or auth schemes.

---

## 1. Kalshi API — verified facts to implement against

Before writing code, fetch and read the current Kalshi API documentation at `https://trading-api.readme.io/` and the WebSocket docs. Implement against what the docs actually say, not assumptions. The following constraints are known and must be respected:

- **Authentication:** RSA-PSS signed requests. Headers required: `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-SIGNATURE`, `KALSHI-ACCESS-TIMESTAMP`. The signature is over `timestamp + method + path` using the user's RSA private key with PSS padding and SHA-256. Implement signing correctly using the `cryptography` library. Do NOT stub with bearer tokens.
- **Environments:** demo (`https://demo-api.kalshi.co`) and prod (`https://api.elections.kalshi.com` / `https://trading-api.kalshi.com` — verify current host from docs). Select via env var `KALSHI_ENV={demo,prod}`. Default to `demo`.
- **WebSocket:** if available on the user's environment, use it for live order book and trade updates. The WS handshake requires the same RSA signing on the upgrade request. If WS is unavailable for a market category, fall back to polling for that category only.
- **Rate limits:** tiered per endpoint class. Implement a per-endpoint-class token bucket, not a single global bucket. Log every 429 and back off exponentially with jitter.
- **Market discovery:** esports market tickers change per tournament. Implement a discovery loop that polls the events/markets endpoints, filters for esports categories (Valorant, Counter-Strike, and any other esports tags Kalshi exposes), and dynamically subscribes the collector to new markets as they appear. Do not hardcode a market list.

## 2. What to collect

For every esports market discovered, capture continuously until settlement:

- Market metadata (ticker, event, series, open/close times, rules, status)
- **Both YES and NO contract sides separately** — store both, do not collapse to a single probability. Kalshi quotes do not always sum to 100¢.
- Order book snapshots (top of book at minimum; full depth if the endpoint returns it)
- Trades (price, size, side, timestamp)
- Volume and open interest
- Status changes (open → closed → settled, plus any intermediate states)
- Settlement outcome and final payout

**Append-only for observations** (price_history, trades, orderbook_snapshots, market_updates). **Idempotent upsert for entities** (markets, events, series, settlements — keyed on Kalshi's natural IDs).

**Order book writes must be change-only**: if a new snapshot is byte-identical to the previous one for that market (same bids, asks, sizes), skip the write. This is mandatory — otherwise the DB will explode.

## 3. Timestamps

- All timestamps `TIMESTAMPTZ`, UTC only. No naive `TIMESTAMP` columns anywhere.
- Every observation row stores BOTH:
  - `kalshi_ts` — the server timestamp from the API response
  - `ingest_ts` — the local timestamp when the collector received it
- This is non-negotiable. Later research needs to distinguish "when the market moved" from "when we saw it move."

## 4. Database schema (PostgreSQL 16+)

Implement these tables with explicit `CREATE TABLE` statements, indexes, and partitioning. Use `TIMESTAMPTZ`, `BIGINT` for IDs where appropriate, `NUMERIC` for prices (never `FLOAT`).

Required tables:
- `events` — Kalshi event-level grouping (e.g., a tournament or match)
- `series` — series hierarchy so related markets (map 1, map 2, series winner) can be joined later. Even if Kalshi doesn't expose series natively for every event, derive a `series_key` from event metadata.
- `markets` — one row per market ticker, with FK to event and series
- `market_updates` — append-only log of any observed change to a market's mutable fields
- `orderbook_snapshots` — change-only snapshots, stores YES and NO sides
- `trades` — every trade observed
- `settlements` — one row per settled market, FK to market
- `ingestion_runs` — per-process-start record (start_ts, end_ts, env, version, host)
- `api_requests` — log of every outbound API call (endpoint, status, latency_ms, retry_count, rate_limited_bool)
- `data_quality_events` — gaps, stale markets, schema drift, validation failures
- `game_events` — **reserve this table now even though unused.** Schema: `(id, market_id, ts, source, event_type, payload JSONB)`. Future ingestion of HLTV / Bayes / GRID game-state will populate it. Documenting this avoids a painful migration later.

Partitioning:
- `market_updates`, `orderbook_snapshots`, `trades`: range partition by `kalshi_ts`, weekly partitions. Auto-create partitions via `pg_partman` or a scheduled job. Weekly (not monthly) because esports traffic is bursty per-event.
- Other tables: no partitioning.

Indexing:
- `(market_id, kalshi_ts DESC)` on all time-series tables
- `(ticker)` unique on `markets`
- `(status)` partial indexes for active markets
- GIN on JSONB columns where queried

Retention: document recommended retention in `docs/retention.md`. Default: keep all raw data indefinitely; document how to archive partitions to Parquet on S3 if/when the DB grows past 500GB.

## 5. Ingestion engine

- Async architecture using `asyncio` + `httpx` for HTTP and `websockets` for WS.
- Per-market-class workers so a stall in one category doesn't block others.
- Checkpointing: persist last-seen offsets/sequence numbers to a `checkpoints` table; resume cleanly on restart.
- Idempotent writes via `INSERT ... ON CONFLICT DO NOTHING` for observations and `ON CONFLICT DO UPDATE` for entities.
- Structured logging (JSON) via `structlog`. Log every API call to `api_requests`.
- Graceful shutdown on SIGTERM (flush buffers, close WS, commit).
- Automatic restart recovery — process must come up, read checkpoints, resume without manual intervention.
- **Backfill script** (`scripts/backfill_settlements.py`): walks the markets endpoint historically to capture settlement outcomes and metadata for already-closed esports markets. This is the only "historical" data actually retrievable from Kalshi and must be implemented.

## 6. Data quality monitoring

Separate `monitor` service. Checks every N seconds:
- API failure rate per endpoint class
- Rate-limit hits in the last hour
- Stale markets (active market with no updates in > 30 min during expected live window — esports markets legitimately go quiet, so this threshold is configurable per category and only alerts during a market's `open_time → close_time` window)
- Schema drift (unexpected fields in API responses — log full payload to `data_quality_events`)
- Ingestion lag (`now() - max(ingest_ts)` per active market)
- Missing partitions for the upcoming week

**Alerting channel: Discord webhook.** Single channel. Env var `DISCORD_WEBHOOK_URL`. If unset, log to stderr only. Do not invent a multi-channel alerting framework.

## 7. Deployment

Generate:
- `docker-compose.yml` with services: `postgres` (16+, with `pg_partman`), `collector`, `monitor`, `adminer` (for DB inspection, dev only via profile)
- `Dockerfile` for the Python services (Python 3.12, slim base, non-root user, multi-stage build)
- `.env.example` with every env var documented
- `Makefile` with targets: `up`, `down`, `logs`, `psql`, `test`, `lint`, `format`, `backfill`
- Volume mounts for Postgres data persistence
- Healthchecks on every service

Env vars (at minimum):
```
KALSHI_ENV=demo
KALSHI_API_KEY_ID=
KALSHI_PRIVATE_KEY_PATH=/run/secrets/kalshi_private_key.pem
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=kalshi
POSTGRES_USER=kalshi
POSTGRES_PASSWORD=
DISCORD_WEBHOOK_URL=
LOG_LEVEL=INFO
ORDERBOOK_POLL_INTERVAL_MS=1000
DISCOVERY_INTERVAL_SEC=60
```

Mount the private key as a Docker secret, not as a baked-in file.

## 8. Repository structure

```
.
├── docker-compose.yml
├── Dockerfile
├── Makefile
├── pyproject.toml
├── .env.example
├── README.md
├── KNOWN_LIMITATIONS.md
├── docs/
│   ├── schema.md
│   ├── retention.md
│   └── runbook.md
├── sql/
│   ├── 001_init.sql
│   ├── 002_partitions.sql
│   └── 003_indexes.sql
├── src/
│   ├── kalshi_collector/
│   │   ├── __init__.py
│   │   ├── auth.py              # RSA-PSS signing
│   │   ├── http_client.py       # signed httpx client + token buckets
│   │   ├── ws_client.py         # signed websocket client
│   │   ├── discovery.py         # esports market discovery loop
│   │   ├── collectors/
│   │   │   ├── orderbook.py
│   │   │   ├── trades.py
│   │   │   ├── metadata.py
│   │   │   └── settlements.py
│   │   ├── storage/
│   │   │   ├── db.py            # asyncpg pool
│   │   │   ├── writers.py       # append-only & upsert helpers
│   │   │   └── checkpoints.py
│   │   ├── monitor/
│   │   │   ├── __init__.py
│   │   │   ├── checks.py
│   │   │   └── discord.py
│   │   ├── models.py            # pydantic models
│   │   ├── config.py
│   │   └── logging.py
│   └── main.py                  # entrypoint
├── scripts/
│   ├── backfill_settlements.py
│   └── verify_partitions.py
└── tests/
    ├── test_auth.py             # signing correctness against known vectors
    ├── test_writers.py
    ├── test_change_only_orderbook.py
    └── test_discovery.py
```

## 9. Testing

- `pytest` + `pytest-asyncio`
- Tests must cover: RSA signing correctness (against known input/output vector documented in the test), change-only orderbook write logic, idempotency of upserts, checkpoint resume behavior, rate-limit backoff
- A `docker-compose.test.yml` that spins up an ephemeral Postgres for integration tests
- No tests that call the real Kalshi API. Use recorded fixtures (`pytest-vcr` or saved JSON).

## 10. README

Must include, in this order:
1. What this is and what it deliberately does NOT do (no strategy, no backtest, no execution)
2. Prerequisites (Docker, Python 3.12 if running locally)
3. Generating a Kalshi API key pair and where to put the private key
4. `cp .env.example .env` and fill in values
5. `make up` to start everything
6. How to verify data is flowing (`make psql`, sample queries)
7. How to run the settlements backfill
8. How to inspect monitoring (Discord setup, log locations)
9. Known limitations (link to KNOWN_LIMITATIONS.md)

## 11. Hard rules

- Python 3.12. Use `uv` or `pip` with `pyproject.toml` (PEP 621). No `requirements.txt`.
- Type hints everywhere. `mypy --strict` must pass on `src/`.
- `ruff` for lint + format. Config in `pyproject.toml`.
- All prices stored as `NUMERIC(10,4)`. Never `FLOAT`/`REAL`.
- All timestamps `TIMESTAMPTZ`, UTC.
- No `print()` in `src/`. Use `structlog`.
- No bare `except:`. Catch specific exceptions and log them.
- Every external call wrapped in retry-with-jitter (`tenacity`).
- If WebSocket is not available for a market type, the collector logs the limitation and falls back to polling that market type only — it does not crash and does not silently skip data.

## 12. Deliverable

Output every file in full. After generating, run `mypy`, `ruff`, and `pytest` inside the container and fix any failures before declaring done. The final state must be: clone the repo, set env vars, drop in a private key, run `make up`, and have data flowing into Postgres within 60 seconds (for demo env) with the monitor service reporting healthy.

Begin by fetching the current Kalshi API docs, then writing `KNOWN_LIMITATIONS.md` with anything in this prompt that conflicts with current reality, then implementing.