# Kalshi Esports Market Data Collector

A production-grade system for building a proprietary historical database of
Kalshi esports prediction market data. Kalshi does not offer historical tick
data — this system exists to collect it in real-time going forward.

## What This Is (and Is Not)

**Collects:**
- Order book snapshots (YES and NO sides separately, change-only)
- Every trade (price, size, side, timestamp)
- Market metadata, status changes, and settlement outcomes
- Volume and open interest

**Does NOT:**
- Generate trading signals
- Execute trades
- Backtest strategies
- Store positions or P&L

---

## Prerequisites

- Docker and Docker Compose (v2)
- A Kalshi account (demo or production)
- Python 3.12 (only needed for local development outside Docker)

---

## Generating a Kalshi API Key Pair

1. Log in at https://kalshi.com (or https://demo.kalshi.co for the demo environment).
2. Go to **Account → API Keys → Generate Key**.
3. Download the private key PEM file — Kalshi does not store it after this step.
4. Note the **Key ID** displayed after generation.

The collector signs every request using RSA-PSS (SHA-256). The private key
never leaves your machine — it is mounted as a Docker secret.

---

## Setup

```bash
# 1. Clone and enter the repo
git clone <repo-url>
cd kalshi-test

# 2. Configure environment
cp .env.example .env
# Edit .env: set KALSHI_API_KEY_ID and POSTGRES_PASSWORD (minimum required)

# 3. Place your private key
mkdir -p secrets
cp /path/to/downloaded/private_key.pem secrets/kalshi_private_key.pem

# 4. Start everything
make up
```

Services started: `postgres`, `collector`, `monitor`.
The collector runs database migrations automatically on first start.

---

## Verifying Data Is Flowing

```bash
make psql
```

```sql
-- Confirm collector is running
SELECT env, start_ts FROM ingestion_runs ORDER BY start_ts DESC LIMIT 1;

-- Check discovered markets
SELECT ticker, status, close_time FROM markets WHERE status = 'active' LIMIT 10;

-- Check orderbook snapshots
SELECT m.ticker, COUNT(*), MAX(os.ingest_ts) AS latest
FROM markets m JOIN orderbook_snapshots os ON os.market_id = m.id
GROUP BY m.ticker ORDER BY latest DESC LIMIT 10;

-- Check trades
SELECT ticker, COUNT(*) AS trades, MAX(kalshi_ts) AS last_trade
FROM trades GROUP BY ticker ORDER BY last_trade DESC LIMIT 10;
```

---

## Running the Settlements Backfill

Recovers settlement outcomes for already-closed esports markets (up to ~90 days
back via the Kalshi historical API, plus any recently finalized markets):

```bash
make backfill
```

For specific series only:
```bash
docker compose run --rm collector \
  python scripts/backfill_settlements.py --series VALORANT-CHAMPS CS2-MAJOR
```

---

## Monitoring and Alerts

**Discord:** set `DISCORD_WEBHOOK_URL` in `.env`. Alerts fire for:
- API error rates > 5%
- Rate-limit hits
- Stale active markets (no updates during their open window)
- Ingestion lag > 5 minutes
- Missing upcoming weekly partitions
- Schema drift (unexpected API response fields)

If `DISCORD_WEBHOOK_URL` is unset, alerts log to stderr only.

**Log tailing:**
```bash
make logs
```

All logs are structured JSON (structlog). Filter with `jq`:
```bash
docker compose logs -f collector | jq 'select(.level == "error")'
```

---

## Known Limitations

See [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md) for a full list. Key points:

- Historical order book data is not available from Kalshi — only trades,
  candlesticks, and market metadata can be backfilled.
- The Kalshi demo environment may have fewer active esports markets than production.
- The system has not been tested against production until an API key is provisioned.

---

## Development

```bash
make test      # run tests (spins up ephemeral Postgres)
make lint      # ruff check
make format    # ruff format
```

Schema reference: [docs/schema.md](docs/schema.md)  
Retention policy: [docs/retention.md](docs/retention.md)  
Operations runbook: [docs/runbook.md](docs/runbook.md)
