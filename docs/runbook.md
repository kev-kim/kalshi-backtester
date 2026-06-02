# Operational Runbook

## Starting the System

```bash
cp .env.example .env          # fill in KALSHI_API_KEY_ID, POSTGRES_PASSWORD
mkdir -p secrets
cp /path/to/your/private.pem secrets/kalshi_private_key.pem
make up
```

Services come up in order: postgres → collector → monitor.
The collector runs migrations automatically on startup.

## Verifying Data Flow

```bash
make psql
```

```sql
-- Check collector is running
SELECT env, version, host, start_ts FROM ingestion_runs ORDER BY start_ts DESC LIMIT 3;

-- Check markets are being discovered
SELECT COUNT(*), status FROM markets GROUP BY status;

-- Check orderbook data is flowing
SELECT m.ticker, MAX(os.ingest_ts) AS last_snap
FROM markets m
JOIN orderbook_snapshots os ON os.market_id = m.id
GROUP BY m.ticker
ORDER BY last_snap DESC
LIMIT 10;

-- Check trades
SELECT ticker, COUNT(*), MAX(kalshi_ts) FROM trades GROUP BY ticker ORDER BY 2 DESC LIMIT 10;

-- Check for recent errors
SELECT event_type, severity, message, ts FROM data_quality_events ORDER BY ts DESC LIMIT 20;
```

## Common Issues

### No markets discovered
- Verify `KALSHI_API_KEY_ID` and private key are correct.
- Check `make logs` for auth errors (403 responses).
- If no esports events are live, discovery returns an empty set — this is normal.
  The loop retries every `DISCOVERY_INTERVAL_SEC` seconds.

### Collector exits immediately
- Check `make logs` for the error. Common causes:
  - Cannot connect to Postgres (wait for `postgres` healthcheck to pass).
  - Private key file not found at `KALSHI_PRIVATE_KEY_PATH`.
  - Invalid `KALSHI_API_KEY_ID` format.

### 429 rate limit errors in logs
- Normal during heavy discovery. The token bucket and exponential backoff handle this automatically.
- If sustained: check `api_requests` for rate-limited counts; consider upgrading Kalshi tier.

### WebSocket disconnects frequently
- Check network stability. The WS client reconnects automatically with exponential backoff.
- Check Kalshi status page for planned maintenance.

### Missing partition warning from monitor
Run the partition verification script:
```bash
docker compose run --rm collector python scripts/verify_partitions.py --run-maintenance
```

### Stale market alert
- If no esports events are live, active markets may legitimately go quiet.
  The stale threshold only fires during a market's `open_time → close_time` window.
- If an event IS live: check WS connection in logs. Look for `ws_disconnected` events.

## Running the Backfill

Captures settlement history for already-closed esports markets:

```bash
make backfill
```

Or for specific series:
```bash
docker compose run --rm collector python scripts/backfill_settlements.py --series VALORANT-CHAMPS CS2-MAJOR
```

## Stopping Gracefully

```bash
make down
```

`docker compose down` sends SIGTERM to running containers. The collector flushes
buffers, closes the WebSocket, and marks the ingestion run `end_ts` before exiting.

## Monitoring Checks Reference

| Check | Threshold | Level |
|---|---|---|
| API failure rate | >5% → warning, >10% → error | Per 60-min window |
| Rate-limit hits | Any → warning, >50 → error | Per 60-min window |
| Stale market | No update > `STALE_MARKET_THRESHOLD_SEC` during open window | Warning |
| Ingestion lag | >5 min → warning, >15 min → error | Per active market |
| Missing partition | Upcoming week not created | Error |
| Schema drift | Any unexpected API field | Warning |

## Log Locations

All logs are JSON via structlog, written to stdout.

```bash
make logs                              # all services
docker compose logs collector          # collector only
docker compose logs monitor            # monitor only
```

## Adminer (DB Browser, dev only)

```bash
docker compose --profile dev up -d
```

Open http://localhost:8080 — server: `postgres`, user/pass from `.env`.
