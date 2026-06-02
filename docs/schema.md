# Database Schema Reference

PostgreSQL 16+. All timestamps `TIMESTAMPTZ` (UTC). All prices `NUMERIC(10,4)`.

## Table Overview

| Table | Type | Partition | Description |
|---|---|---|---|
| `series` | entity | none | Kalshi series (e.g. "Valorant Champions") |
| `events` | entity | none | Match/tournament grouping |
| `markets` | entity | none | One row per market ticker |
| `market_updates` | append-only | weekly by `kalshi_ts` | Mutable field change log |
| `orderbook_snapshots` | append-only (change-only) | weekly by `kalshi_ts` | Full book state at each change |
| `trades` | append-only | weekly by `kalshi_ts` | Every observed trade |
| `settlements` | entity | none | Final outcome per market |
| `ingestion_runs` | operational | none | Per-process-start record |
| `api_requests` | operational | none | Every outbound API call |
| `data_quality_events` | operational | none | Gaps, drift, validation failures |
| `checkpoints` | operational | none | WS sequence numbers for resume |
| `game_events` | reserved | none | Future HLTV/Bayes/GRID data |

---

## Entity Tables

### series
| Column | Type | Notes |
|---|---|---|
| `ticker` | TEXT PK | Kalshi series ticker |
| `title` | TEXT | Human-readable name |
| `category` | TEXT | e.g. `esports` |
| `tags` | TEXT[] | e.g. `{valorant, counter-strike}` |
| `frequency` | TEXT | Recurrence pattern |
| `settlement_sources` | JSONB | Official determination sources |
| `additional_metadata` | JSONB | Full API response extras |
| `ingest_ts` | TIMESTAMPTZ | First seen |
| `last_seen_ts` | TIMESTAMPTZ | Most recently confirmed |

### events
| Column | Type | Notes |
|---|---|---|
| `ticker` | TEXT PK | Kalshi event ticker |
| `series_ticker` | TEXT FK→series | Parent series |
| `title` | TEXT | |
| `category` | TEXT | Deprecated in API; preserved |
| `status` | TEXT | `open`, `closed`, etc. |
| `open_time` | TIMESTAMPTZ | |
| `close_time` | TIMESTAMPTZ | |
| `additional_metadata` | JSONB | |
| `ingest_ts` / `last_seen_ts` | TIMESTAMPTZ | |

### markets
| Column | Type | Notes |
|---|---|---|
| `id` | BIGSERIAL PK | Internal surrogate |
| `ticker` | TEXT UNIQUE | Kalshi market ticker |
| `event_ticker` | TEXT FK→events | |
| `series_ticker` | TEXT FK→series | |
| `market_type` | TEXT | `binary`, `scalar` |
| `title` / `subtitle` | TEXT | |
| `yes_sub_title` / `no_sub_title` | TEXT | Contract outcome labels |
| `open_time` / `close_time` | TIMESTAMPTZ | |
| `status` | TEXT | See lifecycle below |
| `result` | TEXT | `yes`, `no`, `scalar` |
| `rules_primary` / `rules_secondary` | TEXT | Settlement rules text |
| `price_level_structure` | TEXT | |
| `additional_metadata` | JSONB | |
| `ingest_ts` / `last_seen_ts` | TIMESTAMPTZ | |

**Market status lifecycle:** `initialized → active ↔ inactive → closed → determined → finalized`
(with `disputed` and `amended` possible between determined and finalized)

---

## Time-Series Tables (partitioned weekly)

### market_updates
Append-only log of every observed change to a market's mutable fields.

| Column | Type | Notes |
|---|---|---|
| `id` | BIGSERIAL | Part of PK with `kalshi_ts` |
| `market_id` | BIGINT FK→markets | |
| `kalshi_ts` | TIMESTAMPTZ | Server timestamp (partition key) |
| `ingest_ts` | TIMESTAMPTZ | Local receipt timestamp |
| `status` | TEXT | Status at observation time |
| `yes_bid` / `yes_ask` / `no_bid` / `no_ask` | NUMERIC(10,4) | Best bid/ask prices |
| `last_price` | NUMERIC(10,4) | Last trade price |
| `volume` / `volume_24h` / `open_interest` | NUMERIC(16,2) | |
| `additional_metadata` | JSONB | |

### orderbook_snapshots
Change-only: a row is only written when the book differs from the previous snapshot.

| Column | Type | Notes |
|---|---|---|
| `id` | BIGSERIAL | Part of PK |
| `market_id` | BIGINT | |
| `kalshi_ts` | TIMESTAMPTZ | Partition key |
| `ingest_ts` | TIMESTAMPTZ | |
| `yes_bids` | JSONB | `[[price, qty], ...]` sorted descending |
| `no_bids` | JSONB | `[[price, qty], ...]` sorted descending |
| `checksum` | TEXT | SHA-256 of yes+no; used for change-detection |

**Note:** Only bids are stored. This matches the Kalshi API, which returns YES bids
and NO bids only. A NO bid at price *p* implies a YES ask at `$1 − p`.

### trades
| Column | Type | Notes |
|---|---|---|
| `id` | BIGSERIAL | Part of PK |
| `trade_id` | TEXT | Kalshi UUID |
| `market_id` | BIGINT | |
| `ticker` | TEXT | Denormalised for query convenience |
| `kalshi_ts` | TIMESTAMPTZ | Partition key (`ts_ms` from API) |
| `ingest_ts` | TIMESTAMPTZ | |
| `yes_price` / `no_price` | NUMERIC(10,4) | Both sides always stored |
| `count` | NUMERIC(12,2) | Contract quantity (fixed-point) |
| `taker_side` | TEXT | `yes` or `no` |
| `taker_book_side` | TEXT | `bid` or `ask` |
| `is_block_trade` | BOOLEAN | |

---

## Operational Tables

### settlements
One row per settled market, keyed on `market_id` (UNIQUE).

### ingestion_runs
One row per collector process start. `end_ts` is NULL until graceful shutdown.

### api_requests
Every outbound HTTP call: endpoint, status, latency, retry count, rate-limit flag.

### data_quality_events
`event_type` values: `gap`, `stale`, `schema_drift`, `validation`  
`severity` values: `warning`, `error`, `critical`

### checkpoints
Stores `(market_id, channel)` → `(last_seq, last_ts)` for WS and poll resume.

### game_events (reserved)
Schema: `(id, market_id, ts, source, event_type, payload JSONB)`.  
Future ingestion of HLTV / Bayes / GRID game-state will populate this table.

---

## Sample Queries

```sql
-- Latest orderbook for a market
SELECT yes_bids, no_bids, kalshi_ts
FROM orderbook_snapshots
WHERE market_id = (SELECT id FROM markets WHERE ticker = 'YOUR-TICKER')
ORDER BY kalshi_ts DESC
LIMIT 1;

-- All trades for a market today
SELECT yes_price, no_price, count, taker_side, kalshi_ts
FROM trades
WHERE market_id = (SELECT id FROM markets WHERE ticker = 'YOUR-TICKER')
  AND kalshi_ts >= CURRENT_DATE
ORDER BY kalshi_ts;

-- Active esports markets
SELECT m.ticker, m.title, m.status, m.close_time
FROM markets m
JOIN series s ON s.ticker = m.series_ticker
WHERE s.category = 'esports'
  AND m.status IN ('active', 'initialized')
ORDER BY m.close_time;
```
