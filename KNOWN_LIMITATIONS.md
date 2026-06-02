# Known Limitations & API Reality vs. Spec

> Last verified: 2026-06-02 against https://docs.kalshi.com

This document records every place where the current Kalshi API differs from the
assumptions in CLAUDE.md, plus any inherent platform limitations relevant to
this system. Each section states what the spec assumed, what reality is, and
how the implementation handles the difference.

---

## 1. Base URLs have changed

**Spec assumed:** `https://demo-api.kalshi.co` (demo) and
`https://api.elections.kalshi.com` / `https://trading-api.kalshi.com` (prod).

**Reality:** Kalshi now recommends `external-api.*` infrastructure. The old
hosts still work (backward-compatible aliases) but new integrations should use:

| Env  | REST                                                | WebSocket                                              |
|------|-----------------------------------------------------|--------------------------------------------------------|
| Prod | `https://external-api.kalshi.com/trade-api/v2`      | `wss://external-api-ws.kalshi.com/trade-api/ws/v2`     |
| Demo | `https://external-api.demo.kalshi.co/trade-api/v2`  | `wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2` |

**Implementation:** Both environments use the new recommended hosts.
`KALSHI_ENV=demo` selects the demo pair; `KALSHI_ENV=prod` selects prod.

---

## 2. WebSocket signing: fixed path, not full URL path

**Spec assumed:** RSA signing over `timestamp + "GET" + <WebSocket upgrade path>`.

**Reality:** Confirmed correct, but the path that must be signed is the fixed
literal string `/trade-api/ws/v2`, regardless of any query parameters or
sub-paths. The WS URL itself is `wss://external-api-ws.kalshi.com/trade-api/ws/v2`.

**Implementation:** `ws_client.py` signs `timestamp + "GET" + "/trade-api/ws/v2"`
and passes the three auth headers via `additional_headers` during the
`websockets.connect()` call.

---

## 3. Rate limits: two buckets (Read / Write), not per-endpoint-class

**Spec assumed:** "Per-endpoint-class token bucket."

**Reality:** Kalshi uses exactly two independent token buckets — **Read** (all
GET endpoints) and **Write** (order placement, amendments, cancellations,
order groups, RFQ quotes). Each request costs 10 tokens by default; exceptions:
- 2 tokens: order cancellations, single-order reads, quote create/cancel,
  multivariate-collection lookups.

Default tier (Basic): Read = 200 tok/s, Write = 100 tok/s with a 2-second
burst headroom for Write (1 second for Basic Write).

**Note:** 429 responses include **no** `Retry-After` or `X-RateLimit-*`
headers. Backoff must be implemented blind (exponential with jitter).

**Implementation:** Two `asyncio`-based token buckets (`READ` and `WRITE`). The
collector only touches Read endpoints; Write bucket is initialised but unused
for data collection. All 429 responses trigger exponential backoff with jitter
as required by the spec.

---

## 4. Orderbook endpoint returns bids only (no asks)

**Spec assumed:** "Full depth if the endpoint returns it."

**Reality:** `GET /markets/{ticker}/orderbook` returns **YES bids and NO bids
only**. No asks are returned. This is by design: on a binary market, a NO bid
at price *p* is economically equivalent to a YES ask at `$1 − p`. The response
shape is:

```json
{
  "orderbook_fp": {
    "yes_dollars": [["0.6500", "200.00"], ...],
    "no_dollars":  [["0.3500", "150.00"], ...]
  }
}
```

`depth` parameter: `0` or negative = all levels; `1–100` = top-N levels.

**Implementation:** Stored as-is: `yes_bids` and `no_bids` JSONB arrays in
`orderbook_snapshots`. Both sides are always stored; no collapse to a single
probability. Downstream analysts can derive implied asks from NO bids.

---

## 5. WebSocket orderbook is delta-based, not snapshot-only

**Spec assumed:** "Order book snapshots."

**Reality:** The `orderbook_delta` WS channel sends:
1. An initial `orderbook_snapshot` message on subscription (full book state).
2. Subsequent `orderbook_delta` messages (incremental single-level changes).

The collector must maintain a local in-memory orderbook per market, apply
deltas, and then write the reconstructed snapshot to the DB on each update.
The change-detection logic (skip write if snapshot is byte-identical) still
applies to the reconstructed full snapshot.

---

## 6. Market statuses are more granular than open → closed → settled

**Spec assumed:** Three states: open, closed, settled.

**Reality:** Eight states with specific transitions:

| Status      | Meaning                                        |
|-------------|------------------------------------------------|
| initialized | Created, not yet open                          |
| active      | Open for trading                               |
| inactive    | Temporarily deactivated by exchange            |
| closed      | Past `close_time`, no new orders               |
| determined  | Outcome known, settlement timer running        |
| disputed    | Outcome challenged                             |
| amended     | Re-determined after dispute                    |
| finalized   | Settlement complete, positions paid out        |

The WS `market_lifecycle_v2` channel emits events: `deactivated`, `activated`,
`close_date_updated`, `determined`, `settled`. Note: `settled` WS event
precedes the `finalized` REST status — there is a brief lag.

**Implementation:** `market_updates` table stores every observed status change.
`settlements` table is written when `event_type = "settled"` is received or
when REST status transitions to `finalized`.

---

## 7. No direct esports category filter on /events or /markets

**Spec assumed:** Filter the events/markets endpoints "for esports categories."

**Reality:** `GET /events` and `GET /markets` have no `category` or `tag`
query parameter. The category field in event responses is deprecated.

**Correct approach (verified 2026-06-02):** There is no `esports` category.
Esports series are filed under `category=Sports` with `tags=["Esports"]` or
`tags=["Video games"]`. Use `GET /series?tags=Esports` as the primary
discovery query. Discovery loop:
1. `GET /series?category=esports` → collect all esports `series_ticker` values.
2. `GET /series?tags=valorant` / `GET /series?tags=counter-strike` for
   additional coverage if Kalshi tags differ from category names.
3. For each series ticker: `GET /events?series_ticker=<t>` → get live events.
4. For each event: `GET /markets?event_ticker=<t>` → get individual markets.

`GET /search/tags_by_categories` can enumerate the full tag taxonomy; the
discovery loop calls this once at startup (and periodically) to pick up new
esports tags.

---

## 8. Historical data uses a rolling cutoff, not a fixed lookback

**Spec assumed:** A backfill script can recover historical settlement data.

**Reality confirmed:** Kalshi partitions data at a rolling cutoff (~90 days
live). Data older than the cutoff is available via `/historical/*` endpoints:
- `GET /historical/markets` — settled markets (supports `series_ticker` filter)
- `GET /historical/trades` — trades beyond the cutoff
- `GET /historical/cutoff` — retrieve current cutoff timestamps

The backfill script must call `/historical/markets?series_ticker=<esports_series>`
to retrieve pre-existing settled esports markets, then call
`/historical/trades?ticker=<t>` for trade history on each.

**Limitation:** Historical endpoints do not include orderbook snapshots at
arbitrary past timestamps. Only trades, candlesticks, and market metadata are
recoverable. Proprietary tick-level order book history is only buildable going
forward from when this collector starts running.

---

## 9. API key: not yet generated

**Status:** No Kalshi API key pair has been created. The system cannot be
tested against even the demo environment until RSA key pair generation and
registration are complete.

**Steps to unblock:**
1. Log in to https://kalshi.com (or https://demo.kalshi.co for demo).
2. Navigate to Account → API Keys → Generate Key.
3. Download the private key PEM file — Kalshi does not store it.
4. Set `KALSHI_API_KEY_ID` and mount the PEM as `KALSHI_PRIVATE_KEY_PATH`.

**Impact on testing:** Unit tests for RSA signing (`test_auth.py`) use a
locally generated test key pair and do not require a Kalshi account. Integration
tests and end-to-end tests require a real key.

---

## 10. Prices use fixed-point strings, not decimals

**Spec context:** "All prices stored as NUMERIC(10,4)."

**Reality:** Kalshi API returns prices as **strings** with `_fp` or `_dollars`
suffixes. Examples:
- `"yes_price_dollars": "0.6500"` — dollar-denominated, 4 decimal places
- `"count_fp": "100.00"` — fixed-point contract count, 2 decimal places
- `"yes_dollars": [["0.6500", "100.00"], ...]` — orderbook level [price, qty]

**Implementation:** All price strings are parsed to `Decimal` before DB insert.
`NUMERIC(10,4)` columns are correct for prices. Contract counts stored as
`NUMERIC(12,2)`.

---

## 11. WS channel for trades is `trade`, not namespaced per-market

**Reality:** Subscribing to the `trade` channel with no `market_ticker` receives
all public trades across all markets. Subscribe with `market_ticker` or
`market_tickers` to filter. This is efficient — the collector subscribes once
per watched market rather than one global subscription with client-side filtering.

---

## 12. No FIX protocol implementation

The spec explicitly excluded FIX. Kalshi offers FIX for both event-contract
and margin markets. This system uses REST + WebSocket only.

---

## Appendix: Confirmed correct in original spec

The following items from CLAUDE.md were verified against current docs and
require no adjustment:

- Auth headers: `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-SIGNATURE`,
  `KALSHI-ACCESS-TIMESTAMP` — confirmed.
- RSA-PSS with SHA-256, salt = `PSS.DIGEST_LENGTH` — confirmed.
- Signature over `timestamp + method + path` (no query string) — confirmed.
- WebSocket available for orderbook, trades, and market lifecycle — confirmed.
- YES and NO sides stored separately — confirmed (by design).
- Append-only observation tables, idempotent upsert for entities — correct approach.
- `TIMESTAMPTZ` UTC everywhere — correct.
- `NUMERIC` for prices (never FLOAT) — correct.
