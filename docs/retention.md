# Data Retention Policy

## Default: Keep All Raw Data Indefinitely

The system does not automatically delete any data. The append-only, change-only
design means storage growth is bounded by actual market activity, not by polling
frequency on quiet books.

## Estimated Growth Rates

| Table | Driver | Estimate |
|---|---|---|
| `orderbook_snapshots` | Active esports markets × change frequency | ~1–5 MB/market/day during live events |
| `trades` | Trade volume | ~100 KB/market/event |
| `market_updates` | Metadata polls (5 min) | Negligible |
| `api_requests` | ~1 row/request | ~50 MB/day at full rate |

For a portfolio of 50 concurrent esports markets during a major tournament,
expect ~500 MB/day. At that rate, 500 GB is reached after ~3 years of sustained
heavy usage.

## Archival to Parquet on S3 (when DB > 500 GB)

When the database exceeds 500 GB, archive old weekly partitions as follows:

### 1. Export partition to Parquet

```bash
# Install DuckDB for export
pip install duckdb

python - <<'EOF'
import duckdb, os

partition = "orderbook_snapshots_2024_w01"
s3_path = f"s3://your-bucket/kalshi/{partition}.parquet"

duckdb.execute(f"""
  INSTALL httpfs; LOAD httpfs;
  SET s3_region='us-east-1';
  SET s3_access_key_id='{os.environ["AWS_ACCESS_KEY_ID"]}';
  SET s3_secret_access_key='{os.environ["AWS_SECRET_ACCESS_KEY"]}';
  COPY (
    SELECT * FROM postgres_scan(
      'host=... dbname=kalshi user=kalshi password=...',
      'public', '{partition}'
    )
  ) TO '{s3_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
""")
print(f"Exported {partition} → {s3_path}")
EOF
```

### 2. Verify the export

```sql
-- Count rows in the live partition
SELECT COUNT(*) FROM orderbook_snapshots_2024_w01;
```

Compare against the Parquet file row count before proceeding.

### 3. Detach and drop the partition

```sql
-- Detach from parent (partition no longer queryable via parent table)
ALTER TABLE orderbook_snapshots DETACH PARTITION orderbook_snapshots_2024_w01;

-- Drop the partition (data is in S3)
DROP TABLE orderbook_snapshots_2024_w01;
```

### 4. Register in an archive manifest

Maintain a simple table to track what has been archived:

```sql
CREATE TABLE IF NOT EXISTS _archived_partitions (
    partition_name  TEXT PRIMARY KEY,
    s3_path         TEXT NOT NULL,
    archived_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    row_count       BIGINT
);
```

### Querying archived data

Use DuckDB or AWS Athena to query Parquet files directly:

```python
import duckdb
duckdb.execute("SELECT * FROM 's3://your-bucket/kalshi/orderbook_snapshots_2024_w01.parquet' LIMIT 10")
```

## Recommended Retention Tiers

| Age | Location | Access pattern |
|---|---|---|
| 0–12 months | PostgreSQL (live) | Real-time queries, JOINs |
| 12–36 months | PostgreSQL (cold partitions) | Detach from parent; query directly by partition name |
| 36+ months | S3 Parquet | DuckDB / Athena for analytical queries |
