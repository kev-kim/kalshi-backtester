-- 002_partitions.sql — Weekly partition scaffolding
-- Run after 001_init.sql.
-- Creates initial partitions covering the current week and 4 weeks forward.
-- Future partitions are created by scripts/verify_partitions.py (called at
-- collector startup and by the monitor's missing-partition check).
-- Partition naming: <table>_YYYY_Www  (ISO week)

DO $$
DECLARE
    week_start  DATE;
    week_end    DATE;
    i           INT;
BEGIN
    -- Align to Monday of the current ISO week
    week_start := date_trunc('week', CURRENT_DATE)::DATE;

    FOR i IN 0..4 LOOP
        week_end := week_start + 7;

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS market_updates_%s_%s
             PARTITION OF market_updates
             FOR VALUES FROM (%L) TO (%L)',
            to_char(week_start, 'YYYY'),
            to_char(week_start, '"W"IW'),
            week_start::TIMESTAMPTZ,
            week_end::TIMESTAMPTZ
        );

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS orderbook_snapshots_%s_%s
             PARTITION OF orderbook_snapshots
             FOR VALUES FROM (%L) TO (%L)',
            to_char(week_start, 'YYYY'),
            to_char(week_start, '"W"IW'),
            week_start::TIMESTAMPTZ,
            week_end::TIMESTAMPTZ
        );

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS trades_%s_%s
             PARTITION OF trades
             FOR VALUES FROM (%L) TO (%L)',
            to_char(week_start, 'YYYY'),
            to_char(week_start, '"W"IW'),
            week_start::TIMESTAMPTZ,
            week_end::TIMESTAMPTZ
        );

        week_start := week_end;
    END LOOP;
END;
$$;
