-- ============================================================
-- Telemetry pipeline database schema
-- Apply with:  psql <DSN> -f sql/001_schema.sql
-- ============================================================

-- ------------------------------------------------------------
-- MASTER TABLE: the production table queried by dashboards,
-- reports, and downstream systems. It carries indexes so those
-- queries are fast — which is exactly why we DON'T write to it
-- directly at high volume (each insert must update every index).
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS telemetry_events (
    id                BIGSERIAL PRIMARY KEY,
    -- The provider's unique id for the event. The UNIQUE constraint is
    -- our last line of defense against duplicates: even if a duplicate
    -- sneaks past the Redis cache, the merge below skips it.
    event_id          TEXT        NOT NULL UNIQUE,
    event_type        TEXT        NOT NULL,
    event_time_ms     BIGINT,      -- when the event happened (device clock)
    ingestion_id      UUID,        -- our trace id, assigned at ingestion
    ingested_at_ms    BIGINT,      -- when WE received it
    org_id            TEXT,
    vehicle_id        TEXT,
    driver_id         TEXT,
    -- Full flattened record as JSONB. Type-specific fields (latitude,
    -- duty_status, severity, ...) live here and can be queried with
    -- JSON operators, e.g.:  raw_payload->>'speed_mph'
    raw_payload       JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes matching the most common query patterns:
-- "events of type X", "history for vehicle Y", "latest events first".
CREATE INDEX IF NOT EXISTS idx_te_event_type  ON telemetry_events (event_type);
CREATE INDEX IF NOT EXISTS idx_te_vehicle_id  ON telemetry_events (vehicle_id);
CREATE INDEX IF NOT EXISTS idx_te_driver_id   ON telemetry_events (driver_id);
CREATE INDEX IF NOT EXISTS idx_te_event_time  ON telemetry_events (event_time_ms DESC);

-- ------------------------------------------------------------
-- STAGING TABLE: where the processor's batches land first.
--   * No indexes and no constraints → inserts are as cheap as possible.
--   * UNLOGGED → Postgres skips the write-ahead log for this table,
--     roughly doubling insert speed. The trade-off: its contents are
--     lost on a crash. That's acceptable here because rows live in
--     staging only for milliseconds, and Kafka can replay anything
--     that didn't make it to the master table.
-- ------------------------------------------------------------
CREATE UNLOGGED TABLE IF NOT EXISTS staging_telemetry_events (
    ingestion_id   UUID,
    ingested_at_ms BIGINT,
    event_id       TEXT,
    event_type     TEXT,
    event_time_ms  BIGINT,
    org_id         TEXT,
    vehicle_id     TEXT,
    driver_id      TEXT,
    raw_payload    JSONB
);

-- ------------------------------------------------------------
-- MERGE PROCEDURE: called by the processor right after each batch
-- insert. Moves everything from staging into master in one set-based
-- statement, then empties staging.
--
-- ON CONFLICT (event_id) DO NOTHING silently skips rows whose
-- event_id already exists in master — duplicate protection at the
-- database level, independent of the Redis cache.
-- ------------------------------------------------------------
CREATE OR REPLACE PROCEDURE merge_staging_to_master()
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO telemetry_events (
        event_id, event_type, event_time_ms,
        ingestion_id, ingested_at_ms,
        org_id, vehicle_id, driver_id, raw_payload
    )
    SELECT DISTINCT ON (event_id)
        event_id, event_type, event_time_ms,
        ingestion_id, ingested_at_ms,
        org_id, vehicle_id, driver_id, raw_payload
    FROM staging_telemetry_events
    WHERE event_id IS NOT NULL
    ON CONFLICT (event_id) DO NOTHING;

    -- TRUNCATE is much faster than DELETE for emptying a whole table.
    TRUNCATE staging_telemetry_events;
END;
$$;
