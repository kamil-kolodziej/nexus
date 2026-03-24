-- TimescaleDB schema for nexus-ingestion
-- Run against the nexus database after TimescaleDB extension is enabled

-- Market events hypertable
CREATE TABLE IF NOT EXISTS market_events (
    time            TIMESTAMPTZ NOT NULL,
    source          TEXT NOT NULL,
    asset           TEXT,
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    schema_version  TEXT NOT NULL DEFAULT '1.0.0'
);

SELECT create_hypertable('market_events', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_market_events_asset_type
    ON market_events (asset, event_type, time DESC);

-- Health alerts hypertable
CREATE TABLE IF NOT EXISTS health_alerts (
    time        TIMESTAMPTZ NOT NULL,
    alert_type  TEXT NOT NULL,
    adapter_id  TEXT NOT NULL,
    asset       TEXT,
    severity    TEXT NOT NULL,
    message     TEXT NOT NULL
);

SELECT create_hypertable('health_alerts', 'time', if_not_exists => TRUE);
