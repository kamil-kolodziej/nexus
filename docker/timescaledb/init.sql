-- TimescaleDB schema for nexus-ingestion.
-- Mounted into /docker-entrypoint-initdb.d/ by Docker Compose.
-- Runs automatically on first container startup; skipped when volume already contains data.
-- All statements are idempotent — safe to run manually against an existing database.

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

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

-- Sentiment scores hypertable
CREATE TABLE IF NOT EXISTS sentiment_scores (
    time            TIMESTAMPTZ NOT NULL,
    source          TEXT NOT NULL,
    asset           TEXT,
    article_url     TEXT NOT NULL,
    score           DOUBLE PRECISION NOT NULL,
    confidence      DOUBLE PRECISION NOT NULL,
    sentiment_label TEXT NOT NULL,
    model_id        TEXT NOT NULL,
    schema_version  TEXT NOT NULL DEFAULT '1.0.0'
);

SELECT create_hypertable('sentiment_scores', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_sentiment_scores_asset
    ON sentiment_scores (asset, time DESC);
