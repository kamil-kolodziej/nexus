# Quickstart: Sentiment Analysis Pipeline

**Branch**: `002-sentiment-pipeline` | **Date**: 2026-04-02

## Prerequisites

- Python 3.11+
- Redis running on `localhost:6379`
- TimescaleDB running on `localhost:5432` (for persistence)
- `nexus-ingestion` running and producing `NewsArticle` events to `nexus:news-events`

## Install

From the repository root:

```bash
# Core install (VADER processor — default)
pip install -e packages/nexus-common[dev] -e packages/nexus-sentiment[dev]

# With FinBERT support (downloads ~440MB model on first run)
pip install -e packages/nexus-common[dev] -e "packages/nexus-sentiment[dev,finbert]"
```

## Configure

Copy and edit the config (if not already done):

```bash
cp config.example.toml config.toml
```

Add/edit the `[sentiment]` section:

```toml
[sentiment]
processor_type = "vader"          # or "finbert"
active_assets = ["BTC/USDT", "ETH/USDT"]
asset_dictionary_path = "data/asset_dictionary.yaml"
health_port = 8081
```

Or use environment variables:

```bash
export NEXUS_PROCESSOR_TYPE=vader
export NEXUS_ACTIVE_ASSETS='["BTC/USDT","ETH/USDT"]'
```

## Run

```bash
python -m nexus_sentiment.main
```

## Verify

1. **Check health endpoint**:
   ```bash
   curl http://127.0.0.1:8081/health
   ```
   Expected:
   ```json
   {"status": "ok", "processor": {"type": "vader", "state": "loaded", "model_id": "vader:3.3.2"}, "events_processed": 0, "errors": 0}
   ```

2. **Publish a test article** (via redis-cli):
   ```bash
   redis-cli XADD nexus:news-events '*' \
     source "test:news" \
     asset "" \
     timestamp "2026-04-02T12:00:00+00:00" \
     event_type "NEWS_ARTICLE" \
     schema_version "1.0.0" \
     payload '{"headline":"Bitcoin surges past $100K on institutional demand","body_summary":"Major institutions announce Bitcoin purchases.","url":"https://example.com/btc","source_name":"test","published_at":"2026-04-02T12:00:00+00:00","related_assets":["BTC/USDT"]}'
   ```

3. **Read sentiment output**:
   ```bash
   redis-cli XRANGE nexus:sentiment-events - +
   ```
   Expected: one entry with `event_type=SENTIMENT_SCORE`, positive `score`, `asset=BTC/USDT`.

## Run Tests

```bash
# Unit tests (no external services required)
pytest packages/nexus-sentiment/tests/unit

# Contract tests (snapshot-based)
pytest packages/nexus-sentiment/tests/contract

# Integration tests (requires Docker — Redis + TimescaleDB)
docker compose -f docker-compose.dev.yml up -d
pytest packages/nexus-sentiment/tests/integration

# All tests
pytest packages/nexus-sentiment
```

## Docker

```bash
docker compose -f docker-compose.dev.yml up -d
docker compose up nexus-sentiment
```

## Key Configuration Reference

| Setting | Env Var | TOML Key | Default |
|---------|---------|----------|---------|
| Processor type | `NEXUS_PROCESSOR_TYPE` | `[sentiment] processor_type` | `"vader"` |
| Input stream | `NEXUS_INPUT_STREAM` | `[sentiment] input_stream` | `"nexus:news-events"` |
| Output stream | `NEXUS_OUTPUT_STREAM` | `[sentiment] output_stream` | `"nexus:sentiment-events"` |
| Active assets | `NEXUS_ACTIVE_ASSETS` | `[sentiment] active_assets` | `["BTC/USDT"]` |
| Health port | `NEXUS_HEALTH_PORT` | `[sentiment] health_port` | `8081` |
| Claim threshold | `NEXUS_PENDING_CLAIM_THRESHOLD` | `[sentiment] pending_claim_threshold` | `300` (seconds) |
