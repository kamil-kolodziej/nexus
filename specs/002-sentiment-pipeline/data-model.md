# Data Model: Sentiment Analysis Pipeline

**Date**: 2026-04-02 | **Branch**: `002-sentiment-pipeline`

## Entity Relationship Overview

```
                          nexus:news-events (Redis Stream)
                                    │
                                    ▼
                          ┌────────────────────┐
                          │  SentimentService   │
                          │  (consumer loop)    │
                          └─────┬──────────────┘
                                │ XREADGROUP
                                ▼
                     ┌──────────────────────┐
                     │  MarketEvent          │
                     │  event_type=          │
                     │    NEWS_ARTICLE       │
                     │  payload: NewsArticle │
                     └──────┬───────────────┘
                            │
              ┌─────────────┼──────────────┐
              ▼             ▼              ▼
     ┌──────────────┐ ┌──────────┐ ┌──────────────┐
     │AssetExtractor│ │ NLP      │ │ Fan-out      │
     │(dictionary   │ │ Inference│ │ (1 per asset)│
     │ + regex)     │ │ (thread) │ │              │
     └──────┬───────┘ └────┬─────┘ └──────┬───────┘
            │              │              │
            ▼              ▼              ▼
     ┌──────────────────────────────────────┐
     │  SentimentScore (per asset)           │
     │  wrapped in MarketEvent envelope      │
     │  event_type=SENTIMENT_SCORE           │
     └──────────────┬───────────────────────┘
                    │
          ┌─────────┼────────────┐
          ▼         ▼            ▼
   nexus:sentiment  TimescaleDB  nexus:sentiment-
   -events          (async)      health-events
   (Redis Stream)                (Redis Stream)


┌──────────────────────────────────────────────────────┐
│              SentimentScore (Payload)                 │
│  (published inside MarketEvent envelope)              │
│                                                      │
│  article_url: str                                    │
│  asset: str | None                                   │
│  score: float [-1.0, +1.0]                           │
│  confidence: float [0.0, 1.0]                        │
│  sentiment_label: "positive" | "negative" | "neutral"│
│  model_id: str                                       │
└──────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│              MarketEvent (Envelope)                   │
│  (reused from nexus-common)                          │
│                                                      │
│  source: str    ("nexus-sentiment:{processor_type}") │
│  asset: str | None    (mirrors SentimentScore.asset) │
│  timestamp: datetime (UTC)                           │
│  event_type: EventType.SENTIMENT_SCORE               │
│  schema_version: str ("1.0.0")                       │
│  payload: dict  (SentimentScore serialized)          │
└──────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│               HealthAlert                            │
│  (reused from nexus-common, no changes)              │
│                                                      │
│  alert_type: str                                     │
│  adapter_id: str   ("nexus-sentiment")               │
│  asset: str | None                                   │
│  severity: Severity                                  │
│  timestamp: datetime (UTC)                           │
│  message: str                                        │
└──────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│       Health response (RFC draft-inadarei)           │
│  (exposed via GET /health, built as dict per-call)   │
│                                                      │
│  status: str       ("ok" | "degraded" | "error")     │
│  serviceId: str    ("nexus-sentiment")               │
│  version: str      (package version)                 │
│  checks: {                                           │
│    "processor:inference": {status, observedValue}    │
│    "redis:publisher":     {status, observedValue}    │
│    "timescale:writer":    {status}   (if configured) │
│  }                                                   │
└──────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│            AssetDictionary (file)                     │
│  (loaded at startup from data/asset_dictionary.yaml) │
│                                                      │
│  version: str                                        │
│  assets: dict[canonical_id, {aliases: list[str]}]    │
│  sectors: dict[sector_tag, {keywords: list[str]}]    │
└──────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│         BaseSentimentProcessor (ABC)                 │
│  (processors/base.py)                                │
│                                                      │
│  + load() → None                                     │
│  + analyze(text: str) → SentimentResult              │
│  + close() → None                                    │
│  + model_id → str (property)                         │
└──────────────────────────────────────────────────────┘
          ▲                    ▲
          │                    │
┌─────────────────┐  ┌─────────────────┐
│ VaderProcessor  │  │FinBertProcessor │
│                 │  │                 │
│ compound→score  │  │ softmax→score   │
│ abs(comp)→conf  │  │ max(prob)→conf  │
└─────────────────┘  └─────────────────┘
```

## Entities

### SentimentScore (Payload) — NEW in `nexus-common`

The output of NLP inference on a news article. Added to `nexus_common.schemas.market_event` and registered in `PAYLOAD_TYPE_MAP` for `EventType.SENTIMENT_SCORE`.

| Field | Type | Description | Constraints |
|-------|------|-------------|-------------|
| `article_url` | `str` | Source article URL (pass-through from NewsArticle) | Non-empty |
| `asset` | `str \| None` | Trading pair (`"BTC/USDT"`), sector group (`"sector:crypto"`), or `None` (general market) | ccxt symbol, `sector:` prefix, or `None`; never empty string |
| `score` | `float` | Sentiment polarity | Range [-1.0, +1.0] inclusive |
| `confidence` | `float` | Model certainty | Range [0.0, 1.0] inclusive |
| `sentiment_label` | `str` | Classification label | One of: `"positive"`, `"negative"`, `"neutral"` |
| `model_id` | `str` | Processor identifier and version | Non-empty; format `"{processor}:{version}"` (e.g., `"vader:3.3.2"`, `"finbert:4.40.0"`) |

Pydantic validators:
- `score`: `ge=-1.0, le=1.0`
- `confidence`: `ge=0.0, le=1.0`
- `sentiment_label`: Literal `"positive" | "negative" | "neutral"`
- `article_url`: Non-empty string (no URL format validation — pass-through)
- `model_id`: Non-empty string, must not contain file paths or credentials (SRC-003)

### EventType (Enum) — MODIFIED in `nexus-common`

Add `SENTIMENT_SCORE = "SENTIMENT_SCORE"` to existing `EventType` StrEnum.

Updated enum values:
```
TICK
ORDER_BOOK_UPDATE
TRADE
CANDLE
NEWS_ARTICLE
SENTIMENT_SCORE   ← NEW
```

### MarketEvent (Envelope) — UNCHANGED structure

No structural changes. `PAYLOAD_TYPE_MAP` gains new entry: `EventType.SENTIMENT_SCORE: SentimentScore`.

When used for sentiment events:
- `source`: `"nexus-sentiment:vader"` or `"nexus-sentiment:finbert"`
- `asset`: mirrors `SentimentScore.asset` (string, sector tag, or `None`)
- `event_type`: `EventType.SENTIMENT_SCORE`

### HealthAlert — UNCHANGED

Reused as-is from `nexus-common`. Alert types emitted by `nexus-sentiment`:
- `MODEL_INFERENCE_ERROR` — NLP inference failure on a single article
- `MODEL_LOAD_FAILURE` — NLP model failed to load at startup (followed by service exit)
- `REDIS_DISCONNECT` — lost connection to Redis
- `PERSISTENCE_ERROR` — TimescaleDB batch write failure
- `DEAD_LETTER_CLAIMED` — pending message exceeded claim threshold

### Health response body — NEW in `nexus-sentiment`

Runtime status exposed via `GET /health`, shaped per RFC
[draft-inadarei-api-health-check](https://inadarei.github.io/rfc-healthcheck/).
Built as a plain dict per request from in-memory state (not a pydantic model,
not persisted). See `docs/design/nexus-trading-platform-design.md § Monitoring`
for the platform-wide convention.

**Top-level fields**:

| Field | Type | Description | Constraints |
|-------|------|-------------|-------------|
| `status` | `str` | Service health (worst of component checks) | `"ok"`, `"degraded"`, `"error"` |
| `serviceId` | `str` | Service identifier | `"nexus-sentiment"` |
| `version` | `str` | Package version | Semver or `"unknown"` |
| `checks` | `dict` | Per-component status keyed by `componentName:measurementName` | See below |

**Component checks**:

| Key | Status triggers | `observedValue` |
|-----|-----------------|-----------------|
| `processor:inference` | `error` when `consecutive_inference_errors >= 5`; `ok` otherwise | consecutive error count, or model_id when `ok` |
| `redis:publisher` | `degraded` when publisher disconnected (buffering); `ok` otherwise | buffer size (int) |
| `timescale:writer` | `ok` (present only when writer is configured) | — |

Each check is a dict with `status` (required) plus optional `observedValue`,
`observedUnit`, `output` (free-text explanation) per the RFC.

**HTTP status code**: `503` when top-level `status == "error"`, `200` otherwise.
**Media type**: `application/health+json`.

**Anti-pattern avoided**: no lifetime error counters in the health body — the
processor check uses a rolling consecutive-failure counter that self-heals on
the next successful inference.

### BaseSentimentProcessor (ABC) — NEW in `nexus-sentiment`

Abstract base class for NLP processors.

| Method | Signature | Description |
|--------|-----------|-------------|
| `load()` | `async def load() → None` | Load model/resources. Raises on failure. |
| `analyze()` | `def analyze(text: str) → SentimentResult` | Synchronous inference. Called in thread pool. |
| `close()` | `async def close() → None` | Release resources. |
| `model_id` | `@property → str` | Processor + version identifier. |

`SentimentResult` is a NamedTuple:
```python
class SentimentResult(NamedTuple):
    label: str        # "positive", "negative", "neutral"
    score: float      # [-1.0, +1.0]
    confidence: float # [0.0, 1.0]
```

### VaderProcessor — NEW in `nexus-sentiment`

| Behavior | Details |
|----------|---------|
| `load()` | Instantiate `SentimentIntensityAnalyzer`. No I/O. |
| `analyze(text)` | Call `polarity_scores(text)`. Map compound score: ≥0.05 → positive, ≤-0.05 → negative, else neutral. `score = compound`. `confidence = abs(compound)`. |
| `model_id` | `"vader:{vaderSentiment.__version__}"` |

### FinBertProcessor — NEW in `nexus-sentiment`

| Behavior | Details |
|----------|---------|
| `load()` | Load `ProsusAI/finbert` tokenizer + model via `transformers.pipeline("text-classification", ...)`. Downloads ~440MB on first run. |
| `analyze(text)` | Run pipeline with `top_k=3`. Extract softmax probs for `[positive, negative, neutral]`. `score = probs["positive"] - probs["negative"]`. `confidence = max(probs.values())`. |
| `model_id` | `"finbert:{transformers.__version__}"` |
| Missing dependency | If `transformers` or `torch` not installed, `load()` raises `ImportError` with clear message → service exits (SRC-005). |

### AssetDictionary — NEW file at `data/asset_dictionary.yaml`

Versioned file loaded at service startup.

| Field | Type | Description |
|-------|------|-------------|
| `version` | `str` | Dictionary version (semver) |
| `assets` | `dict[str, {aliases: list[str]}]` | Canonical asset ID → alias list |
| `sectors` | `dict[str, {keywords: list[str]}]` | Sector tag → keyword list |

Matching rules (FR-020):
- All matching is case-insensitive with strict word boundaries (`\bpattern\b`)
- No fuzzy matching, no substring matching
- Only curated exact aliases from the dictionary

### SentimentConfig — NEW in `nexus-sentiment`

Pydantic-settings model, same precedence as IngestionConfig.

| Field | Type | Default | Source |
|-------|------|---------|--------|
| `processor_type` | `str` | `"vader"` | `NEXUS_PROCESSOR_TYPE` / `config.toml [sentiment]` |
| `redis_url` | `str` | `"redis://localhost:6379"` | `NEXUS_REDIS_URL` / `config.toml [redis]` |
| `input_stream` | `str` | `"nexus:news-events"` | `NEXUS_INPUT_STREAM` / `config.toml [sentiment]` |
| `output_stream` | `str` | `"nexus:sentiment-events"` | `NEXUS_OUTPUT_STREAM` / `config.toml [sentiment]` |
| `health_stream` | `str` | `"nexus:sentiment-health-events"` | `NEXUS_HEALTH_STREAM` / `config.toml [sentiment]` |
| `consumer_group` | `str` | `"nexus-sentiment-group"` | `NEXUS_CONSUMER_GROUP` / `config.toml [sentiment]` |
| `block_timeout` | `int` | `5000` | ms to block on XREADGROUP |
| `pending_claim_threshold` | `int` | `300` | Seconds before pending message is claimed (FR-015) |
| `claim_sweep_interval` | `int` | `60` | Seconds between XAUTOCLAIM sweeps (FR-015) |
| `active_assets` | `list[str]` | `["BTC/USDT"]` | `NEXUS_ACTIVE_ASSETS` / `config.toml [sentiment]` |
| `asset_dictionary_path` | `str` | `"data/asset_dictionary.yaml"` | Path to extraction dictionary |
| `output_maxlen` | `int` | `50000` | MAXLEN for output stream |
| `health_maxlen` | `int` | `5000` | MAXLEN for health stream |
| `timescaledb_dsn` | `str` | `"postgresql://..."` | `NEXUS_TIMESCALEDB_DSN` / `config.toml [timescaledb]` |
| `batch_size` | `int` | `500` | TimescaleDB batch size |
| `flush_interval` | `float` | `5.0` | TimescaleDB flush interval (seconds) |
| `health_host` | `str` | `"127.0.0.1"` | Health endpoint bind address |
| `health_port` | `int` | `8081` | Health endpoint port (8081 to avoid conflict with ingestion) |
| `max_fan_out` | `int` | `50` | Maximum assets per article fan-out (edge case cap) |
| `log_env` | `str` | `"production"` | Logging environment |

## State Transitions

### Service State Machine

```
                    ┌──────────┐
            ┌──────►│ STARTING │
            │       └────┬─────┘
            │            │ model loaded + consumer group created
            │            ▼
            │       ┌──────────┐
            │       │ RUNNING  │◄──────────────────┐
            │       └────┬─────┘                   │
            │            │                         │
            │     ┌──────┴──────┐                  │
            │     ▼             ▼                  │
            │ inference    Redis disconnect         │
            │ error        ┌──────────┐            │
            │ (continue)   │RECONNECT │────────────┘
            │              └──────────┘  reconnected
            │
            │       model load failure
            │       ┌──────────┐
            └───────│  FAILED  │──► exit(1)
                    └──────────┘
```

### Message Processing Flow

```
XREADGROUP → parse MarketEvent → validate NewsArticle payload
    │
    ├── parse failure → log warning → XACK → continue
    │
    ├── valid NewsArticle:
    │       │
    │       ├── extract assets (dictionary + related_assets → deduplicate)
    │       │       │
    │       │       ├── empty text (headline+body both empty) → score=0, confidence=0, label=neutral
    │       │       │
    │       │       └── non-empty text → run NLP inference in thread pool
    │       │               │
    │       │               ├── inference error → health alert → message NOT acked → continue
    │       │               │
    │       │               └── success → fan-out SentimentScore per asset
    │       │                       │
    │       │                       ├── all publishes succeed → XACK → continue
    │       │                       │
    │       │                       └── any publish fails → message NOT acked → continue
    │       │
    │       └── empty effective asset list → 1 SentimentScore with asset=None
    │
    └── pending claim sweep (periodic) → claim stale → dead-letter log → XACK
```

## Storage Schema

### TimescaleDB: `sentiment_scores` table — NEW

```sql
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
```

### Redis Streams

| Stream | Producer | Consumer(s) | MAXLEN | Purpose |
|--------|----------|-------------|--------|---------|
| `nexus:news-events` | `nexus-ingestion` | `nexus-sentiment` (group: `nexus-sentiment-group`) | ~10,000 | Input: news articles |
| `nexus:sentiment-events` | `nexus-sentiment` | `nexus-strategies` (future) | ~50,000 | Output: sentiment scores |
| `nexus:sentiment-health-events` | `nexus-sentiment` | monitoring (future) | ~5,000 | Health alerts |
