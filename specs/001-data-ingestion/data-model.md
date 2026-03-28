# Data Model: Data Ingestion Layer

**Date**: 2026-03-22 | **Branch**: `001-data-ingestion`

## Entity Relationship Overview

```
┌──────────────────────────────────────────────────────┐
│                   MarketEvent                        │
│  (normalized envelope for all ingested data)         │
│                                                      │
│  id: str (Redis Stream auto-ID)                      │
│  source: str                                         │
│  asset: str | None                                   │
│  timestamp: datetime (UTC)                           │
│  event_type: EventType                               │
│  schema_version: str                                 │
│  payload: Tick | OrderBookUpdate | Trade | Candle    │
│           | NewsArticle                              │
└──────────┬───────────────────────────────────────────┘
           │ event_type determines payload type
           │
     ┌─────┴─────┬──────────┬──────────┬──────────────┐
     ▼           ▼          ▼          ▼              ▼
  ┌──────┐  ┌────────┐  ┌───────┐  ┌───────┐  ┌────────────┐
  │ Tick │  │OrderBook│  │ Trade │  │Candle │  │NewsArticle │
  │      │  │ Update  │  │       │  │       │  │            │
  └──────┘  └────────┘  └───────┘  └───────┘  └────────────┘

┌──────────────────────────────────────────────────────┐
│                   HealthAlert                        │
│  (published to nexus:ingestion-health-events)        │
│                                                      │
│  alert_type: str                                     │
│  adapter_id: str                                     │
│  asset: str | None                                   │
│  severity: Severity                                  │
│  timestamp: datetime (UTC)                           │
│  message: str                                        │
└──────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│                  AdapterHealth                       │
│  (exposed via health endpoint, per adapter)          │
│                                                      │
│  adapter_id: str                                     │
│  adapter_type: "exchange" | "news"                   │
│  status: AdapterStatus                               │
│  last_event_at: datetime | None                      │
│  event_count: int                                    │
│  error_count: int                                    │
│  malformed_count: int                                │
└──────────────────────────────────────────────────────┘
```

## Entities

### MarketEvent (Envelope)

The normalized wrapper for all ingested data. Defined in `packages/nexus-common`.

| Field | Type | Description | Constraints |
|-------|------|-------------|-------------|
| `source` | `str` | Adapter name + exchange id (e.g., `"binance:exchange"`, `"newsapi:news"`) | Non-empty, format: `{exchange_or_source}:{adapter_type}` |
| `asset` | `str \| None` | Unified symbol (e.g., `"BTC/USDT"`), or `None` for non-asset events (news) | ccxt unified symbol format; never empty string — use `None` |
| `timestamp` | `datetime` | Event time in UTC (ISO-8601) | Must be within configurable tolerance of server time |
| `event_type` | `EventType` | Discriminator enum | One of: `TICK`, `ORDER_BOOK_UPDATE`, `TRADE`, `CANDLE`, `NEWS_ARTICLE` |
| `schema_version` | `str` | Schema version for backward compatibility | Semantic version string (e.g., `"1.0.0"`) |
| `payload` | `dict` | Event-type-specific data (discriminated union) | Validated per event_type |

### EventType (Enum)

```
TICK
ORDER_BOOK_UPDATE
TRADE
CANDLE
NEWS_ARTICLE
```

### Tick (Payload)

Best bid/ask snapshot from an exchange.

| Field | Type | Description | Constraints |
|-------|------|-------------|-------------|
| `bid` | `float` | Best bid price | > 0 |
| `ask` | `float` | Best ask price | > 0, >= bid |
| `last` | `float` | Last trade price | > 0 |
| `volume_24h` | `float` | 24-hour trading volume | >= 0 |

### OrderBookUpdate (Payload)

Order book depth snapshot or delta.

| Field | Type | Description | Constraints |
|-------|------|-------------|-------------|
| `bids` | `list[list[float, float]]` | Sorted bid price-levels `[price, quantity]` | Descending by price |
| `asks` | `list[list[float, float]]` | Sorted ask price-levels `[price, quantity]` | Ascending by price |
| `depth` | `int` | Number of levels requested | > 0 |

### Trade (Payload)

Individual trade execution on the exchange.

| Field | Type | Description | Constraints |
|-------|------|-------------|-------------|
| `trade_id` | `str` | Exchange-assigned trade identifier | Non-empty |
| `price` | `float` | Execution price | > 0 |
| `amount` | `float` | Trade quantity | > 0 |
| `side` | `str` | `"buy"` or `"sell"` | Enum-validated |
| `taker_or_maker` | `str \| None` | `"taker"`, `"maker"`, or `None` | Optional |

### Candle (Payload)

OHLCV candlestick data.

| Field | Type | Description | Constraints |
|-------|------|-------------|-------------|
| `open` | `float` | Open price | > 0 |
| `high` | `float` | High price | >= open, >= close |
| `low` | `float` | Low price | <= open, <= close |
| `close` | `float` | Close price | > 0 |
| `volume` | `float` | Period volume | >= 0 |
| `timeframe` | `str` | Candle period (e.g., `"1m"`, `"5m"`, `"1h"`) | ccxt timeframe format |

### NewsArticle (Payload)

A news article fetched from an RSS feed or news API.

| Field | Type | Description | Constraints |
|-------|------|-------------|-------------|
| `headline` | `str` | Article title/headline | Non-empty |
| `body_summary` | `str` | Truncated article body or description | Max 1000 chars |
| `url` | `str` | Link to original article | Valid URL format |
| `source_name` | `str` | News source identifier (e.g., `"coindesk"`, `"newsapi"`) | Non-empty |
| `published_at` | `datetime` | Publication timestamp (UTC) | Must be a valid datetime |
| `related_assets` | `list[str]` | Detected related asset symbols (e.g., `["BTC/USDT"]`) | May be empty list |

### HealthAlert

Published to `nexus:ingestion-health-events` stream. Consumed by Risk Manager.

| Field | Type | Description | Constraints |
|-------|------|-------------|-------------|
| `alert_type` | `str` | Alert category (e.g., `"DATA_GAP"`, `"ADAPTER_DOWN"`, `"PERSISTENCE_ERROR"`) | Non-empty |
| `adapter_id` | `str` | Which adapter triggered this alert | Non-empty |
| `asset` | `str \| None` | Affected asset, if applicable | May be `None` for adapter-level alerts |
| `severity` | `Severity` | `HIGH`, `MEDIUM`, or `LOW` | Enum-validated |
| `timestamp` | `datetime` | Alert creation time (UTC) | ISO-8601 |
| `message` | `str` | Human-readable description | Non-empty |

### Severity (Enum)

```
HIGH    — data gap on active asset, adapter unrecoverable
MEDIUM  — adapter reconnecting, TimescaleDB persistence delayed
LOW     — malformed event rate above threshold, news source temporarily unreachable
```

### AdapterStatus (Enum)

```
CONNECTED     — actively receiving and publishing events
RECONNECTING  — connection lost, attempting reconnection with backoff
DOWN          — max reconnection attempts exhausted; still retrying but flagged
```

### AdapterHealth

Runtime status per adapter, exposed via `GET /health`.

| Field | Type | Description |
|-------|------|-------------|
| `adapter_id` | `str` | Unique adapter identifier (e.g., `"binance:exchange"`) |
| `adapter_type` | `str` | `"exchange"` or `"news"` |
| `status` | `AdapterStatus` | Current connection state |
| `last_event_at` | `datetime \| None` | Timestamp of last successfully published event |
| `event_count` | `int` | Total events published since startup |
| `error_count` | `int` | Total errors encountered since startup |
| `malformed_count` | `int` | Total malformed/dropped payloads since startup |

## Validation Rules

1. **Timestamp tolerance**: Events whose `timestamp` is missing or deviates from server time by more than `config.timestamp_tolerance` (default: 60s) are logged and dropped. No server-time correction is applied.
2. **Price positivity**: All price fields (`bid`, `ask`, `last`, `open`, `high`, `low`, `close`, `price`) must be > 0.
3. **OrderBook ordering**: `bids` must be descending by price; `asks` must be ascending by price.
4. **Schema version**: `schema_version` must be a valid semver string. Consumers check major version for compatibility.
5. **Credential exclusion**: No field in any entity may contain API keys or secrets. Validation at serialization boundary rejects events containing known credential patterns.

## State Transitions

### Adapter Lifecycle

```
INIT ──→ CONNECTED ──→ (receiving events)
              │
              ▼ (connection lost)
         RECONNECTING ──→ CONNECTED (success)
              │
              ▼ (max attempts exhausted)
            DOWN ──→ RECONNECTING (continues retrying)
              │
              ▼ (service shutdown)
           STOPPED
```

### Health Alert Triggers

| Condition | Alert Type | Severity | Trigger |
|-----------|-----------|----------|---------|
| No events for asset within threshold | `DATA_GAP` | HIGH | Gap detector timer fires |
| Adapter enters RECONNECTING | `ADAPTER_RECONNECTING` | MEDIUM | Adapter state change |
| Adapter enters DOWN | `ADAPTER_DOWN` | HIGH | Max reconnection attempts exhausted |
| TimescaleDB write failure | `PERSISTENCE_ERROR` | MEDIUM | Background writer retry |
| TimescaleDB queue full (event dropped by service) | `PERSISTENCE_ERROR` | MEDIUM | `IngestionService.handle_event` enqueue returns `False` |
| Malformed event rate > threshold | `MALFORMED_SPIKE` | LOW | Counter exceeds configured rate |
| News source unreachable (first failure after up) | `NEWS_SOURCE_DOWN` | LOW | HTTP fetch failure (up→down transition only) |
| News source recovers after failure | `NEWS_SOURCE_RECOVERED` | LOW | HTTP fetch succeeds after previous failure (down→up transition only) |

## TimescaleDB Schema

### Table: `market_events`

```sql
CREATE TABLE market_events (
    time        TIMESTAMPTZ NOT NULL,
    source      TEXT NOT NULL,
    asset       TEXT,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    schema_version TEXT NOT NULL DEFAULT '1.0.0'
);

SELECT create_hypertable('market_events', 'time');
CREATE INDEX idx_market_events_asset_type ON market_events (asset, event_type, time DESC);
```

### Table: `health_alerts`

```sql
CREATE TABLE health_alerts (
    time        TIMESTAMPTZ NOT NULL,
    alert_type  TEXT NOT NULL,
    adapter_id  TEXT NOT NULL,
    asset       TEXT,
    severity    TEXT NOT NULL,
    message     TEXT NOT NULL
);

SELECT create_hypertable('health_alerts', 'time');
```
