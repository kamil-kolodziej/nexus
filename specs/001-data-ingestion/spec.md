# Feature Specification: Data Ingestion Layer

**Feature Branch**: `001-data-ingestion`
**Created**: 2026-03-20
**Status**: Implemented
**Input**: Data Ingestion Layer — connect to exchanges, normalize market events, publish to Redis

## Clarifications

### Session 2026-03-20

- Q: TimescaleDB persistence blocking behavior — should events block on DB write, or publish to Redis immediately while persisting asynchronously? → A: Asynchronous queuing. Events publish to Redis immediately; persistence happens in background task for eventual consistency.
- Q: Health alert notification mechanism — how do Risk Manager and other services receive health alerts? → A: Redis Stream. Alerts published to `nexus:ingestion-health-events` stream; Risk Manager consumes via consumer group for durability and replay capability.
- Q: Sentiment pipeline architecture — who owns and runs the NLP sentiment pipeline? → A: Separate sentiment service. Ingestion publishes `NewsArticle` events only; a dedicated `nexus-sentiment` service consumes articles and publishes `SentimentScore` events to the same Redis stream.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Exchange Market Data Flowing into the System (Priority: P1)

As the platform operator, I want live price ticks, order book updates, and trade events from at least one crypto exchange (Binance) to continuously flow into the system so that strategies have real market data to act on.

**Why this priority**: Without market data, no other component of the platform can function. This is the absolute foundation — strategies, aggregation, risk, and execution all depend on a live event stream. Delivering this first provides an independently observable, runnable system.

**Independent Test**: Start the ingestion service pointing at Binance sandbox/testnet, observe that `MarketEvent` records with type `Tick`, `OrderBookUpdate`, and `Candle` appear in the Redis Stream within 5 seconds of startup and continue arriving at least every 10 seconds for each subscribed asset.

**Acceptance Scenarios**:

1. **Given** the ingestion service is running and connected to Binance, **When** a price tick occurs on BTC/USDT, **Then** a `MarketEvent` of type `Tick` with correct `source`, `asset`, `timestamp`, and `payload` fields is published to the Redis Stream within 1 second.
2. **Given** the ingestion service is connected, **When** 60 seconds elapse, **Then** OHLCV candle events of type `Candle` appear in the stream with correct open/high/low/close/volume values.
3. **Given** the ingestion service is running, **When** it is inspected via the health endpoint, **Then** it reports connection status, last event timestamp, and event count per source.

---

### User Story 2 - Automatic Reconnection After Exchange Disconnection (Priority: P2)

As the platform operator, I want the ingestion service to automatically recover from WebSocket disconnections without manual intervention so that brief network interruptions do not require a service restart.

**Why this priority**: Exchange WebSocket connections drop regularly in practice. Missing market data during a disconnect without recovery would leave strategies working on stale data, potentially causing bad trades. Automatic recovery is essential for unattended operation.

**Independent Test**: With the ingestion service running and receiving data, terminate the network connection (or simulate it) and observe that the service reconnects and resumes publishing events within 30 seconds without any manual action.

**Acceptance Scenarios**:

1. **Given** the ingestion service is connected to an exchange, **When** the WebSocket connection drops, **Then** the service detects the disconnect within 5 seconds and begins reconnection attempts with exponential backoff.
2. **Given** the service is reconnecting, **When** the connection is restored, **Then** normal event publishing resumes automatically, a reconnection event is logged, and the reconnect counter for the recovered stream is cleared. Counters for other streams that are still failing are unaffected.
3. **Given** repeated disconnections occur, **When** the service cannot reconnect after the configured max attempts, **Then** it emits a health alert and continues retrying rather than crashing.

---

### User Story 3 - News Articles Entering the Pipeline (Priority: P3)

As a strategy developer, I want news articles from external sources to flow into the event stream so that the separate sentiment engine and sentiment-based strategies receive normalized article events alongside price data.

**Why this priority**: The platform design includes sentiment as a core signal source and sentiment strategies are one of the named strategy categories. However, strategies and probabilities can function with market data only, making this lower priority than the core price feed. Sentiment scoring itself is owned by the separate `nexus-sentiment` service.

**Independent Test**: Configure at least one RSS news source, start the ingestion service, and observe `NewsArticle` events appearing in the `nexus:news-events` Redis Stream within the configured polling interval.

**Acceptance Scenarios**:

1. **Given** a news adapter is configured with a valid source, **When** a new article is fetched, **Then** a `NewsArticle` event with `source`, `asset` (if determinable), `timestamp`, `headline`, and `url` fields is published to the Redis Stream.
2. **Given** the news source is unreachable, **When** a fetch attempt fails, **Then** the failure is logged, a health alert is emitted, and the adapter retries on the next polling interval without crashing the service.

---

### Edge Cases

- What happens when an exchange returns a malformed or incomplete event payload?
  → The adapter logs the exception, drops the event, increments the `malformed_count` counter, and continues.
- What happens when the Redis connection is lost while the ingestion service is running?
  → The service buffers events in memory up to a configurable limit, attempts reconnection with backoff, and resumes publishing once reconnected. If the buffer overflows, oldest events are dropped and a warning is logged.
- What happens when TimescaleDB becomes unavailable while the ingestion service is running?
  → Events continue to flow to Redis uninterrupted. The background persistence task queues write attempts and retries with backoff. Events remain in the persistence queue until TimescaleDB recovers. A `PERSISTENCE_ERROR` health alert is emitted on each write failure. If the in-memory queue reaches its maximum size before TimescaleDB recovers, new events are dropped and a `PERSISTENCE_ERROR` alert is emitted immediately by the service. The backtesting engine may work with partial historical data if the outage persists.
- What happens when the health alert stream (Redis) becomes unavailable?
  → Health alerts are logged locally and dropped. The health publisher does not buffer to avoid circular failure dependencies. Market data publishing continues uninterrupted.
- What happens when system clock drift causes event timestamps to be in the future or far in the past?
  → Events outside the configurable `timestamp_tolerance` window are logged and dropped. No server-time correction is applied.
- What happens when the same asset is available on multiple configured exchanges?
  → Each adapter publishes independently-sourced events; consumers can filter by `source`. This is not deduplicated at the ingestion layer.
- What happens when a new asset is added to the subscription list at runtime?
  → The adapter picks up the new subscription on its next reconnection or reload cycle. Hot asset-subscription changes are not guaranteed without a restart at this specification level.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The ingestion service MUST connect to at least one exchange (Binance) and continuously publish `Tick`, `OrderBookUpdate`, `Trade`, and `Candle` events to a Redis Stream.
- **FR-002**: All events MUST be normalized into the shared `MarketEvent` schema with fields: `source`, `asset`, `timestamp`, `event_type`, `payload`.
- **FR-003**: The service MUST automatically reconnect to exchanges after WebSocket disconnection using exponential backoff without requiring a manual restart. Before each restart attempt the adapter MUST be stopped to release connections and reset internal state.
- **FR-004**: Each adapter MUST run as an independent async task so that one adapter failing or disconnecting does not affect others.
- **FR-005**: The service MUST expose a health endpoint reporting per-adapter status: connection state, last event timestamp, and event count.
- **FR-006**: News adapters MUST fetch from at least one configurable RSS source on a configurable polling interval and publish `NewsArticle` events to the Redis Stream. Source type is validated at startup via `NewsSourceType` enum; unsupported types are rejected before any async work begins.
- **FR-007**: The ingestion service is responsible only for publishing `NewsArticle` events to Redis. A separate `nexus-sentiment` service consumes `NewsArticle` events and publishes `SentimentScore` events with a score in `[-1.0, 1.0]` and a confidence value. (Sentiment service is specified separately.)
- **FR-008**: The service MUST persist all published events to TimescaleDB asynchronously for historical replay and backtesting. Events MUST be published to Redis immediately; persistence to TimescaleDB happens in a background task with its own queue and retry logic. If TimescaleDB is unavailable, events remain in the background queue until it recovers; the Redis stream is unaffected.
- **FR-009**: Malformed or unparseable event payloads MUST be logged and dropped without crashing the adapter.
- **FR-010**: All adapter configuration (exchange credentials, subscribed assets, polling intervals, stream names) MUST be provided via environment variables or `config.toml` — no credentials in source code.
- **FR-011**: Health alerts (data gaps, adapter failures, persistence errors) MUST be published to the Redis Stream `nexus:ingestion-health-events` with standardized schema. The Risk Manager consumes these alerts asynchronously via a consumer group.

### Safety & Risk Constraints

- **SRC-001**: The ingestion layer MUST NOT execute or influence any order. It is read-only with respect to exchange state. Any write operations to exchanges (even test/ping calls beyond authentication) are prohibited.
- **SRC-002**: If the data quality circuit breaker condition is met (gap detection: no events for a configured asset within a threshold window), the service MUST publish a health alert to the Redis Stream `nexus:ingestion-health-events` with fields: `alert_type` (string), `asset` (string), `severity` (HIGH/MEDIUM/LOW), `timestamp` (ISO-8601), and `message` (string). The Risk Manager subscribes to this stream via a consumer group and is responsible for halting trading on affected assets. The ingestion service itself does not directly invoke the Risk Manager.
- **SRC-003**: Exchange API credentials MUST be read from environment variables or Docker secrets only. They MUST NOT appear in logs, metrics output, or event payloads.

### Service Boundary & Contract Impact

- **SBC-001**: This service is the sole producer of market events and news articles. It publishes to Redis Streams (`nexus:market-events` for price/trade data, `nexus:news-events` for articles). All other services (strategy engine, aggregator, sentiment engine, backtester) are consumers. The ingestion service has no dependency on any other Nexus service.
- **SBC-002**: The `MarketEvent` schema is a shared contract defined in `packages/nexus-common`. Any field addition is backward-compatible (new optional fields). Field removal or type change requires a coordinated version bump across all consumer services.
- **SBC-003**: Event types introduced here (`Tick`, `OrderBookUpdate`, `Trade`, `Candle`, `NewsArticle`) define the complete input vocabulary for the Strategy Engine and Sentiment Engine services. The Sentiment Engine consumes `NewsArticle` and produces `SentimentScore`. Both the Strategy Engine and Sentiment Engine specs must reference this list.

### Key Entities

- **MarketEvent**: The normalized envelope for all data. Contains `source` (adapter name + exchange id), `asset` (unified symbol e.g. `BTC/USDT`, or `None` for non-asset events such as news), `timestamp` (UTC ISO-8601), `event_type` (enum), `payload` (event-type-specific data).
- **Tick**: Payload within a MarketEvent. Represents a single best-bid/best-ask snapshot: `bid`, `ask`, `last`, `volume_24h`.
- **OrderBookUpdate**: Payload with `bids` and `asks` as sorted price-level arrays with quantities.
- **Trade**: Payload for a single completed exchange transaction: `trade_id` (exchange-assigned), `price`, `amount`, `side` (`"buy"` or `"sell"`), `taker_or_maker` (optional).
- **Candle**: OHLCV payload with `open`, `high`, `low`, `close`, `volume`, `timeframe`.
- **NewsArticle**: Payload with `headline`, `body_summary`, `url`, `source_name`, `published_at`, `related_assets` (list, may be empty).
- **SentimentScore**: Payload produced by the separate `nexus-sentiment` service (not ingestion). Contains `news_article_event_id` (reference to originating NewsArticle event), `score` (float, -1.0 to 1.0), `confidence` (float, 0.0 to 1.0), `model_version` (string). Published to the same Redis Stream as market events.
- **AdapterHealth**: Per-adapter runtime status: `adapter_id`, `adapter_type`, `status` (CONNECTED / RECONNECTING / DOWN), `last_event_at`, `event_count`, `error_count`, `malformed_count`.

## Assumptions

- Exchange sandbox/testnet credentials are available for development and testing; production keys are provided at deploy time via Docker secrets.
- The `ccxt` library's async (`ccxt.pro`) interface is sufficient for Binance WebSocket market data; a custom adapter is not required for the initial implementation.
- On-chain and fundamental adapters (Glassnode, FRED, SEC filings) are out of scope for this spec. They follow the same `MarketEvent` pattern and will be specified separately.
- The NLP sentiment pipeline is owned by a separate `nexus-sentiment` service, not this ingestion layer. A pre-trained sentiment model (e.g., FinBERT) is loaded and managed by the sentiment service; training is out of scope.
- TimescaleDB persistence of events is a requirement from day one (not deferred) because the backtesting engine depends on it.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Within 5 seconds of the ingestion service starting, at least one `MarketEvent` per subscribed asset appears in the Redis Stream for each configured exchange.
- **SC-002**: After a simulated exchange WebSocket disconnection, the service resumes publishing events for the affected exchange within 30 seconds without manual intervention.
- **SC-003**: Under steady-state operation (all infrastructure healthy, excluding outage windows), 100% of events published to Redis are also persisted to TimescaleDB within 10 seconds, verified by record count comparison over a 10-minute observation window.
- **SC-004**: Zero events from a failed adapter affect the event stream of other adapters — each adapter's failure is fully isolated.
- **SC-005**: The health endpoint responds within 200ms and reflects the true connection status of each adapter.
- **SC-006**: No exchange API credentials appear in application logs, metrics, or event payloads under any operational condition.
