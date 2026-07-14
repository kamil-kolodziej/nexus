# Feature Specification: Strategy Engine

**Feature Branch**: `003-strategy-engine`
**Created**: 2026-07-14
**Status**: Draft
**Input**: User description: "Strategy Engine (nexus-strategies) — a service that runs pluggable, single-horizon trading strategies. Each strategy consumes normalized market data and sentiment scores and emits trade Signals for the aggregator to consume. Ships with one worked example strategy (RSI crossover) and runs end-to-end on the same code paths used for live trading and backtest."

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Market Data In, Trade Signals Out (Priority: P1)

As a platform operator, I want the built-in RSI crossover strategy to consume live market data and automatically publish trade signals so that the signal-to-decision pipeline is proven end-to-end with a real, working strategy before any custom strategies are written.

**Why this priority**: This is the core value of the service — turning market events into actionable signals. Without at least one working strategy producing signals on the output stream, the aggregator has nothing to consume and the entire decision pipeline downstream of ingestion is non-functional. The worked example doubles as living documentation for strategy authors.

**Independent Test**: Publish a sequence of `CANDLE` events to `nexus:market-events` for `BTC/USDT` that drives RSI from below the oversold threshold back above it. Observe that a `Signal` event with `direction=BUY`, `strategy_name="rsi_crossover"`, `asset="BTC/USDT"`, a `confidence` in `[0.0, 1.0]`, a non-empty `reasoning`, and a future `expiry` appears on `nexus:signal-events` within the latency target.

**Acceptance Scenarios**:

1. **Given** the RSI crossover strategy is running with a full warm-up window of candles, **When** RSI crosses up through the oversold threshold, **Then** exactly one `Signal` with `direction=BUY` is published to `nexus:signal-events` within 1 second of the triggering candle event being consumed.
2. **Given** the RSI crossover strategy is running with a full warm-up window, **When** RSI crosses down through the overbought threshold, **Then** exactly one `Signal` with `direction=SELL` is published.
3. **Given** any published `Signal`, **When** its fields are inspected, **Then** `confidence` is in `[0.0, 1.0]`, `direction` is one of `BUY`/`SELL`/`HOLD`, `strategy_name` is non-empty, `timestamp` is UTC, `expiry` is strictly after `timestamp`, and `reasoning` is a non-empty human-readable explanation.
4. **Given** candles arrive that do not produce a threshold crossing, **When** the strategy evaluates them, **Then** no new `Signal` is published (the engine emits on decision changes, not on every event).

---

### User Story 2 — Pluggable Strategy Interface (Priority: P1)

As a strategy developer, I want to add a new strategy by implementing a single documented interface and registering it in configuration so that new trading logic can be deployed without modifying the engine, other strategies, or downstream services.

**Why this priority**: The platform vision is multiple weighted strategies (technical, ML, sentiment, arbitrage, statistical, rule-based) feeding one aggregator. That is only achievable if the strategy contract is stable and self-contained. Together with User Story 1 this defines the MVP: an engine plus one example implementation of the contract.

**Independent Test**: Implement a trivial test strategy (e.g., "always HOLD") against the strategy interface in a test module, register it in configuration alongside the RSI strategy, start the service, and observe both strategies running independently and the test strategy's signals appearing on `nexus:signal-events` with its own `strategy_name`.

**Acceptance Scenarios**:

1. **Given** a new strategy that implements the strategy interface, **When** it is added to configuration and the service is restarted, **Then** it starts receiving market and sentiment events and may publish signals — with zero code changes to the engine or other strategies.
2. **Given** a strategy is listed in configuration but its implementation cannot be loaded (missing module, wrong interface), **When** the service starts, **Then** the service reports a clear per-strategy startup error, emits a health alert, and continues starting all other configured strategies.
3. **Given** a running engine, **When** a strategy is removed from configuration and the service restarts, **Then** that strategy no longer runs and no signals with its `strategy_name` are produced, while all other strategies are unaffected.
4. **Given** each configured strategy, **When** it is instantiated, **Then** it receives its own strategy-specific parameters (e.g., RSI period, thresholds), its assigned asset list, and its single decision horizon from configuration.

---

### User Story 3 — Strategy Fault Isolation (Priority: P2)

As a platform operator, I want a crashing or misbehaving strategy to be contained and restarted without affecting any other strategy so that one buggy strategy can never silence the rest of the signal flow.

**Why this priority**: Strategies are the most frequently changed, least trusted code in the platform (including future ML models and third-party logic). The platform-wide isolation principle (FR-004 of 001-data-ingestion) must extend here or a single bad strategy takes down all signal production.

**Independent Test**: Configure two strategies where one raises an exception on a specific crafted event. Publish that event and observe: the faulty strategy logs the error and is restarted with backoff, a health alert is emitted, and the second strategy continues producing signals throughout with no gap beyond normal processing latency.

**Acceptance Scenarios**:

1. **Given** two running strategies, **When** one raises an unhandled exception while processing an event, **Then** the other strategy continues consuming and emitting without interruption.
2. **Given** a strategy has crashed, **When** the supervisor restarts it, **Then** restarts use exponential backoff with a per-strategy attempt counter, and each crash/restart cycle emits a health alert identifying the strategy.
3. **Given** a strategy exceeds its maximum restart attempts, **When** the limit is reached, **Then** the strategy is marked failed (no further restarts), a high-severity health alert is emitted, the health endpoint reports the degraded state, and all other strategies keep running.
4. **Given** a strategy that processes one event abnormally slowly or hangs, **When** other strategies have events pending, **Then** the slow strategy does not delay event delivery to the others.

---

### User Story 4 — Sentiment as a Strategy Input (Priority: P2)

As a strategy developer, I want strategies to receive sentiment scores alongside market data so that sentiment-aware strategies can combine news tone with price action in a single decision.

**Why this priority**: The sentiment pipeline (002-sentiment-pipeline) already publishes per-asset and sector-level scores. Feeding them into strategies is the reason that pipeline exists. It is P2 because the pipeline is proven end-to-end by User Story 1 with market data alone.

**Independent Test**: Register a test strategy that subscribes to sentiment input. Publish a `SentimentScore` event for `BTC/USDT` to `nexus:sentiment-events` and observe the strategy receives it. Publish a `sector:crypto` score and observe it is delivered to strategies subscribed to that sector tag.

**Acceptance Scenarios**:

1. **Given** a strategy that declares interest in sentiment input, **When** a `SentimentScore` event for one of its configured assets appears on `nexus:sentiment-events`, **Then** the strategy receives it as a typed input alongside its market data.
2. **Given** a `SentimentScore` with a `sector:`-prefixed asset (e.g., `sector:crypto`), **When** it is consumed, **Then** it is delivered to strategies subscribed to that sector tag, and strategies can distinguish sector-level from asset-level scores.
3. **Given** a strategy that declares no interest in sentiment (e.g., the RSI crossover example), **When** sentiment events flow, **Then** the strategy is not invoked for them and its behavior is unchanged.
4. **Given** a `SentimentScore` with `asset=None` (general market sentiment), **When** it is consumed, **Then** it is delivered to strategies that opted in to general market sentiment.

---

### User Story 5 — Identical Behavior Live and in Backtest (Priority: P2)

As a strategy developer, I want a strategy to make identical decisions whether events come from live ingestion or a historical replay so that backtest results are trustworthy predictors of live behavior.

**Why this priority**: The platform's backtesting principle mandates one code path. If strategy code can observe whether it is live (wall clock, real-time assumptions, external calls), backtests silently diverge from production and every downstream validation is built on sand.

**Independent Test**: Feed the same recorded sequence of `MarketEvent`s through a strategy twice (and once via a replay harness at accelerated speed). Assert the emitted `Signal` sequences are identical in order, direction, confidence, and reasoning — with timestamps derived from event time, not wall-clock time.

**Acceptance Scenarios**:

1. **Given** a fixed input event sequence, **When** it is processed twice by the same strategy configuration, **Then** the emitted signal sequences are identical (deterministic given identical inputs and parameters).
2. **Given** a historical replay at faster-than-real-time speed, **When** the strategy evaluates events, **Then** all time-based logic (warm-up, expiry, staleness) is computed from event timestamps, never from the system clock.
3. **Given** the engine running in backtest mode, **When** strategies execute, **Then** the strategy, signal validation, and emission code paths are the same as live — only the event source and signal sink differ.

---

### User Story 6 — Operational Observability (Priority: P3)

As a platform operator, I want a health endpoint and health alerts covering every strategy and the event consumers so that I can detect a stalled or degraded strategy engine before it results in missed trading opportunities.

**Why this priority**: Necessary for production operation, but the engine delivers its core value without it and it follows the established platform convention rather than introducing new behavior.

**Independent Test**: Start the service and request `GET /health`; verify per-component checks for each strategy and each input consumer. Kill one strategy repeatedly to exceed its restart limit and verify the endpoint reports `degraded` and a health alert appeared on the health stream.

**Acceptance Scenarios**:

1. **Given** the running service, **When** `GET /health` is requested, **Then** it responds within 200 milliseconds with the platform-standard health body: 3-state top-level `status` equal to the worst component status, `serviceId`, `version`, and per-strategy plus per-consumer `checks{}`.
2. **Given** any strategy crash, restart, permanent failure, or input-stream stall, **When** it occurs, **Then** a health alert with the appropriate severity is published to the strategy health stream.
3. **Given** a strategy that has emitted no signal for an extended period while events flow, **When** the health endpoint is queried, **Then** the per-strategy check exposes last-event-processed and last-signal-emitted observations so silence is distinguishable from stall.

---

### Edge Cases

- What happens when a strategy has not yet accumulated its warm-up window (e.g., fewer than 14 candles for RSI)?
  → The strategy consumes and records events but emits no signals until warm-up completes. Warm-up progress is visible via the health check. No signals are ever emitted from a partially warmed state.
- What happens on a gap in market data (no events for a configured staleness window)?
  → The engine marks the affected asset's data stale using event-time comparison; strategies do not emit new signals from stale data, and a health alert is emitted for the stalled input. When data resumes after a gap longer than the strategy's horizon, the warm-up window restarts for that asset.
- What happens when a duplicate event is delivered (at-least-once semantics from Redis Streams)?
  → Strategies must tolerate duplicates. A redelivered entry after crash recovery may reach a strategy again; re-emitting the same decision is acceptable — downstream consumers of signals also operate under at-least-once semantics.
- What happens when events arrive out of order (older timestamp after a newer one)?
  → Events are delivered in stream order. An event with a timestamp older than the strategy's last-seen timestamp for that asset is excluded from indicator computation and logged; it never rewinds strategy state.
- What happens when a malformed or schema-invalid event appears on an input stream?
  → It is acknowledged, logged as a warning, and dropped without invoking any strategy. Malformed input must never crash a consumer or a strategy.
- What happens when a strategy emits a signal with an out-of-range confidence or an expiry not after its timestamp?
  → The engine validates every signal before publishing. Invalid signals are rejected (not published, not "clamped"), logged with the offending values, counted per strategy, and a health alert is emitted. Validation fails closed.
- What happens when a strategy emits signals at an abnormally high rate?
  → Per-strategy emission is rate-limited (configurable, default 10 signals per asset per minute). Signals beyond the limit are dropped with a warning and a health alert; the strategy keeps running.
- What happens when Redis is unreachable at startup?
  → Redis is required. The service exits immediately with a non-zero exit code and a clear error message.
- What happens when the Redis connection is lost while publishing signals?
  → The signal publisher buffers in a bounded queue and flushes on reconnect (same pattern as the platform's event publishers). Signals whose expiry has passed by flush time are discarded with a log entry rather than published stale.
- What happens when a `SentimentScore` arrives for an asset no running strategy is configured for?
  → It is consumed, acknowledged, and dropped without dispatch. No error, no alert.
- What happens when two strategies are configured with the same `strategy_name`?
  → Startup fails for the duplicate with a clear configuration error; `strategy_name` must be unique because it identifies the signal source for the aggregator's weighting.
- What happens when the service receives SIGTERM/SIGINT?
  → The engine stops dispatching new events, allows in-flight strategy evaluations to finish (bounded by a shutdown timeout), publishes any resulting signals, acknowledges processed input, and exits. Unprocessed stream entries remain pending for the next startup.
- What happens when a strategy's configured asset never appears in incoming market data?
  → The strategy runs but never completes warm-up for that asset; the health check surfaces per-asset warm-up state so the misconfiguration is observable.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The service MUST consume `MarketEvent`s from the `nexus:market-events` Redis Stream and `SentimentScore` events from the `nexus:sentiment-events` Redis Stream using consumer groups (`XREADGROUP`), with group creation via `XGROUP CREATE ... $ MKSTREAM` at startup (ignoring `BUSYGROUP`).
- **FR-002**: The service MUST define a single strategy interface in which a strategy declares: a unique `strategy_name`, the event types it consumes (market data, sentiment, or both), its configured assets and/or sector tags, its single decision horizon, and its strategy-specific parameters. A strategy receives typed events and returns zero or more `Signal`s; it performs no I/O of its own.
- **FR-003**: Each strategy MUST operate on exactly one decision horizon (a single timeframe such as one candle interval). Multi-horizon logic is out of scope; combining perspectives across horizons is the aggregator's job, achieved by running multiple single-horizon strategy instances.
- **FR-004**: A Strategy Manager MUST load the set of strategies from configuration at startup and run each strategy as an independently supervised unit (per-strategy `asyncio.create_task` with `add_done_callback`, per the platform isolation principle — no `asyncio.TaskGroup`, no `asyncio.gather` for supervision). A failure in one strategy MUST NOT cancel or delay any sibling.
- **FR-005**: A crashed strategy MUST be restarted with exponential backoff and a per-strategy attempt counter. After a configurable maximum number of attempts, the strategy MUST be marked permanently failed for the process lifetime, with a high-severity health alert; other strategies continue.
- **FR-006**: A strategy that fails to load at startup (missing module, interface mismatch, invalid parameters) MUST produce a per-strategy startup error and health alert without preventing other configured strategies from starting. Duplicate `strategy_name`s across configured strategies MUST fail startup with a clear configuration error.
- **FR-007**: Emitted signals MUST conform to the shared `Signal` schema: `strategy_name`, `asset`, `direction` (`BUY`/`SELL`/`HOLD`), `confidence` in `[0.0, 1.0]`, UTC `timestamp`, non-empty `reasoning`, and `expiry` strictly after `timestamp`. The `Signal` type MUST live in `nexus-common`.
- **FR-008**: The engine MUST validate every signal returned by a strategy before publishing. Signals failing validation MUST be rejected (never published or coerced), logged with the offending values, counted per strategy, and reflected in health state. Validation is owned by the engine, not by individual strategies.
- **FR-009**: Valid signals MUST be published to the `nexus:signal-events` Redis Stream, wrapped in the platform `MarketEvent` envelope with a new `SIGNAL` event type and `source="nexus-strategies:{strategy_name}"`, providing durable at-least-once delivery to the aggregator.
- **FR-010**: Strategies MUST emit signals on decision changes (including a change to `HOLD`), not on every consumed event. Re-emitting an unchanged decision is permitted only when the previously emitted signal for that asset has expired.
- **FR-011**: All time-based strategy logic (warm-up, staleness, expiry, horizon boundaries) MUST be computed from event timestamps, never from the system clock, so that historical replay produces identical decisions. Given an identical input event sequence and identical parameters, a strategy MUST produce an identical signal sequence.
- **FR-012**: The engine MUST support a replayed/backtest event source and an alternative signal sink through the same strategy, validation, and emission code paths used live. Strategy code MUST have no way to detect or branch on live-versus-backtest mode.
- **FR-013**: Strategies MUST NOT begin emitting signals for an asset until that asset's warm-up window (strategy-defined, e.g., the RSI period) is filled. A data gap longer than the strategy's horizon MUST restart the warm-up window for the affected asset.
- **FR-014**: The engine MUST track per-asset event-time staleness against a configurable threshold; strategies MUST NOT emit new signals for an asset whose input data is stale, and a stalled input MUST raise a health alert.
- **FR-015**: Malformed or schema-invalid input events MUST be acknowledged, logged, and dropped without invoking strategies. Out-of-order events (event timestamp older than the strategy's last-seen timestamp for that asset) MUST NOT rewind strategy state.
- **FR-016**: Sentiment input MUST be dispatched by asset: asset-level scores to strategies configured for that asset, `sector:`-prefixed scores to strategies subscribed to that sector tag, and `asset=None` (general market) scores to strategies that opted in. Strategies that do not declare sentiment interest MUST NOT be invoked for sentiment events.
- **FR-017**: Per-strategy signal emission MUST be rate-limited (configurable, default 10 signals per asset per minute). Excess signals are dropped with a warning and a health alert.
- **FR-018**: The service MUST ship with one complete worked example strategy — RSI crossover: computes RSI over a configurable period (default 14) on candle closes for a single configured timeframe, emits `BUY` when RSI crosses up through the oversold threshold (default 30) and `SELL` when RSI crosses down through the overbought threshold (default 70), with `confidence` scaled by crossing depth and `reasoning` stating the RSI value and threshold crossed. The example MUST be covered by the same tests expected of any strategy and documented as the template for new strategies.
- **FR-019**: The service MUST expose `GET /health` following the platform health convention (3-state top-level `status` = worst component status, `serviceId`, `version`, `checks{}`), with per-strategy checks (state, restart count, warm-up progress, last event processed, last signal emitted, rejected-signal count) and per-input-consumer checks. HTTP status MUST be 503 when `status == "error"` and 200 otherwise; response time MUST be under 200 milliseconds.
- **FR-020**: Health alerts (strategy crash/restart/permanent failure, input stall, signal validation failures, Redis disconnects) MUST be published to `nexus:strategy-health-events` using the shared `HealthAlert` schema. The health publisher MUST NOT buffer on disconnect (platform convention).
- **FR-021**: Configuration MUST follow the platform precedence chain: explicit kwargs → `NEXUS_*` environment variables → `config.toml [strategies]` section → defaults. Per-strategy blocks define the strategy implementation to load, its assets, horizon, and parameters. No credentials of any kind are accepted or required by this service.
- **FR-022**: The signal publisher MUST buffer signals in a bounded queue on Redis disconnect and flush on reconnect, discarding (with a log entry) any buffered signal whose expiry has passed at flush time.
- **FR-023**: All logging MUST use structlog with snake_case event names and keyword arguments, with per-strategy bound loggers (`strategy_name=...`); `configure_logging` is called once at service startup.

### Safety & Risk Constraints *(mandatory)*

- **SRC-001**: `nexus-strategies` MUST NOT execute, submit, modify, or cancel any order, and MUST NOT hold or accept exchange credentials. Its only output is `Signal` events; all trading decisions belong to the aggregator, risk manager, and executor downstream.
- **SRC-002**: Strategies MUST NOT communicate with each other, with the aggregator, or with any downstream service except via the engine's signal emission path. A strategy MUST NOT perform network I/O, database writes, or direct Redis access; all inputs and outputs flow through the engine.
- **SRC-003**: Signal validation MUST fail closed: any signal with out-of-range `confidence`, invalid `direction`, missing `strategy_name`/`asset`, or non-future `expiry` is dropped, never repaired or published. Every published signal MUST carry an `expiry` so no downstream component can act on an unbounded-lifetime signal.
- **SRC-004**: A strategy crash, hang, or restart storm MUST NOT halt the engine or other strategies (fail-contained). Permanent strategy failure degrades the service health state but never stops remaining signal flow.
- **SRC-005**: Strategies MUST NOT emit signals from stale or partially warmed data (FR-013, FR-014). Absence of a signal is always the safe default.
- **SRC-006**: `Signal` payloads and logs MUST NOT contain credentials, personally identifiable information, or raw model artifacts. `reasoning` is bounded human-readable text, not a data dump.

### Service Boundary & Contract Impact *(mandatory)*

- **SBC-001**: `nexus-strategies` sits between `nexus-ingestion`/`nexus-sentiment` (upstream, via `nexus:market-events` and `nexus:sentiment-events`) and `nexus-aggregator` (downstream, via the new `nexus:signal-events` stream). It depends only on Redis and `nexus-common`.
- **SBC-002**: The `Signal` schema and a `SIGNAL` value in the shared `EventType` enum MUST be added to `nexus-common` — backward-compatible additions (new payload type, new enum value). The `nexus:signal-events` stream and its envelope contract become the input contract for the future `nexus-aggregator`.
- **SBC-003**: This service is the first consumer of `nexus:market-events` and `nexus:sentiment-events` via consumer groups; upstream producers require no changes. The `sector:` prefix handling promised in 002-sentiment-pipeline SBC-003 is fulfilled here (FR-016).
- **SBC-004**: Health alerts on `nexus:strategy-health-events` reuse the shared `HealthAlert` schema — no schema changes.
- **SBC-005**: Spec/task impact: `nexus-common` contract additions (`Signal`, `SIGNAL` event type) must be reflected in this feature's `data-model.md` and `contracts/`; the future `nexus-aggregator` and `nexus-backtest` specs must reference `nexus:signal-events` and the replay source/sink seams defined here (FR-012).

### Key Entities

- **Signal**: A strategy's trading opinion. `strategy_name` (unique source identifier used for aggregator weighting), `asset`, `direction` (`BUY`/`SELL`/`HOLD`), `confidence` (`[0.0, 1.0]`), UTC `timestamp`, `reasoning` (human-readable justification), `expiry` (moment after which the signal must not influence decisions). Defined in `nexus-common`.
- **Strategy (interface)**: The pluggable contract every strategy implements. Declares identity, input interests (event types, assets, sector tags, general-market opt-in), one decision horizon, and parameters; consumes typed events and returns zero or more `Signal`s; performs no I/O.
- **Strategy Manager**: Engine component that loads configured strategies, supervises each as an isolated unit with backoff restarts and failure limits, dispatches events by declared interest, validates and rate-limits returned signals, and publishes them.
- **Decision horizon**: The single timeframe a strategy instance operates on (e.g., 1-minute candles). Fixed per instance via configuration; drives warm-up, staleness, and default expiry.
- **RSI crossover strategy**: The shipped worked example — a momentum strategy emitting `BUY`/`SELL` on RSI threshold crossings over a configurable period and thresholds; the reference implementation and documentation template for strategy authors.
- **Health response body**: Platform-standard `GET /health` document with per-strategy and per-consumer checks (state, restarts, warm-up progress, last event/signal timestamps, rejected-signal counts).

## Assumptions

- "Single-horizon" means each strategy *instance* operates on exactly one timeframe; running the same logic on multiple horizons means multiple configured instances with distinct `strategy_name`s (e.g., `rsi_crossover_1m`, `rsi_crossover_1h`).
- Signals need durable at-least-once delivery to the aggregator, so the output is a Redis Stream (`nexus:signal-events`) rather than pub/sub, consistent with the platform's "streams for decisions, pub/sub for lossy market data" rule.
- Strategy weights are an aggregator concern. This service publishes `confidence` per signal; it neither stores nor applies inter-strategy weights.
- Isolation is implemented as per-strategy supervised asyncio tasks within one service process for this feature. The platform design's "one process per strategy" applies at deployment scale; the strategy interface and supervision seams are process-agnostic so the engine can later shard strategies across processes without touching strategy code. The plan phase makes the container/process decision explicitly.
- RSI defaults follow industry convention: 14-period RSI on candle closes, oversold 30, overbought 70, computed with Wilder's smoothing.
- Default signal `expiry` is one decision horizon after `timestamp`, overridable per strategy in configuration.
- Indicator-based strategies consume `CANDLE` events; ingestion already produces candles. Tick-to-candle aggregation inside the strategy engine is out of scope.
- TimescaleDB persistence of signals is not required in this feature; signals reach the audit trail via downstream persistence of trade decisions. If signal-level audit is required later, it follows the established async writer pattern.
- Backtest *orchestration* (historical data replay tooling, simulated executor) belongs to `nexus-backtest`; this feature's obligation is that the engine exposes source/sink seams and strategies are deterministic and clock-free (FR-011, FR-012).
- Sector membership (which assets belong to `sector:crypto`, etc.) is declared per strategy in configuration as subscribed sector tags; the engine does not maintain a global asset-to-sector mapping in this feature.
- Each input stream is consumed by an engine-owned consumer group; the engine fans events out to strategies in-process by declared interest. Per-strategy consumer lag is exposed via health checks.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Within 1 second of a decision-changing market event being available on the input stream, the corresponding `Signal` is available on the output stream (steady state, per strategy).
- **SC-002**: With 10 strategies configured across 20 assets at seconds-level event rates, end-to-end signal latency stays within SC-001 and no strategy starves another.
- **SC-003**: Replaying a recorded event sequence through the same strategy configuration twice produces identical signal sequences (order, direction, confidence, reasoning); verified in automated tests, including an accelerated-speed replay.
- **SC-004**: A forced crash of one strategy leaves every other strategy's signal output uninterrupted, produces a health alert within 5 seconds, and the crashed strategy resumes within its backoff schedule.
- **SC-005**: 100% of published signals satisfy the `Signal` contract (bounds, expiry, required fields); property-based tests over strategy outputs find zero published violations, and injected invalid signals are rejected and counted.
- **SC-006**: A strategy author can implement, register, and see signals from a new minimal strategy following only the worked example and its documentation, without modifying engine code — validated by the test-strategy fixture used in CI.
- **SC-007**: `GET /health` responds within 200 milliseconds and accurately distinguishes: healthy, strategy in warm-up, strategy restarting, strategy permanently failed, and stalled input.
- **SC-008**: No credentials or PII appear in any signal payload, log line, or health output.
- **SC-009**: The RSI crossover example, fed a canonical fixture of candles with known RSI values, emits exactly the expected `BUY`/`SELL` sequence — no signals during warm-up, none from stale data.
