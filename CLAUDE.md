# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What Nexus Is

A fully automated multi-asset trading platform with seconds-level execution. Multiple pluggable weighted strategies (technical, ML, sentiment, arbitrage, statistical, rule-based) independently produce signals that feed a signal aggregation layer which emits trade decisions. Supports paper trading and backtesting through the **same code paths** as live trading — the only difference is the executor implementation.

## Planned Monorepo Structure

```
packages/
  nexus-common/       ← shared types: MarketEvent, Signal, TradeIntent, serialization  [EXISTS]
  nexus-ingestion/    ← data source adapters service                                    [EXISTS]
  nexus-exchange/     ← ExchangeConnector protocol + ccxt/ib_insync implementations
  nexus-strategies/   ← Strategy interface, built-in strategies, Strategy Manager
  nexus-aggregator/   ← signal aggregation service (consumes Signal, emits TradeIntent)
  nexus-risk/         ← Risk Manager, 5-layer safety, state machine
  nexus-executor/     ← Execution engine, order lifecycle, position tracker
  nexus-api/          ← FastAPI backend for dashboard
  nexus-sentiment/    ← NLP sentiment pipeline: consumes NewsArticle, produces SentimentScore
  nexus-backtest/     ← backtesting engine + simulated executor
dashboard/            ← React + TypeScript frontend
```

## Platform Data Flow

```
Exchanges/APIs → nexus-ingestion → Redis Streams → nexus-strategies (per strategy process)
                                        │                                     ↓ Signal
                                        └─→ nexus-sentiment (NewsArticle → SentimentScore → Redis Streams → nexus-strategies)
                                               nexus-aggregator (signal aggregation)
                                                          ↓ TradeIntent
                                                  nexus-risk (5-layer validation)
                                                          ↓
                                               nexus-executor (ccxt) → Exchanges
                                                          ↓
                                         Redis (hot state) + TimescaleDB (history)
                                                          ↓
                                         nexus-api → WebSocket → dashboard
```

## Core Domain Types (cross all service boundaries)

All defined in `nexus-common` and shared across packages:

- **`MarketEvent`** — normalized envelope for all ingested data: `source`, `asset` (`None` for non-asset events, never `""`), `timestamp` (UTC), `event_type`, `schema_version` (semver), `payload` (dict validated via `validated_payload()`)
- **`Signal`** — strategy output: `strategy_name`, `asset`, `direction` (BUY/SELL/HOLD), `confidence` (0.0–1.0), `timestamp`, `reasoning`, `expiry`
- **`TradeIntent`** — aggregator output: `asset`, `direction`, `composite_score`, `contributing_signals[]`, `timestamp`

## Commands

**Install (from repo root):**
```bash
pip install -e packages/nexus-common[dev] -e packages/nexus-ingestion[dev]
```

**Set up pre-commit hooks (once after cloning):**
```bash
pip install pre-commit
pre-commit install
```

**Run pre-commit manually against all files:**
```bash
pre-commit run --all-files
```

**Run all tests:**
```bash
pytest
```

**Run a single test file:**
```bash
pytest packages/nexus-ingestion/tests/unit/test_exchange_adapter.py
```

**Run a single test by name:**
```bash
pytest -k "test_valid_ticker_produces_tick_event"
```

**Run only unit tests (no Docker required):**
```bash
pytest packages/nexus-ingestion/tests/unit packages/nexus-common/tests
```

**Run integration tests (requires Docker):**
```bash
docker compose -f docker-compose.dev.yml up -d
pytest packages/nexus-ingestion/tests/integration
```

**Update contract snapshots (syrupy):**
```bash
pytest packages/nexus-ingestion/tests/contract --snapshot-update
```

**Run the ingestion service:**
```bash
cp config.example.toml config.toml  # edit as needed
NEXUS_EXCHANGE_API_KEY=... NEXUS_EXCHANGE_API_SECRET=... python -m nexus_ingestion.main
```

## Spec Maintenance (speckit)

This project uses **speckit** — specs live in `specs/<feature-id>/` and are the source of truth for requirements, data models, contracts, and tasks. Each feature has: `spec.md`, `plan.md`, `data-model.md`, `tasks.md`, `quickstart.md`, `contracts/`, and `checklists/`.

After any implementation change, update the relevant spec files to stay in sync with the code:

- **Behaviour changed** (e.g. edge case works differently than specified) → update the edge case in `spec.md`
- **Data model changed** (field added, type changed, new entity) → update `data-model.md` and the relevant `contracts/` file
- **New task completed** → add it as `[X]` in `tasks.md`
- **Feature fully implemented** → set `**Status**: Implemented` in `spec.md`

Specs describe what the code *does*, not what was originally intended. If code and spec diverge, update the spec to match the code.

## Adapter Pattern (nexus-ingestion)

All adapters subclass `BaseAdapter` and implement `connect() / subscribe() / run() / stop()`. Adapters communicate **only** via two injected callbacks stored on `BaseAdapter`:
- `_event_callback(MarketEvent)` — normalized data events; call via `await self._emit_event(event)` (defined on `BaseAdapter`, handles `record_event()` and coroutine-vs-sync dispatch)
- `_health_callback(HealthAlert)` — state change and error alerts

Adapters never import publishers or writers directly. This same callback-injection pattern is the model for inter-component wiring across the platform.

## Service Isolation (FR-004)

`IngestionService` runs each adapter as an independent `asyncio.create_task` with `add_done_callback`. A crashing adapter restarts with exponential backoff. **No `asyncio.TaskGroup`** — one failure must never cancel siblings. Within `ExchangeAdapter.run()`, watch streams are supervised with `asyncio.wait(..., return_when=ALL_COMPLETED)` — never `asyncio.gather` — so a crashing stream task does not cancel its siblings. Per-stream reconnect counters (`_stream_reconnect_attempts`) prevent shared-counter inflation when multiple streams fail simultaneously. This same isolation principle applies to strategy processes in `nexus-strategies` (one process per strategy) and exchange connectors in `nexus-executor`.

## Redis Usage Pattern

- **Redis Pub/Sub** — real-time market data notifications (ticks, order books) where minimal latency matters and message loss is acceptable
- **Redis Streams with consumer groups** — durable delivery for trade intents and risk decisions (at-least-once, replayable)
- **Redis as hot state cache** — current prices, order books, positions, active signals

`RedisPublisher` buffers events in a bounded deque on disconnect and flushes via pipeline on reconnect. `HealthPublisher` does **not** buffer — alerts are dropped when Redis is unavailable to avoid circular dependencies.

## Logging

All services use **structlog** (in `nexus-common`). Never use `logging.getLogger(__name__)`.

- Module-level: `logger = structlog.get_logger()`
- Per-object (preferred for adapters/publishers): `self._logger = structlog.get_logger().bind(adapter_id=self.adapter_id)`
- Log calls use snake_case event names + keyword args — never printf-style format strings:
  ```python
  self._logger.info("exchange_adapter_connected", sandbox=self._sandbox, assets=self._assets)
  self._logger.error("batch_write_failed", max_retries=max_retries, error=str(e), exc_info=True)
  ```
- Call `configure_logging(env=config.log_env)` once at service startup (inside `run()`, after `IngestionConfig()` is loaded), never in library code. Controlled via `NEXUS_LOG_ENV` env var or `log_env` in `config.toml [monitoring]`. `"development"` → human-readable console; `"production"` (default) → JSON.
- The stdlib integration is wired automatically — foreign loggers (uvicorn, asyncpg, ccxt) also emit structured output.

## Config Precedence

`IngestionConfig` (pydantic-settings) loads in this order (highest wins):
1. Explicit `__init__` kwargs
2. `NEXUS_*` environment variables
3. `config.toml` (path overridden by `NEXUS_CONFIG_FILE`)
4. Defaults

Exchange credentials must be set via env vars only (`NEXUS_EXCHANGE_API_KEY`, `NEXUS_EXCHANGE_API_SECRET`), never in TOML.

## Test Layout

| Directory | Type | Infra needed |
|-----------|------|-------------|
| `tests/unit/` | Pure unit, all mocked | None |
| `tests/integration/` | Real containers via testcontainers | Docker |
| `tests/contract/` | Syrupy snapshot tests for serialization stability | None |

Integration tests auto-skip if `testcontainers` is not importable. Root `pytest` config: `asyncio_mode = "auto"`, `--import-mode=importlib`.

## Backtesting Principle

The backtesting engine runs identical strategy, aggregation, and risk code. Only the executor is swapped for a simulated implementation modeling slippage, latency, partial fills, fees, and market impact. No separate backtesting code paths that can diverge from production — paper trading mode is the same single flag.

## Storage

- **TimescaleDB** — all market events, trade history, audit trail, backtest queries. Schema at `docker/timescaledb/init.sql` (mounted into TimescaleDB via Docker Compose on first startup). Writes use `asyncpg.copy_records_to_table` (no ORM in the hot path).
- **Redis** — hot state (prices, positions, active signals)
- **ClickHouse** — planned for large-scale analytics and reporting
