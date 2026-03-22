# Implementation Plan: Data Ingestion Layer

**Branch**: `001-data-ingestion` | **Date**: 2026-03-22 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-data-ingestion/spec.md`

## Summary

Build the foundational data ingestion service (`nexus-ingestion`) that connects to cryptocurrency exchanges via `ccxt.pro` WebSocket streams, normalizes all market data into a shared `MarketEvent` schema, publishes events to Redis Streams in real-time, and persists them asynchronously to TimescaleDB. The service also ingests news articles from RSS/NewsAPI sources. Health alerts are published to a dedicated Redis Stream for downstream consumption by the Risk Manager.

## Technical Context

**Language/Version**: Python 3.11+ with `asyncio` and `uvloop`
**Primary Dependencies**: `ccxt.pro` (exchange WebSocket), `redis.asyncio` (redis-py async), `asyncpg` (TimescaleDB async), `pydantic` (schema validation), `aiohttp` (news HTTP fetching), `feedparser` (RSS parsing), `tomli` (config parsing), `FastAPI` + `uvicorn` (health endpoint)
**Storage**: Redis Streams (hot event delivery: `nexus:market-events`, `nexus:news-events`, `nexus:ingestion-health-events`), TimescaleDB (historical persistence via async background writer)
**Testing**: `pytest`, `pytest-asyncio`, `hypothesis` (property-based for schema validation), `testcontainers-python` (Redis + TimescaleDB integration tests), `syrupy` (snapshot tests for event serialization)
**Target Platform**: Docker Compose (dev profile), Linux server (production)
**Project Type**: Event-driven microservice (long-running asyncio process with health HTTP endpoint)
**Performance Goals**: First event within 5s of startup; sub-1s exchange-to-Redis latency; health endpoint <200ms; TimescaleDB persistence within 5s of Redis publish
**Constraints**: Read-only exchange access (SRC-001); no credentials in logs/events (SRC-003); adapter isolation — one failing adapter must not affect others (FR-004); async TimescaleDB persistence decoupled from Redis publishing (FR-008, Clarification 1)
**Scale/Scope**: Initial: 1 exchange (Binance), 2-5 assets (BTC/USDT, ETH/USDT, etc.), 1 news source (RSS or NewsAPI). Architecture supports N exchanges and M news sources via adapter pattern.

## Constitution Check (Pre-Phase 0)

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- [x] **Safety-first execution gate**: Ingestion is read-only (SRC-001). No order execution paths exist. Data gap detection triggers health alerts to `nexus:ingestion-health-events` stream (SRC-002) — Risk Manager is responsible for halting. Kill-switch is not applicable to ingestion (no write operations).
- [x] **Event-driven Python gate**: All adapters run as independent `asyncio` tasks (FR-004). Service main loop is `asyncio.run()` with `uvloop`. No compute-heavy paths in ingestion that require NumPy (that's aggregator/strategy domain).
- [x] **Redis-first messaging gate**: Three named streams defined: `nexus:market-events` (Tick/OrderBook/Trade/Candle), `nexus:news-events` (NewsArticle), `nexus:ingestion-health-events` (alerts). Schema version field included in `MarketEvent` envelope. Consumer groups documented in SBC-001.
- [x] **Library-first integration gate**: `ccxt.pro` for all exchange WebSocket connections (FR-001, assumption in spec). No custom connectors. `feedparser` + `aiohttp` for RSS/HTTP news sources (FR-006). No exceptions needed.
- [x] **Spec-code traceability gate**: All 11 FRs + 3 SRCs map to planned implementation. Contract changes documented in SBC-001/002/003. Tasks will trace to specific FRs.
- [x] **Safety-critical testing gate**: Property-based tests for MarketEvent schema validation (Hypothesis). Integration tests with testcontainers for Redis Stream delivery and TimescaleDB persistence. Contract snapshot tests for event serialization format (syrupy). Credential leak tests for SRC-003.
- [x] **Service-boundary gate**: Ingestion is sole producer; no consumption from other Nexus services. Clear boundary: publishes events, exposes health endpoint, persists to DB. Does not invoke Risk Manager, Strategy Engine, or any other service directly.

**Result: ALL GATES PASS ✓** — Proceeding to Phase 0.

## Project Structure

### Documentation (this feature)

```text
specs/001-data-ingestion/
├── plan.md              # This file
├── research.md          # Phase 0: technology decisions and rationale
├── data-model.md        # Phase 1: entity definitions and relationships
├── quickstart.md        # Phase 1: developer getting-started guide
├── contracts/           # Phase 1: Redis Stream message contracts
│   ├── market-events.md
│   ├── news-events.md
│   └── health-events.md
└── tasks.md             # Phase 2 output (/speckit.tasks — NOT created here)
```

### Source Code (repository root)

```text
packages/
├── nexus-common/                    # Shared schemas, config, Redis/DB utilities
│   ├── nexus_common/
│   │   ├── __init__.py
│   │   ├── schemas/
│   │   │   ├── __init__.py
│   │   │   ├── market_event.py      # MarketEvent envelope + payload types
│   │   │   ├── health_alert.py      # HealthAlert schema
│   │   │   └── enums.py             # EventType, AdapterStatus enums
│   │   ├── config.py                # Config loading (env + TOML)
│   │   └── redis_client.py          # Shared async Redis connection factory
│   ├── tests/
│   │   └── unit/
│   │       └── test_schemas.py
│   └── pyproject.toml
│
└── nexus-ingestion/                 # This feature's service
    ├── nexus_ingestion/
    │   ├── __init__.py
    │   ├── main.py                  # Entry point: asyncio event loop setup
    │   ├── service.py               # IngestionService orchestrator
    │   ├── adapters/
    │   │   ├── __init__.py
    │   │   ├── base.py              # BaseAdapter ABC (connect, subscribe, reconnect)
    │   │   ├── exchange_adapter.py  # ccxt.pro WebSocket adapter
    │   │   └── news_adapter.py      # RSS/NewsAPI polling adapter
    │   ├── publishers/
    │   │   ├── __init__.py
    │   │   ├── redis_publisher.py   # Async Redis Stream publisher with buffering
    │   │   └── health_publisher.py  # Health alert publisher (nexus:ingestion-health-events)
    │   ├── persistence/
    │   │   ├── __init__.py
    │   │   └── timescale_writer.py  # Async background queue + batch writer
    │   ├── monitoring/
    │   │   ├── __init__.py
    │   │   ├── gap_detector.py      # Data gap detection (SRC-002 circuit breaker)
    │   │   └── health_endpoint.py   # FastAPI health endpoint (FR-005)
    │   └── config.py                # Service-specific config (adapters, subscriptions)
    ├── tests/
    │   ├── unit/
    │   │   ├── test_exchange_adapter.py
    │   │   ├── test_news_adapter.py
    │   │   ├── test_redis_publisher.py
    │   │   ├── test_timescale_writer.py
    │   │   └── test_gap_detector.py
    │   ├── integration/
    │   │   ├── test_redis_streams.py
    │   │   ├── test_timescale_persistence.py
    │   │   └── test_health_endpoint.py
    │   └── contract/
    │       └── test_event_schemas.py  # Snapshot tests for serialization format
    ├── pyproject.toml
    └── Dockerfile

docker-compose.dev.yml           # Dev infrastructure: Redis + TimescaleDB
config.example.toml              # Example config with placeholder values
```

**Structure Decision**: Monorepo `packages/` layout per design document. Two packages: `nexus-common` (shared schemas/utilities used by all future services) and `nexus-ingestion` (this service). This establishes the pattern for all subsequent services (strategy, aggregator, risk, executor, backtester, API).

## Complexity Tracking

> No constitution violations. All gates pass without exceptions.

## Constitution Check (Post-Design Re-evaluation)

*Re-check after Phase 1 design completion.*

- [x] **Safety-first execution gate**: CONFIRMED. No execution paths exist anywhere in design. `ExchangeAdapter` uses only `watch_*()` read methods from ccxt.pro. No `create_order()`, `cancel_order()`, or any exchange write calls. `HealthAlert` stream contract explicitly forbids ingestion from making trading decisions.
- [x] **Event-driven Python gate**: CONFIRMED. All adapters are `asyncio.create_task()` tasks. `uvloop` specified in Technical Context. No blocking I/O — feedparser offloaded via `asyncio.to_thread()`. No NumPy needed (no compute-heavy paths in ingestion).
- [x] **Redis-first messaging gate**: CONFIRMED. Three streams defined with full contracts: `nexus:market-events` ([market-events.md](contracts/market-events.md)), `nexus:news-events` ([news-events.md](contracts/news-events.md)), `nexus:ingestion-health-events` ([health-events.md](contracts/health-events.md)). Schema versioning via `schema_version` field. Consumer group conventions documented. MAXLEN with approximate trimming specified.
- [x] **Library-first integration gate**: CONFIRMED. ccxt.pro for all exchange WebSocket. feedparser for RSS. aiohttp for HTTP. No custom connectors. No exceptions needed.
- [x] **Spec-code traceability gate**: CONFIRMED. All 11 FRs map to concrete modules in project structure. 3 SRCs mapped to implementation constraints. Data model entities map 1:1 to spec Key Entities section. Contract files reference specific SBC requirements.
- [x] **Safety-critical testing gate**: CONFIRMED. Test directory structure includes `unit/`, `integration/`, and `contract/` directories. Property-based tests specified for schema validation (Hypothesis). Integration tests with testcontainers for Redis/TimescaleDB. Snapshot tests for serialization (syrupy). Specific test files listed for each critical component.
- [x] **Service-boundary gate**: CONFIRMED. Ingestion publishes only; consumes from no Nexus service. Health alerts are one-way (publish to stream, Risk Manager consumes independently). Shared schemas in `packages/nexus-common` — no domain logic leaks. Sentiment pipeline explicitly excluded (separate `nexus-sentiment` service per clarification).

**Result: ALL GATES PASS ✓** — Post-design validation complete. No violations.
