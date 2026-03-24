# Tasks: Data Ingestion Layer

**Input**: Design documents from `/specs/001-data-ingestion/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Tests are REQUIRED for this feature — the specification explicitly includes property-based tests (Hypothesis), integration tests (testcontainers), contract snapshot tests (syrupy), and safety-critical assertions (SRC-001, SRC-002, SRC-003).

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Monorepo**: `packages/nexus-common/` (shared schemas/utilities), `packages/nexus-ingestion/` (this service)
- **Infrastructure**: `docker-compose.dev.yml`, `config.example.toml` at repository root

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Establish monorepo package structure, dependency manifests, and dev infrastructure

- [X] T001 Create monorepo packages/ directory structure with all subdirectories and __init__.py files per plan.md project structure
- [X] T002 Initialize nexus-common package with pyproject.toml (pydantic>=2.0, pydantic-settings, redis[hiredis]>=5.0 dependencies) in packages/nexus-common/pyproject.toml
- [X] T003 [P] Initialize nexus-ingestion package with pyproject.toml (ccxt>=4.0, asyncpg, aiohttp, feedparser, fastapi, uvicorn, uvloop + dev: pytest, pytest-asyncio, hypothesis, testcontainers, syrupy) in packages/nexus-ingestion/pyproject.toml
- [X] T004 [P] Create Docker Compose dev profile with Redis 7 (port 6379) and TimescaleDB (port 5432) services in docker-compose.dev.yml
- [X] T005 [P] Create example configuration with exchange, redis, timescaledb, news, and monitoring sections in config.example.toml

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core schemas, config, adapters, publishers, and orchestrator that ALL user stories depend on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T006 Implement EventType, AdapterStatus, Severity enums per data-model.md in packages/nexus-common/nexus_common/schemas/enums.py
- [X] T007 [P] Implement MarketEvent envelope + Tick, OrderBookUpdate, Trade, Candle, NewsArticle Pydantic models with validation rules (price positivity, orderbook ordering, schema_version semver) in packages/nexus-common/nexus_common/schemas/market_event.py
- [X] T008 [P] Implement HealthAlert + AdapterHealth Pydantic models per data-model.md in packages/nexus-common/nexus_common/schemas/health_alert.py
- [X] T009 [P] Implement shared config base with pydantic-settings (BaseSettings, TOML source, env prefix NEXUS_, SecretStr for credentials per SRC-003) in packages/nexus-common/nexus_common/config.py
- [X] T010 [P] Implement async Redis connection factory with retry_on_timeout and ExponentialBackoff(retries=3) in packages/nexus-common/nexus_common/redis_client.py
- [X] T011 Implement ingestion-specific config (exchange list, subscribed assets, polling intervals, persistence batch settings, gap detector thresholds) in packages/nexus-ingestion/nexus_ingestion/config.py
- [X] T012 Implement BaseAdapter ABC with async connect, subscribe, run loop, stop, AdapterStatus state tracking, and AdapterHealth property in packages/nexus-ingestion/nexus_ingestion/adapters/base.py
- [X] T013 [P] Implement async Redis Stream publisher with per-event XADD, in-memory buffer on disconnect (configurable max size), pipeline flush on reconnect, MAXLEN ~100000 approximate trimming in packages/nexus-ingestion/nexus_ingestion/publishers/redis_publisher.py
- [X] T014 [P] Implement health alert publisher for nexus:ingestion-health-events stream with MAXLEN ~5000 approximate trimming in packages/nexus-ingestion/nexus_ingestion/publishers/health_publisher.py
- [X] T015 Implement IngestionService orchestrator with manual asyncio.create_task per adapter, add_done_callback for failure detection, call_later for restart backoff (NO TaskGroup per research.md decision) in packages/nexus-ingestion/nexus_ingestion/service.py
- [X] T016 Implement asyncio entry point with uvloop event loop policy, SIGTERM/SIGINT signal handlers, graceful shutdown sequence in packages/nexus-ingestion/nexus_ingestion/main.py

**Checkpoint**: Foundation ready — user story implementation can now begin in parallel

---

## Phase 3: User Story 1 — Exchange Market Data Flowing into the System (Priority: P1) 🎯 MVP

**Goal**: Live price ticks, order book updates, trade events, and candles from Binance flow continuously into Redis and persist to TimescaleDB. Health endpoint reports adapter status. Gap detector monitors for missing data.

**Independent Test**: Start ingestion service pointing at Binance sandbox, observe MarketEvent records (Tick, OrderBookUpdate, Candle) in nexus:market-events Redis Stream within 5s of startup, continuing at least every 10s per subscribed asset. Verify TimescaleDB records appear within 10s of Redis publish. Verify GET /health returns adapter status within 200ms.

### Safety-Critical & Contract Tests for US1 ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [X] T017 [P] [US1] Property-based schema tests (Hypothesis): validate MarketEvent + all payload types respect constraints (price > 0, orderbook ordering, semver schema_version), verify credential exclusion in serialized output (SRC-003) in packages/nexus-common/tests/unit/test_schemas.py
- [X] T018 [P] [US1] Contract snapshot tests (syrupy): verify Redis serialization format for Tick, OrderBookUpdate, Trade, Candle event types matches contracts/market-events.md in packages/nexus-ingestion/tests/contract/test_event_schemas.py

### Implementation for US1

- [X] T019 [US1] Implement ExchangeAdapter with ccxt.pro WebSocket (watch_ticker → Tick, watch_order_book → OrderBookUpdate, watch_trades → Trade, watch_ohlcv → Candle normalization, sandbox mode toggle, malformed event drop with counter per FR-009, timestamp tolerance check — reject events outside configurable window per data-model.md validation rule #1) in packages/nexus-ingestion/nexus_ingestion/adapters/exchange_adapter.py
- [X] T020 [P] [US1] Create TimescaleDB schema SQL (market_events hypertable with time column, health_alerts hypertable, composite index on asset+event_type+time per data-model.md) in packages/nexus-ingestion/nexus_ingestion/persistence/schema.sql
- [X] T021 [US1] Implement async TimescaleDB batch writer with asyncpg (copy_records_to_table via COPY protocol, asyncio.Queue maxsize=50000, flush on 500 records or 5s interval, put_nowait with QueueFull drop-and-warn, exponential backoff on connection failure, emit PERSISTENCE_ERROR MEDIUM-severity health alert via health publisher on write failure per contracts/health-events.md) in packages/nexus-ingestion/nexus_ingestion/persistence/timescale_writer.py
- [X] T022 [P] [US1] Implement data gap detector with per-asset last-event timers, configurable threshold window, DATA_GAP HIGH-severity health alert emission per SRC-002, MALFORMED_SPIKE LOW-severity alert when per-adapter malformed event rate exceeds configurable threshold per contracts/health-events.md in packages/nexus-ingestion/nexus_ingestion/monitoring/gap_detector.py
- [X] T023 [P] [US1] Implement FastAPI health endpoint (GET /health returning overall status + per-adapter AdapterHealth array, <200ms response target, uvicorn.Server.serve() as asyncio task, shared state via app.state) in packages/nexus-ingestion/nexus_ingestion/monitoring/health_endpoint.py
- [X] T024 [US1] Wire IngestionService for US1: register ExchangeAdapter, connect RedisPublisher + TimescaleWriter + GapDetector + HealthEndpoint, implement event routing (adapter → publisher + writer + gap detector) in packages/nexus-ingestion/nexus_ingestion/service.py

### Verification Tests for US1

- [X] T025 [P] [US1] Unit test for ExchangeAdapter: mock ccxt.pro exchange, verify Tick/OrderBookUpdate/Trade/Candle normalization, assert read-only access (SRC-001: no create_order/cancel_order calls), verify malformed payload handling in packages/nexus-ingestion/tests/unit/test_exchange_adapter.py
- [X] T026 [P] [US1] Unit test for TimescaleDB writer: queue behavior, batch flush on size and timer, error retry with backoff, QueueFull drop behavior in packages/nexus-ingestion/tests/unit/test_timescale_writer.py
- [X] T027 [P] [US1] Unit test for gap detector: threshold triggering, DATA_GAP alert emission, timer reset on event receipt, multi-asset independence in packages/nexus-ingestion/tests/unit/test_gap_detector.py
- [X] T028 [US1] Integration test for Redis Stream: testcontainers Redis, ExchangeAdapter publishes MarketEvent, XREAD verifies correct fields and payload structure per contracts/market-events.md in packages/nexus-ingestion/tests/integration/test_redis_streams.py
- [X] T029 [P] [US1] Integration test for TimescaleDB persistence: testcontainers PostgreSQL/TimescaleDB, batch write via writer, verify record count and field values in market_events hypertable in packages/nexus-ingestion/tests/integration/test_timescale_persistence.py
- [X] T030 [P] [US1] Integration test for health endpoint: start FastAPI in-process, verify GET /health returns per-adapter status with correct fields, verify <200ms response time in packages/nexus-ingestion/tests/integration/test_health_endpoint.py
- [X] T030a [US1] Integration test for adapter failure isolation (SC-004): start two mock adapters, crash one (raise unhandled exception), assert the surviving adapter continues publishing events to Redis uninterrupted and its health status remains CONNECTED in packages/nexus-ingestion/tests/integration/test_redis_streams.py

**Checkpoint**: User Story 1 (MVP) is fully functional — exchange data flows through Redis, persists to TimescaleDB, /health reports adapter status, gap detector monitors data freshness

---

## Phase 4: User Story 2 — Automatic Reconnection After Exchange Disconnection (Priority: P2)

**Goal**: Ingestion service automatically recovers from WebSocket disconnections without manual intervention, emitting appropriate health alerts during state transitions.

**Independent Test**: With service running and receiving data, simulate network disconnection. Observe service reconnects and resumes publishing events within 30s without manual action. Verify ADAPTER_RECONNECTING → ADAPTER_RECOVERED health alerts appear in nexus:ingestion-health-events stream.

### Safety-Critical Tests for US2 ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [X] T031 [P] [US2] Unit tests for reconnection: adapter state transitions (CONNECTED→RECONNECTING→DOWN→CONNECTED), exponential backoff timing, health alert emission (ADAPTER_RECONNECTING, ADAPTER_DOWN, ADAPTER_RECOVERED) on each transition in packages/nexus-ingestion/tests/unit/test_exchange_adapter.py

### Implementation for US2

- [X] T032 [US2] Add reconnection state tracking and error handling to ExchangeAdapter watch loop: catch NetworkError/ExchangeNotAvailable, update AdapterStatus, emit ADAPTER_RECONNECTING/ADAPTER_DOWN/ADAPTER_RECOVERED health alerts, continue retry loop after max attempts (never crash) in packages/nexus-ingestion/nexus_ingestion/adapters/exchange_adapter.py
- [X] T033 [US2] Enhance IngestionService supervisor with configurable max restart attempts and exponential restart backoff for crashed adapter tasks in packages/nexus-ingestion/nexus_ingestion/service.py

### Verification Tests for US2

- [X] T034 [US2] Integration test for reconnection recovery: simulate exchange disconnect (mock), verify adapter reconnects and resumes publishing to Redis within timeout, verify health alert sequence in nexus:ingestion-health-events stream in packages/nexus-ingestion/tests/integration/test_redis_streams.py

**Checkpoint**: User Stories 1 AND 2 both work independently — exchange data flows with automatic recovery from disconnections

---

## Phase 5: User Story 3 — News Articles Entering the Pipeline (Priority: P3)

**Goal**: News articles from external sources (RSS/NewsAPI) flow into the nexus:news-events Redis Stream for consumption by the separate nexus-sentiment service.

**Independent Test**: Configure at least one news source (RSS feed or NewsAPI), start ingestion service, observe NewsArticle events in nexus:news-events Redis Stream within the configured polling interval.

### Contract Tests for US3 ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [X] T035 [P] [US3] Contract snapshot tests (syrupy): verify NewsArticle event Redis serialization format matches contracts/news-events.md in packages/nexus-ingestion/tests/contract/test_event_schemas.py

### Implementation for US3

- [X] T036 [US3] Implement NewsAdapter with aiohttp.ClientSession for async HTTP fetch, feedparser.parse via asyncio.to_thread for RSS XML parsing, configurable polling interval with asyncio.sleep, article dedup by URL, NEWS_SOURCE_DOWN health alert on fetch failure in packages/nexus-ingestion/nexus_ingestion/adapters/news_adapter.py
- [X] T037 [US3] Wire NewsAdapter into IngestionService: register as supervised task alongside exchange adapters, configure nexus:news-events stream with MAXLEN ~10000, route NewsArticle events to RedisPublisher + TimescaleWriter in packages/nexus-ingestion/nexus_ingestion/service.py

### Verification Tests for US3

- [X] T038 [P] [US3] Unit test for NewsAdapter: mock aiohttp response + feedparser output, verify NewsArticle normalization (headline, body_summary, url, source_name, published_at, related_assets), test HTTP failure handling and retry, verify NEWS_SOURCE_DOWN alert emission in packages/nexus-ingestion/tests/unit/test_news_adapter.py

**Checkpoint**: All three user stories functional — exchange data, reconnection resilience, and news articles all flowing through the system

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Infrastructure completeness, security verification, and end-to-end validation

- [X] T039 [P] Unit test for Redis publisher: buffer behavior on disconnect, pipeline flush on reconnect, MAXLEN configuration, verify event ordering preserved in packages/nexus-ingestion/tests/unit/test_redis_publisher.py
- [X] T040 [P] Create multi-stage Dockerfile for nexus-ingestion (Python 3.11-slim base, non-root user, health check CMD) in packages/nexus-ingestion/Dockerfile
- [X] T041 Security audit: verify SRC-001 (grep for create_order/cancel_order — must not exist in any adapter), SRC-003 (SecretStr masking in config repr, no credentials in serialized events or log output) across all modules
- [X] T042 Run quickstart.md end-to-end validation: docker compose up Redis + TimescaleDB, start ingestion service, verify MarketEvents in Redis within 5s (SC-001), check GET /health response <200ms (SC-005), verify TimescaleDB persistence within 10s (SC-003)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion — **BLOCKS all user stories**
- **User Stories (Phase 3–5)**: All depend on Foundational phase completion
  - User stories can proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Phase 6)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Phase 2 — no dependencies on other stories. Delivers the **MVP**.
- **User Story 2 (P2)**: Can start after Phase 2 — enhances ExchangeAdapter from US1 with reconnection resilience. Independently testable.
- **User Story 3 (P3)**: Can start after Phase 2 — fully independent of US1/US2 (different adapter, different stream). Independently testable.

### Within Each User Story

- Safety-critical and contract tests MUST be written and FAIL before implementation
- Models → services → endpoints (within a story)
- Core implementation → wiring → verification tests
- Story complete before moving to next priority

### Parallel Opportunities

**Phase 1**: T003, T004, T005 can run in parallel (after T001, T002 establish structure)

**Phase 2**:
- Tier 1 (after T006): T007 + T008 in parallel (different schema files)
- Tier 1 (independent): T009 + T010 in parallel (config and redis, no schema dependency)
- Tier 2: T013 + T014 in parallel (different publisher files, both depend on T010)

**Phase 3 (US1)**:
- T017 + T018 in parallel (schema tests + contract tests, different files)
- T020 + T022 + T023 in parallel (schema SQL, gap detector, health endpoint — different files)
- T025 + T026 + T027 in parallel (unit tests — different files)

**Phase 5 (US3)**: T035 + T038 in parallel (contract test + unit test)

**Phase 6**: T039 + T040 in parallel (publisher test + Dockerfile)

---

## Parallel Example: User Story 1

```text
# Tier 1 — Safety-critical tests (write first, must fail):
  T017: Property-based schema tests (Hypothesis)       ← packages/nexus-common/tests/unit/test_schemas.py
  T018: Contract snapshot tests (syrupy)                ← packages/nexus-ingestion/tests/contract/test_event_schemas.py

# Tier 2 — Core adapter (sequential):
  T019: ExchangeAdapter (ccxt.pro WebSocket)            ← packages/nexus-ingestion/nexus_ingestion/adapters/exchange_adapter.py

# Tier 3 — Supporting components (parallel, different files):
  T020: TimescaleDB schema SQL                          ← packages/nexus-ingestion/nexus_ingestion/persistence/schema.sql
  T022: Gap detector                                    ← packages/nexus-ingestion/nexus_ingestion/monitoring/gap_detector.py
  T023: Health endpoint                                 ← packages/nexus-ingestion/nexus_ingestion/monitoring/health_endpoint.py

# Tier 4 — Persistence (depends on T020):
  T021: TimescaleDB batch writer                        ← packages/nexus-ingestion/nexus_ingestion/persistence/timescale_writer.py

# Tier 5 — Wiring (depends on T019-T023):
  T024: Wire IngestionService for US1                   ← packages/nexus-ingestion/nexus_ingestion/service.py

# Tier 6 — Verification tests (parallel, different files):
  T025: Unit test ExchangeAdapter                       ← packages/nexus-ingestion/tests/unit/test_exchange_adapter.py
  T026: Unit test TimescaleDB writer                    ← packages/nexus-ingestion/tests/unit/test_timescale_writer.py
  T027: Unit test gap detector                          ← packages/nexus-ingestion/tests/unit/test_gap_detector.py

# Tier 7 — Integration tests (sequential, share infra):
  T028: Integration test Redis Stream                   ← packages/nexus-ingestion/tests/integration/test_redis_streams.py
  T029: Integration test TimescaleDB                    ← packages/nexus-ingestion/tests/integration/test_timescale_persistence.py
  T030: Integration test health endpoint                ← packages/nexus-ingestion/tests/integration/test_health_endpoint.py
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: Test US1 independently — MarketEvents in Redis, TimescaleDB persistence, /health endpoint, gap detection
5. Deploy/demo if ready — this is a fully working ingestion service for exchange data

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Deploy/Demo (**MVP!**)
3. Add User Story 2 → Test independently → Deploy/Demo (resilient reconnection)
4. Add User Story 3 → Test independently → Deploy/Demo (news pipeline active)
5. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (core exchange data pipeline)
   - Developer B: User Story 3 (news adapter — fully independent)
3. After US1 complete:
   - Developer A: User Story 2 (reconnection — builds on US1's ExchangeAdapter)
4. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies on incomplete tasks
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable
- FR/SRC references in task descriptions trace to spec.md requirements
- Safety-critical tests (SRC-001, SRC-002, SRC-003) are explicitly called out in task descriptions
- TaskGroup is explicitly prohibited (research.md decision) — use manual task supervision
- ccxt.pro handles WebSocket reconnection internally; US2 adds state tracking and health alerts on top
- TimescaleDB persistence is async and decoupled from Redis publishing (FR-008, Clarification 1)
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
