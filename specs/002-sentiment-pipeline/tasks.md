# Tasks: Sentiment Analysis Pipeline

**Input**: Design documents from `/specs/002-sentiment-pipeline/`
**Prerequisites**: plan.md ✓, spec.md ✓, research.md ✓, data-model.md ✓, contracts/ ✓, quickstart.md ✓

**Tests**: Per Constitution VI (Test-First Development), every implementation task MUST be preceded by tests that fail before the implementation exists. Tests are written first; implementation makes them pass. Safety-critical tests (SRC-004, SC-005) and contract snapshot tests (SBC-002) carry ⚠️ markers for emphasis, but test-first ordering applies universally.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Create the `nexus-sentiment` package skeleton and shared infrastructure changes

- [X] T001 Create packages/nexus-sentiment/ directory tree with pyproject.toml (dependencies: nexus-common, redis[hiredis], pydantic, pydantic-settings, vaderSentiment, asyncpg, fastapi, uvicorn, structlog; extras: [finbert] for transformers+torch; [dev] for pytest, pytest-asyncio, hypothesis, syrupy, httpx, testcontainers), all __init__.py files, and tests/ subdirectories per plan.md project structure
- [X] T002 [P] Add sentiment_scores hypertable, time index, and asset+time composite index to docker/timescaledb/init.sql per research.md §8 schema
- [X] T003 [P] Update config.example.toml with [sentiment] section showing all SentimentConfig fields and defaults (processor_type, active_assets, asset_dictionary_path, health_port, pending_claim_threshold, claim_sweep_interval, max_fan_out, output/health stream names, batch_size, flush_interval)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: nexus-common schema additions and core modules that ALL user stories depend on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T004 Add SENTIMENT_SCORE = "SENTIMENT_SCORE" to EventType StrEnum in packages/nexus-common/nexus_common/schemas/enums.py
- [X] T005 Add SentimentScore pydantic model (article_url, asset, score, confidence, sentiment_label, model_id with validators per data-model.md) and register EventType.SENTIMENT_SCORE → SentimentScore in PAYLOAD_TYPE_MAP in packages/nexus-common/nexus_common/schemas/market_event.py
- [X] T006 [P] Contract snapshot test for SentimentScore serialization stability (JSON round-trip, Redis field encoding via MarketEvent.to_redis_fields/from_redis_fields) in packages/nexus-sentiment/tests/contract/test_sentiment_schemas.py
- [X] T007 [P] Create SentimentConfig pydantic-settings model with all fields from data-model.md (processor_type, redis_url, input/output/health streams, consumer_group, block_timeout, pending_claim_threshold, claim_sweep_interval, active_assets, asset_dictionary_path, output_maxlen, health_maxlen, timescaledb_dsn, batch_size, flush_interval, health_host, health_port, max_fan_out, log_env) and TOML [sentiment] section + NEXUS_* env var precedence in packages/nexus-sentiment/nexus_sentiment/config.py
- [X] T008 [P] Create BaseSentimentProcessor ABC (async load(), sync analyze(text) → SentimentResult, async close(), model_id property) and SentimentResult NamedTuple (label, score, confidence) in packages/nexus-sentiment/nexus_sentiment/processors/base.py
- [X] T009 [P] Create RedisPublisher with XADD to output stream, MAXLEN ~50000 approximate trimming, disconnect buffering in bounded deque, and pipeline flush on reconnect in packages/nexus-sentiment/nexus_sentiment/publishers/redis_publisher.py
- [X] T010 [P] Create HealthPublisher (fire-and-forget to nexus:sentiment-health-events, MAXLEN ~5000, no buffering on disconnect per contract) in packages/nexus-sentiment/nexus_sentiment/publishers/health_publisher.py
- [X] T011 [P] Create TimescaleWriter with asyncio.Queue, asyncpg.copy_records_to_table batch writes for sentiment_scores table (time, source, asset, article_url, score, confidence, sentiment_label, model_id, schema_version), configurable batch_size and flush_interval in packages/nexus-sentiment/nexus_sentiment/persistence/timescale_writer.py
- [X] T012 [P] Create SentimentHealth pydantic response model (status, processor type/state/model_id, events_processed, errors) and FastAPI HealthEndpoint class with GET /health reading in-memory counters only in packages/nexus-sentiment/nexus_sentiment/monitoring/health_endpoint.py

**Checkpoint**: Foundation ready — user story implementation can now begin

---

## Phase 3: User Story 1 — Sentiment Scores Flowing From News Articles (Priority: P1) 🎯 MVP

**Goal**: Every news article consumed from `nexus:news-events` is scored for sentiment via VADER and the result is published to `nexus:sentiment-events` as a `MarketEvent` with `event_type=SENTIMENT_SCORE`.

**Independent Test**: Publish a `NewsArticle` event with `related_assets: ["BTC/USDT"]` — observe a `SentimentScore` event in `nexus:sentiment-events` within 1 second with positive score, valid confidence, correct asset, and `source="nexus-sentiment:vader"`.

### Tests for User Story 1 (REQUIRED — SC-005 score/confidence ranges) ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [X] T013 [P] [US1] Property-based test (hypothesis) for VADER score ∈ [-1.0, +1.0] and confidence ∈ [0.0, 1.0] across random text inputs, plus label threshold correctness (compound ≥ 0.05 → positive, ≤ -0.05 → negative, else neutral) in packages/nexus-sentiment/tests/unit/test_vader_processor.py

### Implementation for User Story 1

- [X] T014 [US1] Implement VaderProcessor: load() instantiates SentimentIntensityAnalyzer, analyze(text) calls polarity_scores and maps compound → score/confidence/label per research.md §1, model_id returns "vader:{version}" in packages/nexus-sentiment/nexus_sentiment/processors/vader_processor.py
- [X] T015 [US1] Implement SentimentService: create consumer group (XGROUP CREATE ... $ MKSTREAM, ignore BUSYGROUP), consumer loop with XREADGROUP COUNT 1 BLOCK 5000, parse MarketEvent envelope, validate NewsArticle payload, combine headline + body_summary text (FR-002), run VaderProcessor.analyze() in asyncio.run_in_executor, build SentimentScore per asset from related_assets, wrap in MarketEvent envelope (source, asset, timestamp, event_type=SENTIMENT_SCORE), publish via RedisPublisher, queue to TimescaleWriter, XACK on success, update health counters in packages/nexus-sentiment/nexus_sentiment/service.py
- [X] T016 [US1] Implement main.py: parse args, load SentimentConfig, call configure_logging, create processor + publishers + writer + health endpoint, wire SentimentService with callback injection, start consumer loop and health endpoint as independent asyncio.create_task with add_done_callback (FR-009, no TaskGroup), register SIGTERM/SIGINT handlers for graceful shutdown (stop flag, flush writer, close processor) in packages/nexus-sentiment/nexus_sentiment/main.py
- [X] T017 [P] [US1] Unit test for SentimentService: mock Redis + processor, verify valid single-asset article → one SentimentScore published with correct fields + XACK called; verify MarketEvent envelope has event_type=SENTIMENT_SCORE and source="nexus-sentiment:vader" in packages/nexus-sentiment/tests/unit/test_service.py
- [X] T018 [P] [US1] Unit test for health endpoint: verify GET /health returns SentimentHealth JSON with status/processor/events_processed/errors, response < 200ms in packages/nexus-sentiment/tests/unit/test_health_endpoint.py

**Checkpoint**: US1 complete — VADER sentiment scoring works end-to-end for single-asset articles. Validate with quickstart.md §Verify.

---

## Phase 4: User Story 2 — Per-Asset Scoring for Multi-Asset Articles (Priority: P1)

**Goal**: Articles with multiple assets in `related_assets` produce one `SentimentScore` per unique asset, all sharing the same score/confidence/label from a single inference call.

**Independent Test**: Publish a `NewsArticle` with `related_assets: ["BTC/USDT", "ETH/USDT"]` — observe exactly 2 `SentimentScore` events with matching scores but different `asset` values. Publish with `related_assets: ["BTC/USDT", "BTC/USDT"]` — observe exactly 1 event (deduplicated).

### Tests for User Story 2 ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [X] T019 [P] [US2] Unit test for fan-out: multi-asset article → N SentimentScore events, duplicates in related_assets → deduplicated, all events share same score/confidence/label, XACK only after all publishes succeed in packages/nexus-sentiment/tests/unit/test_service.py

### Implementation for User Story 2

- [X] T020 [US2] Add fan-out loop to SentimentService: iterate deduplicated effective asset list, publish one SentimentScore per unique asset (all sharing inference result), XACK only after all publishes succeed (FR-006); if any publish fails the source message remains pending in packages/nexus-sentiment/nexus_sentiment/service.py

**Checkpoint**: US1+US2 complete — single and multi-asset articles produce correct per-asset scores

---

## Phase 5: User Story 3 — Asset Extraction From Article Text (Priority: P1)

**Goal**: The service detects assets and sectors mentioned in article text via dictionary/regex matching, even when `related_assets` is empty, and merges extracted assets with pre-populated `related_assets`.

**Independent Test**: Publish a `NewsArticle` with `related_assets: []` and headline "Bitcoin and Ethereum rally on ETF approval" — observe 2 `SentimentScore` events for `BTC/USDT` and `ETH/USDT` extracted from text.

### Tests for User Story 3 ⚠️

- [X] T021 [P] [US3] Unit test for AssetExtractor: dictionary loading from YAML, word-boundary matching (case-insensitive), active_assets filtering (suppresses inactive), merge with related_assets deduplication, max_fan_out cap, missing dictionary file → error in packages/nexus-sentiment/tests/unit/test_asset_extractor.py

### Implementation for User Story 3

- [X] T022 [P] [US3] Create data/asset_dictionary.yaml with version field, initial assets (BTC/USDT with aliases [Bitcoin, BTC, bitcoin], ETH/USDT with aliases [Ethereum, ETH, Ether]), and sectors (sector:crypto with keywords [crypto market, cryptocurrency market, crypto], sector:stocks with keywords [stock market, equities market, stocks]) per research.md §5
- [X] T023 [US3] Implement AssetExtractor: load YAML dictionary at init, compile case-insensitive \b{alias}\b regex per alias/keyword, extract(text) → deduplicated list of matched canonical IDs filtered against the `active_assets` set passed to `__init__` (empty set = no filter), raise on missing/malformed dictionary in packages/nexus-sentiment/nexus_sentiment/extraction/asset_extractor.py
- [X] T024 [US3] Integrate AssetExtractor into SentimentService: `_build_effective_assets` filters `article.related_assets` against `active_assets`, merges with extractor output (which is already filtered), deduplicates preserving order, and caps at `max_fan_out` (log warning on overflow); exit at startup if dictionary missing/malformed (FR-017) in packages/nexus-sentiment/nexus_sentiment/service.py

**Checkpoint**: US1+US2+US3 complete — asset extraction fills gaps when upstream does not tag articles

---

## Phase 6: User Story 4 — General Market Sentiment for Non-Asset Articles (Priority: P2)

**Goal**: Articles with no specific asset or sector mentions produce a single `SentimentScore` with `asset=None` representing general market sentiment.

**Independent Test**: Publish a `NewsArticle` with `related_assets: []` and headline "Federal Reserve signals aggressive rate hikes" — observe exactly 1 `SentimentScore` with `asset=None`.

### Tests for User Story 4 ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [X] T025 [P] [US4] Unit test for general market sentiment: article with empty related_assets and no dictionary matches → exactly 1 SentimentScore with asset=None; verify MarketEvent.asset is also None in packages/nexus-sentiment/tests/unit/test_service.py

### Implementation for User Story 4

- [X] T026 [US4] Add asset=None fallback path to SentimentService: when effective asset list is empty after extraction + related_assets merge, publish single SentimentScore with asset=None and MarketEvent.asset=None (FR-004) in packages/nexus-sentiment/nexus_sentiment/service.py

**Checkpoint**: US1–US4 complete — all article types produce appropriate sentiment scores

---

## Phase 7: User Story 5 — Asset-Group Tagging for Sector-Wide Articles (Priority: P2)

**Goal**: Articles mentioning an entire asset class (e.g., "crypto market") receive a `sector:`-prefixed tag so strategies can distinguish sector-wide from asset-specific sentiment.

**Independent Test**: Publish a `NewsArticle` with headline "Crypto market crashes amid regulatory crackdown" — observe a `SentimentScore` with `asset="sector:crypto"`.

### Tests for User Story 5 ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [X] T027 [P] [US5] Unit test for sector extraction and tagging: article with "crypto market" → SentimentScore with asset="sector:crypto"; article with both "Bitcoin" and "crypto market" → 2 events (BTC/USDT + sector:crypto); sector: prefix distinguishable from ccxt symbols in packages/nexus-sentiment/tests/unit/test_asset_extractor.py

### Implementation for User Story 5

- [X] T028 [US5] Verify sector: prefix pass-through in SentimentService fan-out (should require no code changes if extraction + fan-out already handle arbitrary strings); add additional sector entries to data/asset_dictionary.yaml if needed in packages/nexus-sentiment/nexus_sentiment/service.py and data/asset_dictionary.yaml

**Checkpoint**: US1–US5 complete — individual assets and sector groups are both tagged correctly

---

## Phase 8: User Story 6 — Resilience to Inference Errors (Priority: P2)

**Goal**: The service continues operating when a single article causes an inference failure or schema error, and stale pending messages are claimed and logged as dead-letters.

**Independent Test**: Publish a `NewsArticle` with text that triggers an inference error (or mock the processor to throw). Observe that the error is logged, a `MODEL_INFERENCE_ERROR` health alert is emitted, the message is NOT acknowledged, and the next valid article is processed normally.

### Tests for User Story 6 (REQUIRED — SRC-004 safety-critical) ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [X] T029 [P] [US6] Unit test for inference error resilience: mock processor to raise on article N, verify MODEL_INFERENCE_ERROR health alert emitted, message NOT XACKed, error counter incremented, article N+1 processes normally in packages/nexus-sentiment/tests/unit/test_service.py
- [X] T030 [P] [US6] Unit test for dead-letter claim sweep: mock XAUTOCLAIM returning a stale message, verify DEAD_LETTER_CLAIMED health alert emitted and message XACKed in packages/nexus-sentiment/tests/unit/test_service.py

### Implementation for User Story 6

- [X] T031 [US6] Add inference error handling around NLP analyze() call: catch exceptions, emit MODEL_INFERENCE_ERROR health alert via HealthPublisher, do NOT XACK (leave pending for retry/claim), increment error counter, log with article_url and error detail, continue consumer loop (SRC-004) in packages/nexus-sentiment/nexus_sentiment/service.py
- [X] T032 [US6] Add malformed payload handling: catch MarketEvent/NewsArticle pydantic ValidationError during parse, log warning with message ID and error, XACK and drop (FR-014, distinct from inference errors which leave message pending) in packages/nexus-sentiment/nexus_sentiment/service.py
- [X] T033 [US6] Add empty-text edge case: when headline and body_summary are both empty, log warning with article_url, publish SentimentScore with score=0.0, confidence=0.0, sentiment_label="neutral", and XACK (zero-signal, not an error) in packages/nexus-sentiment/nexus_sentiment/service.py
- [X] T034 [US6] Implement dead-letter claim sweep as separate asyncio.create_task: run XAUTOCLAIM periodically at claim_sweep_interval (default 60s) for messages pending > pending_claim_threshold (default 300s), log each as dead-letter warning, emit DEAD_LETTER_CLAIMED health alert, XACK in packages/nexus-sentiment/nexus_sentiment/service.py

**Checkpoint**: US1–US6 complete — service is resilient to bad inputs, inference errors, and stale messages

---

## Phase 9: User Story 7 — Pluggable NLP Model Selection (Priority: P3)

**Goal**: Operators can choose between VADER (fast, rule-based) and FinBERT (accurate, transformer-based) via configuration, with clear error on missing FinBERT dependencies.

**Independent Test**: Start with `processor_type="vader"` → verify `model_id` contains "vader". Restart with `processor_type="finbert"` → verify `model_id` contains "finbert". Both produce valid SentimentScore events from the same input.

### Tests for User Story 7 (REQUIRED — SC-005 score/confidence ranges) ⚠️

- [X] T035 [P] [US7] Property-based test (hypothesis) for FinBERT score ∈ [-1.0, +1.0] and confidence ∈ [0.0, 1.0] ranges, plus ImportError guard when transformers/torch missing, in packages/nexus-sentiment/tests/unit/test_finbert_processor.py

### Implementation for User Story 7

- [X] T036 [US7] Implement FinBertProcessor: load ProsusAI/finbert via transformers.pipeline("text-classification", top_k=3), call pipeline with `truncation=True` at call time (HF issue #25994 — init-time truncation is silently dropped), map softmax probs → score (positive−negative), confidence (max prob), label via strict-`>` argmax with ties falling to `neutral`; ImportError on missing transformers/torch with clear message; model_id returns "finbert:{version}" per research.md §2 in packages/nexus-sentiment/nexus_sentiment/processors/finbert_processor.py
- [X] T037 [US7] Add processor factory to SentimentService or main.py: select VaderProcessor or FinBertProcessor based on config.processor_type; raise clear error with exit(1) for unknown processor_type or missing FinBERT dependencies (SRC-005) in packages/nexus-sentiment/nexus_sentiment/service.py

**Checkpoint**: All 7 user stories complete — full sentiment pipeline operational

---

## Phase 10: Polish & Cross-Cutting Concerns

**Purpose**: Config tests, Docker deployment, integration tests, end-to-end validation

- [X] T038 [P] Unit test for SentimentConfig: TOML loading, NEXUS_* env var precedence, defaults, credential exclusion from [sentiment] section in packages/nexus-sentiment/tests/unit/test_config.py
- [X] T039 [P] Unit test for SRC-003/SC-006 compliance: verify SentimentScore rejects model_id values containing '/' (file paths) or credential-like patterns; verify serialized MarketEvent payloads and HealthAlert messages contain no API keys, file paths, or PII in packages/nexus-sentiment/tests/unit/test_service.py
- [X] T040 [P] Unit test for SC-007 task independence: mock health endpoint to raise during startup or operation, verify consumer loop continues processing normally; mock consumer loop to crash, verify health endpoint still responds in packages/nexus-sentiment/tests/unit/test_service.py
- [X] T041 [P] Create Dockerfile for nexus-sentiment (multi-stage build, Python 3.11+, pip install nexus-common + nexus-sentiment, ENTRYPOINT python -m nexus_sentiment.main) in packages/nexus-sentiment/Dockerfile
- [X] T042 [P] Update docker-compose.dev.yml to include nexus-sentiment service with dependency on redis and timescaledb, volume mount for config.toml and data/
- [X] T043 [P] Integration test for Redis Stream consumer/producer round-trip: publish NewsArticle to nexus:news-events via testcontainers Redis, verify SentimentScore appears in nexus:sentiment-events; include latency assertion ≤1s for VADER (SC-001) in packages/nexus-sentiment/tests/integration/test_redis_consumer.py
- [X] T044 [P] Integration test for TimescaleDB persistence: verify SentimentScore batch write to sentiment_scores table via testcontainers PostgreSQL in packages/nexus-sentiment/tests/integration/test_timescale_persistence.py
- [X] T045 Run quickstart.md end-to-end validation: install, configure, start service, publish test article via redis-cli, verify SentimentScore output and health endpoint response

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on T001 (project skeleton). T005 depends on T004 (enum before payload model). T006–T012 can run in parallel after T005.
- **User Stories (Phase 3–9)**: All depend on Phase 2 completion
  - **US1 (Phase 3)**: First user story — no story dependencies
  - **US2 (Phase 4)**: Extends US1's fan-out logic — depends on US1
  - **US3 (Phase 5)**: Adds extraction module — depends on US2 (fan-out must exist)
  - **US4 (Phase 6)**: Adds asset=None fallback — depends on US3 (extraction determines empty asset list)
  - **US5 (Phase 7)**: Sector tagging — depends on US3 (extraction must detect sector keywords)
  - **US6 (Phase 8)**: Error resilience — can start after US1 (error handling wraps the consumer loop). Can run in parallel with US4/US5 if staffed.
  - **US7 (Phase 9)**: FinBERT processor — can start after US1 (needs processor interface). Can run in parallel with US2–US6 if staffed.
- **Polish (Phase 10)**: Depends on all user stories being complete

### Within Each User Story (Constitution VI: Test-First)

1. Tests MUST be written first and FAIL before implementation (⚠️ markers highlight safety-critical tests, but test-first applies to ALL stories)
2. Models and base classes before services
3. Services before integration/wiring
4. Core implementation before edge cases

### Parallel Opportunities

**Phase 1**: T002 and T003 in parallel (different files)
**Phase 2**: T006–T012 all in parallel after T004→T005 completes (7 tasks, all different files)
**Phase 3**: T013 first (test-first), then T014, then T015→T016 (sequential), T017+T018 in parallel
**Phase 4**: T019 first (test-first), then T020
**Phase 5**: T021+T022 in parallel, then T023, then T024
**Phase 6**: T025 first (test-first), then T026
**Phase 7**: T027 first (test-first), then T028
**Phase 8**: T029+T030 in parallel (test-first), then T031–T034 sequentially
**Phase 9**: T035 first (test-first), then T036, then T037
**Phase 10**: T038–T044 all in parallel (different files)

---

## Parallel Example: Phase 2 (Foundational)

```
Sequential: T004 (EventType enum) → T005 (SentimentScore model)
Then parallel:
  T006 (contract test)     ─┐
  T007 (SentimentConfig)   ─┤
  T008 (BaseSentimentProc) ─┤
  T009 (RedisPublisher)    ─┤── all in parallel (different files)
  T010 (HealthPublisher)   ─┤
  T011 (TimescaleWriter)   ─┤
  T012 (HealthEndpoint)    ─┘
```

## Parallel Example: User Story 1

```
T013 (VADER property test, must fail) ─── write first
T014 (VaderProcessor implementation)  ─── makes T013 pass
T015 (SentimentService)               ─── uses T014
T016 (main.py entry point)            ─── uses T015
T017 (service unit test) ──┐
                           ├── parallel (different test files)
T018 (health endpoint test)┘
```

## Parallel Example: User Stories with Team

```
After Phase 2:
  Developer A: US1 → US2 → US3 → US4 → US5  (core pipeline, sequential)
  Developer B: US6 (after US1)                (error resilience, parallel)
  Developer C: US7 (after US1)                (FinBERT, parallel)
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001–T003)
2. Complete Phase 2: Foundational (T004–T012) — CRITICAL, blocks all stories
3. Complete Phase 3: User Story 1 (T013–T018) — VADER scoring end-to-end
4. **STOP and VALIDATE**: Run quickstart.md §Verify against the running service
5. Deploy/demo if ready — articles with `related_assets` produce sentiment scores

### Incremental Delivery

1. Setup + Foundational → Framework ready
2. US1 → Single-asset VADER scoring → **Deploy MVP**
3. US2 → Multi-asset fan-out → Test with multi-asset articles
4. US3 → Asset extraction → Test with `related_assets: []` articles
5. US4 → General market sentiment → Test with untagged macro news
6. US5 → Sector tagging → Test with "crypto market" headlines
7. US6 → Error resilience + dead-letter → Test with malformed inputs
8. US7 → FinBERT option → Test with `processor_type="finbert"`
9. Polish → Docker, integration tests, validation

---

## Notes

- [P] tasks = different files, no dependencies on incomplete tasks
- [Story] label maps task to specific user story for traceability
- Safety-critical tests (SC-005, SRC-004) MUST be written before their implementations
- Contract snapshot test (T006) validates SBC-002 backward compatibility
- Publishers are copied from nexus-ingestion (not imported) per research.md §9 to maintain service boundary
- The asset dictionary (T022) is needed starting from US3; US1/US2 use only `related_assets`
- `claim_sweep_interval` (default 60s) and `max_fan_out` (default 50) must be added to SentimentConfig (not in data-model.md but required by FR-015 and edge case spec)
- Commit after each task or logical group
