# Implementation Plan: Sentiment Analysis Pipeline

**Branch**: `002-sentiment-pipeline` | **Date**: 2026-04-02 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/002-sentiment-pipeline/spec.md`

## Summary

A standalone Python asyncio service (`nexus-sentiment`) that consumes `NewsArticle` events from the `nexus:news-events` Redis Stream, runs NLP inference (pluggable VADER or FinBERT) to produce `SentimentScore` events, and publishes them to `nexus:sentiment-events`. Includes per-asset fan-out, dictionary-based asset extraction from article text, dead-letter handling for stale pending messages, and async persistence to TimescaleDB. Follows the same architectural patterns as `nexus-ingestion`: callback injection, task isolation (no TaskGroup), pydantic-settings config precedence, structlog logging, and FastAPI health endpoint.

## Technical Context

**Language/Version**: Python 3.11+
**Primary Dependencies**: `nexus-common`, `redis[hiredis]>=5.0`, `pydantic>=2.0`, `pydantic-settings>=2.0`, `vaderSentiment>=3.3`, `asyncpg>=0.29`, `fastapi>=0.110`, `uvicorn>=0.27`, `structlog>=24.0`, `uvloop>=0.19` (optional). FinBERT extra: `transformers>=4.40`, `torch>=2.0`.
**Storage**: Redis Streams (input: `nexus:news-events`, output: `nexus:sentiment-events`, health: `nexus:sentiment-health-events`), TimescaleDB (async persistence of sentiment scores)
**Testing**: `pytest`, `pytest-asyncio`, `hypothesis` (property-based for score/confidence ranges), `syrupy` (contract snapshots), `testcontainers-python` (integration), `httpx` (health endpoint)
**Target Platform**: Linux server, Docker Compose (same stack as `nexus-ingestion`)
**Project Type**: Event-driven microservice (Redis Stream consumer → NLP transform → Redis Stream producer)
**Performance Goals**: ≤1s end-to-end latency (VADER), ≤2s (FinBERT on CPU) from `NewsArticle` in `nexus:news-events` to `SentimentScore` in `nexus:sentiment-events` (SC-001)
**Constraints**: Read/transform/publish only — MUST NOT execute, submit, or influence any order (SRC-001/SRC-002). NLP inference MUST run in thread pool (FR-008). At-least-once delivery semantics; XACK only after all publishes succeed (FR-006).
**Scale/Scope**: Processes all news articles from `nexus-ingestion`. Fan-out: 1 article → N `SentimentScore` events (one per unique asset). Expected throughput: ≤100 articles/minute (news feed rates).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- [x] **Safety-first execution gate**: `nexus-sentiment` is a read/transform/publish pipeline only (SRC-001). It MUST NOT execute, submit, modify, or cancel any order. It MUST NOT invoke Risk Manager, Strategy Engine, or Execution Engine (SRC-002). No risk validation or kill-switch needed because this service makes zero trading decisions.
- [x] **Event-driven Python gate**: Runtime uses Python `asyncio`. NLP inference (CPU-bound) runs in `asyncio.run_in_executor()` thread pool (FR-008). No NumPy-backed vectorized operations needed — inference is delegated to VADER/FinBERT libraries.
- [x] **Redis-first messaging gate**: Consumes from `nexus:news-events` (Redis Stream, consumer group `nexus-sentiment-group`). Publishes to `nexus:sentiment-events` (Redis Stream). Health alerts to `nexus:sentiment-health-events`. All messages use versioned `MarketEvent` envelope (schema_version semver). Consumer group behavior: `XREADGROUP` with `XACK` post-publish, pending message claim sweep (FR-015).
- [x] **Library-first integration gate**: No exchange connectivity — this service does not use `ccxt` or `ib_insync`. Exception justified: the service sits between ingestion and strategies, processing only text data via NLP libraries (`vaderSentiment`, `transformers`+`torch`).
- [x] **Spec-code traceability gate**: FR-001–FR-021 map to planned tasks. Contract additions: `SENTIMENT_SCORE` EventType in `nexus-common`, `SentimentScore` payload model, `nexus:sentiment-events` stream contract, TimescaleDB schema addition.
- [x] **Safety-critical testing gate**: Property-based tests for score/confidence ranges (SC-005). Contract snapshot tests for `SentimentScore` serialization stability. Regression tests for: fan-out correctness (SC-002), dead-letter claim (FR-015), inference error resilience (SC-003), asset extraction precision (SC-008–SC-011). No risk/aggregation/execution paths affected.
- [x] **Service-boundary gate**: `nexus-sentiment` sits strictly between `nexus-ingestion` (upstream) and `nexus-strategies` (downstream). Dependencies: Redis, `nexus-common`. No cross-boundary logic. Health alerts follow `nexus-ingestion`'s `HealthAlert` schema (SBC-004).

## Project Structure

### Documentation (this feature)

```text
specs/002-sentiment-pipeline/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── sentiment-events.md
│   └── sentiment-health-events.md
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```text
packages/
├── nexus-common/
│   └── nexus_common/
│       └── schemas/
│           ├── enums.py                 # + SENTIMENT_SCORE EventType
│           └── market_event.py          # + SentimentScore payload model + PAYLOAD_TYPE_MAP entry
├── nexus-sentiment/
│   ├── pyproject.toml
│   ├── Dockerfile
│   └── nexus_sentiment/
│       ├── __init__.py
│       ├── main.py                      # Entry point (signal handling, wiring)
│       ├── service.py                   # SentimentService orchestrator (consumer loop + health)
│       ├── config.py                    # SentimentConfig (pydantic-settings, TOML, env vars)
│       ├── processors/
│       │   ├── __init__.py
│       │   ├── base.py                  # BaseSentimentProcessor ABC
│       │   ├── vader_processor.py       # VADER implementation
│       │   └── finbert_processor.py     # FinBERT implementation
│       ├── extraction/
│       │   ├── __init__.py
│       │   └── asset_extractor.py       # Dictionary/regex asset extraction
│       ├── monitoring/
│       │   ├── __init__.py
│       │   └── health_endpoint.py       # FastAPI GET /health
│       ├── publishers/
│       │   ├── __init__.py
│       │   ├── redis_publisher.py       # Reuse from nexus-ingestion or shared
│       │   └── health_publisher.py      # Health alert publisher
│       └── persistence/
│           ├── __init__.py
│           └── timescale_writer.py      # Async batch writer for sentiment scores
│   └── tests/
│       ├── __init__.py
│       ├── unit/
│       │   ├── __init__.py
│       │   ├── test_config.py
│       │   ├── test_service.py
│       │   ├── test_vader_processor.py
│       │   ├── test_finbert_processor.py
│       │   ├── test_asset_extractor.py
│       │   └── test_health_endpoint.py
│       ├── contract/
│       │   ├── __init__.py
│       │   └── test_sentiment_schemas.py
│       └── integration/
│           ├── __init__.py
│           ├── test_redis_consumer.py
│           └── test_timescale_persistence.py
│
├── data/
│   └── asset_dictionary.yaml            # Versioned asset extraction dictionary (FR-017)

docker/
└── timescaledb/
    └── init.sql                         # + sentiment_scores table
```

**Structure Decision**: Follows the existing monorepo convention (`packages/nexus-{service}/`). Mirrors `nexus-ingestion` layout: adapters→processors, same monitoring/publishers/persistence subpackages. Asset dictionary lives in `data/` at repo root for cross-service visibility and versioning.

## Complexity Tracking

No constitution violations to justify.
