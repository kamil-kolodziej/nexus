# Research: Data Ingestion Layer

**Date**: 2026-03-22 | **Branch**: `001-data-ingestion`

## 1. ccxt.pro WebSocket Reconnection

**Decision**: Rely on ccxt.pro's built-in auto-reconnection with `while True` → `await exchange.watch_*()` loop. Add an outer supervision layer for gap detection and health alerts.

**Rationale**: ccxt.pro handles disconnections transparently — the next `watch_*()` call triggers reconnection with exponential backoff automatically. No explicit on_connect/on_disconnect callbacks are exposed; disconnections surface as exceptions (`NetworkError`, `ExchangeNotAvailable`) caught by the loop's `try/except`. Configurable via `exchange.streaming` properties: `keepAlive` (default 30000ms), `maxPingPongMisses` (default 2.0).

**Alternatives Considered**:
- Manual WebSocket management (websockets lib) — rejected; duplicates ccxt.pro's built-in handling
- Custom exponential backoff wrapper — unnecessary; ccxt.pro applies backoff internally
- REST polling fallback during disconnect — possible gap-filling strategy but adds complexity; better to accept brief gaps

## 2. Redis Streams Async Publisher

**Decision**: Use `redis.asyncio.Redis` with individual `XADD` per event in the hot path. Pipeline mode only for batch-flushing the in-memory buffer after Redis reconnection. `MAXLEN ~100,000` (approximate trimming) on streams.

**Rationale**: Each event must publish immediately per spec (<1s delivery). XADD is O(1) — pipelining adds latency by buffering. On Redis recovery, pipeline mode flushes all buffered events in a single round-trip. Configure `retry_on_timeout=True` with `Retry(ExponentialBackoff(), retries=3)`.

**Alternatives Considered**:
- XADD pipeline batching in hot path — rejected; conflicts with <1s latency requirement
- Redis Pub/Sub instead of Streams — rejected; Streams provide durability, consumer groups, replay
- Separate Redis connection per adapter — unnecessary; single async connection pool sufficient

## 3. asyncpg Batch Writer for TimescaleDB

**Decision**: Use `copy_records_to_table()` (binary COPY protocol) for batch inserts. `asyncio.Queue`-backed background writer flushes on batch size (500 records) or time interval (5 seconds), whichever comes first. Pool: `min_size=2, max_size=5`.

**Rationale**: COPY protocol is asyncpg's fastest bulk insert — 5-10x faster than `executemany`. The writer is a single background task (I/O-bound, not CPU-bound). Queue `maxsize=50,000` (~6MB at ~120 bytes/event). Producers use `put_nowait()` with `try/except QueueFull` — on overflow, the event is dropped from the persistence path (logged with warning) but the Redis publish path is never blocked. This prevents TimescaleDB outages from stalling adapter coroutines and Redis delivery (per FR-008). Retry with exponential backoff on `PostgresConnectionError`.

**Alternatives Considered**:
- `executemany()` with prepared statements — significantly slower for bulk inserts
- SQLAlchemy async with asyncpg backend — ORM overhead unnecessary for append-only writes
- Separate writer process (multiprocessing) — over-engineering; asyncio task sufficient

## 4. Adapter Isolation with asyncio Tasks

**Decision**: Manual task management with `asyncio.create_task()` + supervisor coroutine using `add_done_callback` for failure detection and automatic restart with backoff. **Do NOT use TaskGroup**.

**Rationale**: `asyncio.TaskGroup` cancels all sibling tasks when one fails — this directly violates FR-004 (adapter isolation). Manual task supervision with `_tasks` dict for strong references, `add_done_callback` for failure detection, and `call_later` for restart scheduling provides exactly the required semantics in ~30 lines.

**Alternatives Considered**:
- `asyncio.TaskGroup` — explicitly rejected; cancels siblings on failure
- `asyncio.gather(return_exceptions=True)` — doesn't support restart-on-failure semantics
- Third-party supervisor (aiojobs) — unnecessary dependency

## 5. FastAPI Health Endpoint

**Decision**: Run `uvicorn.Server(config).serve()` as an asyncio task alongside adapter tasks. Share adapter health registry via FastAPI's `app.state`.

**Rationale**: `uvicorn.Server.serve()` is a native coroutine — integrates directly into the existing event loop without threads. Health endpoint reads in-memory state only (no DB calls, no blocking). Use FastAPI `lifespan` context manager for shared state injection.

**Alternatives Considered**:
- Uvicorn in separate thread — adds thread-safety concerns for adapter state access
- `aiohttp.web` instead of FastAPI — works but less consistent with rest of platform
- Raw `asyncio.start_server` — loses OpenAPI docs and validation

## 6. News Ingestion (aiohttp + feedparser)

**Decision**: `aiohttp.ClientSession` for async HTTP fetching, `feedparser.parse()` via `asyncio.to_thread()` for synchronous XML parsing. Poll on configurable interval with `asyncio.sleep()`.

**Rationale**: HTTP fetch is I/O-bound (aiohttp is async-native). feedparser is synchronous but pure Python, taking 10-100ms for large feeds — `asyncio.to_thread()` offloads it without blocking the event loop. The adapter runs as an independent task with the same supervision pattern as exchange adapters.

**Alternatives Considered**:
- `loop.run_in_executor()` — functionally identical but more verbose
- Replace feedparser with `atoma` — feedparser has best error tolerance for malformed feeds
- WebSub push notifications — most news sources don't support it; polling is practical baseline

## 7. Config Management (TOML + env vars)

**Decision**: `pydantic-settings` (`BaseSettings` with `SettingsConfigDict`) for layered config. TOML file for non-secret defaults, env vars (prefix: `NEXUS_`) for overrides and secrets. `SecretStr` for credentials.

**Rationale**: pydantic-settings provides type validation, env var loading, TOML source (v2+), and `SecretStr` (displays as `'**********'` in repr/str/JSON — prevents credential leaks in logs). Layer priority: env vars > `config.toml` > field defaults. Natural fit since Pydantic is already used for `MarketEvent` schema validation.

**Alternatives Considered**:
- `tomli` + manual env overlay — requires hand-rolling validation and merge logic
- `dynaconf` — heavyweight; pydantic-settings is sufficient
- YAML instead of TOML — TOML is Python standard (PEP 680, stdlib `tomllib`); YAML has type coercion footguns
