# Code Review Checklist — Nexus

Dimension-by-dimension pass/fail checklist. Use during every review.

## 1. asyncio Correctness

- [ ] No blocking calls in event loop (`time.sleep`, sync I/O, CPU loops)
- [ ] Every coroutine is `await`ed — no unawaited coroutine warnings
- [ ] No `TaskGroup` for adapter supervision — use manual `create_task` + `add_done_callback`
- [ ] `while self._running` loops catch `asyncio.CancelledError` and break cleanly
- [ ] `stop()` cancels tasks, awaits them, closes connections in reverse startup order
- [ ] Long-running background tasks have proper names via `asyncio.create_task(..., name=...)`

## 2. Adapter Contract

- [ ] Subclasses `BaseAdapter`
- [ ] Implements all abstract methods: `connect()`, `subscribe()`, `run()`, `stop()`
- [ ] Normalizes source data into `MarketEvent` envelope before emitting
- [ ] Uses `event_callback` / `health_callback` — no direct publisher/writer imports
- [ ] Calls `record_event()`, `record_error()`, `record_malformed()` appropriately
- [ ] Reconnection state machine handles all transitions with health alerts
- [ ] `stop()` sets `_running = False` and closes all connections/sessions

## 3. Security

- [ ] **SRC-001**: No `create_order`, `cancel_order`, `edit_order` in read-only adapters
- [ ] **SRC-003**: Credentials use `SecretStr`, not plain strings in shared config
- [ ] No credentials in `to_redis_fields()`, log messages, or health endpoint responses
- [ ] Redis URL sanitized before logging (strip password)
- [ ] `config.toml` in `.gitignore`, `config.example.toml` has no real secrets
- [ ] Health endpoint does not expose internal state beyond adapter health

## 4. Event Envelope

- [ ] `MarketEvent` has: source, asset, timestamp (UTC), event_type, schema_version (semver), payload (dict)
- [ ] `asset=None` for non-asset events — never empty string `""`
- [ ] All datetimes are UTC: `datetime.now(timezone.utc)`, no naive datetimes
- [ ] `to_redis_fields()` / `from_redis_fields()` round-trip without data loss
- [ ] `schema_version` validated as semver via regex

## 5. Redis Streams

- [ ] `XADD` uses `maxlen` + `approximate=True`
- [ ] `RedisPublisher` buffers on disconnect, flushes via pipeline on reconnect
- [ ] `HealthPublisher` does NOT buffer (no circular dependency)
- [ ] Stream names: `nexus:<domain>-events`
- [ ] Buffer has configurable max size (deque with maxlen)

## 6. TimescaleDB

- [ ] Batch writes via `copy_records_to_table` — no row-by-row inserts
- [ ] `asyncio.Queue` decouples ingestion from persistence
- [ ] Retry with exponential backoff; `PERSISTENCE_ERROR` alert on max retries
- [ ] Schema uses `IF NOT EXISTS` and `if_not_exists => TRUE` for idempotency
- [ ] No ORM in the write-hot path

## 7. Pydantic v2

- [ ] `model_validator(mode="after")` for cross-field validation
- [ ] `field_validator` with `@classmethod` for single-field
- [ ] `BaseSettings` + `SettingsConfigDict` for config — no manual `os.getenv`
- [ ] `Field(gt=0)`, `Field(min_length=1)` for declarative constraints
- [ ] `from __future__ import annotations` at top of every module

## 8. Config

- [ ] Precedence: defaults < config.toml < env vars < init args
- [ ] All env vars use `NEXUS_` prefix
- [ ] Secrets only via env vars, never in TOML
- [ ] Adding a new config field is reflected in both TOML loader mapping and env var docs

## 9. Pre-commit / Static Analysis

- [ ] Code is black-formatted (no style nitpicks in review)
- [ ] Imports sorted by isort (black-compatible profile)
- [ ] No flake8 violations: unused imports, bare `except:`, mutable defaults, missing f-string placeholders
- [ ] Type annotations on public APIs
- [ ] `from __future__ import annotations` enables PEP 604 union syntax

## 10. Tests

- [ ] Unit tests: no I/O, no Docker, fast, mocked externals
- [ ] Integration tests: testcontainers for Redis/TimescaleDB, isolated per test
- [ ] Contract tests: syrupy snapshots for serialization stability
- [ ] Property tests: Hypothesis for schema boundaries
- [ ] Each test cleans its own state (truncate, reset) — no cross-test leakage
- [ ] `--import-mode=importlib` for monorepo test discovery
- [ ] No `__init__.py` in test directories
