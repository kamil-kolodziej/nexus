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
- [ ] **SRC-003**: Credentials use `SecretStr` end-to-end: `IngestionConfig` fields, `ExchangeAdapter` constructor params, and stored fields are all `SecretStr`; `.get_secret_value()` is called only inside `connect()` as a local variable, never stored as plain `str`
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

- [ ] Code is ruff-formatted (no style nitpicks — ruff-format is the authority)
- [ ] Imports sorted by ruff; `from __future__ import annotations` is first
- [ ] No ruff lint violations: unused imports, bare `except:`, mutable defaults, asyncio correctness (RUF, ASYNC rules)
- [ ] No bandit violations: no hardcoded secrets, no unsafe calls
- [ ] Type annotations on public APIs; mypy strict passes on source modules
- [ ] `from __future__ import annotations` enables PEP 604 union syntax

## 12. Structured Logging

- [ ] `structlog.get_logger()` used — no `logging.getLogger(__name__)` anywhere
- [ ] Per-object loggers bound at construction: `self._logger = structlog.get_logger().bind(adapter_id=...)`
- [ ] Log calls use snake_case event name + keyword args: `logger.info("event_name", key=value)`
- [ ] No printf-style format strings in log calls (`%s`, `%d`)
- [ ] No credentials in any log call; Redis URLs sanitized via `_sanitize_url()`
- [ ] `configure_logging()` called only at service entry point, not inside library code

## 11. Spec & Documentation Alignment

**Step 0 — determine spec root**: derive feature ID from branch name or arguments → `specs/<feature-id>/`

**spec.md**
- [ ] Every FR-* requirement has a corresponding implementation
- [ ] Every SRC-* safety constraint is enforced in code
- [ ] Acceptance scenario outcomes match actual code behaviour
- [ ] Edge cases are handled exactly as documented

**data-model.md**
- [ ] Pydantic model field names and types match spec entity definitions
- [ ] All validation rules (price > 0, timestamp tolerance, semver regex, orderbook ordering) are implemented
- [ ] `asset=None` for non-asset events — never empty string `""`

**contracts/*.md** (check every contract file in the directory)
- [ ] Stream names in code match contract headers exactly
- [ ] MAXLEN values in publisher calls match contract tables
- [ ] Payload field names match contract payload schemas
- [ ] Every `alert_type` string in code appears in `contracts/health-events.md`; no undocumented types
- [ ] `HealthPublisher` does NOT buffer (contract forbids it)

**CLAUDE.md architectural invariants**
- [ ] No `asyncio.TaskGroup` anywhere in adapters or service
- [ ] No `asyncio.gather` inside `ExchangeAdapter.run()` — uses `asyncio.wait(ALL_COMPLETED)`
- [ ] Per-stream reconnect counters keyed by `f"{method}:{asset}"` (not a single shared counter)
- [ ] `SecretStr` end-to-end; `.get_secret_value()` only inside `connect()`
- [ ] Shutdown uses `asyncio.Event` — never `loop.stop()` from within a task
- [ ] Adapters communicate via injected callbacks only — no direct publisher/writer imports

**tasks.md**
- [ ] Each recently marked `[X]` task: the described file and behaviour exist in code
- [ ] New fixes or requirements surfaced in this review are captured in tasks.md

**README.md**
- [ ] Install and run commands still accurate
- [ ] Monorepo structure diagram matches actual `packages/` layout

## 10. Tests

- [ ] Unit tests: no I/O, no Docker, fast, mocked externals
- [ ] Integration tests: testcontainers for Redis/TimescaleDB, isolated per test
- [ ] Contract tests: syrupy snapshots for serialization stability
- [ ] Property tests: Hypothesis for schema boundaries
- [ ] Each test cleans its own state (truncate, reset) — no cross-test leakage
- [ ] `--import-mode=importlib` for monorepo test discovery
- [ ] No `__init__.py` in test directories
