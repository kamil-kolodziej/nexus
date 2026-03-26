---
name: code-reviewer
description: "Nexus trading platform code review skill. Python asyncio, pydantic v2, Redis Streams, TimescaleDB. Covers adapter pattern compliance, async correctness, security (SRC-001/SRC-003), event envelope consistency, config precedence, and pre-commit static analysis. Use when reviewing code, PRs, or ensuring quality standards."
---

# Code Reviewer — Nexus Trading Platform

Review guide tailored to the Nexus monorepo: Python 3.11+, asyncio, pydantic v2, Redis Streams, TimescaleDB, ccxt.pro.

## How to Use

This skill is instruction-driven — no external scripts. When asked to review code:

1. Read the file(s) under review.
2. Walk through each dimension below, checking for violations.
3. Output findings grouped by severity: **Critical > Design > Minor**.
4. For each finding: state the issue, reference the file + line, and suggest a concrete fix.

## Review Dimensions

### 1. asyncio Correctness

- No blocking calls (`time.sleep`, synchronous I/O, CPU-heavy loops) in the event loop — use `asyncio.sleep`, `asyncio.to_thread`, or process pools.
- Proper `await` on all coroutines. Watch for missing `await` producing unawaited coroutine warnings.
- **No `TaskGroup`** — adapter isolation (FR-004) requires manual `asyncio.create_task` + `add_done_callback` so one adapter crash never cancels siblings.
- Proper cancellation handling: every `while self._running` loop must catch `asyncio.CancelledError` and clean up.
- Graceful shutdown: `stop()` must cancel tasks, await them, and close connections in correct order.

### 2. Adapter Contract Compliance

- Every adapter subclasses `BaseAdapter` and implements: `connect()`, `subscribe()`, `run()`, `stop()`.
- Adapters normalize raw data into `MarketEvent` envelope before emitting — consumers never see source-specific shapes.
- Adapters use `event_callback` and `health_callback` for output — no direct imports of publishers/writers.
- Each adapter tracks its own `_event_count`, `_error_count`, `_malformed_count` via `record_event()` etc.
- Reconnection state machine (`CONNECTED → RECONNECTING → DOWN → CONNECTED`) lives inside the adapter.

### 3. Security (SRC-001 / SRC-003)

- **SRC-001**: Read-only adapters must never call `create_order`, `cancel_order`, `edit_order`, or any write API.
- **SRC-003**: All credentials use `SecretStr` (pydantic). No credentials in serialization, logs, or Redis fields.
- Redis URL logging must sanitize passwords.
- Health endpoint must not expose secrets or internal state beyond adapter health.
- `config.toml` is gitignored; `config.example.toml` has no real credentials.

### 4. Event Envelope Consistency

- `MarketEvent` fields: `source`, `asset` (str | None), `timestamp` (UTC), `event_type` (EventType enum), `schema_version` (semver), `payload` (dict).
- `asset=None` for non-asset events (e.g., news). Never use empty string `""` as asset.
- Timestamps must be UTC. Use `datetime.now(timezone.utc)`, never naive datetimes.
- `to_redis_fields()` / `from_redis_fields()` must round-trip losslessly.
- `HealthAlert` uses `to_redis_fields()` with all required fields.

### 5. Redis Streams Conventions

- `XADD` with `maxlen=N` and `approximate=True` for stream trimming.
- `RedisPublisher` buffers events on disconnect (deque with configurable max), flushes via pipeline on reconnect.
- `HealthPublisher` does NOT buffer — if Redis is down, alerts are logged and dropped (no circular dependency).
- Stream names follow `nexus:<domain>-events` convention.

### 6. TimescaleDB Patterns

- Batch writes via `asyncpg.copy_records_to_table` — no ORM in the hot write path.
- `asyncio.Queue` for decoupling ingestion from persistence.
- Retry with exponential backoff on write failure; emit `PERSISTENCE_ERROR` health alert on max retries.
- Schema uses `CREATE TABLE IF NOT EXISTS` + `create_hypertable(..., if_not_exists => TRUE)`.

### 7. Pydantic v2 Idioms

- Use `model_validator(mode="after")` for cross-field validation (e.g., ask >= bid).
- Use `field_validator` with `@classmethod` for single-field validation.
- Use `BaseSettings` with `SettingsConfigDict` for config — not manual `os.getenv`.
- Use `Field(gt=0)`, `Field(min_length=1)` etc. for declarative constraints.

### 8. Config Precedence

- Precedence order: defaults < `config.toml` < environment variables < explicit init args.
- TOML source is wired via `settings_customise_sources` in `IngestionConfig`.
- `NEXUS_` prefix for all env vars. Field name maps: `NEXUS_EXCHANGE_API_KEY` → `exchange_api_key`.
- Secrets via env vars only, never in TOML.

### 9. Pre-commit / Static Analysis

The project uses pre-commit hooks for automated Python quality gates. When reviewing:

- **black** — code must be formatted. No manual style debates. Check for unformatted files.
- **isort** — imports sorted with black-compatible profile. Verify `from __future__ import annotations` is first.
- **flake8** — no lint violations. Watch for: unused imports, bare `except:`, mutable default arguments, f-string without placeholders.
- **mypy / type hints** — type annotations on public APIs. `from __future__ import annotations` enables PEP 604 syntax (`X | Y`).
- If pre-commit is not yet configured, flag files that would fail these checks.

### 10. Test Quality

- Unit tests: no I/O, no Docker, fast. Mock external dependencies.
- Integration tests: use `testcontainers` for Redis/TimescaleDB, isolated per test.
- Contract/snapshot tests: syrupy snapshots for serialization stability.
- Property-based tests: Hypothesis for schema boundary validation.
- Test isolation: each test must clean up (truncate tables, reset state). No cross-test data leakage.
- `pytest-asyncio` with `asyncio_mode = "auto"`. `--import-mode=importlib` for monorepo test discovery.

## Output Format

```
## Code Review: <file or scope>

### Critical
- **<Issue title>** — <file>:<line>
  <Description>. Fix: <concrete suggestion>.

### Design
- **<Issue title>** — <file>:<line>
  <Description>. Fix: <concrete suggestion>.

### Minor
- **<Issue title>** — <file>:<line>
  <Description>. Fix: <concrete suggestion>.

### Summary
| Severity | Count |
|----------|-------|
| Critical | N     |
| Design   | N     |
| Minor    | N     |
```

## Reference Documentation

- `references/code_review_checklist.md` — dimension-by-dimension checklist with pass/fail criteria
- `references/coding_standards.md` — Python and Nexus conventions
- `references/common_antipatterns.md` — real antipatterns found in this codebase
