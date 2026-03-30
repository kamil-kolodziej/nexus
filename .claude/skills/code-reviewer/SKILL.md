---
name: code-reviewer
description: "Nexus trading platform code review skill. Python asyncio, pydantic v2, Redis Streams, TimescaleDB. Covers adapter pattern compliance, async correctness, security (SRC-001/SRC-003), event envelope consistency, config precedence, and pre-commit static analysis. Use when reviewing code, PRs, or ensuring quality standards."
---

# Code Reviewer — Nexus Trading Platform

Review guide tailored to the Nexus monorepo: Python 3.11+, asyncio, pydantic v2, Redis Streams, TimescaleDB, ccxt.pro.

## How to Use

This skill is instruction-driven — no external scripts. When asked to review code:

1. Read the file(s) under review **and** the relevant spec/contract files listed in Dimension 11.
2. Walk through each dimension below, checking for violations.
3. Output findings grouped by severity: **Critical > Design > Minor**, followed by a **Spec Alignment** section.
4. For each finding: state the issue, reference the file + line, and suggest a concrete fix.

## Review Dimensions

### 1. asyncio Correctness

- No blocking calls (`time.sleep`, synchronous I/O, CPU-heavy loops) in the event loop — use `asyncio.sleep`, `asyncio.to_thread`, or process pools.
- Proper `await` on all coroutines. Watch for missing `await` producing unawaited coroutine warnings.
- **No `TaskGroup`** — adapter isolation (FR-004) requires manual `asyncio.create_task` + `add_done_callback` so one adapter crash never cancels siblings.
- **No `asyncio.gather` inside adapter `run()`** — use `asyncio.wait(..., return_when=ALL_COMPLETED)` so a crashing watch-stream task does not cancel its siblings.
- **Per-stream reconnect counters** — reconnect attempt state must be tracked per stream key (`f"{method}:{asset}"`), not on a single shared counter; shared counters inflate under concurrent failures and cause premature DOWN transitions.
- Proper cancellation handling: every `while self._running` loop must catch `asyncio.CancelledError` and clean up.
- Graceful shutdown: `stop()` must cancel tasks, await them, and close connections in correct order.
- **No `loop.stop()` from within a task** — calling `loop.stop()` from an `asyncio` task while `asyncio.run()` still owns the loop causes `RuntimeError: Event loop stopped before Future completed`. Use an `asyncio.Event` instead: signal handlers set it, the main coroutine awaits it, then calls `stop()` once.

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

- **ruff** — lint + import sorting + pyupgrade + bugbear + asyncio correctness checks. No manual style debates.
- **ruff-format** — code formatting (replaces black). Verify `from __future__ import annotations` is first import.
- **mypy strict** — type annotations on all source modules. `from __future__ import annotations` enables PEP 604 syntax (`X | Y`). Tests are excluded from strict checking.
- **bandit** — security scanning. Flag hardcoded secrets, unsafe calls, shell injection.
- **detect-secrets** — credential leak prevention. Any new baseline entries need justification.
- If pre-commit is not yet configured, flag files that would fail these checks.

### 12. Structured Logging

- All modules must use `structlog.get_logger()` — never `logging.getLogger(__name__)`.
- Per-object context bound at construction: `self._logger = structlog.get_logger().bind(adapter_id=self.adapter_id)`.
- Log calls use snake_case event names + keyword args: `logger.info("event_name", key=value)` — never printf-style `%s` format strings.
- No credentials in any log call. Redis URLs must be sanitized via `_sanitize_url()` before logging.
- `configure_logging()` called once at service entry point (`main()`), never inside library code.

### 11. Spec & Documentation Alignment

**Determine the spec directory first.** The feature ID is the branch name prefix or explicitly passed in arguments (e.g. `branch:001-data-ingestion` → feature `001-data-ingestion`). The spec root is `specs/<feature-id>/`. All paths below are relative to that root. If the feature ID is ambiguous, list the `specs/` subdirectories and pick the best match.

**Read these files before starting a review** (load once, reference throughout):
- `CLAUDE.md` — architectural decisions and platform-wide invariants
- `README.md` — install/run commands, package structure
- `{SPEC_DIR}/spec.md` — FR-* requirements, SRC-* safety constraints, acceptance scenarios, edge cases
- `{SPEC_DIR}/data-model.md` — entity field names, types, validation rules
- `{SPEC_DIR}/contracts/` — one file per stream/interface; all must be checked
- `{SPEC_DIR}/tasks.md` — which tasks are claimed complete
- `{SPEC_DIR}/plan.md` — key design decisions and rationale (when relevant)
- `{SPEC_DIR}/checklists/` — any requirements or acceptance checklists

**Checks to perform:**

#### Against `{SPEC_DIR}/spec.md`
- Every FR-* requirement: identify the code that implements it; flag any that are missing or contradict the spec.
- Every SRC-* safety constraint: verify it is enforced in code, not just documented.
- Acceptance scenarios: for each "Given/When/Then", verify the code behaviour matches the documented outcome.
- Edge cases: for each documented edge case, verify the code handles it as described.

#### Against `{SPEC_DIR}/data-model.md`
- Pydantic model field names and types must match the spec's entity definitions.
- Validation rules (e.g. price > 0, timestamp tolerance, semver regex) must be enforced.
- `asset=None` for non-asset events — never `""`.

#### Against `{SPEC_DIR}/contracts/*.md`
- Stream names in code must match contract headers exactly.
- MAXLEN values in publisher calls must match contract tables.
- Payload field names emitted by normalizers must match the contract payload schemas.
- Every `alert_type` string used in code must appear in the health-events contract; any new one is drift.
- `HealthPublisher` MUST NOT buffer — contract explicitly forbids it.

#### Against `CLAUDE.md`
- No `asyncio.TaskGroup` — use manual `create_task` + `add_done_callback`.
- No `asyncio.gather` inside `ExchangeAdapter.run()` — use `asyncio.wait(ALL_COMPLETED)`.
- Per-stream reconnect counters (`_stream_reconnect_attempts`) keyed by `f"{method}:{asset}"`.
- `SecretStr` end-to-end; `.get_secret_value()` only inside `connect()`.
- `asyncio.Event` for shutdown signal — never `loop.stop()` from within a task.
- Adapters communicate via callbacks only — no direct publisher/writer imports.

#### Against `{SPEC_DIR}/tasks.md`
- For any task recently marked `[X]`: spot-check the described file and behaviour exist in code.
- If the review reveals a new requirement or fix not in `tasks.md`, flag it as missing.

#### Against `README.md`
- Install commands, run commands, and package names are still accurate.
- Monorepo structure diagram matches the actual `packages/` layout.

**Output rule**: If everything aligns, write "✓ No spec drift detected". If drift is found, list each item: which doc it violates, what the doc says, what the code does, and whether the doc or the code is the source of truth.

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

### Spec Alignment
**Spec root**: `specs/<feature-id>/`

#### Drift found
- **<Doc file> ↔ <code file>:<line>** — <what the doc says> vs <what the code does>. Fix: update [doc|code].

#### ✓ Verified aligned
- spec.md FR-001 … FR-N — all requirements implemented
- data-model.md fields — match Pydantic models
- contracts/*.md — stream names, MAXLEN, payload fields, alert_type catalog
- CLAUDE.md architectural invariants — no TaskGroup, per-stream counters, SecretStr, asyncio.Event shutdown
- tasks.md — all [X] tasks spot-checked
- README.md — commands and structure accurate

### Summary
| Severity | Count |
|----------|-------|
| Critical | N     |
| Design   | N     |
| Minor    | N     |
| Spec drift | N   |
```

## Reference Documentation

- `references/code_review_checklist.md` — dimension-by-dimension checklist with pass/fail criteria
- `references/coding_standards.md` — Python and Nexus conventions
- `references/common_antipatterns.md` — real antipatterns found in this codebase
- `references/spec_alignment.md` — step-by-step guide: which spec files to load, what to compare, common drift patterns, when to update docs vs code
