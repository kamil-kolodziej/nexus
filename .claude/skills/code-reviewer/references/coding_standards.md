# Coding Standards — Nexus

Python conventions and patterns for the Nexus trading platform monorepo.

## Language & Runtime

- **Python 3.11+** (3.12 preferred). Use `from __future__ import annotations` in every module.
- **asyncio** with **uvloop** (Linux/macOS) as event loop policy.
- Type annotations on all public APIs. Use PEP 604 union syntax: `str | None`, not `Optional[str]`.
- f-strings for formatting. No `.format()` or `%` interpolation.

## Architectural Boundaries

- Keep modules grouped by role (adapters, publishers, persistence, monitoring, shared schemas/config).
- Prefer dependency direction: adapters -> service callbacks -> publishers/persistence. Avoid reverse imports.
- Keep event schema definitions centralized and reused by all services.
- Keep config loading centralized in settings classes, not scattered across runtime code.

## Naming Conventions

- **Modules**: `snake_case.py` — one primary class per module.
- **Classes**: `PascalCase` — `ExchangeAdapter`, `MarketEvent`, `TimescaleWriter`.
- **Private attributes**: `self._field` — single leading underscore.
- **Constants**: `UPPER_SNAKE_CASE` — `HEALTH_STREAM`, `HEALTH_MAXLEN`.
- **Config env vars**: `NEXUS_` prefix, `UPPER_SNAKE_CASE` — `NEXUS_EXCHANGE_API_KEY`.
- **Adapter IDs**: `{exchange_id}:{type}` — `binance:exchange`, `coindesk-rss:news`.
- **Stream names**: `nexus:{domain}-events` — `nexus:market-events`.

## Imports

Enforced by **isort** with black-compatible profile:

```python
from __future__ import annotations          # always first

import asyncio                              # stdlib
from datetime import datetime, timezone

import aiohttp                              # third-party
import structlog
from pydantic import BaseModel, Field

from nexus_common.schemas.enums import EventType   # local packages
from nexus_ingestion.adapters.base import BaseAdapter
```

- No wildcard imports (`from x import *`).
- No inline imports except when avoiding circular dependencies (document with a comment).

## Formatting

Enforced by **black** (line length 120, target Python 3.11+) and **flake8**:

- No manual style discussions in reviews — black is the authority.
- Max line length 120 (black default).
- Trailing commas in multi-line collections and function signatures.

## Error Handling

- Catch specific exceptions, never bare `except:`.
- Use `except Exception:` only as a last-resort catch-all in top-level loops with logging.
- Adapters: log + `record_error()` + continue. Never crash the event loop.
- Writers: retry with exponential backoff, emit health alert on max retries.
- Log with `exc_info=True` for unexpected errors; `exc_info=False` for expected/handled ones.

## Logging

- Use `structlog.get_logger()` (module-level or bound in `__init__`). Never use `logging.getLogger(__name__)`.
- Bind per-object context at construction — `self._logger = structlog.get_logger().bind(adapter_id=self.adapter_id)` — so every log line from that instance carries the context automatically.
- Use snake_case event names as the first positional arg; all context as keyword args:
  ```python
  # correct
  self._logger.info("exchange_adapter_connected", exchange_id=self._exchange_id, sandbox=self._sandbox)
  self._logger.error("batch_write_failed", max_retries=max_retries, error=str(e))

  # wrong — printf-style format strings
  logger.info("ExchangeAdapter connected: %s (sandbox=%s)", exchange_id, sandbox)
  ```
- Levels: `DEBUG` for normalization/trace details, `INFO` for lifecycle events, `WARNING` for recoverable errors, `ERROR` for failures requiring attention.
- Never log credentials or full Redis URLs with passwords — use `_sanitize_url()` for Redis URLs.
- Call `configure_logging(env=os.getenv("NEXUS_LOG_ENV", "production"))` once at service startup (`main()`). `NEXUS_LOG_ENV=development` produces human-readable console output; default is JSON.
- `exc_info=True` as a keyword arg surfaces the full traceback in structured output: `logger.error("event", exc_info=True)`.

## asyncio Patterns

```python
# Correct: manual task supervision (FR-004 adapter isolation)
task = asyncio.create_task(self._run_adapter(adapter), name=f"adapter:{aid}")
task.add_done_callback(lambda t, aid=aid: self._on_adapter_done(aid, t))

# Wrong: TaskGroup (one failure cancels all siblings)
# async with asyncio.TaskGroup() as tg:
#     tg.create_task(...)
```

- Use `asyncio.wait_for(coro, timeout=N)` for operations that might hang.
- Use `asyncio.to_thread(blocking_fn, ...)` for CPU/blocking work (e.g., RSS parsing).
- Use `asyncio.Queue` to decouple producers from consumers.

## Pydantic Patterns

```python
class Tick(BaseModel):
    bid: float = Field(gt=0)
    ask: float = Field(gt=0)

    @model_validator(mode="after")
    def ask_gte_bid(self) -> Tick:
        if self.ask < self.bid:
            raise ValueError(f"ask ({self.ask}) must be >= bid ({self.bid})")
        return self
```

- Use `Field()` constraints for simple bounds.
- Use `model_validator` for cross-field validation.
- Use `field_validator` for single-field transforms (e.g., ensure UTC).
- Config classes: `BaseSettings` + `SettingsConfigDict`, never manual `os.getenv`.

## Testing Conventions

- Separate tests by intent: unit, integration, and contract/snapshot.
- No `__init__.py` in test directories (use `--import-mode=importlib`).
- Fixtures in the test file or a `conftest.py` at the test directory level.
- Name tests `test_<behavior>`, not `test_<method_name>`.
- Integration tests requiring Docker are separated so unit tests can run without infrastructure.

## Pre-commit Hooks

Expected hooks (configured in `.pre-commit-config.yaml`):

| Hook | Purpose | Config |
|------|---------|--------|
| **ruff** | Lint + import sorting + pyupgrade + bugbear + asyncio correctness | `[tool.ruff]` in `pyproject.toml` |
| **ruff-format** | Code formatting (replaces black) | `[tool.ruff]` in `pyproject.toml` |
| **mypy strict** | Type checking on all source modules (tests excluded) | `[tool.mypy]` in `pyproject.toml` |
| **bandit** | Security scanning (hardcoded secrets, unsafe calls) | `[tool.bandit]` in `pyproject.toml` |
| **detect-secrets** | Credential leak prevention | `.secrets.baseline` |
| **pre-commit-hooks** | Trailing whitespace, EOF, YAML/TOML syntax, merge conflicts, debug statements | — |

Run manually: `pre-commit run --all-files`. Runs automatically on `git commit`.
