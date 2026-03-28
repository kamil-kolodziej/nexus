# Spec Alignment Guide — Nexus

How to systematically verify that code and documentation stay in sync.

---

## 1. Determine the Spec Root

Derive the feature ID from the branch name or review arguments:

| Branch / argument | Feature ID | Spec root |
|-------------------|-----------|-----------|
| `branch:001-data-ingestion` | `001-data-ingestion` | `specs/001-data-ingestion/` |
| `branch:002-strategy-engine` | `002-strategy-engine` | `specs/002-strategy-engine/` |

If ambiguous: `ls specs/` and pick the best match.

---

## 2. Files to Load (read before reviewing code)

```
{SPEC_DIR}/spec.md           ← requirements (FR-*, SRC-*), acceptance scenarios, edge cases
{SPEC_DIR}/data-model.md     ← entity fields, types, validation rules
{SPEC_DIR}/contracts/        ← one file per stream/interface; read them all
{SPEC_DIR}/tasks.md          ← which tasks are claimed complete
{SPEC_DIR}/plan.md           ← design decisions and rationale
CLAUDE.md                    ← platform-wide architectural invariants
README.md                    ← commands and package structure
```

---

## 3. What to Compare

### spec.md → code

| Spec element | Where to look in code | What can drift |
|---|---|---|
| FR-* functional requirements | adapter implementations, service wiring | feature missing, partially implemented, or contradicts spec |
| SRC-* safety constraints | adapters, config, serialization | constraint bypassed or weakened |
| Acceptance scenarios (Given/When/Then) | adapters, service, tests | documented outcome doesn't match code path |
| Edge cases | normalizer methods, error handlers | behavior changed but edge case text not updated |

**Common drift pattern**: a bug fix changes edge-case behavior but only updates the code comment, not `spec.md`.

### data-model.md → Pydantic models

| Data-model element | Where to look | What can drift |
|---|---|---|
| Entity field names | `nexus_common/schemas/*.py` | field renamed in code but not in spec |
| Field types | model annotations | type widened/narrowed without spec update |
| Validation rules | `field_validator`, `model_validator`, `Field(gt=0)` | rule removed or weakened |
| `asset=None` for non-asset events | news normalizer | `asset=""` used instead |

### contracts/*.md → publishers and normalizers

| Contract element | Where to look in code | What can drift |
|---|---|---|
| Stream name | `RedisPublisher` constructor calls in `main.py`, `service.py` | typo, renamed stream |
| MAXLEN value | publisher constructor `maxlen=` arg | value changed without contract update |
| Payload field names | normalizer `payload={...}` dicts | field added/renamed without contract update |
| `alert_type` strings | every `HealthAlert(alert_type=...)` call | new alert type added in code but not in `contracts/health-events.md` |
| `HealthPublisher` no-buffer rule | `HealthPublisher.__init__` | buffer accidentally added |

**Common drift pattern**: a new health alert type is added to code, never added to the contract. `NEWS_SOURCE_RECOVERED` was an example of this.

### CLAUDE.md → structural code patterns

| CLAUDE.md invariant | Grep target | Violation looks like |
|---|---|---|
| No `asyncio.TaskGroup` | `TaskGroup` | any match in adapters or service |
| No `asyncio.gather` in `run()` | `asyncio.gather` in adapter files | replaces `asyncio.wait(ALL_COMPLETED)` |
| Per-stream counters by key | `_stream_reconnect_attempts` | single `int` counter instead of `dict` |
| `SecretStr` end-to-end | `get_secret_value()` call sites | called outside `connect()` |
| `asyncio.Event` for shutdown | `loop.stop()` | called from within a task |
| Callbacks only — no direct imports | adapter files | `from nexus_ingestion.publishers import ...` |

### tasks.md → code

For each recently marked `[X]` task: verify the described file and behavior exist. If a task says "emit `PERSISTENCE_ERROR` health alert on queue full" — grep for it.

If the review surfaces new work not in `tasks.md`, add it.

### README.md → repo state

- `pip install -e packages/nexus-common[dev] -e packages/nexus-ingestion[dev]` — does this still work?
- `pytest` — does the root config still discover all tests?
- `packages/` structure diagram — does it match `ls packages/`?

---

## 4. Output Format

Report as a sub-section of the code review:

```
### Spec Alignment
**Spec root**: `specs/<feature-id>/`

#### Drift found
- **contracts/health-events.md ↔ news_adapter.py:195**
  Code emits `NEWS_SOURCE_RECOVERED` but this alert_type is not in the contract.
  Fix: add row to contracts/health-events.md alert type table.

#### ✓ Verified aligned
- spec.md FR-001 … FR-011 — all requirements implemented
- data-model.md fields — match Pydantic models in nexus_common/schemas/
- contracts/market-events.md — stream name, MAXLEN, all payload fields correct
- contracts/health-events.md — all alert_type strings present
- contracts/news-events.md — payload fields match news normalizer output
- CLAUDE.md invariants — no TaskGroup, no gather in run(), per-stream counters, SecretStr end-to-end
- tasks.md — spot-checked [X] tasks T036, T038, T043, T044
- README.md — commands accurate, packages/ structure matches
```

If no drift: write `✓ No spec drift detected` and list what was verified.

---

## 5. When to Update Docs vs Code

| Situation | Action |
|-----------|--------|
| Code correctly implements new behavior; spec describes old behavior | Update the spec to match the code |
| Spec describes correct requirement; code diverges from it | Fix the code |
| New alert type added to code | Add to `contracts/health-events.md` and to `spec.md` edge cases if relevant |
| New payload field added to a normalizer | Add to relevant `contracts/*.md` payload schema |
| New config field added | Update `data-model.md`, TOML mapping docs, and README env var list |
| Task completed | Mark `[X]` in `tasks.md` with the implementing file |

**Rule of thumb**: specs describe what the code *does*, not what was originally intended. If code and spec diverge, update the spec — unless the divergence is a bug.
