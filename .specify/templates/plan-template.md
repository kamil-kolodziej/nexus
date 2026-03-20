# Implementation Plan: [FEATURE]

**Branch**: `[###-feature-name]` | **Date**: [DATE] | **Spec**: [link]
**Input**: Feature specification from `/specs/[###-feature-name]/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

[Extract from feature spec: primary requirement + technical approach from research]

## Technical Context

<!--
  ACTION REQUIRED: Replace the content in this section with the technical details
  for the project. The structure here is presented in advisory capacity to guide
  the iteration process.
-->

**Language/Version**: [e.g., Python 3.11+ or NEEDS CLARIFICATION]
**Primary Dependencies**: [e.g., FastAPI, NumPy, Pydantic, redis-py, ccxt, ib_insync or NEEDS CLARIFICATION]
**Storage**: [if applicable, e.g., TimescaleDB, Redis, ClickHouse, files or N/A]
**Testing**: [e.g., pytest, hypothesis, syrupy, testcontainers-python or NEEDS CLARIFICATION]
**Target Platform**: [e.g., Linux server, Docker Compose, local workstation or NEEDS CLARIFICATION]
**Project Type**: [e.g., event-driven microservice, web-service, worker, library or NEEDS CLARIFICATION]
**Performance Goals**: [domain-specific, e.g., seconds-level decision latency, replay throughput, p95 targets or NEEDS CLARIFICATION]
**Constraints**: [domain-specific, e.g., mandatory risk validation, contract compatibility, paper-mode requirement or NEEDS CLARIFICATION]
**Scale/Scope**: [domain-specific, e.g., assets tracked, event rates, strategy count or NEEDS CLARIFICATION]

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- [ ] Safety-first execution gate: Every execution path requires explicit risk validation and kill-switch handling.
- [ ] Event-driven Python gate: Runtime uses Python `asyncio`; compute-heavy scoring paths use NumPy/vectorized operations where relevant.
- [ ] Redis-first messaging gate: Pub/Sub and Streams channel usage is defined, with schema versioning and consumer-group behavior documented.
- [ ] Library-first integration gate: `ccxt`/`ib_insync` usage is documented, or exceptions are justified with fallback/test coverage.
- [ ] Spec-code traceability gate: Requirements map to planned tasks and contract updates.
- [ ] Safety-critical testing gate: Regression/property/replay tests are listed for risk, aggregation, execution, or contract changes.
- [ ] Service-boundary gate: Ingestion, strategy, aggregator, risk, executor, and API/backtest responsibilities remain explicit.

## Project Structure

### Documentation (this feature)

```text
specs/[###-feature]/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)
<!--
  ACTION REQUIRED: Replace the placeholder tree below with the concrete layout
  for this feature. Delete unused options and expand the chosen structure with
  real paths (e.g., apps/admin, packages/something). The delivered plan must
  not include Option labels.
-->

```text
# [REMOVE IF UNUSED] Option 1: Single project (DEFAULT)
src/
├── models/
├── services/
├── cli/
└── lib/

tests/
├── contract/
├── integration/
└── unit/

# [REMOVE IF UNUSED] Option 2: Web application (when "frontend" + "backend" detected)
backend/
├── src/
│   ├── models/
│   ├── services/
│   └── api/
└── tests/

frontend/
├── src/
│   ├── components/
│   ├── pages/
│   └── services/
└── tests/

# [REMOVE IF UNUSED] Option 3: Mobile + API (when "iOS/Android" detected)
api/
└── [same as backend above]

ios/ or android/
└── [platform-specific structure: feature modules, UI flows, platform tests]
```

**Structure Decision**: [Document the selected structure and reference the real
directories captured above]

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| [e.g., 4th project] | [current need] | [why 3 projects insufficient] |
| [e.g., Repository pattern] | [specific problem] | [why direct DB access insufficient] |
