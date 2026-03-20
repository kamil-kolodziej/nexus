<!--
Sync Impact Report
- Version change: template (unversioned placeholders) -> 1.0.0
- Modified principles:
	- PRINCIPLE_1_NAME -> I. Safety-First Execution Gates (NON-NEGOTIABLE)
	- PRINCIPLE_2_NAME -> II. Event-Driven Python Runtime
	- PRINCIPLE_3_NAME -> III. Redis-First Messaging Contracts
	- PRINCIPLE_4_NAME -> IV. Library-First Exchange Integrations
	- PRINCIPLE_5_NAME -> V. Spec-Code Traceability
- Added principles:
	- VI. Safety-Critical Test Coverage
	- VII. Clear Service Boundaries
- Added sections:
	- Architecture & Operational Constraints
	- Development Workflow & Quality Gates
- Removed sections:
	- Placeholder section names/content from template
- Templates requiring updates:
	- ✅ .specify/templates/plan-template.md
	- ✅ .specify/templates/spec-template.md
	- ✅ .specify/templates/tasks-template.md
	- ⚠ pending: .specify/templates/commands/*.md (directory not present)
	- ✅ docs/workflow.md
	- ✅ README.md (reviewed; no change required)
- Follow-up TODOs:
	- TODO(COMMAND_TEMPLATES_DIR): Initialize .specify/templates/commands/ when command templates are added.
-->

# Nexus Constitution

## Core Principles

### I. Safety-First Execution Gates (NON-NEGOTIABLE)
Every trade intent MUST pass risk validation before any execution path can submit,
modify, or cancel orders. The execution service MUST reject unvalidated intents and
emit an auditable reason. Circuit-breaker and kill-switch states MUST short-circuit
all new order flow immediately.

Rationale: Capital preservation and controllable failure modes are more important than
throughput or strategy aggressiveness.

### II. Event-Driven Python Runtime
Core services MUST be implemented in Python 3.11+ and communicate through asynchronous,
event-driven interfaces using `asyncio`. Signal aggregation and probability-matrix math
MUST use NumPy-backed vectorized operations instead of ad hoc per-item loops where
performance is material.

Rationale: A unified Python runtime reduces operational complexity while meeting
seconds-level latency goals.

### III. Redis-First Messaging Contracts
Redis Pub/Sub MUST be the default low-latency fanout path, and Redis Streams with
consumer groups MUST be the durable path for recoverable workflows such as trade intents,
risk decisions, and replay-capable event channels. Message schemas MUST be versioned and
validated at service boundaries.

Rationale: One messaging substrate reduces failure surface and keeps replay and
durability behavior explicit.

### IV. Library-First Exchange Integrations
Exchange connectivity MUST use vetted libraries first: `ccxt`/`ccxt.pro` for crypto and
`ib_insync` for Interactive Brokers. Custom connectors MAY be introduced only when a
required capability is unavailable in those libraries, and the exception MUST document
the gap, fallback behavior, and test strategy.

Rationale: Reusing mature integrations reduces implementation risk and maintenance load.

### V. Spec-Code Traceability
Behavior-changing code MUST be linked to a feature spec, and spec updates MUST be
committed in the same branch when behavior changes intentionally. Plans and tasks MUST
trace to explicit spec requirements, constraints, and success criteria.

Rationale: Specs are living control documents, not throwaway planning artifacts.

### VI. Safety-Critical Test Coverage
Changes to risk manager logic, signal aggregation logic, execution safeguards, and
inter-service contracts MUST include automated tests. Regression tests and property-based
tests are mandatory for safety-critical logic; replay-based end-to-end tests are mandatory
when changing trade or risk decision flow.

Rationale: The highest-impact failures are logic regressions in decision and risk paths.

### VII. Clear Service Boundaries
Responsibilities MUST remain explicit across ingestion, strategy engine, aggregator,
risk manager, executor, and backtester/API layers. Cross-service changes MUST update
contracts and avoid mixing domain logic with transport, storage, or UI concerns.

Rationale: Strong boundaries limit blast radius and keep reasoning tractable as
capabilities expand.

## Architecture & Operational Constraints

- The authoritative service model is event-driven microservices with Python-only runtime.
- All externally sourced market/exchange payloads MUST be normalized into shared event
	schemas before strategy or risk processing.
- Paper trading mode MUST be available for any change touching execution behavior.
- Runtime configuration, risk limits, and kill-switch controls MUST be auditable.
- Services MUST be deployable independently, and schema compatibility MUST be preserved
	before rolling out producer and consumer changes.

## Development Workflow & Quality Gates

- The default lifecycle is: specify -> plan -> tasks -> implement -> review -> test.
- Every plan MUST include a Constitution Check and fail fast on unmet principles.
- Every implementation branch MUST pass tests relevant to changed safety and contract
	surfaces before merge.
- Reviews MUST explicitly check: risk gating, messaging contracts, spec alignment,
	and service-boundary compliance.
- Drift between code and spec MUST be resolved before merge by either code changes or
	spec updates in the same branch.

## Governance

This constitution overrides conflicting guidance in project workflow documents and
templates.

Amendment process:
- Propose changes via PR with rationale, impacted principles, and migration notes.
- Include a Sync Impact Report covering affected templates, commands, and runtime docs.
- Obtain explicit approval from the repository owner before merge.

Versioning policy (semantic versioning):
- MAJOR: Backward-incompatible governance or principle removals/redefinitions.
- MINOR: New principle/section or materially expanded mandatory guidance.
- PATCH: Clarifications, wording fixes, and non-semantic refinements.

Compliance review expectations:
- Each planning artifact MUST document how constitutional gates are satisfied.
- Each PR review MUST verify test obligations for safety-critical and contract changes.
- Periodic governance review MUST occur at least monthly and after any incident.

**Version**: 1.0.0 | **Ratified**: 2026-03-20 | **Last Amended**: 2026-03-20
