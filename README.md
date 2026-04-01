# Nexus

A fully automated multi-asset trading platform built around a continuously running **probability matrix** — a live weighted consensus engine that combines signals from multiple strategy types (technical, ML, sentiment, on-chain, statistical, arbitrage) into a single composite score per asset, then acts when confidence and confirmation thresholds are met.

## What makes it different

Most trading platforms run one strategy at a time, or let multiple strategies operate independently without coordinating. Nexus takes a different approach:

- Every strategy votes with a confidence score (0.0–1.0) and a direction (BUY/SELL/HOLD)
- Signals decay exponentially toward their expiry — stale views carry less weight
- When strategies disagree, a conflict penalty dampens overall confidence rather than letting one side win
- A confirmation window prevents acting on momentary spikes
- Strategy weights are configurable and optionally adaptive — a meta-strategy tracks per-strategy accuracy over time and adjusts weights within configurable bounds
- The live probability matrix is the primary UI: you can see exactly what the system believes about each asset right now, and why

## Architecture

Event-driven microservices, all Python, deployed via Docker Compose.

```
Exchanges/APIs → nexus-ingestion → Redis Streams → nexus-strategies (per strategy)
                                                          ↓ Signal
                                               nexus-aggregator (probability loop)
                                                          ↓ TradeIntent
                                                  nexus-risk (5-layer validation)
                                                          ↓
                                               nexus-executor (ccxt) → Exchanges
                                                          ↓
                                         Redis (hot state) + TimescaleDB (history)
                                                          ↓
                                         nexus-api → WebSocket → dashboard
```

### Services

| Package | Role |
|---|---|
| `nexus-common` | Shared types: `MarketEvent`, `Signal`, `TradeIntent`, serialization |
| `nexus-ingestion` | Data source adapters: exchanges (ccxt.pro WebSocket) and news (RSS). Publishes `MarketEvent` and `NewsArticle` events to Redis Streams |
| `nexus-strategies` | Strategy interface, built-in strategies, Strategy Manager with hot-reload |
| `nexus-aggregator` | Signal aggregation + probability loop (NumPy), emits `TradeIntent` |
| `nexus-risk` | 5-layer risk validation, progressive state machine |
| `nexus-executor` | Order lifecycle, smart routing, position tracking (ccxt + ib_insync) |
| `nexus-api` | FastAPI backend for the dashboard |
| `nexus-backtest` | Backtesting engine with simulated executor |
| `dashboard` | React + TypeScript frontend |

### Probability Matrix

The aggregator maintains one row per tracked asset, updated on every new signal and every 100–500ms:

```
Asset       | Score  | Direction | Active Signals | Strongest Signal    | Updated
BTC/USDT    | +0.73  | BUY       | 5/8            | ML Model (0.91)     | 12s ago
ETH/USDT    | -0.42  | SELL      | 3/8            | Sentiment (-0.88)   | 3s ago
AAPL        | +0.15  | HOLD      | 2/6            | Technical (0.31)    | 45s ago
EUR/USD     | +0.61  | BUY       | 4/7            | Arbitrage (0.85)    | 1s ago
```

Score range: **-1.0 (strong sell)** to **+1.0 (strong buy)**. A `TradeIntent` is emitted when the score exceeds a threshold and stays there for the confirmation window.

### Risk State Machine

The Risk Manager runs a progressive state machine — not binary on/off:

```
NORMAL → CAUTIOUS → RESTRICTED → HALTED → EMERGENCY
```

Position sizes are reduced at each stage (50% in CAUTIOUS, 25% in RESTRICTED) and new entries are blocked entirely in HALTED. Every transition is logged and alerted.

### Backtesting

The backtesting engine replays historical data through the **same strategy, aggregation, and risk code** — only the executor is swapped for a simulated implementation. No separate backtesting code paths that can diverge from production behavior.

Modes: historical replay, walk-forward analysis, Monte Carlo simulation, parameter sweep, strategy comparison.

## Tech stack

- **Python** — asyncio + uvloop for all services, NumPy for signal math
- **Redis** — Pub/Sub for real-time market data, Streams with consumer groups for durable delivery
- **TimescaleDB** — market events, trade history, audit trail
- **ClickHouse** — large-scale analytics (planned)
- **React + TypeScript** — dashboard frontend
- **ccxt / ib_insync** — exchange connectivity (crypto + Interactive Brokers)

## Getting started

**Install:**
```bash
pip install -e packages/nexus-common[dev] -e packages/nexus-ingestion[dev]
```

**Set up pre-commit hooks (run once after cloning):**
```bash
pip install pre-commit
pre-commit install
```

**Run all tests:**
```bash
pytest
```

**Run unit tests only (no Docker required):**
```bash
pytest packages/nexus-ingestion/tests/unit packages/nexus-common/tests
```

**Run integration tests (requires Docker):**
```bash
docker compose -f docker-compose.dev.yml up -d
pytest packages/nexus-ingestion/tests/integration
```

**Run the full stack (all services in containers):**
```bash
cp .env.example .env              # required; add credentials for live trading (sandbox works without)
cp config.example.toml config.toml  # required; edit to add news sources, change assets, etc.
docker compose up --build
curl http://localhost:8080/health
```

**Run the ingestion service locally (infra in Docker, service on host):**
```bash
cp .env.example .env        # add credentials; .env is not loaded automatically by Python
docker compose -f docker-compose.dev.yml up -d
python -m nexus_ingestion.main  # export .env vars in your shell first
```

`.env` is only loaded automatically by `docker compose`. When running the service directly,
export the variables in your shell first (e.g. via a zsh dotenv plugin or
`export $(grep -v '^#' .env | xargs)`).

Set `NEXUS_LOG_ENV=development` for human-readable log output (default is JSON).

## Status

Active development. `nexus-common` and `nexus-ingestion` are implemented. Remaining services are in design/planning.
