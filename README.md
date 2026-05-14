# Nexus

A fully automated multi-asset trading platform with seconds-level execution. Multiple pluggable weighted strategies — technical, ML, sentiment, on-chain, statistical, arbitrage — run independently and produce signals. A signal aggregation layer combines those signals into trade decisions, which then pass through a 5-layer risk manager before reaching the execution engine.

## What makes it different

Most trading platforms run one strategy at a time, or let multiple strategies operate independently without coordinating. Nexus takes a different approach:

- Every strategy votes with a confidence score (0.0–1.0) and a direction (BUY/SELL/HOLD)
- Strategies are scoped to trading horizons (SCALP, INTRADAY, SWING, POSITION) — each horizon is independent, so a SCALP SELL and a SWING BUY on the same asset can coexist as separate positions
- Strategy weights are configurable per `(strategy, horizon)` pair and apply immediately from the dashboard
- The aggregation layer is designed as a swappable service — the public implementation is deliberately simple; you can drop in your own aggregation logic by subclassing `TradeIntentService` without changing anything else
- Same code paths for live trading, paper trading, and backtesting — no divergence between what was backtested and what runs live

## Architecture

Event-driven microservices, all Python, deployed via Docker Compose.

```
Exchanges/APIs → nexus-ingestion → Redis Streams → nexus-strategies (per strategy)
                       │                                  ↓ Signal
                       └─→ nexus-sentiment ──→ Redis Streams ─┘
                                               nexus-aggregator (signal aggregation)
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
| `nexus-sentiment` | NLP pipeline that consumes `NewsArticle` events, scores them with VADER or FinBERT, fans out one `SentimentScore` per asset/sector |
| `nexus-strategies` | Strategy interface, built-in strategies, Strategy Manager with hot-reload |
| `nexus-aggregator` | Signal aggregation service, emits `TradeIntent` |
| `nexus-risk` | 5-layer risk validation, progressive state machine |
| `nexus-executor` | Order lifecycle, smart routing, position tracking (ccxt + ib_insync) |
| `nexus-api` | FastAPI backend for the dashboard |
| `nexus-backtest` | Backtesting engine with simulated executor |
| `dashboard` | React + TypeScript frontend |

### Signal Aggregation

The aggregator tracks active signals per `(asset, horizon)` and emits a `TradeIntent` when the strongest signal exceeds a configurable confidence threshold. The dashboard shows the current signal state per asset:

```
Asset       | Score  | Direction | Active Signals | Strongest Signal    | Updated
BTC/USDT    | +0.91  | BUY       | 3              | Technical (0.91)    | 12s ago
ETH/USDT    | -0.88  | SELL      | 2              | Sentiment (-0.88)   | 3s ago
AAPL        | +0.31  | HOLD      | 1              | Technical (0.31)    | 45s ago
EUR/USD     | +0.85  | BUY       | 2              | Arbitrage (0.85)    | 1s ago
```

Score range: **-1.0 (strong sell)** to **+1.0 (strong buy)**. The aggregator is a swappable service — implement `TradeIntentService` to replace the default logic with your own.

### Risk State Machine

The Risk Manager runs a progressive state machine — not binary on/off:

```
NORMAL → CAUTIOUS → RESTRICTED → HALTED → EMERGENCY
```

Position sizes are reduced at each stage (50% in CAUTIOUS, 25% in RESTRICTED) and new entries are blocked entirely in HALTED. Every transition is logged and alerted.

### Backtesting

The backtesting engine replays historical data through the **same strategy, aggregation, and risk code** — only the executor is swapped for a simulated implementation. No separate backtesting code paths that can diverge from production behavior.

Modes: historical replay, walk-forward analysis, strategy comparison.

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
pip install -e packages/nexus-common[dev] -e packages/nexus-ingestion[dev] -e packages/nexus-sentiment[dev]
```

For the FinBERT processor (optional, ~440 MB model download on first run):
```bash
pip install -e packages/nexus-sentiment[finbert]
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

**Run the sentiment service locally:**
```bash
docker compose -f docker-compose.dev.yml up -d   # needs Redis + TimescaleDB
python -m nexus_sentiment.main                    # consumes nexus:news-events, publishes nexus:sentiment-events
```

`.env` is only loaded automatically by `docker compose`. When running the service directly,
export the variables in your shell first (e.g. via a zsh dotenv plugin or
`export $(grep -v '^#' .env | xargs)`).

Set `NEXUS_LOG_ENV=development` for human-readable log output (default is JSON).

## Status

Active development. `nexus-common`, `nexus-ingestion`, and `nexus-sentiment` are implemented. Remaining services are in design/planning.
