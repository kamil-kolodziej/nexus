# Quickstart: Data Ingestion Layer

**Branch**: `001-data-ingestion`

## Prerequisites

- Python 3.11+
- Docker & Docker Compose (for Redis and TimescaleDB)
- Binance testnet API credentials (free at https://testnet.binance.vision/)

## Setup

### 1. Clone and switch branch

```bash
git clone git@github.com:kamil-kolodziej/nexus.git
cd nexus
git checkout 001-data-ingestion
```

### 2. Start infrastructure

```bash
docker compose -f docker-compose.dev.yml up -d redis timescaledb
```

This starts:
- **Redis** on `localhost:6379`
- **TimescaleDB** on `localhost:5432`

### 3. Install dependencies

```bash
cd packages/nexus-common
pip install -e ".[dev]"

cd ../nexus-ingestion
pip install -e ".[dev]"
```

### 4. Configure

Copy the example config and set your credentials:

```bash
cp config.example.toml config.toml
```

Set exchange credentials via environment variables:

```bash
export NEXUS_EXCHANGE_API_KEY="your-testnet-api-key"
export NEXUS_EXCHANGE_API_SECRET="your-testnet-api-secret"
```

### 5. Run the service

```bash
python -m nexus_ingestion.main
```

The service will:
1. Connect to Binance testnet WebSocket
2. Subscribe to configured assets (default: `BTC/USDT`)
3. Start publishing `MarketEvent` records to `nexus:market-events` Redis Stream
4. Expose health endpoint at `http://localhost:8080/health`

### 6. Verify events

In another terminal, read from the Redis Stream:

```bash
redis-cli XREAD COUNT 5 BLOCK 5000 STREAMS nexus:market-events 0
```

You should see events within 5 seconds of startup.

### 7. Check health

```bash
curl http://localhost:8080/health
```

Expected response:

```json
{
  "status": "ok",
  "adapters": [
    {
      "adapter_id": "binance:exchange",
      "adapter_type": "exchange",
      "status": "CONNECTED",
      "last_event_at": "2026-03-22T14:30:00.123Z",
      "event_count": 42,
      "error_count": 0,
      "malformed_count": 0
    }
  ]
}
```

## Running Tests

```bash
# Unit tests (no infrastructure required)
cd packages/nexus-ingestion
pytest tests/unit/ -v

# Integration tests (requires Docker containers running)
pytest tests/integration/ -v

# Contract/snapshot tests
pytest tests/contract/ -v

# All tests
pytest -v
```

## Key Configuration Options

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `NEXUS_EXCHANGE_ID` | `binance` | Exchange identifier (ccxt exchange id) |
| `NEXUS_EXCHANGE_API_KEY` | — | Exchange API key (required) |
| `NEXUS_EXCHANGE_API_SECRET` | — | Exchange API secret (required) |
| `NEXUS_SUBSCRIBED_ASSETS` | `["BTC/USDT"]` | JSON array of asset symbols |
| `NEXUS_REDIS_URL` | `redis://localhost:6379` | Redis connection string |
| `NEXUS_TIMESCALEDB_DSN` | `postgresql://...` | TimescaleDB connection string |
| `NEXUS_REDIS_BUFFER_MAX` | `10000` | Max in-memory buffer during Redis outage |
| `NEXUS_BATCH_SIZE` | `500` | TimescaleDB write batch size |
| `NEXUS_FLUSH_INTERVAL` | `5.0` | TimescaleDB flush interval (seconds) |
| `NEXUS_NEWS_POLL_INTERVAL` | `300` | News source polling interval (seconds) |
| `NEXUS_EXCHANGE_SANDBOX` | `true` | Use exchange sandbox/testnet mode |

## Architecture Overview

```
                    ┌─────────────────────┐
                    │   Binance WebSocket  │
                    │   (ccxt.pro)         │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  ExchangeAdapter    │
                    │  (asyncio task)     │
                    └──────────┬──────────┘
                               │ MarketEvent
            ┌──────────────────┼──────────────────┐
            ▼                  ▼                  ▼
┌───────────────────┐ ┌───────────────┐ ┌──────────────────┐
│ Redis Publisher   │ │ TimescaleDB   │ │ Gap Detector     │
│ → market-events   │ │ Writer (async │ │ → health-events  │
│ → news-events     │ │ background)   │ │   stream         │
└───────────────────┘ └───────────────┘ └──────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Health Endpoint    │
                    │  GET /health        │
                    │  (FastAPI+uvicorn)  │
                    └─────────────────────┘
```
