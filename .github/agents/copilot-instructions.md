# nexus Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-04-02

## Active Technologies
- Python 3.11+ (`asyncio`, optional `uvloop`) + `pydantic`/`pydantic-settings`, `redis.asyncio`, `asyncpg`, `FastAPI` + `uvicorn`, `vaderSentiment`, optional `transformers` + `torch` for FinBERT, `orjson` for payload serialization (002-sentiment-pipeline)
- Redis Streams (`nexus:news-events`, `nexus:sentiment-events`, `nexus:sentiment-health-events`), TimescaleDB async persistence, versioned dictionary file in repository (002-sentiment-pipeline)
- Python 3.11+ with `asyncio` + `redis.asyncio` (redis-py async), `asyncpg` (TimescaleDB async), `pydantic` (schema validation), `vaderSentiment` (default NLP processor), `transformers` + `torch` (optional FinBERT processor), `FastAPI` + `uvicorn` (health endpoint), `nexus-common` (shared schemas) (002-sentiment-pipeline)
- Redis Streams (input: `nexus:news-events`, output: `nexus:sentiment-events`, health: `nexus:sentiment-health-events`), TimescaleDB (historical persistence of `SentimentScore` events) (002-sentiment-pipeline)
- Python 3.11+ + `nexus-common`, `redis[hiredis]>=5.0`, `pydantic>=2.0`, `pydantic-settings>=2.0`, `vaderSentiment>=3.3`, `asyncpg>=0.29`, `fastapi>=0.110`, `uvicorn>=0.27`, `structlog>=24.0`, `uvloop>=0.19` (optional). FinBERT extra: `transformers>=4.40`, `torch>=2.0`. (002-sentiment-pipeline)
- Redis Streams (input: `nexus:news-events`, output: `nexus:sentiment-events`, health: `nexus:sentiment-health-events`), TimescaleDB (async persistence of sentiment scores) (002-sentiment-pipeline)

- Python 3.11+ with `asyncio` and `uvloop` + `ccxt.pro` (exchange WebSocket), `redis.asyncio` (redis-py async), `asyncpg` (TimescaleDB async), `pydantic` (schema validation), `aiohttp` (news HTTP fetching), `feedparser` (RSS parsing), `tomli` (config parsing), `FastAPI` + `uvicorn` (health endpoint) (001-data-ingestion)

## Project Structure

```text
src/
tests/
```

## Commands

cd src [ONLY COMMANDS FOR ACTIVE TECHNOLOGIES][ONLY COMMANDS FOR ACTIVE TECHNOLOGIES] pytest [ONLY COMMANDS FOR ACTIVE TECHNOLOGIES][ONLY COMMANDS FOR ACTIVE TECHNOLOGIES] ruff check .

## Code Style

Python 3.11+ with `asyncio` and `uvloop`: Follow standard conventions

## Recent Changes
- 002-sentiment-pipeline: Added Python 3.11+ + `nexus-common`, `redis[hiredis]>=5.0`, `pydantic>=2.0`, `pydantic-settings>=2.0`, `vaderSentiment>=3.3`, `asyncpg>=0.29`, `fastapi>=0.110`, `uvicorn>=0.27`, `structlog>=24.0`, `uvloop>=0.19` (optional). FinBERT extra: `transformers>=4.40`, `torch>=2.0`.
- 002-sentiment-pipeline: Added Python 3.11+ with `asyncio` + `redis.asyncio` (redis-py async), `asyncpg` (TimescaleDB async), `pydantic` (schema validation), `vaderSentiment` (default NLP processor), `transformers` + `torch` (optional FinBERT processor), `FastAPI` + `uvicorn` (health endpoint), `nexus-common` (shared schemas)
- 002-sentiment-pipeline: Added Python 3.11+ (`asyncio`, optional `uvloop`) + `pydantic`/`pydantic-settings`, `redis.asyncio`, `asyncpg`, `FastAPI` + `uvicorn`, `vaderSentiment`, optional `transformers` + `torch` for FinBERT, `orjson` for payload serialization


<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
