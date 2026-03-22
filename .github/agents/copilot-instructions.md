# nexus Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-03-22

## Active Technologies

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

- 001-data-ingestion: Added Python 3.11+ with `asyncio` and `uvloop` + `ccxt.pro` (exchange WebSocket), `redis.asyncio` (redis-py async), `asyncpg` (TimescaleDB async), `pydantic` (schema validation), `aiohttp` (news HTTP fetching), `feedparser` (RSS parsing), `tomli` (config parsing), `FastAPI` + `uvicorn` (health endpoint)

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
