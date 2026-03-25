"""Integration tests for TimescaleDB persistence.

Tests batch write via writer, verifies record count and field values.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import pytest

from nexus_common.schemas.enums import EventType
from nexus_common.schemas.market_event import MarketEvent

from nexus_ingestion.persistence.timescale_writer import TimescaleWriter

try:
    from testcontainers.postgres import PostgresContainer

    HAS_TESTCONTAINERS = True
except ImportError:
    HAS_TESTCONTAINERS = False

pytestmark = pytest.mark.skipif(
    not HAS_TESTCONTAINERS, reason="testcontainers not available"
)

SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS market_events (
    time            TIMESTAMPTZ NOT NULL,
    source          TEXT NOT NULL,
    asset           TEXT,
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    schema_version  TEXT NOT NULL DEFAULT '1.0.0'
);

SELECT create_hypertable('market_events', 'time', if_not_exists => TRUE);
"""


def _make_event(n: int = 0) -> MarketEvent:
    return MarketEvent(
        source="binance:exchange",
        asset="BTC/USDT",
        timestamp=datetime(2026, 3, 22, 14, 30, n % 60, tzinfo=timezone.utc),
        event_type=EventType.TICK,
        schema_version="1.0.0",
        payload={"bid": 100.0 + n, "ask": 101.0 + n, "last": 100.5 + n, "volume_24h": float(n)},
    )


@pytest.fixture(scope="module")
def timescaledb_container():
    with PostgresContainer(
        "timescale/timescaledb:latest-pg16",
        username="nexus",
        password="nexus_test",
        dbname="nexus_test",
    ) as container:
        yield container


@pytest.fixture
def dsn(timescaledb_container) -> str:
    return timescaledb_container.get_connection_url().replace("+psycopg2", "")


@pytest.fixture
async def setup_schema(dsn: str):
    import asyncpg

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(SCHEMA_SQL)
        await conn.execute("TRUNCATE market_events")
    finally:
        await conn.close()


class TestTimescaleDBPersistence:
    @pytest.mark.asyncio
    async def test_batch_write_persists_records(self, dsn: str, setup_schema) -> None:
        writer = TimescaleWriter(dsn, batch_size=10, flush_interval=1.0)
        await writer.start()

        for i in range(5):
            writer.enqueue(_make_event(i))

        # Wait for flush
        await asyncio.sleep(2.0)
        await writer.stop()

        # Verify records
        import asyncpg

        conn = await asyncpg.connect(dsn)
        try:
            count = await conn.fetchval("SELECT COUNT(*) FROM market_events")
            assert count == 5

            rows = await conn.fetch("SELECT * FROM market_events ORDER BY time")
            assert len(rows) == 5
            assert rows[0]["source"] == "binance:exchange"
            assert rows[0]["asset"] == "BTC/USDT"
            assert rows[0]["event_type"] == "TICK"

            payload = json.loads(rows[0]["payload"])
            assert "bid" in payload
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_large_batch_triggers_flush(self, dsn: str, setup_schema) -> None:
        writer = TimescaleWriter(dsn, batch_size=5, flush_interval=60.0)
        await writer.start()

        # Enqueue exactly batch_size events — should trigger flush
        for i in range(5):
            writer.enqueue(_make_event(i + 10))

        await asyncio.sleep(1.0)
        await writer.stop()

        import asyncpg

        conn = await asyncpg.connect(dsn)
        try:
            count = await conn.fetchval("SELECT COUNT(*) FROM market_events WHERE (payload->>'volume_24h')::float >= 10")
            assert count == 5
        finally:
            await conn.close()
