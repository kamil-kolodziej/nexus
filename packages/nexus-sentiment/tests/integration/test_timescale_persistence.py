"""Integration test for TimescaleDB persistence of sentiment scores."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

try:
    from testcontainers.postgres import PostgresContainer
except ImportError:
    pytest.skip("testcontainers not installed", allow_module_level=True)

import asyncpg
from nexus_common.schemas.enums import EventType
from nexus_common.schemas.market_event import MarketEvent
from nexus_sentiment.persistence.timescale_writer import TimescaleWriter

INIT_SQL = """
CREATE TABLE IF NOT EXISTS sentiment_scores (
    time            TIMESTAMPTZ NOT NULL,
    source          TEXT NOT NULL,
    asset           TEXT,
    article_url     TEXT NOT NULL,
    score           DOUBLE PRECISION NOT NULL,
    confidence      DOUBLE PRECISION NOT NULL,
    sentiment_label TEXT NOT NULL,
    model_id        TEXT NOT NULL,
    schema_version  TEXT NOT NULL DEFAULT '1.0.0'
);
"""


@pytest.fixture(scope="module")
def pg_container():
    with PostgresContainer("postgres:16") as container:
        yield container


@pytest.fixture
async def pg_dsn(pg_container):
    dsn = pg_container.get_connection_url().replace("psycopg2", "asyncpg").replace("+asyncpg", "")
    # Normalize to asyncpg-compatible DSN
    dsn = dsn.replace("postgresql+asyncpg", "postgresql").replace(
        "postgresql+psycopg2", "postgresql"
    )
    conn = await asyncpg.connect(dsn)
    await conn.execute(INIT_SQL)
    await conn.close()
    return dsn


class TestTimescalePersistence:
    """Integration: verify SentimentScore batch write to sentiment_scores table."""

    async def test_batch_write(self, pg_dsn):
        writer = TimescaleWriter(pg_dsn, batch_size=10, flush_interval=1.0)
        await writer.start()

        event = MarketEvent(
            source="nexus-sentiment:vader",
            asset="BTC/USDT",
            timestamp=datetime.now(UTC),
            event_type=EventType.SENTIMENT_SCORE,
            schema_version="1.0.0",
            payload={
                "article_url": "https://example.com/btc",
                "asset": "BTC/USDT",
                "score": 0.75,
                "confidence": 0.85,
                "sentiment_label": "positive",
                "model_id": "vader:3.3.2",
            },
        )
        writer.enqueue(event)
        await asyncio.sleep(2)  # wait for flush
        await writer.stop()

        conn = await asyncpg.connect(pg_dsn)
        rows = await conn.fetch("SELECT * FROM sentiment_scores")
        await conn.close()

        assert len(rows) == 1
        assert rows[0]["asset"] == "BTC/USDT"
        assert rows[0]["score"] == 0.75
        assert rows[0]["model_id"] == "vader:3.3.2"
