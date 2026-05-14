"""Integration test for Redis Stream consumer/producer round-trip."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

try:
    from testcontainers.redis import RedisContainer
except ImportError:
    pytest.skip("testcontainers not installed", allow_module_level=True)

from nexus_common.schemas.enums import EventType
from nexus_common.schemas.market_event import MarketEvent, NewsArticle, SentimentScore
from nexus_sentiment.config import SentimentConfig
from nexus_sentiment.processors.vader_processor import VaderProcessor
from nexus_sentiment.publishers.health_publisher import HealthPublisher
from nexus_sentiment.publishers.redis_publisher import RedisPublisher
from nexus_sentiment.service import SentimentService


@pytest.fixture(scope="module")
def redis_container():
    with RedisContainer("redis:7-alpine") as container:
        yield container


@pytest.fixture
async def redis_client(redis_container):
    from nexus_common.redis_client import create_redis_client

    url = f"redis://localhost:{redis_container.get_exposed_port(6379)}"
    client = await create_redis_client(url)
    yield client
    await client.aclose()


class TestRedisConsumerRoundTrip:
    """Integration: publish NewsArticle, verify SentimentScore in output stream."""

    async def test_vader_end_to_end(self, redis_client, monkeypatch):
        monkeypatch.setenv("NEXUS_CONFIG_FILE", "/nonexistent")
        config = SentimentConfig(
            redis_url="unused",
            active_assets=["BTC/USDT"],
        )

        processor = VaderProcessor()
        await processor.load()

        redis_publisher = RedisPublisher(redis_client, config.output_stream, maxlen=1000)
        health_publisher = HealthPublisher(redis_client, config.health_stream, maxlen=100)

        service = SentimentService(
            config=config,
            redis=redis_client,
            processor=processor,
            redis_publisher=redis_publisher,
            health_publisher=health_publisher,
        )

        # Create consumer group
        try:
            await redis_client.xgroup_create(
                config.input_stream, config.consumer_group, id="$", mkstream=True
            )
        except Exception:
            pass

        # Publish a NewsArticle
        article_event = MarketEvent(
            source="test:news",
            asset=None,
            timestamp=datetime.now(UTC),
            event_type=EventType.NEWS_ARTICLE,
            schema_version="1.0.0",
            payload=NewsArticle(
                headline="Bitcoin surges past $100K on institutional demand",
                body_summary="BTC rallied strongly.",
                url="https://example.com/btc-rally",
                source_name="test",
                published_at=datetime.now(UTC),
                related_assets=["BTC/USDT"],
            ).model_dump(),
        )
        await redis_client.xadd(config.input_stream, article_event.to_redis_fields())

        # Start service, process one message, stop
        await service.start()
        await asyncio.sleep(1)
        await service.stop()

        # Read output stream
        entries = await redis_client.xrange(config.output_stream, "-", "+")
        assert len(entries) >= 1

        _entry_id, fields = entries[0]
        output_event = MarketEvent.from_redis_fields(fields)
        assert output_event.event_type == EventType.SENTIMENT_SCORE
        assert output_event.source == "nexus-sentiment:vader"

        payload = SentimentScore.model_validate(output_event.payload)
        assert payload.asset == "BTC/USDT"
        assert payload.score > 0  # positive news
        assert payload.model_id.startswith("vader:")

        await processor.close()
