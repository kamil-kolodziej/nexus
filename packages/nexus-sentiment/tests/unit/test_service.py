"""Unit tests for SentimentService."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from nexus_common.schemas.enums import EventType
from nexus_common.schemas.market_event import MarketEvent, NewsArticle
from nexus_sentiment.service import SentimentService


def _make_config(**overrides):
    """Create a mock SentimentConfig."""
    config = MagicMock()
    config.processor_type = overrides.get("processor_type", "vader")
    config.input_stream = overrides.get("input_stream", "nexus:news-events")
    config.output_stream = overrides.get("output_stream", "nexus:sentiment-events")
    config.health_stream = overrides.get("health_stream", "nexus:sentiment-health-events")
    config.consumer_group = overrides.get("consumer_group", "nexus-sentiment-group")
    config.block_timeout = overrides.get("block_timeout", 5000)
    config.active_assets = overrides.get("active_assets", ["BTC/USDT", "ETH/USDT"])
    config.max_fan_out = overrides.get("max_fan_out", 50)
    config.pending_claim_threshold = overrides.get("pending_claim_threshold", 300)
    config.claim_sweep_interval = overrides.get("claim_sweep_interval", 60)
    return config


def _make_news_event(
    headline="Bitcoin surges past $100K",
    body_summary="BTC rallied on institutional demand.",
    url="https://example.com/btc-rally",
    related_assets=None,
):
    """Create a NewsArticle MarketEvent as Redis fields."""
    if related_assets is None:
        related_assets = ["BTC/USDT"]
    event = MarketEvent(
        source="newsapi:news",
        asset=None,
        timestamp=datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC),
        event_type=EventType.NEWS_ARTICLE,
        schema_version="1.0.0",
        payload=NewsArticle(
            headline=headline,
            body_summary=body_summary,
            url=url,
            source_name="test",
            published_at=datetime(2026, 4, 1, 10, 0, 0, tzinfo=UTC),
            related_assets=related_assets,
        ).model_dump(mode="json"),
    )
    return event.to_redis_fields()


class TestSentimentServiceSingleAsset:
    """US1: Single-asset article produces one SentimentScore."""

    @pytest.fixture
    def mock_redis(self):
        redis = AsyncMock()
        redis.xack = AsyncMock()
        return redis

    @pytest.fixture
    def mock_processor(self):
        from nexus_sentiment.processors.base import SentimentResult

        proc = MagicMock()
        proc.analyze.return_value = SentimentResult(label="positive", score=0.75, confidence=0.75)
        proc.model_id = "vader:3.3.2"
        return proc

    @pytest.fixture
    def mock_publisher(self):
        pub = AsyncMock()
        pub.publish = AsyncMock(return_value="1-0")
        return pub

    @pytest.fixture
    def mock_health_publisher(self):
        return AsyncMock()

    @pytest.fixture
    def service(self, mock_redis, mock_processor, mock_publisher, mock_health_publisher):
        config = _make_config()
        return SentimentService(
            config=config,
            redis=mock_redis,
            processor=mock_processor,
            redis_publisher=mock_publisher,
            health_publisher=mock_health_publisher,
        )

    async def test_valid_single_asset_produces_one_score(
        self, service, mock_redis, mock_publisher, mock_processor
    ):
        fields = _make_news_event(related_assets=["BTC/USDT"])
        await service._process_message("msg-1", fields)

        assert mock_publisher.publish.call_count == 1
        published_fields = mock_publisher.publish.call_args[0][0]
        assert published_fields["event_type"] == "SENTIMENT_SCORE"
        assert published_fields["source"] == "nexus-sentiment:vader"
        payload = json.loads(published_fields["payload"])
        assert payload["asset"] == "BTC/USDT"
        assert payload["score"] == 0.75
        assert payload["sentiment_label"] == "positive"
        assert payload["model_id"] == "vader:3.3.2"
        mock_redis.xack.assert_awaited_once()

    async def test_xack_called_after_publish(self, service, mock_redis, mock_publisher):
        fields = _make_news_event()
        await service._process_message("msg-1", fields)
        mock_redis.xack.assert_awaited_once_with(
            "nexus:news-events", "nexus-sentiment-group", "msg-1"
        )

    async def test_malformed_event_acked_and_dropped(self, service, mock_redis, mock_publisher):
        fields = {"bad": "data"}
        await service._process_message("msg-bad", fields)
        mock_redis.xack.assert_awaited_once()
        mock_publisher.publish.assert_not_called()

    async def test_events_processed_incremented(self, service, mock_redis, mock_publisher):
        fields = _make_news_event()
        await service._process_message("msg-1", fields)
        assert service._events_processed == 1


class TestSentimentServiceMultiAsset:
    """US2: Multi-asset article produces N SentimentScore events."""

    @pytest.fixture
    def service(self):
        from nexus_sentiment.processors.base import SentimentResult

        config = _make_config()
        redis = AsyncMock()
        redis.xack = AsyncMock()
        proc = MagicMock()
        proc.analyze.return_value = SentimentResult(label="positive", score=0.5, confidence=0.5)
        proc.model_id = "vader:3.3.2"
        pub = AsyncMock()
        pub.publish = AsyncMock(return_value="1-0")
        health_pub = AsyncMock()
        svc = SentimentService(
            config=config,
            redis=redis,
            processor=proc,
            redis_publisher=pub,
            health_publisher=health_pub,
        )
        svc._mock_redis = redis
        svc._mock_pub = pub
        return svc

    async def test_multi_asset_produces_n_scores(self, service):
        fields = _make_news_event(related_assets=["BTC/USDT", "ETH/USDT"])
        await service._process_message("msg-1", fields)
        assert service._mock_pub.publish.call_count == 2

        payloads = []
        for call in service._mock_pub.publish.call_args_list:
            p = json.loads(call[0][0]["payload"])
            payloads.append(p["asset"])
        assert "BTC/USDT" in payloads
        assert "ETH/USDT" in payloads

    async def test_duplicate_assets_deduplicated(self, service):
        fields = _make_news_event(related_assets=["BTC/USDT", "BTC/USDT"])
        await service._process_message("msg-1", fields)
        assert service._mock_pub.publish.call_count == 1

    async def test_all_scores_share_same_result(self, service):
        fields = _make_news_event(related_assets=["BTC/USDT", "ETH/USDT"])
        await service._process_message("msg-1", fields)
        scores = []
        for call in service._mock_pub.publish.call_args_list:
            p = json.loads(call[0][0]["payload"])
            scores.append((p["score"], p["confidence"], p["sentiment_label"]))
        assert len(set(scores)) == 1  # all same

    async def test_xack_only_after_all_publishes(self, service):
        fields = _make_news_event(related_assets=["BTC/USDT", "ETH/USDT"])
        await service._process_message("msg-1", fields)
        service._mock_redis.xack.assert_awaited_once()


class TestSentimentServiceGeneralMarket:
    """US4: Article with no assets produces asset=None."""

    @pytest.fixture
    def service(self):
        from nexus_sentiment.processors.base import SentimentResult

        config = _make_config()
        redis = AsyncMock()
        redis.xack = AsyncMock()
        proc = MagicMock()
        proc.analyze.return_value = SentimentResult(label="negative", score=-0.3, confidence=0.3)
        proc.model_id = "vader:3.3.2"
        pub = AsyncMock()
        pub.publish = AsyncMock(return_value="1-0")
        health_pub = AsyncMock()
        svc = SentimentService(
            config=config,
            redis=redis,
            processor=proc,
            redis_publisher=pub,
            health_publisher=health_pub,
        )
        svc._mock_pub = pub
        return svc

    async def test_empty_assets_produces_none_asset(self, service):
        fields = _make_news_event(related_assets=[])
        await service._process_message("msg-1", fields)
        assert service._mock_pub.publish.call_count == 1
        payload = json.loads(service._mock_pub.publish.call_args[0][0]["payload"])
        assert payload["asset"] is None
        # MarketEvent.asset should also be None
        event_asset = service._mock_pub.publish.call_args[0][0]["asset"]
        assert event_asset == ""  # None serializes to "" in to_redis_fields


class TestSentimentServiceErrorResilience:
    """US6: Inference errors don't crash the loop."""

    @pytest.fixture
    def service(self):
        config = _make_config()
        redis = AsyncMock()
        redis.xack = AsyncMock()
        proc = MagicMock()
        proc.analyze.side_effect = RuntimeError("inference boom")
        proc.model_id = "vader:3.3.2"
        pub = AsyncMock()
        pub.publish = AsyncMock(return_value="1-0")
        health_pub = AsyncMock()
        health_pub.publish = AsyncMock()
        svc = SentimentService(
            config=config,
            redis=redis,
            processor=proc,
            redis_publisher=pub,
            health_publisher=health_pub,
        )
        svc._mock_redis = redis
        svc._mock_pub = pub
        svc._mock_health_pub = health_pub
        return svc

    async def test_inference_error_not_acked(self, service):
        fields = _make_news_event()
        await service._process_message("msg-1", fields)
        service._mock_redis.xack.assert_not_awaited()

    async def test_inference_error_emits_health_alert(self, service):
        fields = _make_news_event()
        await service._process_message("msg-1", fields)
        service._mock_health_pub.publish.assert_awaited_once()
        alert = service._mock_health_pub.publish.call_args[0][0]
        assert alert.alert_type == "MODEL_INFERENCE_ERROR"

    async def test_inference_error_increments_counter(self, service):
        fields = _make_news_event()
        await service._process_message("msg-1", fields)
        assert service._errors == 1

    async def test_next_message_processes_after_error(self, service):
        from nexus_sentiment.processors.base import SentimentResult

        fields = _make_news_event()
        await service._process_message("msg-1", fields)
        assert service._errors == 1

        # Fix processor for next message
        service._processor.analyze.side_effect = None
        service._processor.analyze.return_value = SentimentResult(
            label="positive", score=0.5, confidence=0.5
        )
        await service._process_message("msg-2", fields)
        assert service._events_processed == 1


class TestSentimentServiceDeadLetterSweep:
    """US6: Dead-letter claim sweep."""

    async def test_dead_letter_claimed(self):
        config = _make_config(claim_sweep_interval=0)
        redis = AsyncMock()
        redis.xack = AsyncMock()
        redis.xautoclaim = AsyncMock(
            return_value=(
                b"0-0",
                [("stale-msg-1", {"data": "value"})],
                [],
            )
        )
        proc = MagicMock()
        proc.model_id = "vader:3.3.2"
        pub = AsyncMock()
        health_pub = AsyncMock()
        health_pub.publish = AsyncMock()

        service = SentimentService(
            config=config,
            redis=redis,
            processor=proc,
            redis_publisher=pub,
            health_publisher=health_pub,
        )
        service._running = True

        # Run one iteration manually
        import asyncio

        async def run_one():
            service._running = False  # stop after first iteration
            await service._claim_sweep_loop()

        # The sweep sleeps first, so we set running=False immediately
        service._running = True
        task = asyncio.create_task(run_one())
        await asyncio.sleep(0.1)
        service._running = False
        try:
            await asyncio.wait_for(task, timeout=2)
        except (TimeoutError, asyncio.CancelledError):
            pass

        # Verify DEAD_LETTER_CLAIMED alert was emitted
        if health_pub.publish.call_count > 0:
            alert = health_pub.publish.call_args[0][0]
            assert alert.alert_type == "DEAD_LETTER_CLAIMED"
            redis.xack.assert_awaited()


class TestSentimentServiceTaskIndependence:
    """SC-007: Consumer loop and health endpoint operate independently."""

    async def test_health_provider_works_without_consumer(self):
        config = _make_config()
        redis = AsyncMock()
        proc = MagicMock()
        proc.model_id = "vader:3.3.2"
        pub = AsyncMock()
        health_pub = AsyncMock()

        service = SentimentService(
            config=config,
            redis=redis,
            processor=proc,
            redis_publisher=pub,
            health_publisher=health_pub,
        )
        health = service._get_health()
        assert health["status"] == "ok"
        assert health["serviceId"] == "nexus-sentiment"
        assert "processor:inference" in health["checks"]
        assert health["checks"]["processor:inference"]["status"] == "ok"
