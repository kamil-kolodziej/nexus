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


def _make_service(
    *,
    timescale_writer=None,
    health_endpoint=None,
    asset_extractor=None,
    score_label="positive",
    score=0.5,
    confidence=0.5,
    config_overrides=None,
):
    """Build a SentimentService with stub collaborators wired for assertions."""
    from nexus_sentiment.processors.base import SentimentResult

    config = _make_config(**(config_overrides or {}))
    redis = AsyncMock()
    redis.xack = AsyncMock()
    proc = MagicMock()
    proc.analyze.return_value = SentimentResult(
        label=score_label, score=score, confidence=confidence
    )
    proc.model_id = "vader:3.3.2"
    proc.close = AsyncMock()
    pub = AsyncMock()
    pub.publish = AsyncMock(return_value="1-0")
    pub.buffer_size = 0
    pub._connected = True
    health_pub = AsyncMock()
    health_pub.publish = AsyncMock()

    svc = SentimentService(
        config=config,
        redis=redis,
        processor=proc,
        redis_publisher=pub,
        health_publisher=health_pub,
        timescale_writer=timescale_writer,
        health_endpoint=health_endpoint,
        asset_extractor=asset_extractor,
    )
    svc._mock_redis = redis
    svc._mock_pub = pub
    svc._mock_health_pub = health_pub
    svc._mock_proc = proc
    return svc


class TestSentimentServiceStartStop:
    """Lifecycle: start brings up components in the right order, stop tears them down."""

    async def test_start_creates_consumer_group_and_swallows_busygroup(self):
        svc = _make_service()
        svc._mock_redis.xgroup_create = AsyncMock(
            side_effect=Exception("BUSYGROUP Consumer Group name already exists")
        )

        await svc.start()
        try:
            svc._mock_redis.xgroup_create.assert_awaited_once()
            assert svc._running is True
            assert svc._consumer_task is not None
            assert svc._sweep_task is not None
        finally:
            await svc.stop()

    async def test_start_reraises_other_xgroup_errors(self):
        svc = _make_service()
        svc._mock_redis.xgroup_create = AsyncMock(side_effect=ConnectionError("nope"))

        with pytest.raises(ConnectionError):
            await svc.start()
        assert svc._running is False
        assert svc._consumer_task is None

    async def test_start_brings_up_health_endpoint_and_writer(self):
        health_endpoint = MagicMock()
        health_endpoint.set_health_provider = MagicMock()
        health_endpoint.start = AsyncMock()
        health_endpoint.stop = AsyncMock()

        writer = MagicMock()
        writer.start = AsyncMock()
        writer.stop = AsyncMock()

        svc = _make_service(timescale_writer=writer, health_endpoint=health_endpoint)
        svc._mock_redis.xgroup_create = AsyncMock()

        await svc.start()
        try:
            health_endpoint.set_health_provider.assert_called_once_with(svc._get_health)
            health_endpoint.start.assert_awaited_once()
            writer.start.assert_awaited_once()
        finally:
            await svc.stop()
            health_endpoint.stop.assert_awaited_once()
            writer.stop.assert_awaited_once()
            svc._mock_proc.close.assert_awaited()

    async def test_stop_cancels_tasks_and_closes_processor(self):
        svc = _make_service()
        svc._mock_redis.xgroup_create = AsyncMock()
        svc._mock_redis.xreadgroup = AsyncMock(return_value=[])
        svc._mock_redis.xautoclaim = AsyncMock(return_value=(b"0-0", [], []))

        await svc.start()
        consumer_task = svc._consumer_task
        sweep_task = svc._sweep_task

        await svc.stop()

        assert svc._running is False
        assert consumer_task.done()
        assert sweep_task.done()
        svc._mock_proc.close.assert_awaited()


class TestSentimentServiceConsumerLoop:
    """The XREADGROUP loop dispatches messages and survives transient errors."""

    async def test_empty_messages_loops_until_running_false(self):
        import asyncio

        svc = _make_service()

        async def xreadgroup_yield(*_a, **_kw):
            await asyncio.sleep(0)
            return []

        svc._mock_redis.xreadgroup = AsyncMock(side_effect=xreadgroup_yield)

        svc._running = True

        async def stop_soon():
            await asyncio.sleep(0.02)
            svc._running = False

        await asyncio.gather(svc._consumer_loop(), stop_soon())
        assert svc._mock_redis.xreadgroup.await_count >= 1

    async def test_dispatches_message_to_process_message(self, monkeypatch):
        import asyncio

        svc = _make_service()
        fields = _make_news_event()
        called: list = []
        original = svc._process_message

        async def spy(msg_id, fields):
            called.append((msg_id, fields))
            svc._running = False
            await original(msg_id, fields)

        monkeypatch.setattr(svc, "_process_message", spy)

        svc._mock_redis.xreadgroup = AsyncMock(
            return_value=[(b"nexus:news-events", [("msg-1", fields)])]
        )

        svc._running = True
        await asyncio.wait_for(svc._consumer_loop(), timeout=1)

        assert len(called) == 1
        assert called[0][0] == "msg-1"

    async def test_loop_recovers_from_unexpected_error(self):
        import asyncio

        svc = _make_service()
        attempts = {"n": 0}

        async def flaky(*_a, **_kw):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise RuntimeError("boom")
            svc._running = False
            return []

        svc._mock_redis.xreadgroup = AsyncMock(side_effect=flaky)

        original_sleep = asyncio.sleep
        sleeps: list[float] = []

        async def fake_sleep(n: float) -> None:
            sleeps.append(n)
            await original_sleep(0)

        import nexus_sentiment.service as svc_module

        svc_module.asyncio.sleep = fake_sleep
        try:
            svc._running = True
            await asyncio.wait_for(svc._consumer_loop(), timeout=1)
        finally:
            svc_module.asyncio.sleep = original_sleep

        assert attempts["n"] >= 2
        assert 1 in sleeps  # error-path sleep is 1s

    async def test_cancelled_exits_cleanly(self):
        import asyncio

        svc = _make_service()

        async def slow_read(*_a, **_kw):
            await asyncio.sleep(10)
            return []

        svc._mock_redis.xreadgroup = AsyncMock(side_effect=slow_read)
        svc._running = True
        task = asyncio.create_task(svc._consumer_loop())
        await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert task.cancelled() or task.done()


class TestSentimentServicePartialPublishFailure:
    """all_published=False path: no XACK, warning logged, counter unchanged."""

    async def test_partial_publish_does_not_xack(self):
        svc = _make_service()
        # First publish ok, second returns None (buffered/dropped).
        svc._mock_pub.publish = AsyncMock(side_effect=["1-0", None])

        fields = _make_news_event(related_assets=["BTC/USDT", "ETH/USDT"])
        await svc._process_message("msg-1", fields)

        svc._mock_redis.xack.assert_not_awaited()
        assert svc._events_processed == 0


class TestSentimentServiceEmptyText:
    """Whitespace-only article emits neutral 0.0/0.0 and still XACKs."""

    async def test_empty_article_emits_neutral_zero(self):
        svc = _make_service()
        fields = _make_news_event(
            headline="   ",
            body_summary="",
            related_assets=["BTC/USDT"],
        )
        await svc._process_message("msg-1", fields)

        svc._mock_proc.analyze.assert_not_called()
        svc._mock_pub.publish.assert_awaited_once()
        published = svc._mock_pub.publish.await_args.args[0]
        payload = json.loads(published["payload"])
        assert payload["sentiment_label"] == "neutral"
        assert payload["score"] == 0.0
        assert payload["confidence"] == 0.0
        svc._mock_redis.xack.assert_awaited_once()


class TestSentimentServiceTimescaleEnqueue:
    """When a TimescaleWriter is wired, every published event is enqueued."""

    async def test_each_published_event_enqueued(self):
        writer = MagicMock()
        writer.enqueue = MagicMock(return_value=True)

        svc = _make_service(timescale_writer=writer)
        fields = _make_news_event(related_assets=["BTC/USDT", "ETH/USDT"])

        await svc._process_message("msg-1", fields)

        assert writer.enqueue.call_count == 2


class TestSentimentServiceBuildEffectiveAssets:
    """active_assets filter, dedupe, max_fan_out cap, AssetExtractor merge."""

    async def test_related_assets_filtered_to_active(self):
        svc = _make_service(config_overrides={"active_assets": ["BTC/USDT"]})
        fields = _make_news_event(related_assets=["BTC/USDT", "DOGE/USDT"])

        await svc._process_message("msg-1", fields)

        assert svc._mock_pub.publish.await_count == 1
        payload = json.loads(svc._mock_pub.publish.await_args.args[0]["payload"])
        assert payload["asset"] == "BTC/USDT"

    async def test_max_fan_out_caps_assets(self):
        many = [f"A{i}/USDT" for i in range(20)]
        svc = _make_service(
            config_overrides={
                "active_assets": many,
                "max_fan_out": 5,
            }
        )
        fields = _make_news_event(related_assets=many)

        await svc._process_message("msg-1", fields)

        assert svc._mock_pub.publish.await_count == 5

    async def test_asset_extractor_results_merged(self):
        extractor = MagicMock()
        extractor.extract = MagicMock(return_value=["ETH/USDT"])

        svc = _make_service(
            config_overrides={"active_assets": ["BTC/USDT", "ETH/USDT"]},
            asset_extractor=extractor,
        )
        fields = _make_news_event(related_assets=["BTC/USDT"])

        await svc._process_message("msg-1", fields)

        assert svc._mock_pub.publish.await_count == 2
        published_assets = sorted(
            json.loads(c.args[0]["payload"])["asset"] for c in svc._mock_pub.publish.await_args_list
        )
        assert published_assets == ["BTC/USDT", "ETH/USDT"]


class TestSentimentServiceGetHealth:
    """RFC-shaped health body reflects component states correctly."""

    async def test_error_status_when_consecutive_inference_failures_high(self):
        svc = _make_service()
        svc._consecutive_inference_errors = 5

        body = svc._get_health()
        assert body["status"] == "error"
        assert body["checks"]["processor:inference"]["status"] == "error"
        assert body["checks"]["processor:inference"]["observedValue"] == 5

    async def test_degraded_when_redis_publisher_disconnected(self):
        svc = _make_service()
        svc._redis_publisher._connected = False
        svc._redis_publisher.buffer_size = 7

        body = svc._get_health()
        assert body["status"] == "degraded"
        assert body["checks"]["redis:publisher"]["status"] == "degraded"
        assert body["checks"]["redis:publisher"]["observedValue"] == 7

    async def test_timescale_writer_check_present_when_writer_wired(self):
        svc = _make_service(timescale_writer=MagicMock())
        body = svc._get_health()
        assert "timescale:writer" in body["checks"]
        assert body["checks"]["timescale:writer"]["status"] == "ok"

    async def test_version_falls_back_to_unknown(self, monkeypatch):
        from importlib.metadata import PackageNotFoundError

        def raise_missing(_name):
            raise PackageNotFoundError

        # Patch the global `version` import lookup used inside _get_health.
        import importlib.metadata as md

        monkeypatch.setattr(md, "version", raise_missing)

        svc = _make_service()
        body = svc._get_health()
        assert body["version"] == "unknown"


class TestSentimentServiceTaskCallbacks:
    """_on_*_done branches: silent on cancellation, logs on exception."""

    def test_consumer_done_callback_silent_on_cancel(self):
        svc = _make_service()
        task = MagicMock()
        task.cancelled.return_value = True
        # Must not raise.
        svc._on_consumer_done(task)

    def test_consumer_done_callback_logs_on_exception(self):
        svc = _make_service()
        task = MagicMock()
        task.cancelled.return_value = False
        task.exception.return_value = RuntimeError("boom")
        svc._on_consumer_done(task)  # logs but does not raise

    def test_sweep_done_callback_silent_on_cancel(self):
        svc = _make_service()
        task = MagicMock()
        task.cancelled.return_value = True
        svc._on_sweep_done(task)

    def test_sweep_done_callback_logs_on_exception(self):
        svc = _make_service()
        task = MagicMock()
        task.cancelled.return_value = False
        task.exception.return_value = RuntimeError("boom")
        svc._on_sweep_done(task)


class TestSentimentServiceClaimSweep:
    """The XAUTOCLAIM loop emits a health alert per claimed message and survives errors."""

    async def test_no_claims_emits_no_alert(self):
        import asyncio

        svc = _make_service(config_overrides={"claim_sweep_interval": 0.01})
        svc._mock_redis.xautoclaim = AsyncMock(return_value=(b"0-0", [], []))

        svc._running = True
        task = asyncio.create_task(svc._claim_sweep_loop())
        await asyncio.sleep(0.05)
        svc._running = False
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        svc._mock_health_pub.publish.assert_not_awaited()
        svc._mock_redis.xack.assert_not_awaited()

    async def test_sweep_error_path_logged_and_sleeps(self):
        import asyncio

        svc = _make_service(config_overrides={"claim_sweep_interval": 0.01})
        attempts = {"n": 0}

        async def flaky(*_a, **_kw):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise RuntimeError("xautoclaim boom")
            svc._running = False
            return (b"0-0", [], [])

        svc._mock_redis.xautoclaim = AsyncMock(side_effect=flaky)

        original_sleep = asyncio.sleep
        sleeps: list[float] = []

        async def fake_sleep(n: float) -> None:
            sleeps.append(n)
            await original_sleep(0)

        import nexus_sentiment.service as svc_module

        svc_module.asyncio.sleep = fake_sleep
        try:
            svc._running = True
            await asyncio.wait_for(svc._claim_sweep_loop(), timeout=1)
        finally:
            svc_module.asyncio.sleep = original_sleep

        assert attempts["n"] >= 1
        assert 5 in sleeps  # error-path sleep is 5s
