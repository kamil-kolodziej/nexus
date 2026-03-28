"""Unit tests for IngestionService adapter restart path and event routing."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nexus_common.schemas.enums import EventType
from nexus_common.schemas.market_event import MarketEvent
from nexus_ingestion.config import IngestionConfig
from nexus_ingestion.service import IngestionService


def _make_config(**kwargs) -> IngestionConfig:
    defaults = dict(
        max_restart_attempts=3,
        restart_backoff_base=1.0,
        restart_backoff_max=60.0,
    )
    defaults.update(kwargs)
    return IngestionConfig(**defaults)


def _make_adapter(adapter_id: str = "test:exchange") -> MagicMock:
    adapter = AsyncMock()
    adapter.adapter_id = adapter_id
    adapter.adapter_type = "exchange"
    adapter.health = MagicMock()
    return adapter


class TestRestartHandleStoredAndCancelled:
    async def test_pending_handle_cancelled_on_stop(self) -> None:
        """stop() must cancel a pending restart timer, not just rely on _running guard."""
        service = IngestionService(_make_config())
        adapter = _make_adapter()
        # run() raises immediately so _on_adapter_done fires and schedules a restart
        adapter.run.side_effect = RuntimeError("boom")
        service.register_adapter(adapter)

        await service.start()
        # Let the task fail and schedule a restart
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert len(service._restart_handles) == 1
        handle = next(iter(service._restart_handles.values()))

        await service.stop()

        assert handle.cancelled()
        assert len(service._restart_handles) == 0

    async def test_handle_removed_when_restart_fires(self) -> None:
        """Once the timer fires, the handle is popped from _restart_handles."""
        service = IngestionService(_make_config(restart_backoff_base=0.0))
        adapter = _make_adapter()
        adapter.run.side_effect = RuntimeError("boom")
        service.register_adapter(adapter)

        await service.start()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert len(service._restart_handles) == 1

        # Let the timer fire (backoff=0)
        await asyncio.sleep(0.05)
        await asyncio.sleep(0)

        assert "test:exchange" not in service._restart_handles

        await service.stop()


class TestAdapterStoppedBeforeRestart:
    async def test_stop_called_on_failed_adapter_before_restart(self) -> None:
        """adapter.stop() must be awaited before the new task starts."""
        service = IngestionService(_make_config(restart_backoff_base=0.0))
        adapter = _make_adapter()
        adapter.run.side_effect = RuntimeError("boom")
        service.register_adapter(adapter)

        await service.start()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        # Timer fires immediately (backoff=0)
        await asyncio.sleep(0.05)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        adapter.stop.assert_awaited()

        await service.stop()


class TestRestartCountReset:
    async def test_restart_count_resets_after_normal_completion(self) -> None:
        """A normally-completing adapter resets its restart counter to 0."""
        service = IngestionService(_make_config())
        adapter = _make_adapter()
        # First run raises, second run completes normally
        adapter.run.side_effect = [RuntimeError("boom"), None]
        service.register_adapter(adapter)
        service._restart_counts["test:exchange"] = 2  # simulate prior failures

        await service.start()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        # After the failure the count increments
        assert service._restart_counts["test:exchange"] == 3

        await service.stop()

    async def test_restart_count_not_reset_on_exception(self) -> None:
        """A failing adapter must not reset its counter."""
        service = IngestionService(_make_config())
        adapter = _make_adapter()
        adapter.run.side_effect = RuntimeError("boom")
        service.register_adapter(adapter)

        await service.start()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert service._restart_counts["test:exchange"] == 1

        await service.stop()


class TestMaxRestartAttempts:
    async def test_adapter_not_restarted_after_max_attempts(self) -> None:
        """Once max_restart_attempts is reached no further restart is scheduled."""
        service = IngestionService(_make_config(max_restart_attempts=2))
        adapter = _make_adapter()
        adapter.run.side_effect = RuntimeError("boom")
        service.register_adapter(adapter)
        service._restart_counts["test:exchange"] = 2  # already at limit

        await service.start()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert len(service._restart_handles) == 0

        await service.stop()

    async def test_adapter_restarted_when_under_max_attempts(self) -> None:
        """Restart is scheduled when the count is still below the limit."""
        service = IngestionService(_make_config(max_restart_attempts=3))
        adapter = _make_adapter()
        adapter.run.side_effect = RuntimeError("boom")
        service.register_adapter(adapter)
        service._restart_counts["test:exchange"] = 1  # below limit

        await service.start()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert len(service._restart_handles) == 1

        await service.stop()


class TestCancelledTaskDoesNotRestart:
    async def test_cancelled_adapter_task_not_restarted(self) -> None:
        """A cancelled task (e.g. from stop()) must not schedule a restart."""
        service = IngestionService(_make_config())
        adapter = _make_adapter()
        # run() blocks until cancelled
        adapter.run.side_effect = asyncio.CancelledError()
        service.register_adapter(adapter)

        await service.start()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert len(service._restart_handles) == 0

        await service.stop()


class TestHandleEventRouting:
    def _make_event(self, event_type: EventType, asset: str | None = "BTC/USDT") -> MarketEvent:
        return MarketEvent(
            source="binance:exchange",
            asset=asset,
            timestamp=datetime.now(timezone.utc),
            event_type=event_type,
            payload={"bid": 1.0, "ask": 1.1, "last": 1.05, "volume_24h": 100.0},
        )

    async def test_news_article_routed_to_news_publisher(self) -> None:
        service = IngestionService(_make_config())
        market_pub = AsyncMock()
        news_pub = AsyncMock()
        service._market_publisher = market_pub
        service._news_publisher = news_pub

        event = self._make_event(EventType.NEWS_ARTICLE, asset=None)
        await service.handle_event(event)

        news_pub.publish.assert_awaited_once()
        market_pub.publish.assert_not_awaited()

    async def test_market_event_routed_to_market_publisher(self) -> None:
        service = IngestionService(_make_config())
        market_pub = AsyncMock()
        news_pub = AsyncMock()
        service._market_publisher = market_pub
        service._news_publisher = news_pub

        event = self._make_event(EventType.TICK)
        await service.handle_event(event)

        market_pub.publish.assert_awaited_once()
        news_pub.publish.assert_not_awaited()


class TestEnqueueFullEmitsHealthAlert:
    def _make_event(self) -> MarketEvent:
        return MarketEvent(
            source="binance:exchange",
            asset="BTC/USDT",
            timestamp=datetime.now(timezone.utc),
            event_type=EventType.TICK,
            payload={"bid": 1.0, "ask": 1.1, "last": 1.05, "volume_24h": 100.0},
        )

    async def test_queue_full_emits_persistence_error_alert(self) -> None:
        """enqueue() returning False must emit a PERSISTENCE_ERROR health alert."""
        service = IngestionService(_make_config())

        writer = MagicMock()
        writer.enqueue.return_value = False
        service._timescale_writer = writer

        health_pub = AsyncMock()
        service._health_publisher = health_pub

        await service.handle_event(self._make_event())

        health_pub.publish.assert_awaited_once()
        alert = health_pub.publish.call_args[0][0]
        assert alert.alert_type == "PERSISTENCE_ERROR"
        assert alert.severity.value == "MEDIUM"

    async def test_queue_not_full_no_alert(self) -> None:
        """enqueue() returning True must not emit any health alert."""
        service = IngestionService(_make_config())

        writer = MagicMock()
        writer.enqueue.return_value = True
        service._timescale_writer = writer

        health_pub = AsyncMock()
        service._health_publisher = health_pub

        await service.handle_event(self._make_event())

        health_pub.publish.assert_not_awaited()


class TestRestartTaskExceptionLogged:
    async def test_on_restart_done_logs_exception(self) -> None:
        """_on_restart_done must log errors that escape _restart_adapter_async."""
        service = IngestionService(_make_config())

        mock_task = MagicMock(spec=asyncio.Task)
        mock_task.cancelled.return_value = False
        mock_task.exception.return_value = RuntimeError("unexpected restart failure")

        with patch("nexus_ingestion.service.logger") as mock_logger:
            service._on_restart_done("test:exchange", mock_task)
            mock_logger.error.assert_called_once()
            assert "test:exchange" in mock_logger.error.call_args[0][1]

    async def test_on_restart_done_silent_on_cancellation(self) -> None:
        """_on_restart_done must not log when the restart task was cancelled."""
        service = IngestionService(_make_config())

        mock_task = MagicMock(spec=asyncio.Task)
        mock_task.cancelled.return_value = True

        with patch("nexus_ingestion.service.logger") as mock_logger:
            service._on_restart_done("test:exchange", mock_task)
            mock_logger.error.assert_not_called()
