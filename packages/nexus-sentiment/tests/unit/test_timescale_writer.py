"""Unit tests for TimescaleWriter (batching + retries + persistence-error alert)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

from nexus_common.schemas.enums import EventType, Severity
from nexus_common.schemas.market_event import MarketEvent, SentimentScore
from nexus_sentiment.persistence import timescale_writer as tw_module
from nexus_sentiment.persistence.timescale_writer import TimescaleWriter


def _score_event(asset: str = "BTC/USDT", url: str = "https://example.com/a") -> MarketEvent:
    payload = SentimentScore(
        article_url=url,
        asset=asset,
        score=0.5,
        confidence=0.5,
        sentiment_label="positive",
        model_id="vader:3.3.2",
    )
    return MarketEvent(
        source="nexus-sentiment:vader",
        asset=asset,
        timestamp=datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC),
        event_type=EventType.SENTIMENT_SCORE,
        schema_version="1.0.0",
        payload=payload.model_dump(),
    )


def _invalid_payload_event() -> MarketEvent:
    """A SENTIMENT_SCORE envelope with payload that fails SentimentScore validation."""
    return MarketEvent.model_construct(
        source="nexus-sentiment:vader",
        asset=None,
        timestamp=datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC),
        event_type=EventType.SENTIMENT_SCORE,
        schema_version="1.0.0",
        payload={"not": "a valid sentiment score"},
    )


def _make_pool(copy_mock: AsyncMock) -> MagicMock:
    """Build a fake asyncpg pool whose acquire() yields a conn with copy_records_to_table."""
    conn = MagicMock()
    conn.copy_records_to_table = copy_mock
    acquire_cm = MagicMock()
    acquire_cm.__aenter__ = AsyncMock(return_value=conn)
    acquire_cm.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acquire_cm)
    pool.close = AsyncMock()
    return pool


class TestEnqueue:
    async def test_enqueue_success(self):
        writer = TimescaleWriter(dsn="postgresql://x", queue_maxsize=10)
        assert writer.enqueue(_score_event()) is True
        assert writer._queue.qsize() == 1

    async def test_enqueue_full_returns_false(self):
        writer = TimescaleWriter(dsn="postgresql://x", queue_maxsize=1)
        assert writer.enqueue(_score_event()) is True
        assert writer.enqueue(_score_event()) is False
        assert writer._queue.qsize() == 1


class TestWriteBatch:
    async def test_writes_records_with_correct_columns(self):
        copy = AsyncMock()
        pool = _make_pool(copy)
        writer = TimescaleWriter(dsn="postgresql://x")
        writer._pool = pool

        ev = _score_event()
        await writer._write_batch([ev])

        copy.assert_awaited_once()
        kwargs = copy.await_args.kwargs
        assert kwargs["columns"] == [
            "time",
            "source",
            "asset",
            "article_url",
            "score",
            "confidence",
            "sentiment_label",
            "model_id",
            "schema_version",
        ]
        (record,) = kwargs["records"]
        assert record[0] == ev.timestamp
        assert record[1] == "nexus-sentiment:vader"
        assert record[2] == "BTC/USDT"
        assert record[3] == "https://example.com/a"
        assert record[4] == 0.5
        assert record[5] == 0.5
        assert record[6] == "positive"
        assert record[7] == "vader:3.3.2"
        assert record[8] == "1.0.0"

    async def test_skips_invalid_payload(self):
        copy = AsyncMock()
        pool = _make_pool(copy)
        writer = TimescaleWriter(dsn="postgresql://x")
        writer._pool = pool

        good = _score_event(asset="BTC/USDT")
        bad = _invalid_payload_event()
        await writer._write_batch([bad, good])

        copy.assert_awaited_once()
        records = copy.await_args.kwargs["records"]
        assert len(records) == 1
        assert records[0][2] == "BTC/USDT"

    async def test_skips_when_all_records_invalid(self):
        copy = AsyncMock()
        pool = _make_pool(copy)
        writer = TimescaleWriter(dsn="postgresql://x")
        writer._pool = pool

        await writer._write_batch([_invalid_payload_event()])

        copy.assert_not_awaited()

    async def test_empty_batch_is_noop(self):
        writer = TimescaleWriter(dsn="postgresql://x")
        writer._pool = _make_pool(AsyncMock())
        await writer._write_batch([])
        writer._pool.acquire.assert_not_called()

    async def test_no_pool_is_noop(self):
        writer = TimescaleWriter(dsn="postgresql://x")
        writer._pool = None
        # Should not raise.
        await writer._write_batch([_score_event()])

    async def test_retry_then_success(self, monkeypatch):
        sleeps: list[float] = []

        async def fake_sleep(n: float) -> None:
            sleeps.append(n)

        monkeypatch.setattr(tw_module.asyncio, "sleep", fake_sleep)

        copy = AsyncMock(side_effect=[ConnectionError("boom"), None])
        pool = _make_pool(copy)
        writer = TimescaleWriter(dsn="postgresql://x")
        writer._pool = pool

        await writer._write_batch([_score_event()])

        assert copy.await_count == 2
        # First retry uses delay 2**1 = 2 seconds.
        assert sleeps == [2]

    async def test_exhausts_retries_emits_persistence_error_alert(self, monkeypatch):
        async def fake_sleep(_n: float) -> None:
            return

        monkeypatch.setattr(tw_module.asyncio, "sleep", fake_sleep)

        copy = AsyncMock(side_effect=ConnectionError("permanent"))
        pool = _make_pool(copy)

        captured: list = []

        async def health_cb(alert) -> None:
            captured.append(alert)

        writer = TimescaleWriter(dsn="postgresql://x", health_callback=health_cb)
        writer._pool = pool

        await writer._write_batch([_score_event(), _score_event()])

        # 1 initial + 3 retries = 4 attempts.
        assert copy.await_count == 4
        assert len(captured) == 1
        alert = captured[0]
        assert alert.alert_type == "PERSISTENCE_ERROR"
        assert alert.adapter_id == "nexus-sentiment"
        assert alert.severity == Severity.MEDIUM
        assert "permanent" in alert.message
        assert "Queue depth: 2" in alert.message


class TestEmitPersistenceError:
    async def test_no_callback_short_circuits(self):
        writer = TimescaleWriter(dsn="postgresql://x")
        # Should not raise.
        await writer._emit_persistence_error(queue_depth=0, error_msg="x")

    async def test_sync_callback_invoked(self):
        captured: list = []

        def sync_cb(alert) -> None:
            captured.append(alert)

        writer = TimescaleWriter(dsn="postgresql://x", health_callback=sync_cb)
        await writer._emit_persistence_error(queue_depth=5, error_msg="boom")
        assert len(captured) == 1
        assert captured[0].alert_type == "PERSISTENCE_ERROR"

    async def test_async_callback_awaited(self):
        captured: list = []

        async def async_cb(alert) -> None:
            captured.append(alert)

        writer = TimescaleWriter(dsn="postgresql://x", health_callback=async_cb)
        await writer._emit_persistence_error(queue_depth=5, error_msg="boom")
        assert len(captured) == 1


class TestStartStop:
    async def test_start_creates_pool_and_writer_task(self, monkeypatch):
        copy = AsyncMock()
        pool = _make_pool(copy)
        create_pool = AsyncMock(return_value=pool)
        monkeypatch.setattr(tw_module.asyncpg, "create_pool", create_pool)

        writer = TimescaleWriter(dsn="postgresql://x", flush_interval=10.0)
        await writer.start()
        try:
            create_pool.assert_awaited_once_with("postgresql://x", min_size=2, max_size=5)
            assert writer._pool is pool
            assert writer._running is True
            assert writer._task is not None
        finally:
            writer._running = False
            assert writer._task is not None
            writer._task.cancel()
            try:
                await writer._task
            except asyncio.CancelledError:
                pass

    async def test_stop_cancels_task_flushes_and_closes_pool(self, monkeypatch):
        copy = AsyncMock()
        pool = _make_pool(copy)
        create_pool = AsyncMock(return_value=pool)
        monkeypatch.setattr(tw_module.asyncpg, "create_pool", create_pool)

        writer = TimescaleWriter(dsn="postgresql://x", flush_interval=10.0)
        await writer.start()
        writer.enqueue(_score_event())

        await writer.stop()

        assert writer._running is False
        pool.close.assert_awaited_once()
        # The remaining enqueued event must have been written by either the
        # writer-loop or the post-cancel _flush_batch.
        assert copy.await_count >= 1


class TestWriterLoop:
    async def test_loop_flushes_when_batch_size_hit(self):
        copy = AsyncMock()
        pool = _make_pool(copy)
        writer = TimescaleWriter(dsn="postgresql://x", batch_size=2, flush_interval=10.0)
        writer._pool = pool
        writer._running = True

        writer.enqueue(_score_event())
        writer.enqueue(_score_event())

        loop_task = asyncio.create_task(writer._writer_loop())
        await asyncio.sleep(0.05)
        writer._running = False
        loop_task.cancel()
        try:
            await loop_task
        except asyncio.CancelledError:
            pass

        copy.assert_awaited()
        records = copy.await_args.kwargs["records"]
        assert len(records) == 2

    async def test_loop_flushes_on_timeout(self):
        copy = AsyncMock()
        pool = _make_pool(copy)
        writer = TimescaleWriter(dsn="postgresql://x", batch_size=100, flush_interval=0.02)
        writer._pool = pool
        writer._running = True

        writer.enqueue(_score_event())

        loop_task = asyncio.create_task(writer._writer_loop())
        await asyncio.sleep(0.1)
        writer._running = False
        loop_task.cancel()
        try:
            await loop_task
        except asyncio.CancelledError:
            pass

        copy.assert_awaited()
        records = copy.await_args.kwargs["records"]
        assert len(records) == 1

    async def test_loop_recovers_from_unexpected_error(self, monkeypatch):
        # Force the inner await asyncio.wait_for() to raise something other than
        # TimeoutError / CancelledError so the broad except path fires.
        original_wait_for = asyncio.wait_for
        original_sleep = asyncio.sleep
        crashes = {"count": 0}

        async def flaky_wait_for(coro, timeout):
            crashes["count"] += 1
            if crashes["count"] == 1:
                # Discard the coroutine to avoid a 'never awaited' warning.
                coro.close()
                raise RuntimeError("synthetic")
            return await original_wait_for(coro, timeout)

        sleeps: list[float] = []

        async def fake_sleep(n: float) -> None:
            sleeps.append(n)
            await original_sleep(0)

        monkeypatch.setattr(tw_module.asyncio, "wait_for", flaky_wait_for)
        monkeypatch.setattr(tw_module.asyncio, "sleep", fake_sleep)

        writer = TimescaleWriter(dsn="postgresql://x", flush_interval=0.01)
        writer._pool = _make_pool(AsyncMock())
        writer._running = True

        task = asyncio.create_task(writer._writer_loop())
        # Use the un-patched sleep so we actually yield real time.
        await original_sleep(0.05)
        writer._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert crashes["count"] >= 1
        assert 1 in sleeps  # error-path sleep is 1s


class TestFlushBatch:
    async def test_flush_drains_remaining_queue(self):
        copy = AsyncMock()
        pool = _make_pool(copy)
        writer = TimescaleWriter(dsn="postgresql://x")
        writer._pool = pool
        writer.enqueue(_score_event())
        writer.enqueue(_score_event())

        await writer._flush_batch()

        copy.assert_awaited_once()
        records = copy.await_args.kwargs["records"]
        assert len(records) == 2
        assert writer._queue.empty()

    async def test_flush_empty_queue_noop(self):
        copy = AsyncMock()
        pool = _make_pool(copy)
        writer = TimescaleWriter(dsn="postgresql://x")
        writer._pool = pool
        await writer._flush_batch()
        copy.assert_not_awaited()
