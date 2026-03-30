"""Unit tests for TimescaleDB writer.

Tests queue behavior, batch flush on size + timer, error retry, QueueFull drop.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nexus_common.schemas.enums import EventType
from nexus_common.schemas.market_event import MarketEvent
from nexus_ingestion.persistence.timescale_writer import TimescaleWriter


def _make_event(n: int = 0) -> MarketEvent:
    return MarketEvent(
        source="test:exchange",
        asset="BTC/USDT",
        timestamp=datetime(2026, 3, 22, 14, 30, n % 60, tzinfo=UTC),
        event_type=EventType.TICK,
        schema_version="1.0.0",
        payload={"bid": 100.0 + n, "ask": 101.0 + n, "last": 100.5 + n, "volume_24h": 0},
    )


class TestEnqueue:
    def test_enqueue_returns_true(self) -> None:
        writer = TimescaleWriter("postgresql://test", queue_maxsize=10)
        assert writer.enqueue(_make_event()) is True

    def test_enqueue_full_returns_false(self) -> None:
        writer = TimescaleWriter("postgresql://test", queue_maxsize=2)
        writer.enqueue(_make_event(1))
        writer.enqueue(_make_event(2))
        assert writer.enqueue(_make_event(3)) is False

    def test_queue_size_matches_enqueued(self) -> None:
        writer = TimescaleWriter("postgresql://test", queue_maxsize=100)
        for i in range(5):
            writer.enqueue(_make_event(i))
        assert writer._queue.qsize() == 5


class TestWriteBatch:
    @pytest.mark.asyncio
    async def test_write_batch_calls_copy_records(self) -> None:
        writer = TimescaleWriter("postgresql://test")
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        writer._pool = mock_pool

        event = _make_event()
        await writer._write_batch([event])

        mock_conn.copy_records_to_table.assert_called_once()
        call_args = mock_conn.copy_records_to_table.call_args
        assert call_args[0][0] == "market_events"
        assert len(call_args[1]["records"]) == 1

    @pytest.mark.asyncio
    async def test_write_batch_retries_on_failure(self) -> None:
        writer = TimescaleWriter("postgresql://test")
        mock_conn = AsyncMock()
        mock_conn.copy_records_to_table.side_effect = [
            Exception("connection lost"),
            None,  # success on retry
        ]
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        writer._pool = mock_pool

        await writer._write_batch([_make_event()])
        assert mock_conn.copy_records_to_table.call_count == 2

    @pytest.mark.asyncio
    async def test_write_batch_emits_health_alert_on_max_retries(self) -> None:
        alerts = []
        writer = TimescaleWriter(
            "postgresql://test",
            health_callback=lambda a: alerts.append(a),
        )
        mock_conn = AsyncMock()
        mock_conn.copy_records_to_table.side_effect = Exception("connection lost")
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        writer._pool = mock_pool

        await writer._write_batch([_make_event()])
        assert len(alerts) == 1
        assert alerts[0].alert_type == "PERSISTENCE_ERROR"

    @pytest.mark.asyncio
    async def test_write_batch_skips_when_no_pool(self) -> None:
        writer = TimescaleWriter("postgresql://test")
        writer._pool = None
        # Should return without error
        await writer._write_batch([_make_event()])

    @pytest.mark.asyncio
    async def test_write_batch_skips_empty_batch(self) -> None:
        writer = TimescaleWriter("postgresql://test")
        mock_pool = MagicMock()
        writer._pool = mock_pool
        await writer._write_batch([])
        mock_pool.acquire.assert_not_called()


class TestFlushBatch:
    @pytest.mark.asyncio
    async def test_flush_batch_drains_queue(self) -> None:
        writer = TimescaleWriter("postgresql://test")
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        writer._pool = mock_pool

        for i in range(3):
            writer.enqueue(_make_event(i))

        await writer._flush_batch()

        assert writer._queue.empty()
        mock_conn.copy_records_to_table.assert_called_once()
        records = mock_conn.copy_records_to_table.call_args[1]["records"]
        assert len(records) == 3

    @pytest.mark.asyncio
    async def test_flush_batch_empty_queue_does_nothing(self) -> None:
        writer = TimescaleWriter("postgresql://test")
        mock_pool = MagicMock()
        writer._pool = mock_pool
        await writer._flush_batch()
        mock_pool.acquire.assert_not_called()


class TestStop:
    @pytest.mark.asyncio
    async def test_stop_cancels_task_and_closes_pool(self) -> None:
        writer = TimescaleWriter("postgresql://test")
        mock_pool = AsyncMock()
        writer._pool = mock_pool
        writer._running = True

        # Start a real background task that we can cancel
        async def _dummy_loop() -> None:
            import asyncio

            await asyncio.sleep(100)

        import asyncio

        writer._task = asyncio.create_task(_dummy_loop())

        await writer.stop()

        assert not writer._running
        mock_pool.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_with_no_task_does_not_raise(self) -> None:
        writer = TimescaleWriter("postgresql://test")
        writer._pool = None
        writer._task = None
        writer._running = False
        await writer.stop()

    @pytest.mark.asyncio
    async def test_stop_flushes_remaining_events(self) -> None:
        writer = TimescaleWriter("postgresql://test")
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_pool.close = AsyncMock()
        writer._pool = mock_pool
        writer._running = False
        writer._task = None

        writer.enqueue(_make_event(1))
        writer.enqueue(_make_event(2))

        await writer.stop()

        records = mock_conn.copy_records_to_table.call_args[1]["records"]
        assert len(records) == 2


class TestEmitPersistenceError:
    @pytest.mark.asyncio
    async def test_no_callback_returns_silently(self) -> None:
        writer = TimescaleWriter("postgresql://test", health_callback=None)
        await writer._emit_persistence_error(100, "disk full")

    @pytest.mark.asyncio
    async def test_sync_callback_called_with_alert(self) -> None:
        alerts = []
        writer = TimescaleWriter("postgresql://test", health_callback=lambda a: alerts.append(a))
        await writer._emit_persistence_error(50, "connection refused")
        assert len(alerts) == 1
        assert alerts[0].alert_type == "PERSISTENCE_ERROR"
        assert "connection refused" in alerts[0].message

    @pytest.mark.asyncio
    async def test_async_callback_is_awaited(self) -> None:
        async_cb = AsyncMock()
        writer = TimescaleWriter("postgresql://test", health_callback=async_cb)
        await writer._emit_persistence_error(10, "timeout")
        async_cb.assert_awaited_once()
        assert async_cb.call_args[0][0].alert_type == "PERSISTENCE_ERROR"


class TestStart:
    @pytest.mark.asyncio
    async def test_start_creates_pool_and_task(self) -> None:
        writer = TimescaleWriter("postgresql://test")
        mock_pool = MagicMock()

        with patch("asyncpg.create_pool", new_callable=AsyncMock, return_value=mock_pool):
            await writer.start()

        assert writer._pool is mock_pool
        assert writer._running is True
        assert writer._task is not None

        # Clean up the background task
        writer._running = False
        if writer._task and not writer._task.done():
            writer._task.cancel()
            try:
                await writer._task
            except asyncio.CancelledError:
                pass


class TestWriterLoop:
    @pytest.mark.asyncio
    async def test_writer_loop_flushes_on_batch_size(self) -> None:
        writer = TimescaleWriter("postgresql://test", batch_size=2, flush_interval=10.0)
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        writer._pool = mock_pool

        for i in range(2):
            writer.enqueue(_make_event(i))

        import asyncio

        writer._running = True
        task = asyncio.create_task(writer._writer_loop())

        # Give the loop a tick to process the batch
        await asyncio.sleep(0.05)
        writer._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        mock_conn.copy_records_to_table.assert_called()

    @pytest.mark.asyncio
    async def test_writer_loop_cancellation_flushes_remaining(self) -> None:
        writer = TimescaleWriter("postgresql://test", batch_size=100, flush_interval=10.0)
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        writer._pool = mock_pool

        writer._running = True

        # Queue 3 events before starting loop so they get picked up
        for i in range(3):
            writer.enqueue(_make_event(i))

        task = asyncio.create_task(writer._writer_loop())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Events should have been written (either in loop body or on cancel)
        mock_conn.copy_records_to_table.assert_called()

    @pytest.mark.asyncio
    async def test_writer_loop_timeout_path_flushes_accumulated_batch(self) -> None:
        """TimeoutError on wait_for causes loop to flush via the if-batch branch."""
        writer = TimescaleWriter("postgresql://test", batch_size=100, flush_interval=0.01)
        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        writer._pool = mock_pool

        # Add one event, then let the loop timeout on the next get() to trigger the
        # flush path via the timeout (empty batch after timeout → no write, but code runs)
        writer.enqueue(_make_event(0))
        writer._running = True

        task = asyncio.create_task(writer._writer_loop())
        await asyncio.sleep(0.05)  # let the loop iterate: first get returns event, second times out
        writer._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        mock_conn.copy_records_to_table.assert_called()

    @pytest.mark.asyncio
    async def test_writer_loop_exception_continues(self) -> None:
        """Non-CancelledError exceptions outside _write_batch are caught and loop continues."""
        writer = TimescaleWriter("postgresql://test", batch_size=1, flush_interval=0.01)
        call_count = 0

        original_write_batch = writer._write_batch

        async def raise_once(batch: list) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                writer._running = False  # stop after this iteration
                raise RuntimeError("unexpected failure")
            await original_write_batch(batch)

        writer._write_batch = raise_once  # type: ignore[method-assign]
        writer.enqueue(_make_event(0))
        writer._running = True

        with patch("asyncio.sleep", new_callable=AsyncMock):
            task = asyncio.create_task(writer._writer_loop())
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except TimeoutError:
                writer._running = False
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # The exception was caught by except Exception: and loop exited cleanly
        assert call_count >= 1
