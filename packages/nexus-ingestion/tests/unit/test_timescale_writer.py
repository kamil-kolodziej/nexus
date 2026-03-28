"""Unit tests for TimescaleDB writer.

Tests queue behavior, batch flush on size + timer, error retry, QueueFull drop.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nexus_common.schemas.enums import EventType
from nexus_common.schemas.market_event import MarketEvent

from nexus_ingestion.persistence.timescale_writer import TimescaleWriter


def _make_event(n: int = 0) -> MarketEvent:
    return MarketEvent(
        source="test:exchange",
        asset="BTC/USDT",
        timestamp=datetime(2026, 3, 22, 14, 30, n % 60, tzinfo=timezone.utc),
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
