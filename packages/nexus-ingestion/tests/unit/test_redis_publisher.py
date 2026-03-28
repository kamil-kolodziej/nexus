"""Unit tests for Redis publisher.

Tests buffer behavior on disconnect, pipeline flush on reconnect,
MAXLEN configuration, and event ordering preservation.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nexus_ingestion.publishers.redis_publisher import RedisPublisher


def _fields(n: int = 0) -> dict[str, str]:
    return {
        "source": "test:exchange",
        "asset": "BTC/USDT",
        "event_type": "TICK",
        "timestamp": "2026-03-22T14:30:00+00:00",
        "schema_version": "1.0.0",
        "payload": f'{{"bid": {100 + n}}}',
    }


class TestPublish:
    @pytest.mark.asyncio
    async def test_publish_calls_xadd(self) -> None:
        redis = AsyncMock()
        redis.xadd.return_value = "1234-0"
        pub = RedisPublisher(redis, "test:stream", maxlen=5000)

        entry_id = await pub.publish(_fields())
        assert entry_id == "1234-0"
        redis.xadd.assert_called_once()

    @pytest.mark.asyncio
    async def test_maxlen_passed_to_xadd(self) -> None:
        redis = AsyncMock()
        redis.xadd.return_value = "1234-0"
        pub = RedisPublisher(redis, "test:stream", maxlen=42)

        await pub.publish(_fields())
        call_kwargs = redis.xadd.call_args[1]
        assert call_kwargs["maxlen"] == 42
        assert call_kwargs["approximate"] is True


class TestBufferOnDisconnect:
    @pytest.mark.asyncio
    async def test_buffers_on_publish_failure(self) -> None:
        redis = AsyncMock()
        redis.xadd.side_effect = ConnectionError("Redis down")
        pub = RedisPublisher(redis, "test:stream", buffer_max=100)

        result = await pub.publish(_fields())
        assert result is None
        assert pub.buffer_size == 1

    @pytest.mark.asyncio
    async def test_buffer_respects_max_size(self) -> None:
        redis = AsyncMock()
        redis.xadd.side_effect = ConnectionError("Redis down")
        pub = RedisPublisher(redis, "test:stream", buffer_max=3)

        # Publish 5, only last 3 kept (deque maxlen)
        for i in range(5):
            await pub.publish(_fields(i))
        assert pub.buffer_size == 3

    @pytest.mark.asyncio
    async def test_continues_buffering_after_disconnect(self) -> None:
        redis = AsyncMock()
        redis.xadd.side_effect = ConnectionError("Redis down")
        pub = RedisPublisher(redis, "test:stream", buffer_max=100)

        await pub.publish(_fields(0))
        await pub.publish(_fields(1))
        assert pub.buffer_size == 2


class TestFlushOnReconnect:
    @pytest.mark.asyncio
    async def test_flush_sends_all_buffered_events(self) -> None:
        pipe_mock = MagicMock()
        pipe_mock.xadd = MagicMock(return_value=pipe_mock)
        pipe_mock.execute = AsyncMock(return_value=[])
        pipe_mock.__aenter__ = AsyncMock(return_value=pipe_mock)
        pipe_mock.__aexit__ = AsyncMock(return_value=False)

        redis = MagicMock()
        redis.pipeline = MagicMock(return_value=pipe_mock)
        redis.xadd = AsyncMock(return_value="1234-0")

        pub = RedisPublisher(redis, "test:stream", buffer_max=100)
        # Manually fill buffer
        pub._connected = False
        pub._buffer.append(_fields(0))
        pub._buffer.append(_fields(1))

        flushed = await pub.flush_buffer()
        assert flushed == 2
        assert pub.buffer_size == 0
        assert pub._connected is True

    @pytest.mark.asyncio
    async def test_flush_empty_buffer_noop(self) -> None:
        redis = AsyncMock()
        pub = RedisPublisher(redis, "test:stream")
        flushed = await pub.flush_buffer()
        assert flushed == 0


class TestEventOrdering:
    @pytest.mark.asyncio
    async def test_buffer_preserves_fifo_order(self) -> None:
        redis = AsyncMock()
        redis.xadd.side_effect = ConnectionError("Redis down")
        pub = RedisPublisher(redis, "test:stream", buffer_max=100)

        for i in range(5):
            await pub.publish(_fields(i))

        # Check FIFO order
        for i in range(5):
            assert pub._buffer[i]["payload"] == f'{{"bid": {100 + i}}}'
