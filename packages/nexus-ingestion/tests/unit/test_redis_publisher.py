"""Unit tests for Redis publisher.

Tests buffer behavior on disconnect, self-healing pipeline flush on
recovery, MAXLEN configuration, and event ordering preservation.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

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


def _always_failing_redis() -> MagicMock:
    """Mock Redis where both xadd and pipeline.execute always raise."""
    pipe_mock = MagicMock()
    pipe_mock.xadd = MagicMock(return_value=pipe_mock)
    pipe_mock.execute = AsyncMock(side_effect=ConnectionError("Redis down"))
    pipe_mock.__aenter__ = AsyncMock(return_value=pipe_mock)
    pipe_mock.__aexit__ = AsyncMock(return_value=False)

    redis = MagicMock()
    redis.xadd = AsyncMock(side_effect=ConnectionError("Redis down"))
    redis.pipeline = MagicMock(return_value=pipe_mock)
    return redis


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
        pub = RedisPublisher(_always_failing_redis(), "test:stream", buffer_max=3)

        # Publish 5, only last 3 kept (deque maxlen)
        for i in range(5):
            await pub.publish(_fields(i))
        assert pub.buffer_size == 3

    @pytest.mark.asyncio
    async def test_continues_buffering_after_disconnect(self) -> None:
        pub = RedisPublisher(_always_failing_redis(), "test:stream", buffer_max=100)

        await pub.publish(_fields(0))
        await pub.publish(_fields(1))
        assert pub.buffer_size == 2


class TestSelfHealingRecovery:
    """publish() drains the buffer in FIFO order on the next successful call."""

    @staticmethod
    def _build_pipe_mock(execute_side_effect: list[object]) -> MagicMock:
        pipe_mock = MagicMock()
        pipe_mock.xadd = MagicMock(return_value=pipe_mock)
        pipe_mock.execute = AsyncMock(side_effect=execute_side_effect)
        pipe_mock.__aenter__ = AsyncMock(return_value=pipe_mock)
        pipe_mock.__aexit__ = AsyncMock(return_value=False)
        return pipe_mock

    @pytest.mark.asyncio
    async def test_next_successful_publish_drains_backlog(self) -> None:
        # publish 0: single xadd path fails -> buffer=[0]
        # publish 1: drain path fails -> buffer=[0, 1]
        # publish 2: drain path succeeds -> buffer=[]
        pipe_mock = self._build_pipe_mock([ConnectionError("still down"), ["10-0", "20-0", "30-0"]])
        redis = MagicMock()
        redis.xadd = AsyncMock(side_effect=ConnectionError("down"))
        redis.pipeline = MagicMock(return_value=pipe_mock)

        pub = RedisPublisher(redis, "test:stream", buffer_max=100)

        assert await pub.publish(_fields(0)) is None
        assert await pub.publish(_fields(1)) is None
        assert pub.buffer_size == 2
        assert pub.connected is False

        entry_id = await pub.publish(_fields(2))

        assert entry_id == "30-0"
        assert pub.buffer_size == 0
        assert pub.connected is True

    @pytest.mark.asyncio
    async def test_drain_failure_keeps_events_buffered(self) -> None:
        pipe_mock = self._build_pipe_mock(
            [ConnectionError("still down"), ConnectionError("still down")]
        )
        redis = MagicMock()
        redis.xadd = AsyncMock(side_effect=ConnectionError("down"))
        redis.pipeline = MagicMock(return_value=pipe_mock)

        pub = RedisPublisher(redis, "test:stream", buffer_max=100)

        await pub.publish(_fields(0))
        await pub.publish(_fields(1))
        result = await pub.publish(_fields(2))

        assert result is None
        assert pub.buffer_size == 3
        assert pub.connected is False

    @pytest.mark.asyncio
    async def test_drain_preserves_fifo_order(self) -> None:
        captured: list[dict[str, str]] = []

        # 4 drain attempts fail (after the first single-xadd failure on
        # publish 0). The 5th drain succeeds with all 5 events pipelined.
        pipe_mock = MagicMock()
        pipe_mock.__aenter__ = AsyncMock(return_value=pipe_mock)
        pipe_mock.__aexit__ = AsyncMock(return_value=False)
        pipe_mock.execute = AsyncMock(
            side_effect=[
                ConnectionError("down"),
                ConnectionError("down"),
                ConnectionError("down"),
                ["1-0", "2-0", "3-0", "4-0", "5-0"],
            ]
        )

        def capture(stream: str, fields: dict[str, str], **kwargs: object) -> MagicMock:
            captured.append(fields)
            return pipe_mock

        pipe_mock.xadd = MagicMock(side_effect=capture)

        redis = MagicMock()
        redis.xadd = AsyncMock(side_effect=ConnectionError("down"))
        redis.pipeline = MagicMock(return_value=pipe_mock)

        pub = RedisPublisher(redis, "test:stream", buffer_max=100)
        for i in range(5):
            await pub.publish(_fields(i))

        final_drain_payloads = [c["payload"] for c in captured[-5:]]
        assert final_drain_payloads == [f'{{"bid": {100 + i}}}' for i in range(5)]
        assert pub.buffer_size == 0
        assert pub.connected is True

    @pytest.mark.asyncio
    async def test_empty_buffer_takes_single_xadd_path(self) -> None:
        redis = MagicMock()
        redis.xadd = AsyncMock(return_value="1234-0")
        redis.pipeline = MagicMock()

        pub = RedisPublisher(redis, "test:stream")
        entry_id = await pub.publish(_fields())

        assert entry_id == "1234-0"
        assert redis.pipeline.call_count == 0
        assert pub.connected is True


class TestEventOrdering:
    @pytest.mark.asyncio
    async def test_buffer_preserves_fifo_order(self) -> None:
        pub = RedisPublisher(_always_failing_redis(), "test:stream", buffer_max=100)

        for i in range(5):
            await pub.publish(_fields(i))

        # Check FIFO order
        for i in range(5):
            assert pub._buffer[i]["payload"] == f'{{"bid": {100 + i}}}'
