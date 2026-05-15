"""Unit tests for the sentiment RedisPublisher.

Tests buffer behavior on disconnect, self-healing pipeline flush on
recovery, MAXLEN configuration, and event ordering preservation.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from nexus_sentiment.publishers.redis_publisher import RedisPublisher


def _pipeline_cm(execute_mock: AsyncMock, xadd_mock: MagicMock) -> MagicMock:
    """Build an async context manager that yields a pipeline-like object."""
    pipe = MagicMock()
    pipe.xadd = xadd_mock
    pipe.execute = execute_mock
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=pipe)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


def _always_failing_redis() -> MagicMock:
    """Mock Redis where both xadd and pipeline.execute always raise."""
    execute = AsyncMock(side_effect=ConnectionError("Redis down"))
    xadd_in_pipe = MagicMock()
    redis = MagicMock()
    redis.xadd = AsyncMock(side_effect=ConnectionError("Redis down"))
    redis.pipeline = MagicMock(return_value=_pipeline_cm(execute, xadd_in_pipe))
    return redis


@pytest.fixture
def redis():
    return AsyncMock()


class TestRedisPublisherPublish:
    """publish() happy path and disconnect/buffer behavior."""

    async def test_publish_returns_entry_id_on_success(self, redis):
        redis.xadd = AsyncMock(return_value="1-0")
        pub = RedisPublisher(redis, stream="nexus:sentiment-events")

        entry_id = await pub.publish({"k": "v"})

        assert entry_id == "1-0"
        redis.xadd.assert_awaited_once_with(
            "nexus:sentiment-events", {"k": "v"}, maxlen=50000, approximate=True
        )
        assert pub.buffer_size == 0
        assert pub.connected is True

    async def test_publish_uses_configured_maxlen(self, redis):
        redis.xadd = AsyncMock(return_value="1-0")
        pub = RedisPublisher(redis, stream="s", maxlen=123)

        await pub.publish({"k": "v"})

        assert redis.xadd.await_args.kwargs["maxlen"] == 123

    async def test_publish_xadd_failure_marks_disconnected_and_buffers(self, redis):
        redis.xadd = AsyncMock(side_effect=ConnectionError("boom"))
        pub = RedisPublisher(redis, stream="s")

        result = await pub.publish({"a": "1"})

        assert result is None
        assert pub.connected is False
        assert pub.buffer_size == 1

    async def test_buffer_bounded_drops_oldest(self):
        pub = RedisPublisher(_always_failing_redis(), stream="s", buffer_max=2)

        await pub.publish({"n": "1"})
        await pub.publish({"n": "2"})
        await pub.publish({"n": "3"})

        assert pub.buffer_size == 2
        # deque keeps the newest two when bounded
        assert list(pub._buffer) == [{"n": "2"}, {"n": "3"}]


class TestSelfHealingRecovery:
    """publish() drains the buffer in FIFO order on the next successful call."""

    @staticmethod
    def _redis_with_pipeline_side_effects(
        execute_side_effect: list[object],
    ) -> tuple[MagicMock, MagicMock]:
        """Build a redis mock whose xadd always fails and whose pipeline.execute
        consumes the given side_effect list. Returns (redis_mock, xadd_in_pipe_mock).
        """
        xadd_in_pipe = MagicMock()
        execute = AsyncMock(side_effect=execute_side_effect)
        redis = MagicMock()
        redis.xadd = AsyncMock(side_effect=ConnectionError("down"))
        redis.pipeline = MagicMock(return_value=_pipeline_cm(execute, xadd_in_pipe))
        return redis, xadd_in_pipe

    async def test_next_successful_publish_drains_backlog(self):
        # publish 0: single xadd path fails -> buffer=[0]
        # publish 1: drain path fails -> buffer=[0, 1]
        # publish 2: drain path succeeds -> buffer=[]
        redis, _ = self._redis_with_pipeline_side_effects(
            [ConnectionError("still down"), ["10-0", "20-0", "30-0"]]
        )
        pub = RedisPublisher(redis, stream="s", buffer_max=100)

        assert await pub.publish({"n": "0"}) is None
        assert await pub.publish({"n": "1"}) is None
        assert pub.buffer_size == 2
        assert pub.connected is False

        entry_id = await pub.publish({"n": "2"})

        assert entry_id == "30-0"
        assert pub.buffer_size == 0
        assert pub.connected is True

    async def test_drain_failure_keeps_events_buffered(self):
        redis, _ = self._redis_with_pipeline_side_effects(
            [ConnectionError("still down"), ConnectionError("still down")]
        )
        pub = RedisPublisher(redis, stream="s", buffer_max=100)

        await pub.publish({"n": "0"})
        await pub.publish({"n": "1"})
        result = await pub.publish({"n": "2"})

        assert result is None
        assert pub.buffer_size == 3
        assert pub.connected is False

    async def test_drain_preserves_fifo_order(self):
        captured: list[dict[str, str]] = []

        # 4 drain attempts fail (after the first single-xadd failure on
        # publish 0). The 5th drain succeeds with all 5 events pipelined.
        def capture(stream: str, fields: dict[str, str], **kwargs: object) -> None:
            captured.append(fields)

        xadd_in_pipe = MagicMock(side_effect=capture)
        execute = AsyncMock(
            side_effect=[
                ConnectionError("down"),
                ConnectionError("down"),
                ConnectionError("down"),
                ["1-0", "2-0", "3-0", "4-0", "5-0"],
            ]
        )
        redis = MagicMock()
        redis.xadd = AsyncMock(side_effect=ConnectionError("down"))
        redis.pipeline = MagicMock(return_value=_pipeline_cm(execute, xadd_in_pipe))

        pub = RedisPublisher(redis, stream="s", buffer_max=100)
        for i in range(5):
            await pub.publish({"n": str(i)})

        final_drain_payloads = [c["n"] for c in captured[-5:]]
        assert final_drain_payloads == [str(i) for i in range(5)]
        assert pub.buffer_size == 0
        assert pub.connected is True

    async def test_empty_buffer_takes_single_xadd_path(self):
        redis = MagicMock()
        redis.xadd = AsyncMock(return_value="1-0")
        redis.pipeline = MagicMock()

        pub = RedisPublisher(redis, stream="s")
        entry_id = await pub.publish({"k": "v"})

        assert entry_id == "1-0"
        assert redis.pipeline.call_count == 0
        assert pub.connected is True


class TestRedisPublisherProperties:
    def test_stream_property(self, redis):
        pub = RedisPublisher(redis, stream="nexus:sentiment-events")
        assert pub.stream == "nexus:sentiment-events"

    def test_buffer_size_property(self, redis):
        pub = RedisPublisher(redis, stream="s")
        assert pub.buffer_size == 0
        pub._buffer.append({"a": "1"})
        assert pub.buffer_size == 1

    def test_connected_property(self, redis):
        pub = RedisPublisher(redis, stream="s")
        assert pub.connected is True
        pub._connected = False
        assert pub.connected is False
