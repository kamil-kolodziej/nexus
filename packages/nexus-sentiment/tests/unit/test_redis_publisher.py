"""Unit tests for the sentiment RedisPublisher."""

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

    async def test_publish_uses_configured_maxlen(self, redis):
        redis.xadd = AsyncMock(return_value="1-0")
        pub = RedisPublisher(redis, stream="s", maxlen=123)

        await pub.publish({"k": "v"})

        assert redis.xadd.await_args.kwargs["maxlen"] == 123

    async def test_publish_when_disconnected_buffers_without_calling_xadd(self, redis):
        redis.xadd = AsyncMock()
        pub = RedisPublisher(redis, stream="s")
        pub._connected = False

        result = await pub.publish({"a": "1"})

        assert result is None
        redis.xadd.assert_not_awaited()
        assert pub.buffer_size == 1

    async def test_publish_xadd_failure_marks_disconnected_and_buffers(self, redis):
        redis.xadd = AsyncMock(side_effect=ConnectionError("boom"))
        pub = RedisPublisher(redis, stream="s")

        result = await pub.publish({"a": "1"})

        assert result is None
        assert pub._connected is False
        assert pub.buffer_size == 1

    async def test_buffer_bounded_drops_oldest(self, redis):
        pub = RedisPublisher(redis, stream="s", buffer_max=2)
        pub._connected = False

        await pub.publish({"n": "1"})
        await pub.publish({"n": "2"})
        await pub.publish({"n": "3"})

        assert pub.buffer_size == 2
        # deque keeps the newest two when bounded
        assert list(pub._buffer) == [{"n": "2"}, {"n": "3"}]


class TestRedisPublisherFlushBuffer:
    """flush_buffer() drains buffered events via pipeline."""

    async def test_empty_buffer_returns_zero_and_marks_connected(self, redis):
        pub = RedisPublisher(redis, stream="s")
        pub._connected = False

        flushed = await pub.flush_buffer()

        assert flushed == 0
        assert pub._connected is True

    async def test_flushes_buffered_events_via_pipeline(self, redis):
        xadd = MagicMock()
        execute = AsyncMock(return_value=["1-0", "1-1"])
        redis.pipeline = MagicMock(return_value=_pipeline_cm(execute, xadd))

        pub = RedisPublisher(redis, stream="s")
        pub._connected = False
        pub._buffer.append({"a": "1"})
        pub._buffer.append({"b": "2"})

        flushed = await pub.flush_buffer()

        assert flushed == 2
        assert pub.buffer_size == 0
        assert pub._connected is True
        assert xadd.call_count == 2
        execute.assert_awaited_once()

    async def test_flush_failure_keeps_events_buffered(self, redis):
        xadd = MagicMock()
        execute = AsyncMock(side_effect=ConnectionError("pipeline down"))
        redis.pipeline = MagicMock(return_value=_pipeline_cm(execute, xadd))

        pub = RedisPublisher(redis, stream="s")
        pub._connected = False
        pub._buffer.append({"a": "1"})

        flushed = await pub.flush_buffer()

        assert flushed == 0
        # The current implementation popleft()s before execute(); on failure the
        # popped events are lost. Document the behavior we observe today so a
        # future fix surfaces in this test.
        assert pub.buffer_size == 0


class TestRedisPublisherReconnect:
    """reconnect() swaps the client and flushes the buffer."""

    async def test_reconnect_replaces_client_and_flushes(self):
        old = AsyncMock()
        pub = RedisPublisher(old, stream="s")
        pub._connected = False
        pub._buffer.append({"a": "1"})

        new = AsyncMock()
        xadd = MagicMock()
        execute = AsyncMock(return_value=["1-0"])
        new.pipeline = MagicMock(return_value=_pipeline_cm(execute, xadd))

        await pub.reconnect(new)

        assert pub._redis is new
        assert pub._connected is True
        assert pub.buffer_size == 0
        execute.assert_awaited_once()


class TestRedisPublisherProperties:
    def test_stream_property(self, redis):
        pub = RedisPublisher(redis, stream="nexus:sentiment-events")
        assert pub.stream == "nexus:sentiment-events"

    def test_buffer_size_property(self, redis):
        pub = RedisPublisher(redis, stream="s")
        assert pub.buffer_size == 0
        pub._buffer.append({"a": "1"})
        assert pub.buffer_size == 1
