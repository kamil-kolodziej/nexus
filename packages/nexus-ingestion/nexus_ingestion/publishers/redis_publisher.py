"""Async Redis Stream publisher with in-memory buffer on disconnect."""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, Any, cast

import structlog

if TYPE_CHECKING:
    from redis.asyncio import Redis


class RedisPublisher:
    """Publishes events to a Redis Stream with buffering on disconnect."""

    def __init__(
        self,
        redis: Redis[Any],
        stream: str,
        maxlen: int = 100000,
        buffer_max: int = 10000,
    ) -> None:
        self._redis = redis
        self._stream = stream
        self._maxlen = maxlen
        self._buffer: deque[dict[str, str]] = deque(maxlen=buffer_max)
        self._connected = True
        self._logger = structlog.get_logger().bind(stream=stream)

    @property
    def stream(self) -> str:
        return self._stream

    @property
    def buffer_size(self) -> int:
        return len(self._buffer)

    async def publish(self, fields: dict[str, str]) -> str | None:
        """Publish a single event to the Redis Stream.

        Returns the stream entry ID on success, None if buffered.
        """
        if not self._connected:
            self._buffer.append(fields)
            return None

        try:
            entry_id = await self._redis.xadd(
                self._stream,
                fields,
                maxlen=self._maxlen,
                approximate=True,
            )
            return cast(str, entry_id)
        except Exception:
            self._logger.warning(
                "redis_publish_failed",
                buffer_size=len(self._buffer),
            )
            self._connected = False
            self._buffer.append(fields)
            return None

    async def flush_buffer(self) -> int:
        """Flush buffered events via pipeline after reconnection.

        Returns the number of flushed events.
        """
        if not self._buffer:
            self._connected = True
            return 0

        flushed = 0
        try:
            async with self._redis.pipeline(transaction=False) as pipe:
                while self._buffer:
                    fields = self._buffer.popleft()
                    pipe.xadd(
                        self._stream,
                        fields,
                        maxlen=self._maxlen,
                        approximate=True,
                    )
                    flushed += 1
                await pipe.execute()

            self._connected = True
            self._logger.info("redis_buffer_flushed", flushed=flushed)
            return flushed

        except Exception:
            self._logger.error("redis_buffer_flush_failed", events_remaining=len(self._buffer))
            return 0

    async def reconnect(self, redis: Redis[Any]) -> None:
        """Update the Redis client after reconnection and flush buffer."""
        self._redis = redis
        self._connected = True
        await self.flush_buffer()
