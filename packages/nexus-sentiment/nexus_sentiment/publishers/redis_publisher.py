"""Async Redis Stream publisher with self-healing buffer on disconnect."""

from __future__ import annotations

import asyncio
from collections import deque
from typing import TYPE_CHECKING, Any, cast

import structlog

if TYPE_CHECKING:
    from redis.asyncio import Redis


class RedisPublisher:
    """Publishes events to a Redis Stream with a self-healing buffer.

    On xadd failure, events are appended to a bounded in-memory deque. The next
    successful publish drains the backlog in FIFO order via pipeline alongside
    the new event, so order is preserved across the outage and recovery needs
    no external orchestration.
    """

    def __init__(
        self,
        redis: Redis[Any],
        stream: str,
        maxlen: int = 50000,
        buffer_max: int = 10000,
    ) -> None:
        self._redis = redis
        self._stream = stream
        self._maxlen = maxlen
        self._buffer: deque[dict[str, str]] = deque(maxlen=buffer_max)
        self._connected = True
        self._lock = asyncio.Lock()
        self._logger = structlog.get_logger().bind(stream=stream)

    @property
    def stream(self) -> str:
        return self._stream

    @property
    def buffer_size(self) -> int:
        return len(self._buffer)

    @property
    def connected(self) -> bool:
        return self._connected

    async def publish(self, fields: dict[str, str]) -> str | None:
        """Publish a single event to the Redis Stream.

        If the buffer holds events from a prior outage, drains them and the new
        event in a single pipeline. On failure, the new event joins the buffer
        and None is returned. Concurrent callers serialize on an internal lock
        so the backlog is drained exactly once.

        Returns the stream entry ID on success, None if buffered.
        """
        async with self._lock:
            if not self._buffer:
                return await self._xadd_single(fields)
            return await self._xadd_with_drain(fields)

    async def _xadd_single(self, fields: dict[str, str]) -> str | None:
        try:
            entry_id = await self._redis.xadd(
                self._stream,
                fields,
                maxlen=self._maxlen,
                approximate=True,
            )
        except Exception as exc:
            self._logger.warning(
                "redis_publish_failed",
                buffer_size=len(self._buffer),
                error=str(exc),
            )
            self._connected = False
            self._buffer.append(fields)
            return None

        self._connected = True
        return cast(str, entry_id)

    async def _xadd_with_drain(self, fields: dict[str, str]) -> str | None:
        backlog_count = len(self._buffer)
        try:
            async with self._redis.pipeline(transaction=False) as pipe:
                for buffered in self._buffer:
                    pipe.xadd(
                        self._stream,
                        buffered,
                        maxlen=self._maxlen,
                        approximate=True,
                    )
                pipe.xadd(
                    self._stream,
                    fields,
                    maxlen=self._maxlen,
                    approximate=True,
                )
                results = await pipe.execute()
        except Exception as exc:
            self._logger.warning(
                "redis_publish_failed",
                buffer_size=backlog_count,
                error=str(exc),
            )
            self._connected = False
            self._buffer.append(fields)
            return None

        for _ in range(backlog_count):
            self._buffer.popleft()
        self._connected = True
        self._logger.info("redis_buffer_flushed", flushed=backlog_count)
        return cast(str, results[-1])
