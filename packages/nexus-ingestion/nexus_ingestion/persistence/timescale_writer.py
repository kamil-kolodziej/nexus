"""Async TimescaleDB batch writer using asyncpg COPY protocol."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import asyncpg
import structlog
from nexus_common.schemas.enums import Severity
from nexus_common.schemas.health_alert import HealthAlert
from nexus_common.schemas.market_event import MarketEvent

logger = structlog.get_logger()


class TimescaleWriter:
    """Background async writer that batches events and writes to TimescaleDB."""

    def __init__(
        self,
        dsn: str,
        *,
        batch_size: int = 500,
        flush_interval: float = 5.0,
        queue_maxsize: int = 50000,
        health_callback: Callable[[HealthAlert], Any] | None = None,
    ) -> None:
        self._dsn = dsn
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=queue_maxsize)
        self._health_callback = health_callback
        self._pool: asyncpg.Pool | None = None
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Initialize connection pool and start the background writer task."""
        self._pool = await asyncpg.create_pool(self._dsn, min_size=2, max_size=5)
        self._running = True
        self._task = asyncio.create_task(self._writer_loop(), name="timescale-writer")
        logger.info(
            "timescale_writer_started",
            batch_size=self._batch_size,
            flush_interval_s=self._flush_interval,
        )

    async def stop(self) -> None:
        """Flush remaining events and shut down."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        # Final flush
        await self._flush_batch()
        if self._pool:
            await self._pool.close()
        logger.info("timescale_writer_stopped")

    def enqueue(self, event: MarketEvent) -> bool:
        """Add event to the write queue. Returns False if queue is full (event dropped)."""
        try:
            self._queue.put_nowait(event)
            return True
        except asyncio.QueueFull:
            logger.warning(
                "timescale_queue_full_event_dropped",
                queue_size=self._queue.qsize(),
            )
            return False

    async def _writer_loop(self) -> None:
        """Background loop: flush batch on size or timer."""
        batch: list[MarketEvent] = []
        while self._running:
            try:
                # Wait for events with timeout
                try:
                    event = await asyncio.wait_for(self._queue.get(), timeout=self._flush_interval)
                    batch.append(event)
                except TimeoutError:
                    pass

                # Drain queue up to batch size
                while len(batch) < self._batch_size:
                    try:
                        event = self._queue.get_nowait()
                        batch.append(event)
                    except asyncio.QueueEmpty:
                        break

                # Flush if we have enough or timeout elapsed
                if batch:
                    await self._write_batch(batch)
                    batch = []

            except asyncio.CancelledError:
                # Flush remaining on cancellation
                if batch:
                    await self._write_batch(batch)
                break
            except Exception:
                logger.error("timescale_writer_loop_error", exc_info=True)
                await asyncio.sleep(1)

    async def _write_batch(self, batch: list[MarketEvent]) -> None:
        """Write a batch of events to TimescaleDB using COPY protocol."""
        if not batch or not self._pool:
            return

        records = [
            (
                event.timestamp,
                event.source,
                event.asset,
                event.event_type.value,
                json.dumps(event.payload),
                event.schema_version,
            )
            for event in batch
        ]

        retries = 0
        max_retries = 3
        while retries <= max_retries:
            try:
                async with self._pool.acquire() as conn:
                    await conn.copy_records_to_table(
                        "market_events",
                        records=records,
                        columns=[
                            "time",
                            "source",
                            "asset",
                            "event_type",
                            "payload",
                            "schema_version",
                        ],
                    )
                logger.debug("timescale_batch_written", record_count=len(records))
                return
            except Exception as e:
                retries += 1
                if retries > max_retries:
                    logger.error(
                        "timescale_batch_write_failed",
                        max_retries=max_retries,
                        error=str(e),
                    )
                    await self._emit_persistence_error(len(batch), str(e))
                    return

                delay = 2**retries
                logger.warning(
                    "timescale_write_retry",
                    attempt=retries,
                    max_retries=max_retries,
                    delay_s=delay,
                    error=str(e),
                )
                await asyncio.sleep(delay)

    async def _flush_batch(self) -> None:
        """Drain queue and write all remaining events."""
        batch: list[MarketEvent] = []
        while not self._queue.empty():
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if batch:
            await self._write_batch(batch)

    async def _emit_persistence_error(self, queue_depth: int, error_msg: str) -> None:
        """Emit a PERSISTENCE_ERROR health alert."""
        if not self._health_callback:
            return

        alert = HealthAlert(
            alert_type="PERSISTENCE_ERROR",
            adapter_id="timescale-writer",
            severity=Severity.MEDIUM,
            timestamp=datetime.now(UTC),
            message=f"TimescaleDB batch write failed: {error_msg}. Queue depth: {queue_depth}",
        )
        result = self._health_callback(alert)
        if asyncio.iscoroutine(result):
            await result
