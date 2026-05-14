"""Async TimescaleDB batch writer for sentiment scores."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import asyncpg
import structlog
from nexus_common.schemas.enums import Severity
from nexus_common.schemas.health_alert import HealthAlert
from nexus_common.schemas.market_event import MarketEvent, SentimentScore

logger = structlog.get_logger()


class TimescaleWriter:
    """Background async writer that batches sentiment scores and writes to TimescaleDB."""

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
        self._task = asyncio.create_task(self._writer_loop(), name="sentiment-timescale-writer")
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
        await self._flush_batch()
        if self._pool:
            await self._pool.close()
        logger.info("timescale_writer_stopped")

    def enqueue(self, event: MarketEvent) -> bool:
        """Add event to the write queue. Returns False if queue is full."""
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
                try:
                    event = await asyncio.wait_for(self._queue.get(), timeout=self._flush_interval)
                    batch.append(event)
                except TimeoutError:
                    pass

                while len(batch) < self._batch_size:
                    try:
                        event = self._queue.get_nowait()
                        batch.append(event)
                    except asyncio.QueueEmpty:
                        break

                if batch:
                    await self._write_batch(batch)
                    batch = []

            except asyncio.CancelledError:
                if batch:
                    await self._write_batch(batch)
                break
            except Exception:
                logger.error("timescale_writer_loop_error", exc_info=True)
                await asyncio.sleep(1)

    async def _write_batch(self, batch: list[MarketEvent]) -> None:
        """Write a batch of sentiment scores to TimescaleDB using COPY protocol."""
        if not batch or not self._pool:
            return

        records = []
        for event in batch:
            try:
                payload = SentimentScore.model_validate(event.payload)
            except Exception:
                logger.warning("timescale_invalid_payload_skipped", event_type=event.event_type)
                continue
            records.append(
                (
                    event.timestamp,
                    event.source,
                    payload.asset,
                    payload.article_url,
                    payload.score,
                    payload.confidence,
                    payload.sentiment_label,
                    payload.model_id,
                    event.schema_version,
                )
            )

        if not records:
            return

        retries = 0
        max_retries = 3
        while retries <= max_retries:
            try:
                async with self._pool.acquire() as conn:
                    await conn.copy_records_to_table(
                        "sentiment_scores",
                        records=records,
                        columns=[
                            "time",
                            "source",
                            "asset",
                            "article_url",
                            "score",
                            "confidence",
                            "sentiment_label",
                            "model_id",
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
            adapter_id="nexus-sentiment",
            severity=Severity.MEDIUM,
            timestamp=datetime.now(UTC),
            message=f"TimescaleDB batch write failed: {error_msg}. Queue depth: {queue_depth}",
        )
        result = self._health_callback(alert)
        if asyncio.iscoroutine(result):
            await result
