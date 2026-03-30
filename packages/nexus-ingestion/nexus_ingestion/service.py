"""IngestionService orchestrator — manages adapter lifecycle with manual task supervision."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog
from nexus_common.schemas.enums import EventType, Severity
from nexus_common.schemas.health_alert import AdapterHealth, HealthAlert
from nexus_common.schemas.market_event import MarketEvent

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from nexus_ingestion.adapters.base import BaseAdapter
    from nexus_ingestion.config import IngestionConfig
    from nexus_ingestion.monitoring.gap_detector import GapDetector
    from nexus_ingestion.monitoring.health_endpoint import HealthEndpoint
    from nexus_ingestion.persistence.timescale_writer import TimescaleWriter
    from nexus_ingestion.publishers.health_publisher import HealthPublisher
    from nexus_ingestion.publishers.redis_publisher import RedisPublisher

logger = structlog.get_logger()


class IngestionService:
    """Orchestrates adapter tasks with independent failure handling.

    Uses manual asyncio.create_task + add_done_callback for adapter isolation
    (NO TaskGroup — a failing adapter must not cancel siblings per FR-004).
    """

    def __init__(self, config: IngestionConfig) -> None:
        self._config = config
        self._adapters: dict[str, BaseAdapter] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._restart_counts: dict[str, int] = {}
        self._restart_handles: dict[str, asyncio.TimerHandle] = {}
        self._restart_tasks: dict[str, asyncio.Task[None]] = {}
        self._running = False
        self._event_callback: Callable[[Any], Any] | None = None

        # Components wired during setup
        self._redis: Redis[Any] | None = None
        self._market_publisher: RedisPublisher | None = None
        self._news_publisher: RedisPublisher | None = None
        self._health_publisher: HealthPublisher | None = None
        self._timescale_writer: TimescaleWriter | None = None
        self._gap_detector: GapDetector | None = None
        self._health_endpoint: HealthEndpoint | None = None

    def register_adapter(self, adapter: BaseAdapter) -> None:
        """Register an adapter for supervised execution."""
        self._adapters[adapter.adapter_id] = adapter
        self._restart_counts[adapter.adapter_id] = 0

    def set_event_callback(self, callback: Callable[[Any], Any]) -> None:
        """Set the callback invoked for each event produced by adapters."""
        self._event_callback = callback

    def set_publishers(
        self,
        redis: Redis[Any],
        market_publisher: RedisPublisher,
        health_publisher: HealthPublisher,
        news_publisher: RedisPublisher | None = None,
    ) -> None:
        """Wire publishers for event routing."""
        self._redis = redis
        self._market_publisher = market_publisher
        self._news_publisher = news_publisher
        self._health_publisher = health_publisher

    def set_timescale_writer(self, writer: TimescaleWriter) -> None:
        """Wire the TimescaleDB writer."""
        self._timescale_writer = writer

    def set_gap_detector(self, detector: GapDetector) -> None:
        """Wire the gap detector."""
        self._gap_detector = detector

    def set_health_endpoint(self, endpoint: HealthEndpoint) -> None:
        """Wire the health endpoint."""
        self._health_endpoint = endpoint
        endpoint.set_adapter_healths_provider(lambda: self.adapter_healths)

    @property
    def adapter_healths(self) -> list[AdapterHealth]:
        """Current health for all registered adapters."""
        return [adapter.health for adapter in self._adapters.values()]

    async def handle_event(self, event: MarketEvent) -> None:
        """Route an event to all consumers: publisher, writer, gap detector."""
        # Publish to appropriate Redis stream
        if event.event_type == EventType.NEWS_ARTICLE:
            if self._news_publisher:
                await self._news_publisher.publish(event.to_redis_fields())
        else:
            if self._market_publisher:
                await self._market_publisher.publish(event.to_redis_fields())

        # Persist to TimescaleDB
        if self._timescale_writer:
            if not self._timescale_writer.enqueue(event):
                logger.warning(
                    "timescale_queue_full_event_dropped",
                    source=event.source,
                    event_type=event.event_type,
                )
                await self.handle_health_alert(
                    HealthAlert(
                        alert_type="PERSISTENCE_ERROR",
                        adapter_id=event.source,
                        severity=Severity.MEDIUM,
                        timestamp=datetime.now(UTC),
                        message=(f"TimescaleDB queue full — event dropped for {event.source}"),
                    )
                )

        # Update gap detector
        if self._gap_detector and event.asset:
            self._gap_detector.record_event(event.source, event.asset)

        # Call additional callback if set
        if self._event_callback:
            result = self._event_callback(event)
            if asyncio.iscoroutine(result):
                await result

    async def handle_health_alert(self, alert: HealthAlert) -> None:
        """Publish a health alert to the health events stream."""
        if self._health_publisher:
            await self._health_publisher.publish(alert)

    async def start(self) -> None:
        """Start all registered adapters and supporting components as supervised tasks."""
        self._running = True

        # Start supporting components
        if self._timescale_writer:
            await self._timescale_writer.start()
        if self._gap_detector:
            await self._gap_detector.start()
        if self._health_endpoint:
            await self._health_endpoint.start()

        # Start adapters
        for adapter_id, adapter in self._adapters.items():
            self._start_adapter_task(adapter_id, adapter)
        logger.info("ingestion_service_started", adapter_count=len(self._adapters))

    def _start_adapter_task(self, adapter_id: str, adapter: BaseAdapter) -> None:
        """Create a task for an adapter and attach failure callback."""
        task = asyncio.create_task(self._run_adapter(adapter), name=f"adapter:{adapter_id}")
        self._tasks[adapter_id] = task
        task.add_done_callback(lambda t: self._on_adapter_done(adapter_id, t))

    async def _run_adapter(self, adapter: BaseAdapter) -> None:
        """Run an adapter's full lifecycle."""
        await adapter.connect()
        await adapter.subscribe()
        await adapter.run()

    def _on_adapter_done(self, adapter_id: str, task: asyncio.Task[None]) -> None:
        """Callback when an adapter task completes (normally or via exception)."""
        if not self._running:
            return

        if task.cancelled():
            logger.info("adapter_cancelled", adapter_id=adapter_id)
            return

        exc = task.exception()
        if exc is not None:
            logger.error("adapter_failed", adapter_id=adapter_id, error=str(exc), exc_info=exc)
            self._schedule_restart(adapter_id)
        else:
            logger.info("adapter_completed", adapter_id=adapter_id)
            self._restart_counts[adapter_id] = 0

    def _schedule_restart(self, adapter_id: str) -> None:
        """Schedule adapter restart with exponential backoff."""
        count = self._restart_counts.get(adapter_id, 0)
        max_attempts = self._config.max_restart_attempts

        if max_attempts > 0 and count >= max_attempts:
            logger.error(
                "adapter_max_restarts_exceeded",
                adapter_id=adapter_id,
                max_attempts=max_attempts,
            )
            return

        self._restart_counts[adapter_id] = count + 1
        delay = min(
            self._config.restart_backoff_base * (2**count),
            self._config.restart_backoff_max,
        )
        logger.info(
            "adapter_restart_scheduled",
            adapter_id=adapter_id,
            delay_s=round(delay, 1),
            attempt=count + 1,
        )

        loop = asyncio.get_running_loop()
        handle = loop.call_later(delay, self._restart_adapter, adapter_id)
        self._restart_handles[adapter_id] = handle

    def _restart_adapter(self, adapter_id: str) -> None:
        """Sync entry point called by call_later; hands off to async restart."""
        self._restart_handles.pop(adapter_id, None)
        adapter = self._adapters.get(adapter_id)
        if adapter and self._running:
            task = asyncio.create_task(
                self._restart_adapter_async(adapter_id, adapter),
                name=f"restart:{adapter_id}",
            )
            self._restart_tasks[adapter_id] = task
            task.add_done_callback(lambda t: self._on_restart_done(adapter_id, t))

    def _on_restart_done(self, adapter_id: str, task: asyncio.Task[None]) -> None:
        """Log any exception that escapes the restart coroutine."""
        self._restart_tasks.pop(adapter_id, None)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error(
                "adapter_restart_error",
                adapter_id=adapter_id,
                error=str(exc),
                exc_info=exc,
            )

    async def _restart_adapter_async(self, adapter_id: str, adapter: BaseAdapter) -> None:
        """Clean up the adapter then start a fresh task."""
        logger.info("adapter_restarting", adapter_id=adapter_id)
        try:
            await adapter.stop()
        except Exception:
            logger.warning(
                "adapter_stop_error_before_restart", adapter_id=adapter_id, exc_info=True
            )
        if self._running:
            self._start_adapter_task(adapter_id, adapter)

    async def stop(self) -> None:
        """Gracefully shut down all adapter tasks and supporting components."""
        self._running = False
        logger.info("ingestion_service_stopping")

        # Cancel any pending restart timers
        for handle in self._restart_handles.values():
            handle.cancel()
        self._restart_handles.clear()

        # Cancel any in-flight restart tasks
        for task in self._restart_tasks.values():
            if not task.done():
                task.cancel()
        if self._restart_tasks:
            await asyncio.gather(*self._restart_tasks.values(), return_exceptions=True)
        self._restart_tasks.clear()

        # Stop all adapters
        for adapter in self._adapters.values():
            try:
                await adapter.stop()
            except Exception:
                logger.warning("adapter_stop_error", adapter_id=adapter.adapter_id, exc_info=True)

        # Cancel all tasks
        for task in self._tasks.values():
            if not task.done():
                task.cancel()

        # Wait for tasks to finish
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)

        self._tasks.clear()

        # Stop supporting components
        if self._health_endpoint:
            await self._health_endpoint.stop()
        if self._gap_detector:
            await self._gap_detector.stop()
        if self._timescale_writer:
            await self._timescale_writer.stop()
        if self._redis:
            await self._redis.aclose()  # type: ignore[attr-defined]

        logger.info("ingestion_service_stopped")
