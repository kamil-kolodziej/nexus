"""SentimentService — consumer loop orchestrator."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog
from nexus_common.schemas.enums import EventType, Severity
from nexus_common.schemas.health_alert import HealthAlert
from nexus_common.schemas.market_event import MarketEvent, NewsArticle, SentimentScore

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from nexus_sentiment.config import SentimentConfig
    from nexus_sentiment.extraction.asset_extractor import AssetExtractor
    from nexus_sentiment.monitoring.health_endpoint import HealthEndpoint
    from nexus_sentiment.persistence.timescale_writer import TimescaleWriter
    from nexus_sentiment.processors.base import BaseSentimentProcessor
    from nexus_sentiment.publishers.health_publisher import HealthPublisher
    from nexus_sentiment.publishers.redis_publisher import RedisPublisher

logger = structlog.get_logger()


class SentimentService:
    """Orchestrates the sentiment pipeline: consume, analyze, fan-out, publish."""

    def __init__(
        self,
        config: SentimentConfig,
        redis: Redis[Any],
        processor: BaseSentimentProcessor,
        redis_publisher: RedisPublisher,
        health_publisher: HealthPublisher,
        timescale_writer: TimescaleWriter | None = None,
        health_endpoint: HealthEndpoint | None = None,
        asset_extractor: AssetExtractor | None = None,
    ) -> None:
        self._config = config
        self._redis = redis
        self._processor = processor
        self._redis_publisher = redis_publisher
        self._health_publisher = health_publisher
        self._timescale_writer = timescale_writer
        self._health_endpoint = health_endpoint
        self._asset_extractor = asset_extractor

        self._events_processed = 0
        self._errors = 0
        self._consecutive_inference_errors = 0
        self._running = False
        self._consumer_task: asyncio.Task[None] | None = None
        self._health_task: asyncio.Task[None] | None = None
        self._sweep_task: asyncio.Task[None] | None = None
        self._consumer_name = f"consumer-{id(self)}"

    async def start(self) -> None:
        """Start consumer loop, health endpoint, and claim sweep as independent tasks."""
        # Create consumer group (ignore BUSYGROUP if exists)
        try:
            await self._redis.xgroup_create(
                self._config.input_stream,
                self._config.consumer_group,
                id="$",
                mkstream=True,
            )
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                raise

        # Bring up supporting components first — if any of these raise, no
        # supervised tasks have been created yet, so the failure propagates
        # cleanly without orphaning the consumer loop. Mirrors the ordering
        # in nexus-ingestion's IngestionService.start().
        if self._health_endpoint:
            self._health_endpoint.set_health_provider(self._get_health)
            await self._health_endpoint.start()

        if self._timescale_writer:
            await self._timescale_writer.start()

        self._running = True

        # FR-009: independent tasks with add_done_callback, no TaskGroup
        self._consumer_task = asyncio.create_task(self._consumer_loop(), name="sentiment-consumer")
        self._consumer_task.add_done_callback(self._on_consumer_done)

        self._sweep_task = asyncio.create_task(
            self._claim_sweep_loop(), name="sentiment-claim-sweep"
        )
        self._sweep_task.add_done_callback(self._on_sweep_done)

        logger.info(
            "sentiment_service_started",
            processor=self._config.processor_type,
            input_stream=self._config.input_stream,
            output_stream=self._config.output_stream,
        )

    async def stop(self) -> None:
        """Graceful shutdown."""
        self._running = False

        if self._consumer_task and not self._consumer_task.done():
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass

        if self._sweep_task and not self._sweep_task.done():
            self._sweep_task.cancel()
            try:
                await self._sweep_task
            except asyncio.CancelledError:
                pass

        if self._health_endpoint:
            await self._health_endpoint.stop()

        if self._timescale_writer:
            await self._timescale_writer.stop()

        await self._processor.close()
        logger.info("sentiment_service_stopped")

    async def _consumer_loop(self) -> None:
        """Main consumer loop: XREADGROUP, process, publish, XACK."""
        while self._running:
            try:
                messages = await self._redis.xreadgroup(
                    self._config.consumer_group,
                    self._consumer_name,
                    {self._config.input_stream: ">"},
                    count=1,
                    block=self._config.block_timeout,
                )

                if not messages:
                    continue

                for _stream, entries in messages:
                    for message_id, fields in entries:
                        await self._process_message(message_id, fields)

            except asyncio.CancelledError:
                break
            except Exception:
                logger.error(
                    "consumer_loop_error",
                    events_processed=self._events_processed,
                    consecutive_inference_errors=self._consecutive_inference_errors,
                    input_stream=self._config.input_stream,
                    exc_info=True,
                )
                await asyncio.sleep(1)

    async def _process_message(self, message_id: str, fields: dict[str, str]) -> None:
        """Process a single message from the input stream."""
        # Parse MarketEvent envelope
        try:
            event = MarketEvent.from_redis_fields(fields)
        except Exception:
            logger.warning("malformed_market_event", message_id=message_id, exc_info=True)
            await self._redis.xack(  # type: ignore[no-untyped-call]
                self._config.input_stream, self._config.consumer_group, message_id
            )
            return

        # Validate NewsArticle payload
        try:
            article = NewsArticle.model_validate(event.payload)
        except Exception:
            logger.warning(
                "malformed_news_article",
                message_id=message_id,
                exc_info=True,
            )
            await self._redis.xack(  # type: ignore[no-untyped-call]
                self._config.input_stream, self._config.consumer_group, message_id
            )
            return

        # FR-002: Combine headline + body_summary
        if article.body_summary:
            combined_text = f"{article.headline}. {article.body_summary}"
        else:
            combined_text = article.headline

        # Edge case: empty text
        if not combined_text.strip():
            logger.warning("empty_article_text", article_url=article.url)
            result_label = "neutral"
            result_score = 0.0
            result_confidence = 0.0
        else:
            # FR-008: Run inference in thread pool
            try:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(None, self._processor.analyze, combined_text)
                result_label = result.label
                result_score = result.score
                result_confidence = result.confidence
                self._consecutive_inference_errors = 0
            except Exception:
                logger.error(
                    "inference_error",
                    article_url=article.url,
                    message_id=message_id,
                    exc_info=True,
                )
                self._errors += 1
                self._consecutive_inference_errors += 1
                await self._emit_health_alert(
                    "MODEL_INFERENCE_ERROR",
                    Severity.HIGH,
                    f"Inference failed for {article.url}",
                )
                # SRC-004: do NOT XACK — leave pending for retry/claim
                return

        # Build effective asset list
        effective_assets: list[str | None] = list(
            self._build_effective_assets(article, combined_text)
        )

        # Fan-out: one SentimentScore per asset (or asset=None for general market)
        if not effective_assets:
            effective_assets = [None]

        all_published = True
        for asset in effective_assets:
            score_payload = SentimentScore(
                article_url=article.url,
                asset=asset,
                score=result_score,
                confidence=result_confidence,
                sentiment_label=result_label,
                model_id=self._processor.model_id,
            )

            sentiment_event = MarketEvent(
                source=f"nexus-sentiment:{self._config.processor_type}",
                asset=asset,
                timestamp=datetime.now(UTC),
                event_type=EventType.SENTIMENT_SCORE,
                schema_version="1.0.0",
                payload=score_payload.model_dump(),
            )

            entry_id = await self._redis_publisher.publish(sentiment_event.to_redis_fields())
            if entry_id is None:
                all_published = False
                break

            # Queue for TimescaleDB persistence
            if self._timescale_writer:
                self._timescale_writer.enqueue(sentiment_event)

        # FR-006: XACK only after all publishes succeed
        if all_published:
            await self._redis.xack(  # type: ignore[no-untyped-call]
                self._config.input_stream, self._config.consumer_group, message_id
            )
            self._events_processed += 1
        else:
            logger.warning(
                "partial_publish_failure",
                message_id=message_id,
                article_url=article.url,
            )

    def _build_effective_assets(self, article: Any, combined_text: str) -> list[str]:
        """Build deduplicated, active-asset-filtered effective asset list."""
        active = set(self._config.active_assets)
        assets: list[str] = []

        # related_assets from the article — filter against active_assets
        if article.related_assets:
            assets.extend(a for a in article.related_assets if a in active)

        # AssetExtractor already filters against active_assets internally
        if self._asset_extractor:
            assets.extend(self._asset_extractor.extract(combined_text))

        # Deduplicate preserving order
        seen: set[str] = set()
        unique: list[str] = []
        for a in assets:
            if a not in seen:
                seen.add(a)
                unique.append(a)

        # Cap at max_fan_out
        if len(unique) > self._config.max_fan_out:
            logger.warning(
                "max_fan_out_exceeded",
                total=len(unique),
                max_fan_out=self._config.max_fan_out,
            )
            unique = unique[: self._config.max_fan_out]

        return unique

    async def _claim_sweep_loop(self) -> None:
        """Periodically claim stale pending messages via XAUTOCLAIM."""
        while self._running:
            try:
                await asyncio.sleep(self._config.claim_sweep_interval)
                if not self._running:
                    break  # type: ignore[unreachable]

                min_idle_time = self._config.pending_claim_threshold * 1000  # ms

                result = await self._redis.xautoclaim(
                    self._config.input_stream,
                    self._config.consumer_group,
                    self._consumer_name,
                    min_idle_time=min_idle_time,
                    count=10,
                )

                # xautoclaim returns (next_start_id, [(id, fields), ...], [deleted_ids])
                if len(result) >= 2:
                    claimed_messages = result[1]
                    for msg_id, _fields in claimed_messages:
                        logger.warning("dead_letter_claimed", message_id=msg_id)
                        await self._emit_health_alert(
                            "DEAD_LETTER_CLAIMED",
                            Severity.MEDIUM,
                            f"Claimed stale pending message: {msg_id}",
                        )
                        await self._redis.xack(  # type: ignore[no-untyped-call]
                            self._config.input_stream,
                            self._config.consumer_group,
                            msg_id,
                        )

            except asyncio.CancelledError:
                break
            except Exception:
                logger.error("claim_sweep_error", exc_info=True)
                await asyncio.sleep(5)

    async def _emit_health_alert(self, alert_type: str, severity: Severity, message: str) -> None:
        """Emit a health alert via HealthPublisher."""
        alert = HealthAlert(
            alert_type=alert_type,
            adapter_id="nexus-sentiment",
            severity=severity,
            timestamp=datetime.now(UTC),
            message=message,
        )
        await self._health_publisher.publish(alert)

    def _get_health(self) -> dict[str, Any]:
        """Return RFC-shaped health body. See docs/design § Monitoring."""
        from importlib.metadata import PackageNotFoundError, version

        checks: dict[str, dict[str, Any]] = {}

        # Processor: rolling window of consecutive inference failures.
        if self._consecutive_inference_errors >= 5:
            checks["processor:inference"] = {
                "status": "error",
                "output": f"{self._consecutive_inference_errors} consecutive inference failures",
                "observedValue": self._consecutive_inference_errors,
            }
        else:
            checks["processor:inference"] = {
                "status": "ok",
                "observedValue": self._processor.model_id,
            }

        # Redis publisher: degraded when disconnected (buffering).
        redis_connected = getattr(self._redis_publisher, "_connected", True)
        buffer_size = getattr(self._redis_publisher, "buffer_size", 0)
        if not redis_connected:
            checks["redis:publisher"] = {
                "status": "degraded",
                "output": f"disconnected, buffering {buffer_size} events",
                "observedValue": buffer_size,
            }
        else:
            checks["redis:publisher"] = {"status": "ok", "observedValue": buffer_size}

        # TimescaleDB writer (optional).
        if self._timescale_writer is not None:
            checks["timescale:writer"] = {"status": "ok"}

        # Top-level status = worst of components.
        severities = {"ok": 0, "degraded": 1, "error": 2}
        worst = max((severities[c["status"]] for c in checks.values()), default=0)
        status = ("ok", "degraded", "error")[worst]

        try:
            pkg_version = version("nexus-sentiment")
        except PackageNotFoundError:
            pkg_version = "unknown"

        return {
            "status": status,
            "serviceId": "nexus-sentiment",
            "version": pkg_version,
            "checks": checks,
        }

    def _on_consumer_done(self, task: asyncio.Task[None]) -> None:
        """Callback when consumer task completes."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error("consumer_task_crashed", error=str(exc), exc_info=exc)

    def _on_sweep_done(self, task: asyncio.Task[None]) -> None:
        """Callback when claim sweep task completes."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error("claim_sweep_task_crashed", error=str(exc), exc_info=exc)
