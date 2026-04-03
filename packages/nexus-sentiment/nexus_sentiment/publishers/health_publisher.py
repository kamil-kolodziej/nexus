"""Health alert publisher for the sentiment health events stream."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import structlog
from nexus_common.schemas.health_alert import HealthAlert

if TYPE_CHECKING:
    from redis.asyncio import Redis

HEALTH_STREAM = "nexus:sentiment-health-events"
HEALTH_MAXLEN = 5000


class HealthPublisher:
    """Publishes health alerts to the sentiment health events stream.

    No buffering on disconnect — alerts are dropped when Redis is unavailable
    to avoid circular dependencies.
    """

    def __init__(
        self,
        redis: Redis[Any],
        stream: str = HEALTH_STREAM,
        maxlen: int = HEALTH_MAXLEN,
    ) -> None:
        self._redis = redis
        self._stream = stream
        self._maxlen = maxlen
        self._logger = structlog.get_logger().bind(stream=stream)

    async def publish(self, alert: HealthAlert) -> str | None:
        """Publish a health alert to the stream."""
        try:
            entry_id = await self._redis.xadd(
                self._stream,
                alert.to_redis_fields(),
                maxlen=self._maxlen,
                approximate=True,
            )
            self._logger.info(
                "health_alert_published",
                alert_type=alert.alert_type,
                severity=alert.severity,
                adapter_id=alert.adapter_id,
            )
            return cast(str, entry_id)
        except Exception:
            self._logger.error(
                "health_alert_publish_failed",
                alert_type=alert.alert_type,
                adapter_id=alert.adapter_id,
                exc_info=True,
            )
            return None
