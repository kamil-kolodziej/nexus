"""Health alert publisher for the ingestion health events stream."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, cast

from nexus_common.schemas.health_alert import HealthAlert

if TYPE_CHECKING:
    from redis.asyncio import Redis

logger = logging.getLogger(__name__)

HEALTH_STREAM = "nexus:ingestion-health-events"
HEALTH_MAXLEN = 5000


class HealthPublisher:
    """Publishes health alerts to the ingestion health events stream."""

    def __init__(
        self,
        redis: Redis[Any],
        stream: str = HEALTH_STREAM,
        maxlen: int = HEALTH_MAXLEN,
    ) -> None:
        self._redis = redis
        self._stream = stream
        self._maxlen = maxlen

    async def publish(self, alert: HealthAlert) -> str | None:
        """Publish a health alert to the stream."""
        try:
            entry_id = await self._redis.xadd(
                self._stream,
                alert.to_redis_fields(),
                maxlen=self._maxlen,
                approximate=True,
            )
            logger.info(
                "Health alert published: %s %s %s",
                alert.alert_type,
                alert.severity,
                alert.adapter_id,
            )
            return cast(str, entry_id)
        except Exception:
            logger.error(
                "Failed to publish health alert: %s %s",
                alert.alert_type,
                alert.adapter_id,
                exc_info=True,
            )
            return None
