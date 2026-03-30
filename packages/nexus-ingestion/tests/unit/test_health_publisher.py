"""Unit tests for HealthPublisher."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from nexus_common.schemas.enums import Severity
from nexus_common.schemas.health_alert import HealthAlert
from nexus_ingestion.publishers.health_publisher import (
    HEALTH_MAXLEN,
    HEALTH_STREAM,
    HealthPublisher,
)


def _make_alert(alert_type: str = "ADAPTER_DOWN") -> HealthAlert:
    return HealthAlert(
        alert_type=alert_type,
        adapter_id="binance:exchange",
        severity=Severity.HIGH,
        timestamp=datetime(2026, 3, 29, 12, 0, 0, tzinfo=UTC),
        message="Test alert",
    )


class TestHealthPublisherPublish:
    @pytest.mark.asyncio
    async def test_publish_success_returns_entry_id(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.xadd.return_value = "1711706400000-0"
        publisher = HealthPublisher(mock_redis)

        result = await publisher.publish(_make_alert())

        assert result == "1711706400000-0"

    @pytest.mark.asyncio
    async def test_publish_calls_xadd_with_correct_args(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.xadd.return_value = "1-0"
        publisher = HealthPublisher(mock_redis)
        alert = _make_alert("ADAPTER_RECONNECTING")

        await publisher.publish(alert)

        mock_redis.xadd.assert_called_once_with(
            HEALTH_STREAM,
            alert.to_redis_fields(),
            maxlen=HEALTH_MAXLEN,
            approximate=True,
        )

    @pytest.mark.asyncio
    async def test_publish_uses_custom_stream_and_maxlen(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.xadd.return_value = "1-0"
        publisher = HealthPublisher(mock_redis, stream="custom:stream", maxlen=100)

        await publisher.publish(_make_alert())

        call_kwargs = mock_redis.xadd.call_args
        assert call_kwargs[0][0] == "custom:stream"
        assert call_kwargs[1]["maxlen"] == 100

    @pytest.mark.asyncio
    async def test_publish_redis_error_returns_none(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.xadd.side_effect = ConnectionError("Redis unavailable")
        publisher = HealthPublisher(mock_redis)

        result = await publisher.publish(_make_alert())

        assert result is None

    @pytest.mark.asyncio
    async def test_publish_redis_error_does_not_raise(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.xadd.side_effect = RuntimeError("unexpected")
        publisher = HealthPublisher(mock_redis)

        # Must not raise — alerts are dropped on Redis unavailability
        result = await publisher.publish(_make_alert())
        assert result is None
