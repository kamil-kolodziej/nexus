"""Unit tests for the sentiment HealthPublisher."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from nexus_common.schemas.enums import Severity
from nexus_common.schemas.health_alert import HealthAlert
from nexus_sentiment.publishers.health_publisher import (
    HEALTH_MAXLEN,
    HEALTH_STREAM,
    HealthPublisher,
)


def _alert(**overrides) -> HealthAlert:
    fields = {
        "alert_type": "MODEL_INFERENCE_ERROR",
        "adapter_id": "nexus-sentiment",
        "severity": Severity.HIGH,
        "timestamp": datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC),
        "message": "test",
    }
    fields.update(overrides)
    return HealthAlert(**fields)


@pytest.fixture
def redis():
    return AsyncMock()


class TestHealthPublisher:
    async def test_publish_writes_to_default_stream(self, redis):
        redis.xadd = AsyncMock(return_value="1-0")
        pub = HealthPublisher(redis)

        entry_id = await pub.publish(_alert())

        assert entry_id == "1-0"
        redis.xadd.assert_awaited_once()
        args, kwargs = redis.xadd.await_args
        assert args[0] == HEALTH_STREAM
        assert kwargs["maxlen"] == HEALTH_MAXLEN
        assert kwargs["approximate"] is True

    async def test_publish_passes_alert_redis_fields(self, redis):
        redis.xadd = AsyncMock(return_value="1-0")
        pub = HealthPublisher(redis)
        alert = _alert(alert_type="DEAD_LETTER_CLAIMED", severity=Severity.MEDIUM)

        await pub.publish(alert)

        fields = redis.xadd.await_args.args[1]
        assert fields == alert.to_redis_fields()
        assert fields["alert_type"] == "DEAD_LETTER_CLAIMED"
        assert fields["severity"] == Severity.MEDIUM.value

    async def test_publish_swallows_xadd_exception_and_returns_none(self, redis):
        redis.xadd = AsyncMock(side_effect=ConnectionError("boom"))
        pub = HealthPublisher(redis)

        result = await pub.publish(_alert())

        assert result is None
        # No buffering — verify no buffer attribute exists at all.
        assert not hasattr(pub, "_buffer")

    async def test_publish_honors_custom_stream_and_maxlen(self, redis):
        redis.xadd = AsyncMock(return_value="1-0")
        pub = HealthPublisher(redis, stream="custom:stream", maxlen=42)

        await pub.publish(_alert())

        args, kwargs = redis.xadd.await_args
        assert args[0] == "custom:stream"
        assert kwargs["maxlen"] == 42
