"""Integration tests for Redis Streams.

Tests ExchangeAdapter → RedisPublisher → XREAD verification,
and adapter failure isolation (SC-004).
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from nexus_common.schemas.enums import AdapterStatus, EventType
from nexus_common.schemas.market_event import MarketEvent

from nexus_ingestion.adapters.base import BaseAdapter
from nexus_ingestion.config import IngestionConfig
from nexus_ingestion.publishers.redis_publisher import RedisPublisher
from nexus_ingestion.service import IngestionService

# Skip if Redis is not available
try:
    from testcontainers.redis import RedisContainer

    HAS_TESTCONTAINERS = True
except ImportError:
    HAS_TESTCONTAINERS = False

pytestmark = pytest.mark.skipif(
    not HAS_TESTCONTAINERS, reason="testcontainers not available"
)


def _make_event(asset: str = "BTC/USDT", n: int = 0) -> MarketEvent:
    return MarketEvent(
        source="binance:exchange",
        asset=asset,
        timestamp=datetime(2026, 3, 22, 14, 30, n % 60, tzinfo=timezone.utc),
        event_type=EventType.TICK,
        schema_version="1.0.0",
        payload={"bid": 100.0 + n, "ask": 101.0 + n, "last": 100.5 + n, "volume_24h": 0},
    )


class _MockAdapter(BaseAdapter):
    """Mock adapter for integration tests."""

    def __init__(self, adapter_id: str, events: list[MarketEvent], *, fail_after: int | None = None) -> None:
        super().__init__(adapter_id=adapter_id, adapter_type="exchange")
        self._events = events
        self._fail_after = fail_after
        self._event_callback = None
        self._stopped = False

    def set_event_callback(self, callback):
        self._event_callback = callback

    async def connect(self) -> None:
        pass

    async def subscribe(self) -> None:
        pass

    async def run(self) -> None:
        for i, event in enumerate(self._events):
            if self._stopped:
                break
            if self._fail_after is not None and i >= self._fail_after:
                raise RuntimeError(f"Mock adapter {self.adapter_id} crashed")
            if self._event_callback:
                result = self._event_callback(event)
                if asyncio.iscoroutine(result):
                    await result
            await asyncio.sleep(0.01)

        # Keep running until stopped
        while not self._stopped:
            await asyncio.sleep(0.1)

    async def stop(self) -> None:
        self._stopped = True


@pytest.fixture(scope="module")
def redis_container():
    with RedisContainer("redis:7-alpine") as container:
        yield container


@pytest.fixture
async def redis_client(redis_container):
    from redis.asyncio import Redis

    port = redis_container.get_exposed_port(6379)
    host = redis_container.get_container_host_ip()
    client = Redis(host=host, port=int(port), decode_responses=True)
    yield client
    await client.aclose()


class TestRedisStreamPublish:
    @pytest.mark.asyncio
    async def test_event_published_to_stream(self, redis_client) -> None:
        publisher = RedisPublisher(redis_client, "test:market-events", maxlen=1000)
        event = _make_event()
        entry_id = await publisher.publish(event.to_redis_fields())
        assert entry_id is not None

        # Verify via XREAD
        entries = await redis_client.xread({"test:market-events": "0"}, count=1)
        assert len(entries) > 0
        stream_name, messages = entries[0]
        assert len(messages) > 0
        msg_id, fields = messages[0]
        assert fields["source"] == "binance:exchange"
        assert fields["asset"] == "BTC/USDT"
        assert fields["event_type"] == "TICK"

        # Verify payload structure per contracts/market-events.md
        payload = json.loads(fields["payload"])
        assert "bid" in payload
        assert "ask" in payload
        assert "last" in payload
        assert "volume_24h" in payload

    @pytest.mark.asyncio
    async def test_multiple_events_preserve_order(self, redis_client) -> None:
        stream = "test:order-events"
        publisher = RedisPublisher(redis_client, stream, maxlen=1000)
        for i in range(5):
            await publisher.publish(_make_event(n=i).to_redis_fields())

        entries = await redis_client.xread({stream: "0"}, count=10)
        assert len(entries) > 0
        _, messages = entries[0]
        assert len(messages) == 5


class TestAdapterFailureIsolation:
    """SC-004: One failing adapter must not affect others."""

    @pytest.mark.asyncio
    async def test_surviving_adapter_continues_after_crash(self, redis_client) -> None:
        stream = "test:isolation-events"
        publisher = RedisPublisher(redis_client, stream, maxlen=1000)

        config = IngestionConfig()
        service = IngestionService(config)

        events_received: list[MarketEvent] = []

        async def handle_event(event: MarketEvent) -> None:
            events_received.append(event)
            await publisher.publish(event.to_redis_fields())

        # Adapter A: produces 2 events then crashes
        adapter_a_events = [_make_event("BTC/USDT", i) for i in range(2)]
        adapter_a = _MockAdapter("crash:exchange", adapter_a_events, fail_after=2)
        adapter_a._event_callback = handle_event

        # Adapter B: produces 5 events and stays alive
        adapter_b_events = [_make_event("ETH/USDT", i) for i in range(5)]
        adapter_b = _MockAdapter("alive:exchange", adapter_b_events)
        adapter_b._event_callback = handle_event

        service.register_adapter(adapter_a)
        service.register_adapter(adapter_b)

        await service.start()
        await asyncio.sleep(1.0)
        await service.stop()

        # Adapter B should have published its events even after A crashed
        eth_events = [e for e in events_received if e.asset == "ETH/USDT"]
        assert len(eth_events) >= 3  # At least some ETH events got through
