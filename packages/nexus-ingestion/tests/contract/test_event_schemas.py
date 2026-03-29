"""Contract snapshot tests for Redis serialization format.

Verify serialization format matches contracts/market-events.md and contracts/news-events.md.
Uses syrupy for snapshot testing.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from nexus_common.schemas.enums import EventType
from nexus_common.schemas.market_event import MarketEvent

# Fixed timestamp for reproducible snapshots
FIXED_TS = datetime(2026, 3, 22, 14, 30, 0, 123000, tzinfo=UTC)


class TestMarketEventContracts:
    """Snapshot tests verifying Redis serialization format per contracts/market-events.md."""

    def test_tick_serialization(self, snapshot) -> None:
        event = MarketEvent(
            source="binance:exchange",
            asset="BTC/USDT",
            timestamp=FIXED_TS,
            event_type=EventType.TICK,
            schema_version="1.0.0",
            payload={"bid": 67234.50, "ask": 67235.10, "last": 67234.80, "volume_24h": 12345.67},
        )
        fields = event.to_redis_fields()
        # Normalize payload JSON for stable snapshots
        fields["payload"] = json.dumps(json.loads(fields["payload"]), sort_keys=True)
        assert fields == snapshot

    def test_orderbook_serialization(self, snapshot) -> None:
        event = MarketEvent(
            source="binance:exchange",
            asset="BTC/USDT",
            timestamp=FIXED_TS,
            event_type=EventType.ORDER_BOOK_UPDATE,
            schema_version="1.0.0",
            payload={
                "bids": [[67234.50, 1.5], [67234.00, 2.3]],
                "asks": [[67235.10, 0.8], [67235.50, 1.2]],
                "depth": 10,
            },
        )
        fields = event.to_redis_fields()
        fields["payload"] = json.dumps(json.loads(fields["payload"]), sort_keys=True)
        assert fields == snapshot

    def test_trade_serialization(self, snapshot) -> None:
        event = MarketEvent(
            source="binance:exchange",
            asset="BTC/USDT",
            timestamp=FIXED_TS,
            event_type=EventType.TRADE,
            schema_version="1.0.0",
            payload={
                "trade_id": "123456789",
                "price": 67234.80,
                "amount": 0.15,
                "side": "buy",
                "taker_or_maker": "taker",
            },
        )
        fields = event.to_redis_fields()
        fields["payload"] = json.dumps(json.loads(fields["payload"]), sort_keys=True)
        assert fields == snapshot

    def test_candle_serialization(self, snapshot) -> None:
        event = MarketEvent(
            source="binance:exchange",
            asset="BTC/USDT",
            timestamp=FIXED_TS,
            event_type=EventType.CANDLE,
            schema_version="1.0.0",
            payload={
                "open": 67200.00,
                "high": 67300.00,
                "low": 67150.00,
                "close": 67234.80,
                "volume": 456.78,
                "timeframe": "1m",
            },
        )
        fields = event.to_redis_fields()
        fields["payload"] = json.dumps(json.loads(fields["payload"]), sort_keys=True)
        assert fields == snapshot


class TestNewsEventContracts:
    """Snapshot tests verifying Redis serialization format per contracts/news-events.md."""

    def test_news_article_serialization(self, snapshot) -> None:
        event = MarketEvent(
            source="newsapi:news",
            asset="",
            timestamp=FIXED_TS,
            event_type=EventType.NEWS_ARTICLE,
            schema_version="1.0.0",
            payload={
                "headline": "Bitcoin Surges Past $70K as ETF Inflows Accelerate",
                "body_summary": "Bitcoin prices reached a new all-time high...",
                "url": "https://example.com/article/bitcoin-surges",
                "source_name": "coindesk",
                "published_at": "2026-03-22T10:15:00Z",
                "related_assets": ["BTC/USDT", "ETH/USDT"],
            },
        )
        fields = event.to_redis_fields()
        fields["payload"] = json.dumps(json.loads(fields["payload"]), sort_keys=True)
        assert fields == snapshot
