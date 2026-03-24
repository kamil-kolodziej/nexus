"""Property-based schema tests using Hypothesis.

These tests validate MarketEvent + payload types respect constraints:
- Price positivity
- OrderBook ordering
- Semver schema_version
- Credential exclusion in serialized output (SRC-003)
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from nexus_common.schemas.enums import EventType
from nexus_common.schemas.market_event import (
    Candle,
    MarketEvent,
    NewsArticle,
    OrderBookUpdate,
    Tick,
    Trade,
)


# --- Strategies ---

positive_float = st.floats(min_value=0.01, max_value=1e9, allow_nan=False, allow_infinity=False)
non_negative_float = st.floats(min_value=0.0, max_value=1e9, allow_nan=False, allow_infinity=False)


@st.composite
def tick_strategy(draw: st.DrawFn) -> Tick:
    bid = draw(positive_float)
    spread = draw(st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False))
    ask = bid + spread
    last = draw(positive_float)
    volume = draw(non_negative_float)
    return Tick(bid=bid, ask=ask, last=last, volume_24h=volume)


@st.composite
def orderbook_strategy(draw: st.DrawFn) -> OrderBookUpdate:
    n = draw(st.integers(min_value=1, max_value=10))
    bid_prices = sorted(draw(st.lists(positive_float, min_size=n, max_size=n)), reverse=True)
    ask_prices = sorted(draw(st.lists(positive_float, min_size=n, max_size=n)))
    bid_qtys = draw(st.lists(non_negative_float, min_size=n, max_size=n))
    ask_qtys = draw(st.lists(non_negative_float, min_size=n, max_size=n))
    bids = [[p, q] for p, q in zip(bid_prices, bid_qtys)]
    asks = [[p, q] for p, q in zip(ask_prices, ask_qtys)]
    return OrderBookUpdate(bids=bids, asks=asks, depth=n)


@st.composite
def trade_strategy(draw: st.DrawFn) -> Trade:
    return Trade(
        trade_id=draw(st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("L", "N")))),
        price=draw(positive_float),
        amount=draw(positive_float),
        side=draw(st.sampled_from(["buy", "sell"])),
        taker_or_maker=draw(st.sampled_from(["taker", "maker", None])),
    )


@st.composite
def candle_strategy(draw: st.DrawFn) -> Candle:
    o = draw(positive_float)
    c = draw(positive_float)
    h = max(o, c) + draw(st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False))
    l_ = min(o, c) - draw(st.floats(min_value=0.0, max_value=min(o, c) - 0.01 if min(o, c) > 0.01 else 0.0, allow_nan=False, allow_infinity=False))
    if l_ <= 0:
        l_ = 0.01
    return Candle(
        open=o, high=h, low=l_, close=c,
        volume=draw(non_negative_float),
        timeframe=draw(st.sampled_from(["1m", "5m", "15m", "1h", "4h", "1d"])),
    )


# --- Property Tests: Tick ---


class TestTickProperties:
    @given(tick_strategy())
    @settings(max_examples=50)
    def test_tick_always_valid(self, tick: Tick) -> None:
        assert tick.bid > 0
        assert tick.ask > 0
        assert tick.ask >= tick.bid
        assert tick.last > 0
        assert tick.volume_24h >= 0

    def test_tick_rejects_negative_bid(self) -> None:
        with pytest.raises(ValueError):
            Tick(bid=-1.0, ask=2.0, last=1.5, volume_24h=0)

    def test_tick_rejects_ask_less_than_bid(self) -> None:
        with pytest.raises(ValueError):
            Tick(bid=100.0, ask=99.0, last=99.5, volume_24h=0)

    def test_tick_rejects_zero_price(self) -> None:
        with pytest.raises(ValueError):
            Tick(bid=0, ask=1.0, last=0.5, volume_24h=0)


# --- Property Tests: OrderBookUpdate ---


class TestOrderBookProperties:
    @given(orderbook_strategy())
    @settings(max_examples=50)
    def test_orderbook_always_valid(self, ob: OrderBookUpdate) -> None:
        bid_prices = [b[0] for b in ob.bids]
        ask_prices = [a[0] for a in ob.asks]
        assert bid_prices == sorted(bid_prices, reverse=True)
        assert ask_prices == sorted(ask_prices)

    def test_orderbook_rejects_unsorted_bids(self) -> None:
        with pytest.raises(ValueError, match="descending"):
            OrderBookUpdate(
                bids=[[100.0, 1.0], [200.0, 1.0]],
                asks=[[300.0, 1.0]],
                depth=2,
            )

    def test_orderbook_rejects_unsorted_asks(self) -> None:
        with pytest.raises(ValueError, match="ascending"):
            OrderBookUpdate(
                bids=[[200.0, 1.0]],
                asks=[[300.0, 1.0], [200.0, 1.0]],
                depth=1,
            )


# --- Property Tests: Trade ---


class TestTradeProperties:
    @given(trade_strategy())
    @settings(max_examples=50)
    def test_trade_always_valid(self, trade: Trade) -> None:
        assert trade.price > 0
        assert trade.amount > 0
        assert trade.side in ("buy", "sell")

    def test_trade_rejects_empty_id(self) -> None:
        with pytest.raises(ValueError):
            Trade(trade_id="", price=100.0, amount=1.0, side="buy")


# --- Property Tests: Candle ---


class TestCandleProperties:
    @given(candle_strategy())
    @settings(max_examples=50)
    def test_candle_always_valid(self, candle: Candle) -> None:
        assert candle.high >= candle.open
        assert candle.high >= candle.close
        assert candle.low <= candle.open
        assert candle.low <= candle.close
        assert candle.volume >= 0

    def test_candle_rejects_high_below_open(self) -> None:
        with pytest.raises(ValueError):
            Candle(open=100.0, high=99.0, low=98.0, close=99.5, volume=1.0, timeframe="1m")


# --- Property Tests: MarketEvent ---


class TestMarketEventProperties:
    def test_schema_version_must_be_semver(self) -> None:
        with pytest.raises(ValueError, match="semver"):
            MarketEvent(
                source="test:exchange",
                asset="BTC/USDT",
                timestamp=datetime.now(timezone.utc),
                event_type=EventType.TICK,
                schema_version="not-semver",
                payload={"bid": 1.0, "ask": 2.0, "last": 1.5, "volume_24h": 0},
            )

    def test_valid_semver_accepted(self) -> None:
        event = MarketEvent(
            source="test:exchange",
            asset="BTC/USDT",
            timestamp=datetime.now(timezone.utc),
            event_type=EventType.TICK,
            schema_version="1.0.0",
            payload={"bid": 1.0, "ask": 2.0, "last": 1.5, "volume_24h": 0},
        )
        assert event.schema_version == "1.0.0"

    def test_validated_payload_returns_correct_type(self) -> None:
        event = MarketEvent(
            source="binance:exchange",
            asset="BTC/USDT",
            timestamp=datetime.now(timezone.utc),
            event_type=EventType.TICK,
            schema_version="1.0.0",
            payload={"bid": 100.0, "ask": 101.0, "last": 100.5, "volume_24h": 1000.0},
        )
        tick = event.validated_payload()
        assert isinstance(tick, Tick)

    def test_to_redis_fields_excludes_credentials(self) -> None:
        """SRC-003: No credentials in serialized output."""
        event = MarketEvent(
            source="binance:exchange",
            asset="BTC/USDT",
            timestamp=datetime.now(timezone.utc),
            event_type=EventType.TICK,
            schema_version="1.0.0",
            payload={"bid": 100.0, "ask": 101.0, "last": 100.5, "volume_24h": 1000.0},
        )
        fields = event.to_redis_fields()
        all_values = " ".join(str(v) for v in fields.values())
        # Ensure no API key patterns leak into serialized output
        assert "api_key" not in all_values.lower()
        assert "api_secret" not in all_values.lower()
        assert "password" not in all_values.lower()

    def test_round_trip_redis_serialization(self) -> None:
        original = MarketEvent(
            source="binance:exchange",
            asset="BTC/USDT",
            timestamp=datetime(2026, 3, 22, 14, 30, 0, tzinfo=timezone.utc),
            event_type=EventType.TICK,
            schema_version="1.0.0",
            payload={"bid": 100.0, "ask": 101.0, "last": 100.5, "volume_24h": 1000.0},
        )
        fields = original.to_redis_fields()
        restored = MarketEvent.from_redis_fields(fields)
        assert restored.source == original.source
        assert restored.asset == original.asset
        assert restored.event_type == original.event_type
        assert restored.schema_version == original.schema_version
        assert restored.payload == original.payload


# --- Property Tests: NewsArticle ---


class TestNewsArticleProperties:
    def test_body_summary_max_length(self) -> None:
        with pytest.raises(ValueError):
            NewsArticle(
                headline="Test",
                body_summary="x" * 1001,
                url="https://example.com",
                source_name="test",
                published_at=datetime.now(timezone.utc),
            )

    def test_valid_news_article(self) -> None:
        article = NewsArticle(
            headline="Bitcoin Surges",
            body_summary="Bitcoin prices reached...",
            url="https://example.com/article",
            source_name="coindesk",
            published_at=datetime(2026, 3, 22, 10, 15, 0, tzinfo=timezone.utc),
            related_assets=["BTC/USDT"],
        )
        assert article.headline == "Bitcoin Surges"
        assert article.related_assets == ["BTC/USDT"]
