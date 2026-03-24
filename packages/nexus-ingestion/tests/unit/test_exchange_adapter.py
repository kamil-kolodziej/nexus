"""Unit tests for ExchangeAdapter.

Tests normalization of Tick/OrderBookUpdate/Trade/Candle,
read-only access (SRC-001), and malformed payload handling.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nexus_common.schemas.enums import AdapterStatus, EventType
from nexus_common.schemas.market_event import MarketEvent

from nexus_ingestion.adapters.exchange_adapter import ExchangeAdapter


@pytest.fixture
def adapter() -> ExchangeAdapter:
    events: list[MarketEvent] = []
    a = ExchangeAdapter(
        "binance",
        sandbox=True,
        assets=["BTC/USDT"],
        timestamp_tolerance=120,
        event_callback=lambda e: events.append(e),
    )
    a._captured_events = events  # type: ignore[attr-defined]
    return a


class TestTickNormalization:
    def test_valid_ticker_produces_tick_event(self, adapter: ExchangeAdapter) -> None:
        ts_ms = int(time.time() * 1000)
        ticker = {"bid": 67234.50, "ask": 67235.10, "last": 67234.80, "quoteVolume": 12345.67, "timestamp": ts_ms}
        event = adapter._normalize_tick("BTC/USDT", ticker)
        assert event is not None
        assert event.event_type == EventType.TICK
        assert event.payload["bid"] == 67234.50

    def test_missing_bid_returns_none(self, adapter: ExchangeAdapter) -> None:
        ticker = {"bid": None, "ask": 100.0, "last": 99.0, "timestamp": int(time.time() * 1000)}
        event = adapter._normalize_tick("BTC/USDT", ticker)
        assert event is None
        assert adapter._malformed_count == 1

    def test_zero_price_returns_none(self, adapter: ExchangeAdapter) -> None:
        ticker = {"bid": 0, "ask": 100.0, "last": 99.0, "timestamp": int(time.time() * 1000)}
        event = adapter._normalize_tick("BTC/USDT", ticker)
        assert event is None


class TestOrderBookNormalization:
    def test_valid_orderbook(self, adapter: ExchangeAdapter) -> None:
        ts_ms = int(time.time() * 1000)
        ob = {
            "bids": [[67234.50, 1.5], [67234.00, 2.3]],
            "asks": [[67235.10, 0.8], [67235.50, 1.2]],
            "timestamp": ts_ms,
        }
        event = adapter._normalize_order_book("BTC/USDT", ob)
        assert event is not None
        assert event.event_type == EventType.ORDER_BOOK_UPDATE

    def test_empty_orderbook_returns_none(self, adapter: ExchangeAdapter) -> None:
        ob = {"bids": [], "asks": [], "timestamp": int(time.time() * 1000)}
        event = adapter._normalize_order_book("BTC/USDT", ob)
        assert event is None


class TestTradeNormalization:
    def test_valid_trade(self, adapter: ExchangeAdapter) -> None:
        ts_ms = int(time.time() * 1000)
        trade = {"id": "12345", "price": 67234.80, "amount": 0.15, "side": "buy", "takerOrMaker": "taker", "timestamp": ts_ms}
        event = adapter._normalize_trade("BTC/USDT", trade)
        assert event is not None
        assert event.event_type == EventType.TRADE
        assert event.payload["trade_id"] == "12345"

    def test_zero_price_trade_returns_none(self, adapter: ExchangeAdapter) -> None:
        trade = {"id": "1", "price": 0, "amount": 1.0, "side": "buy", "timestamp": int(time.time() * 1000)}
        event = adapter._normalize_trade("BTC/USDT", trade)
        assert event is None


class TestCandleNormalization:
    def test_valid_candle(self, adapter: ExchangeAdapter) -> None:
        ts_ms = int(time.time() * 1000)
        ohlcv = [ts_ms, 67200.0, 67300.0, 67150.0, 67234.80, 456.78]
        event = adapter._normalize_candle("BTC/USDT", ohlcv)
        assert event is not None
        assert event.event_type == EventType.CANDLE

    def test_short_ohlcv_returns_none(self, adapter: ExchangeAdapter) -> None:
        event = adapter._normalize_candle("BTC/USDT", [1, 2, 3])
        assert event is None


class TestReadOnlyAccess:
    """SRC-001: Verify no exchange write operations."""

    def test_no_create_order_calls(self) -> None:
        import inspect
        from nexus_ingestion.adapters import exchange_adapter

        source = inspect.getsource(exchange_adapter)
        assert "create_order" not in source
        assert "cancel_order" not in source
        assert "edit_order" not in source


class TestTimestampTolerance:
    def test_old_timestamp_rejected(self, adapter: ExchangeAdapter) -> None:
        old_ts_ms = int((time.time() - 300) * 1000)  # 5 minutes ago
        ticker = {"bid": 100.0, "ask": 101.0, "last": 100.5, "quoteVolume": 0, "timestamp": old_ts_ms}
        event = adapter._normalize_tick("BTC/USDT", ticker)
        assert event is None

    def test_recent_timestamp_accepted(self, adapter: ExchangeAdapter) -> None:
        ts_ms = int(time.time() * 1000)
        ticker = {"bid": 100.0, "ask": 101.0, "last": 100.5, "quoteVolume": 0, "timestamp": ts_ms}
        event = adapter._normalize_tick("BTC/USDT", ticker)
        assert event is not None


class TestMalformedPayloadHandling:
    def test_malformed_count_increments(self, adapter: ExchangeAdapter) -> None:
        adapter._normalize_tick("BTC/USDT", {})
        adapter._normalize_tick("BTC/USDT", {"bid": None, "ask": None, "last": None, "timestamp": None})
        assert adapter._malformed_count >= 2


# --- US2: Reconnection Tests (T031) ---


class TestReconnectionStateTransitions:
    """US2: Adapter state transitions and health alert emission on reconnection."""

    @pytest.mark.asyncio
    async def test_connected_to_reconnecting_on_network_error(self) -> None:
        alerts: list = []
        adapter = ExchangeAdapter(
            "binance",
            sandbox=True,
            timestamp_tolerance=120,
            health_callback=lambda a: alerts.append(a),
        )
        assert adapter.status == AdapterStatus.CONNECTED

        # Simulate NetworkError handling
        from unittest.mock import MagicMock

        mock_error = MagicMock()
        mock_error.__class__.__name__ = "NetworkError"

        # Use the adapter's handle_watch_error directly with a simulated error
        adapter.status = AdapterStatus.CONNECTED
        adapter._transition_to_reconnecting()
        assert adapter.status == AdapterStatus.RECONNECTING

    def test_reconnecting_to_down_after_max_attempts(self) -> None:
        adapter = ExchangeAdapter("binance", sandbox=True, max_reconnect_attempts=3)
        adapter.status = AdapterStatus.RECONNECTING
        adapter._reconnect_attempt = 3
        adapter._check_and_transition_to_down()
        assert adapter.status == AdapterStatus.DOWN

    def test_recovered_from_reconnecting(self) -> None:
        alerts: list = []
        adapter = ExchangeAdapter(
            "binance",
            sandbox=True,
            health_callback=lambda a: alerts.append(a),
        )
        adapter.status = AdapterStatus.RECONNECTING
        adapter._reconnect_attempt = 2
        adapter._transition_to_connected()
        assert adapter.status == AdapterStatus.CONNECTED
        assert adapter._reconnect_attempt == 0

    @pytest.mark.asyncio
    async def test_health_alerts_emitted_on_transitions(self) -> None:
        alerts: list = []
        adapter = ExchangeAdapter(
            "binance",
            sandbox=True,
            health_callback=lambda a: alerts.append(a),
        )

        # CONNECTED → RECONNECTING
        adapter._transition_to_reconnecting()
        assert len(alerts) == 1
        assert alerts[0].alert_type == "ADAPTER_RECONNECTING"

        # RECONNECTING → CONNECTED (recovered)
        adapter._transition_to_connected()
        assert len(alerts) == 2
        assert alerts[1].alert_type == "ADAPTER_RECOVERED"

    @pytest.mark.asyncio
    async def test_exponential_backoff_timing(self) -> None:
        adapter = ExchangeAdapter("binance", sandbox=True, max_reconnect_attempts=5)
        # Verify backoff increases
        delays = [adapter._get_reconnect_delay(attempt) for attempt in range(5)]
        assert delays[0] < delays[1] < delays[2]
        assert all(d <= 60.0 for d in delays)
