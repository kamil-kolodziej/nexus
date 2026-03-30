"""Unit tests for ExchangeAdapter.

Tests normalization of Tick/OrderBookUpdate/Trade/Candle,
read-only access (SRC-001), and malformed payload handling.
"""

from __future__ import annotations

import inspect
import time
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
        ticker = {
            "bid": 67234.50,
            "ask": 67235.10,
            "last": 67234.80,
            "quoteVolume": 12345.67,
            "timestamp": ts_ms,
        }
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
        trade = {
            "id": "12345",
            "price": 67234.80,
            "amount": 0.15,
            "side": "buy",
            "takerOrMaker": "taker",
            "timestamp": ts_ms,
        }
        event = adapter._normalize_trade("BTC/USDT", trade)
        assert event is not None
        assert event.event_type == EventType.TRADE
        assert event.payload["trade_id"] == "12345"

    def test_zero_price_trade_returns_none(self, adapter: ExchangeAdapter) -> None:
        trade = {
            "id": "1",
            "price": 0,
            "amount": 1.0,
            "side": "buy",
            "timestamp": int(time.time() * 1000),
        }
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
        from nexus_ingestion.adapters import exchange_adapter

        source = inspect.getsource(exchange_adapter)
        assert "create_order" not in source
        assert "cancel_order" not in source
        assert "edit_order" not in source


class TestCredentialHandling:
    """SRC-003: Credentials must be stored as SecretStr and never exposed in repr."""

    def test_credentials_stored_as_secret_str(self) -> None:
        from pydantic import SecretStr

        adapter = ExchangeAdapter(
            "binance",
            api_key=SecretStr("my_api_key"),
            api_secret=SecretStr("my_api_secret"),
            sandbox=True,
        )
        assert isinstance(adapter._api_key, SecretStr)
        assert isinstance(adapter._api_secret, SecretStr)

    def test_credentials_not_exposed_in_repr(self) -> None:
        from pydantic import SecretStr

        adapter = ExchangeAdapter(
            "binance",
            api_key=SecretStr("super_secret_key"),
            api_secret=SecretStr("super_secret_value"),
            sandbox=True,
        )
        assert "super_secret_key" not in repr(adapter._api_key)
        assert "super_secret_value" not in repr(adapter._api_secret)


class TestTimestampTolerance:
    def test_old_timestamp_rejected(self, adapter: ExchangeAdapter) -> None:
        old_ts_ms = int((time.time() - 300) * 1000)  # 5 minutes ago
        ticker = {
            "bid": 100.0,
            "ask": 101.0,
            "last": 100.5,
            "quoteVolume": 0,
            "timestamp": old_ts_ms,
        }
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
        adapter._normalize_tick(
            "BTC/USDT", {"bid": None, "ask": None, "last": None, "timestamp": None}
        )
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

        mock_error = MagicMock()
        mock_error.__class__.__name__ = "NetworkError"

        # Use the adapter's handle_watch_error directly with a simulated error
        adapter.status = AdapterStatus.CONNECTED
        await adapter._transition_to_reconnecting()
        assert adapter.status == AdapterStatus.RECONNECTING

    @pytest.mark.asyncio
    async def test_reconnecting_to_down_after_max_attempts(self) -> None:
        adapter = ExchangeAdapter("binance", sandbox=True, max_reconnect_attempts=3)
        adapter.status = AdapterStatus.RECONNECTING
        await adapter._check_and_transition_to_down(3)
        assert adapter.status == AdapterStatus.DOWN

    @pytest.mark.asyncio
    async def test_recovered_from_reconnecting(self) -> None:
        alerts: list = []
        adapter = ExchangeAdapter(
            "binance",
            sandbox=True,
            health_callback=lambda a: alerts.append(a),
        )
        adapter.status = AdapterStatus.RECONNECTING
        adapter._stream_reconnect_attempts["watch_ticker:BTC/USDT"] = 2
        await adapter._transition_to_connected("watch_ticker:BTC/USDT")
        assert adapter.status == AdapterStatus.CONNECTED
        assert adapter._stream_reconnect_attempts == {}

    @pytest.mark.asyncio
    async def test_health_alerts_emitted_on_transitions(self) -> None:
        alerts: list = []
        adapter = ExchangeAdapter(
            "binance",
            sandbox=True,
            health_callback=lambda a: alerts.append(a),
        )

        # CONNECTED → RECONNECTING
        await adapter._transition_to_reconnecting()
        assert len(alerts) == 1
        assert alerts[0].alert_type == "ADAPTER_RECONNECTING"

        # RECONNECTING → CONNECTED (recovered)
        await adapter._transition_to_connected("watch_ticker:BTC/USDT")
        assert len(alerts) == 2
        assert alerts[1].alert_type == "ADAPTER_RECOVERED"

    @pytest.mark.asyncio
    async def test_async_health_callback_is_awaited(self) -> None:
        """Async health callbacks must be awaited — regression guard for Bug 1."""
        async_callback = AsyncMock()
        adapter = ExchangeAdapter("binance", sandbox=True, health_callback=async_callback)

        await adapter._transition_to_reconnecting()
        async_callback.assert_awaited_once()
        assert async_callback.call_args[0][0].alert_type == "ADAPTER_RECONNECTING"

        adapter.status = AdapterStatus.RECONNECTING
        await adapter._transition_to_connected("watch_ticker:BTC/USDT")
        assert async_callback.await_count == 2
        assert async_callback.call_args[0][0].alert_type == "ADAPTER_RECOVERED"

    @pytest.mark.asyncio
    async def test_per_stream_counters_are_independent(self) -> None:
        """Two streams failing simultaneously must each get their own counter.

        Regression guard for Bug 3.
        """
        adapter = ExchangeAdapter("binance", sandbox=True, max_reconnect_attempts=10)
        adapter.status = AdapterStatus.RECONNECTING

        try:
            from ccxt.base.errors import NetworkError
        except ImportError:
            pytest.skip("ccxt not installed")

        error = NetworkError("disconnected")
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await adapter._handle_watch_error("watch_ticker", "BTC/USDT", error)
            await adapter._handle_watch_error("watch_order_book", "BTC/USDT", error)

        assert adapter._stream_reconnect_attempts["watch_ticker:BTC/USDT"] == 1
        assert adapter._stream_reconnect_attempts["watch_order_book:BTC/USDT"] == 1

    @pytest.mark.asyncio
    async def test_check_and_transition_to_down_below_threshold(self) -> None:
        """Status must stay RECONNECTING when attempts < max."""
        adapter = ExchangeAdapter("binance", sandbox=True, max_reconnect_attempts=5)
        adapter.status = AdapterStatus.RECONNECTING
        await adapter._check_and_transition_to_down(4)
        assert adapter.status == AdapterStatus.RECONNECTING

    @pytest.mark.asyncio
    async def test_exponential_backoff_timing(self) -> None:
        adapter = ExchangeAdapter("binance", sandbox=True, max_reconnect_attempts=5)
        # Verify backoff increases
        delays = [adapter._get_reconnect_delay(attempt) for attempt in range(5)]
        assert delays[0] < delays[1] < delays[2]
        assert all(d <= 60.0 for d in delays)

    @pytest.mark.asyncio
    async def test_only_recovered_stream_counter_is_cleared(self) -> None:
        """Counter for the recovering stream is removed; other streams are unaffected."""
        adapter = ExchangeAdapter("binance", sandbox=True)
        adapter.status = AdapterStatus.RECONNECTING
        adapter._stream_reconnect_attempts["watch_ticker:BTC/USDT"] = 3
        adapter._stream_reconnect_attempts["watch_order_book:BTC/USDT"] = 2

        await adapter._transition_to_connected("watch_ticker:BTC/USDT")

        assert "watch_ticker:BTC/USDT" not in adapter._stream_reconnect_attempts
        assert adapter._stream_reconnect_attempts["watch_order_book:BTC/USDT"] == 2


class TestAdapterDownFloodGuard:
    @pytest.mark.asyncio
    async def test_adapter_down_emitted_once_not_again_when_already_down(self) -> None:
        """ADAPTER_DOWN must fire only on RECONNECTING→DOWN; silent when already DOWN."""
        alerts: list = []
        adapter = ExchangeAdapter(
            "binance",
            sandbox=True,
            max_reconnect_attempts=1,
            health_callback=lambda a: alerts.append(a),
        )
        adapter.status = AdapterStatus.RECONNECTING

        # First call: RECONNECTING → DOWN, alert emitted
        await adapter._check_and_transition_to_down(1)
        assert adapter.status == AdapterStatus.DOWN
        assert len([a for a in alerts if a.alert_type == "ADAPTER_DOWN"]) == 1

        # Subsequent calls while already DOWN: silent
        await adapter._check_and_transition_to_down(2)
        await adapter._check_and_transition_to_down(3)
        assert len([a for a in alerts if a.alert_type == "ADAPTER_DOWN"]) == 1


class TestOrderBookTimestamp:
    def test_missing_timestamp_returns_none(self, adapter: ExchangeAdapter) -> None:
        ob = {"bids": [[67234.50, 1.5]], "asks": [[67235.10, 0.8]], "timestamp": None}
        event = adapter._normalize_order_book("BTC/USDT", ob)
        assert event is None
        assert adapter._malformed_count == 1

    def test_stale_timestamp_returns_none(self, adapter: ExchangeAdapter) -> None:
        old_ts_ms = int((time.time() - 300) * 1000)  # 5 minutes ago, outside 120s tolerance
        ob = {"bids": [[67234.50, 1.5]], "asks": [[67235.10, 0.8]], "timestamp": old_ts_ms}
        event = adapter._normalize_order_book("BTC/USDT", ob)
        assert event is None
        assert adapter._malformed_count == 1


class TestConnect:
    """Tests for ExchangeAdapter.connect() — patches ccxt.pro via sys.modules."""

    def _patch_ccxtpro(self, exchange_id: str, exchange_instance: object) -> object:
        import sys
        from unittest.mock import MagicMock

        mock_ccxtpro = MagicMock()
        setattr(mock_ccxtpro, exchange_id, MagicMock(return_value=exchange_instance))
        return patch.dict(sys.modules, {"ccxt.pro": mock_ccxtpro})

    @pytest.mark.asyncio
    async def test_connect_creates_exchange_instance(self) -> None:
        import sys

        adapter = ExchangeAdapter("binance", sandbox=True)
        mock_exchange = MagicMock()
        mock_ccxtpro = MagicMock()
        mock_ccxtpro.binance = MagicMock(return_value=mock_exchange)

        with patch.dict(sys.modules, {"ccxt.pro": mock_ccxtpro}):
            await adapter.connect()

        assert adapter._exchange is mock_exchange
        mock_exchange.set_sandbox_mode.assert_called_once_with(True)

    @pytest.mark.asyncio
    async def test_connect_passes_api_credentials(self) -> None:
        import sys

        from pydantic import SecretStr

        adapter = ExchangeAdapter(
            "binance",
            api_key=SecretStr("key123"),
            api_secret=SecretStr("secret456"),
            sandbox=False,
        )
        mock_exchange = MagicMock()
        mock_exchange_class = MagicMock(return_value=mock_exchange)
        mock_ccxtpro = MagicMock()
        mock_ccxtpro.binance = mock_exchange_class

        with patch.dict(sys.modules, {"ccxt.pro": mock_ccxtpro}):
            await adapter.connect()

        config = mock_exchange_class.call_args[0][0]
        assert config["apiKey"] == "key123"  # pragma: allowlist secret
        assert config["secret"] == "secret456"  # pragma: allowlist secret

    @pytest.mark.asyncio
    async def test_connect_unsupported_exchange_raises(self) -> None:
        import sys

        adapter = ExchangeAdapter("notarealexchange", sandbox=True)
        # spec=[] → attribute access raises AttributeError → getattr(obj, name, None) returns None
        mock_ccxtpro = MagicMock(spec=[])

        with patch.dict(sys.modules, {"ccxt.pro": mock_ccxtpro}):
            with pytest.raises(ValueError, match="Unsupported exchange"):
                await adapter.connect()

    @pytest.mark.asyncio
    async def test_connect_sets_status_to_connected(self) -> None:
        import sys

        from nexus_common.schemas.enums import AdapterStatus

        adapter = ExchangeAdapter("binance", sandbox=True)
        mock_ccxtpro = MagicMock()
        mock_ccxtpro.binance = MagicMock(return_value=MagicMock())

        with patch.dict(sys.modules, {"ccxt.pro": mock_ccxtpro}):
            await adapter.connect()

        assert adapter.status == AdapterStatus.CONNECTED

    @pytest.mark.asyncio
    async def test_connect_skips_sandbox_when_false(self) -> None:
        import sys

        adapter = ExchangeAdapter("binance", sandbox=False)
        mock_exchange = MagicMock()
        mock_ccxtpro = MagicMock()
        mock_ccxtpro.binance = MagicMock(return_value=mock_exchange)

        with patch.dict(sys.modules, {"ccxt.pro": mock_ccxtpro}):
            await adapter.connect()

        mock_exchange.set_sandbox_mode.assert_not_called()


class TestStop:
    @pytest.mark.asyncio
    async def test_stop_sets_running_false(self) -> None:
        adapter = ExchangeAdapter("binance", sandbox=True)
        adapter._running = True
        adapter._exchange = None
        await adapter.stop()
        assert not adapter._running

    @pytest.mark.asyncio
    async def test_stop_closes_exchange(self) -> None:
        adapter = ExchangeAdapter("binance", sandbox=True)
        mock_exchange = AsyncMock()
        adapter._exchange = mock_exchange
        await adapter.stop()
        mock_exchange.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_handles_exchange_close_error(self) -> None:
        adapter = ExchangeAdapter("binance", sandbox=True)
        mock_exchange = AsyncMock()
        mock_exchange.close.side_effect = RuntimeError("close failed")
        adapter._exchange = mock_exchange
        # Must not raise
        await adapter.stop()

    @pytest.mark.asyncio
    async def test_stop_no_exchange_does_not_raise(self) -> None:
        adapter = ExchangeAdapter("binance", sandbox=True)
        adapter._exchange = None
        await adapter.stop()


class TestRun:
    @pytest.mark.asyncio
    async def test_run_creates_tasks_per_asset(self) -> None:
        adapter = ExchangeAdapter("binance", sandbox=True, assets=["BTC/USDT", "ETH/USDT"])
        adapter._exchange = AsyncMock()

        created_tasks: list[str] = []

        async def _stop_after_start() -> None:
            adapter._running = False

        import asyncio
        from unittest.mock import patch as _patch

        # Track task names; stop immediately so run() completes
        original_create_task = asyncio.create_task

        def fake_create_task(coro: object, *, name: str | None = None) -> asyncio.Task:  # type: ignore[type-arg]
            if name:
                created_tasks.append(name)
            t = original_create_task(coro, name=name)  # type: ignore[arg-type]
            return t

        with _patch("asyncio.create_task", side_effect=fake_create_task):
            # stop adapter after first iteration to avoid infinite loops
            adapter._running = True
            # Cancel all watch methods immediately
            adapter._exchange.watch_ticker.side_effect = asyncio.CancelledError
            adapter._exchange.watch_order_book.side_effect = asyncio.CancelledError
            adapter._exchange.watch_trades.side_effect = asyncio.CancelledError
            adapter._exchange.watch_ohlcv.side_effect = asyncio.CancelledError
            await adapter.run()

        # 4 streams x 2 assets = 8 tasks
        assert len(created_tasks) == 8
        assert any("watch_ticker:BTC/USDT" in t for t in created_tasks)
        assert any("watch_ohlcv:ETH/USDT" in t for t in created_tasks)


class TestWatchLoops:
    """Test the _watch_* loops for event emission, CancelledError exit, and error handling."""

    def _make_running_adapter(self) -> ExchangeAdapter:
        events: list[MarketEvent] = []
        adapter = ExchangeAdapter(
            "binance",
            sandbox=True,
            assets=["BTC/USDT"],
            timestamp_tolerance=300,
            event_callback=lambda e: events.append(e),
        )
        adapter._running = True
        adapter._captured_events = events  # type: ignore[attr-defined]
        return adapter

    @pytest.mark.asyncio
    async def test_watch_ticker_emits_event_then_stops(self) -> None:
        adapter = self._make_running_adapter()
        ts_ms = int(time.time() * 1000)
        ticker = {"bid": 100.0, "ask": 101.0, "last": 100.5, "quoteVolume": 0, "timestamp": ts_ms}
        call_count = 0

        async def fake_watch_ticker(asset: str) -> dict:  # type: ignore[type-arg]
            nonlocal call_count
            call_count += 1
            adapter._running = False  # stop after first tick
            return ticker

        adapter._exchange = AsyncMock()
        adapter._exchange.watch_ticker = fake_watch_ticker
        await adapter._watch_ticker("BTC/USDT")
        assert len(adapter._captured_events) == 1  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_watch_ticker_cancelled_error_exits(self) -> None:
        import asyncio

        adapter = self._make_running_adapter()
        adapter._exchange = AsyncMock()
        adapter._exchange.watch_ticker.side_effect = asyncio.CancelledError
        # Must exit cleanly, not propagate
        await adapter._watch_ticker("BTC/USDT")

    @pytest.mark.asyncio
    async def test_watch_ticker_exception_calls_handle_error(self) -> None:
        adapter = self._make_running_adapter()
        call_count = 0

        async def fake_watch_ticker(asset: str) -> None:
            nonlocal call_count
            call_count += 1
            adapter._running = False
            raise RuntimeError("boom")

        adapter._exchange = AsyncMock()
        adapter._exchange.watch_ticker = fake_watch_ticker

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await adapter._watch_ticker("BTC/USDT")

        assert adapter._error_count >= 1

    @pytest.mark.asyncio
    async def test_watch_order_book_cancelled_error_exits(self) -> None:
        import asyncio

        adapter = self._make_running_adapter()
        adapter._exchange = AsyncMock()
        adapter._exchange.watch_order_book.side_effect = asyncio.CancelledError
        await adapter._watch_order_book("BTC/USDT")

    @pytest.mark.asyncio
    async def test_watch_trades_emits_multiple_events(self) -> None:
        adapter = self._make_running_adapter()
        ts_ms = int(time.time() * 1000)
        trades = [
            {"id": "1", "price": 100.0, "amount": 0.1, "side": "buy", "timestamp": ts_ms},
            {"id": "2", "price": 101.0, "amount": 0.2, "side": "sell", "timestamp": ts_ms},
        ]

        async def fake_watch_trades(asset: str) -> list:  # type: ignore[type-arg]
            adapter._running = False
            return trades

        adapter._exchange = AsyncMock()
        adapter._exchange.watch_trades = fake_watch_trades
        await adapter._watch_trades("BTC/USDT")
        assert len(adapter._captured_events) == 2  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_watch_trades_cancelled_error_exits(self) -> None:
        import asyncio

        adapter = self._make_running_adapter()
        adapter._exchange = AsyncMock()
        adapter._exchange.watch_trades.side_effect = asyncio.CancelledError
        await adapter._watch_trades("BTC/USDT")

    @pytest.mark.asyncio
    async def test_watch_ohlcv_emits_candle_events(self) -> None:
        adapter = self._make_running_adapter()
        ts_ms = int(time.time() * 1000)
        ohlcv_list = [[ts_ms, 100.0, 110.0, 90.0, 105.0, 500.0]]

        async def fake_watch_ohlcv(asset: str, timeframe: str) -> list:  # type: ignore[type-arg]
            adapter._running = False
            return ohlcv_list

        adapter._exchange = AsyncMock()
        adapter._exchange.watch_ohlcv = fake_watch_ohlcv
        await adapter._watch_ohlcv("BTC/USDT")
        assert len(adapter._captured_events) == 1  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_watch_ohlcv_cancelled_error_exits(self) -> None:
        import asyncio

        adapter = self._make_running_adapter()
        adapter._exchange = AsyncMock()
        adapter._exchange.watch_ohlcv.side_effect = asyncio.CancelledError
        await adapter._watch_ohlcv("BTC/USDT")

    @pytest.mark.asyncio
    async def test_watch_ticker_transitions_to_connected_when_reconnecting(self) -> None:
        from nexus_common.schemas.enums import AdapterStatus

        alerts: list = []
        adapter = ExchangeAdapter(
            "binance",
            sandbox=True,
            timestamp_tolerance=300,
            health_callback=lambda a: alerts.append(a),
        )
        adapter._running = True
        adapter.status = AdapterStatus.RECONNECTING
        ts_ms = int(time.time() * 1000)
        ticker = {"bid": 100.0, "ask": 101.0, "last": 100.5, "quoteVolume": 0, "timestamp": ts_ms}

        async def fake_watch_ticker(asset: str) -> dict:  # type: ignore[type-arg]
            adapter._running = False
            return ticker

        adapter._exchange = AsyncMock()
        adapter._exchange.watch_ticker = fake_watch_ticker
        await adapter._watch_ticker("BTC/USDT")
        assert adapter.status == AdapterStatus.CONNECTED
        assert any(a.alert_type == "ADAPTER_RECOVERED" for a in alerts)


class TestNormalizationEdgeCases:
    """Cover exception-path branches (lines 201-204, 229-232, 267-270, 304-307)."""

    def test_normalize_tick_exception_returns_none(self, adapter: ExchangeAdapter) -> None:
        # Pass an object that raises on .get() to trigger the except branch
        class BadTicker:
            def get(self, *args: object, **kwargs: object) -> object:
                raise RuntimeError("unexpected error")

        event = adapter._normalize_tick("BTC/USDT", BadTicker())  # type: ignore[arg-type]
        assert event is None
        assert adapter._malformed_count >= 1

    def test_normalize_order_book_exception_returns_none(self, adapter: ExchangeAdapter) -> None:
        class BadOrderBook:
            def get(self, *args: object, **kwargs: object) -> object:
                raise RuntimeError("unexpected error")

        event = adapter._normalize_order_book("BTC/USDT", BadOrderBook())  # type: ignore[arg-type]
        assert event is None

    def test_normalize_trade_negative_amount_returns_none(self, adapter: ExchangeAdapter) -> None:
        trade = {
            "id": "1",
            "price": 100.0,
            "amount": -1.0,
            "side": "buy",
            "timestamp": int(time.time() * 1000),
        }
        event = adapter._normalize_trade("BTC/USDT", trade)
        assert event is None
        assert adapter._malformed_count >= 1

    def test_normalize_trade_missing_side_returns_none(self, adapter: ExchangeAdapter) -> None:
        trade = {
            "id": "1",
            "price": 100.0,
            "amount": 1.0,
            "side": None,
            "timestamp": int(time.time() * 1000),
        }
        event = adapter._normalize_trade("BTC/USDT", trade)
        assert event is None

    def test_normalize_trade_exception_returns_none(self, adapter: ExchangeAdapter) -> None:
        class BadTrade:
            def get(self, *args: object, **kwargs: object) -> object:
                raise RuntimeError("boom")

        event = adapter._normalize_trade("BTC/USDT", BadTrade())  # type: ignore[arg-type]
        assert event is None

    def test_normalize_candle_zero_open_returns_none(self, adapter: ExchangeAdapter) -> None:
        ts_ms = int(time.time() * 1000)
        event = adapter._normalize_candle("BTC/USDT", [ts_ms, 0, 110.0, 90.0, 105.0, 500.0])
        assert event is None
        assert adapter._malformed_count >= 1

    def test_normalize_candle_none_value_returns_none(self, adapter: ExchangeAdapter) -> None:
        ts_ms = int(time.time() * 1000)
        event = adapter._normalize_candle("BTC/USDT", [ts_ms, None, 110.0, 90.0, 105.0, 500.0])
        assert event is None

    def test_normalize_candle_exception_returns_none(self, adapter: ExchangeAdapter) -> None:
        event = adapter._normalize_candle("BTC/USDT", "not-a-list")  # type: ignore[arg-type]
        assert event is None

    def test_parse_timestamp_overflow_returns_none(self, adapter: ExchangeAdapter) -> None:
        # An absurdly large timestamp triggers OverflowError/OSError
        result = adapter._parse_timestamp(9999999999999999)
        assert result is None

    def test_normalize_trade_stale_timestamp_returns_none(self, adapter: ExchangeAdapter) -> None:
        """Covers the ts is None branch inside _normalize_trade (stale timestamp path)."""
        old_ts_ms = int((time.time() - 300) * 1000)
        trade = {
            "id": "1",
            "price": 100.0,
            "amount": 1.0,
            "side": "buy",
            "timestamp": old_ts_ms,
        }
        event = adapter._normalize_trade("BTC/USDT", trade)
        assert event is None
        assert adapter._malformed_count >= 1

    def test_normalize_candle_stale_timestamp_returns_none(self, adapter: ExchangeAdapter) -> None:
        """Covers the ts is None branch inside _normalize_candle (stale timestamp path)."""
        old_ts_ms = int((time.time() - 300) * 1000)
        event = adapter._normalize_candle("BTC/USDT", [old_ts_ms, 100.0, 110.0, 90.0, 105.0, 500.0])
        assert event is None
        assert adapter._malformed_count >= 1


class TestTransitionNoCallback:
    """Cover branches where health_callback is None."""

    @pytest.mark.asyncio
    async def test_transition_to_reconnecting_no_callback(self) -> None:
        adapter = ExchangeAdapter("binance", sandbox=True, health_callback=None)
        await adapter._transition_to_reconnecting()
        from nexus_common.schemas.enums import AdapterStatus

        assert adapter.status == AdapterStatus.RECONNECTING

    @pytest.mark.asyncio
    async def test_check_and_transition_to_down_no_callback(self) -> None:
        adapter = ExchangeAdapter(
            "binance", sandbox=True, max_reconnect_attempts=1, health_callback=None
        )
        adapter.status = __import__(
            "nexus_common.schemas.enums", fromlist=["AdapterStatus"]
        ).AdapterStatus.RECONNECTING
        await adapter._check_and_transition_to_down(1)
        from nexus_common.schemas.enums import AdapterStatus

        assert adapter.status == AdapterStatus.DOWN

    @pytest.mark.asyncio
    async def test_check_and_transition_to_down_async_callback_awaited(self) -> None:
        """Covers the asyncio.iscoroutine branch in _check_and_transition_to_down."""
        async_cb = AsyncMock()
        adapter = ExchangeAdapter(
            "binance", sandbox=True, max_reconnect_attempts=1, health_callback=async_cb
        )
        from nexus_common.schemas.enums import AdapterStatus

        adapter.status = AdapterStatus.RECONNECTING
        await adapter._check_and_transition_to_down(1)
        async_cb.assert_awaited_once()
