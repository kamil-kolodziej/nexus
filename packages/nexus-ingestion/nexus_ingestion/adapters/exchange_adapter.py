"""Exchange adapter using ccxt.pro WebSocket streams."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Callable

from nexus_common.schemas.enums import AdapterStatus, EventType, Severity
from nexus_common.schemas.health_alert import HealthAlert
from nexus_common.schemas.market_event import MarketEvent

from nexus_ingestion.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class ExchangeAdapter(BaseAdapter):
    """Adapter for exchange market data via ccxt.pro WebSocket streams."""

    def __init__(
        self,
        exchange_id: str,
        *,
        api_key: str = "",
        api_secret: str = "",
        sandbox: bool = True,
        assets: list[str] | None = None,
        timestamp_tolerance: int = 60,
        max_reconnect_attempts: int = 10,
        event_callback: Callable[[MarketEvent], Any] | None = None,
        health_callback: Callable[[HealthAlert], Any] | None = None,
    ) -> None:
        super().__init__(
            adapter_id=f"{exchange_id}:exchange",
            adapter_type="exchange",
        )
        self._exchange_id = exchange_id
        self._api_key = api_key
        self._api_secret = api_secret
        self._sandbox = sandbox
        self._assets = ["BTC/USDT"] if assets is None else assets
        self._timestamp_tolerance = timestamp_tolerance
        self._max_reconnect_attempts = max_reconnect_attempts
        self._event_callback = event_callback
        self._health_callback = health_callback
        self._exchange: Any = None
        self._running = False
        self._reconnect_attempt = 0

    async def connect(self) -> None:
        """Create and configure the ccxt.pro exchange instance."""
        import ccxt.pro as ccxtpro

        exchange_class = getattr(ccxtpro, self._exchange_id, None)
        if exchange_class is None:
            msg = f"Unsupported exchange: {self._exchange_id}"
            raise ValueError(msg)

        config: dict[str, Any] = {"enableRateLimit": True}
        if self._api_key:
            config["apiKey"] = self._api_key
        if self._api_secret:
            config["secret"] = self._api_secret

        self._exchange = exchange_class(config)

        if self._sandbox:
            self._exchange.set_sandbox_mode(True)

        self.status = AdapterStatus.CONNECTED
        logger.info(
            "ExchangeAdapter connected: %s (sandbox=%s, assets=%s)",
            self._exchange_id,
            self._sandbox,
            self._assets,
        )

    async def subscribe(self) -> None:
        """No explicit subscription needed — ccxt.pro subscribes on first watch call."""

    async def run(self) -> None:
        """Main event loop — watch all configured streams concurrently."""
        self._running = True
        tasks = []
        for asset in self._assets:
            tasks.append(asyncio.create_task(self._watch_ticker(asset)))
            tasks.append(asyncio.create_task(self._watch_order_book(asset)))
            tasks.append(asyncio.create_task(self._watch_trades(asset)))
            tasks.append(asyncio.create_task(self._watch_ohlcv(asset)))

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        """Gracefully close the exchange connection."""
        self._running = False
        if self._exchange:
            try:
                await self._exchange.close()
            except Exception:
                logger.warning("Error closing exchange connection", exc_info=True)

    async def _watch_ticker(self, asset: str) -> None:
        """Watch ticker (best bid/ask) for an asset."""
        while self._running:
            try:
                ticker = await self._exchange.watch_ticker(asset)
                if self.status != AdapterStatus.CONNECTED:
                    self._transition_to_connected()
                event = self._normalize_tick(asset, ticker)
                if event:
                    await self._emit_event(event)
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self._handle_watch_error("watch_ticker", asset, e)

    async def _watch_order_book(self, asset: str) -> None:
        """Watch order book updates for an asset."""
        while self._running:
            try:
                ob = await self._exchange.watch_order_book(asset)
                if self.status != AdapterStatus.CONNECTED:
                    self._transition_to_connected()
                event = self._normalize_order_book(asset, ob)
                if event:
                    await self._emit_event(event)
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self._handle_watch_error("watch_order_book", asset, e)

    async def _watch_trades(self, asset: str) -> None:
        """Watch trade executions for an asset."""
        while self._running:
            try:
                trades = await self._exchange.watch_trades(asset)
                if self.status != AdapterStatus.CONNECTED:
                    self._transition_to_connected()
                for trade in trades:
                    event = self._normalize_trade(asset, trade)
                    if event:
                        await self._emit_event(event)
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self._handle_watch_error("watch_trades", asset, e)

    async def _watch_ohlcv(self, asset: str) -> None:
        """Watch OHLCV candles for an asset."""
        while self._running:
            try:
                ohlcv_list = await self._exchange.watch_ohlcv(asset, "1m")
                if self.status != AdapterStatus.CONNECTED:
                    self._transition_to_connected()
                for ohlcv in ohlcv_list:
                    event = self._normalize_candle(asset, ohlcv)
                    if event:
                        await self._emit_event(event)
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self._handle_watch_error("watch_ohlcv", asset, e)

    def _normalize_tick(self, asset: str, ticker: dict) -> MarketEvent | None:
        """Normalize ccxt ticker to MarketEvent with Tick payload."""
        try:
            bid = ticker.get("bid")
            ask = ticker.get("ask")
            last = ticker.get("last")
            volume = ticker.get("quoteVolume") or ticker.get("baseVolume") or 0

            if not all(v is not None and v > 0 for v in (bid, ask, last)):
                self.record_malformed()
                return None

            ts = self._parse_timestamp(ticker.get("timestamp"))
            if ts is None:
                self.record_malformed()
                return None

            return MarketEvent(
                source=self.adapter_id,
                asset=asset,
                timestamp=ts,
                event_type=EventType.TICK,
                payload={"bid": bid, "ask": ask, "last": last, "volume_24h": volume},
            )
        except Exception:
            self.record_malformed()
            logger.debug("Failed to normalize tick for %s", asset, exc_info=True)
            return None

    def _normalize_order_book(self, asset: str, ob: dict) -> MarketEvent | None:
        """Normalize ccxt order book to MarketEvent."""
        try:
            bids = ob.get("bids", [])[:10]
            asks = ob.get("asks", [])[:10]
            depth = len(bids)

            if not bids and not asks:
                self.record_malformed()
                return None

            ts = self._parse_timestamp(ob.get("timestamp")) or datetime.now(timezone.utc)

            return MarketEvent(
                source=self.adapter_id,
                asset=asset,
                timestamp=ts,
                event_type=EventType.ORDER_BOOK_UPDATE,
                payload={"bids": bids, "asks": asks, "depth": depth},
            )
        except Exception:
            self.record_malformed()
            logger.debug("Failed to normalize order book for %s", asset, exc_info=True)
            return None

    def _normalize_trade(self, asset: str, trade: dict) -> MarketEvent | None:
        """Normalize ccxt trade to MarketEvent."""
        try:
            price = trade.get("price")
            amount = trade.get("amount")
            side = trade.get("side")

            if not all(v is not None for v in (price, amount, side)):
                self.record_malformed()
                return None

            if price <= 0 or amount <= 0:
                self.record_malformed()
                return None

            ts = self._parse_timestamp(trade.get("timestamp"))
            if ts is None:
                self.record_malformed()
                return None

            return MarketEvent(
                source=self.adapter_id,
                asset=asset,
                timestamp=ts,
                event_type=EventType.TRADE,
                payload={
                    "trade_id": str(trade.get("id", "")),
                    "price": price,
                    "amount": amount,
                    "side": side,
                    "taker_or_maker": trade.get("takerOrMaker"),
                },
            )
        except Exception:
            self.record_malformed()
            logger.debug("Failed to normalize trade for %s", asset, exc_info=True)
            return None

    def _normalize_candle(self, asset: str, ohlcv: list) -> MarketEvent | None:
        """Normalize ccxt OHLCV array [ts, o, h, l, c, v] to MarketEvent."""
        try:
            if len(ohlcv) < 6:
                self.record_malformed()
                return None

            ts_ms, o, h, l_, c, v = ohlcv[:6]

            if any(x is None or x <= 0 for x in (o, h, l_, c)):
                self.record_malformed()
                return None

            ts = self._parse_timestamp(ts_ms)
            if ts is None:
                self.record_malformed()
                return None

            return MarketEvent(
                source=self.adapter_id,
                asset=asset,
                timestamp=ts,
                event_type=EventType.CANDLE,
                payload={
                    "open": o,
                    "high": h,
                    "low": l_,
                    "close": c,
                    "volume": v or 0,
                    "timeframe": "1m",
                },
            )
        except Exception:
            self.record_malformed()
            logger.debug("Failed to normalize candle for %s", asset, exc_info=True)
            return None

    def _parse_timestamp(self, ts: int | float | None) -> datetime | None:
        """Parse millisecond timestamp, validate against tolerance window."""
        if ts is None:
            return None

        try:
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            now = datetime.now(timezone.utc)
            diff = abs((now - dt).total_seconds())
            if diff > self._timestamp_tolerance:
                logger.debug(
                    "Timestamp outside tolerance: %s (diff=%.1fs, tolerance=%ds)",
                    dt.isoformat(),
                    diff,
                    self._timestamp_tolerance,
                )
                return None
            return dt
        except (ValueError, OSError, OverflowError):
            return None

    async def _emit_event(self, event: MarketEvent) -> None:
        """Send event to the registered callback."""
        self.record_event()
        if self._event_callback:
            result = self._event_callback(event)
            if asyncio.iscoroutine(result):
                await result

    async def _handle_watch_error(self, method: str, asset: str, error: Exception) -> None:
        """Handle errors from watch methods with health alert emission."""
        self.record_error()
        logger.warning(
            "%s error for %s: %s", method, asset, error
        )

        # Emit health alert for reconnection state changes
        try:
            from ccxt.base.errors import ExchangeNotAvailable, NetworkError

            if isinstance(error, (NetworkError, ExchangeNotAvailable)):
                if self.status == AdapterStatus.CONNECTED:
                    self._transition_to_reconnecting()
                else:
                    self._reconnect_attempt += 1
                    self._check_and_transition_to_down()
        except ImportError:
            pass

        # Brief delay before retry (ccxt.pro handles reconnection internally)
        delay = self._get_reconnect_delay(self._reconnect_attempt)
        await asyncio.sleep(delay)

    def _transition_to_reconnecting(self) -> None:
        """Transition to RECONNECTING state and emit alert."""
        self.status = AdapterStatus.RECONNECTING
        self._reconnect_attempt = 1
        if self._health_callback:
            alert = HealthAlert(
                alert_type="ADAPTER_RECONNECTING",
                adapter_id=self.adapter_id,
                severity=Severity.MEDIUM,
                timestamp=datetime.now(timezone.utc),
                message=f"{self.adapter_id} WebSocket disconnected, starting reconnection",
            )
            self._health_callback(alert)

    def _check_and_transition_to_down(self) -> None:
        """Transition to DOWN if max reconnect attempts exhausted."""
        if self._reconnect_attempt >= self._max_reconnect_attempts:
            self.status = AdapterStatus.DOWN
            if self._health_callback:
                alert = HealthAlert(
                    alert_type="ADAPTER_DOWN",
                    adapter_id=self.adapter_id,
                    severity=Severity.HIGH,
                    timestamp=datetime.now(timezone.utc),
                    message=(
                        f"{self.adapter_id} entered DOWN state after "
                        f"{self._reconnect_attempt} reconnection attempts"
                    ),
                )
                self._health_callback(alert)

    def _transition_to_connected(self) -> None:
        """Transition back to CONNECTED after successful reconnection."""
        was_reconnecting = self.status in (AdapterStatus.RECONNECTING, AdapterStatus.DOWN)
        self.status = AdapterStatus.CONNECTED
        self._reconnect_attempt = 0
        if was_reconnecting and self._health_callback:
            alert = HealthAlert(
                alert_type="ADAPTER_RECOVERED",
                adapter_id=self.adapter_id,
                severity=Severity.LOW,
                timestamp=datetime.now(timezone.utc),
                message=f"{self.adapter_id} reconnected successfully",
            )
            self._health_callback(alert)

    def _get_reconnect_delay(self, attempt: int) -> float:
        """Calculate exponential backoff delay for reconnection."""
        return min(1.0 * (2 ** attempt), 60.0)
