"""Unit tests for gap detector.

Tests threshold triggering, DATA_GAP alert emission, timer reset, multi-asset independence.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from nexus_common.schemas.health_alert import HealthAlert

from nexus_ingestion.monitoring.gap_detector import GapDetector


@pytest.fixture
def alerts() -> list[HealthAlert]:
    return []


@pytest.fixture
def detector(alerts: list[HealthAlert]) -> GapDetector:
    return GapDetector(
        gap_threshold=5,
        malformed_rate_threshold=2,
        check_interval=0.5,
        health_callback=lambda a: alerts.append(a),
    )


class TestGapDetection:
    @pytest.mark.asyncio
    async def test_no_alert_when_events_recent(self, detector: GapDetector, alerts: list) -> None:
        detector.record_event("binance:exchange", "BTC/USDT")
        await detector._check_gaps()
        assert len(alerts) == 0

    @pytest.mark.asyncio
    async def test_alert_on_stale_event(self, detector: GapDetector, alerts: list) -> None:
        # Manually set old timestamp
        key = "binance:exchange:BTC/USDT"
        detector._last_event_times[key] = datetime.now(timezone.utc) - timedelta(seconds=10)
        await detector._check_gaps()
        assert len(alerts) == 1
        assert alerts[0].alert_type == "DATA_GAP"
        assert alerts[0].severity.value == "HIGH"

    @pytest.mark.asyncio
    async def test_timer_reset_on_new_event(self, detector: GapDetector, alerts: list) -> None:
        key = "binance:exchange:BTC/USDT"
        detector._last_event_times[key] = datetime.now(timezone.utc) - timedelta(seconds=10)
        # New event resets timer
        detector.record_event("binance:exchange", "BTC/USDT")
        await detector._check_gaps()
        assert len(alerts) == 0

    @pytest.mark.asyncio
    async def test_multi_asset_independence(self, detector: GapDetector, alerts: list) -> None:
        # BTC is stale, ETH is fresh
        detector._last_event_times["binance:exchange:BTC/USDT"] = datetime.now(timezone.utc) - timedelta(seconds=10)
        detector.record_event("binance:exchange", "ETH/USDT")
        await detector._check_gaps()
        assert len(alerts) == 1
        assert "BTC/USDT" in alerts[0].message


class TestMalformedRateDetection:
    @pytest.mark.asyncio
    async def test_no_alert_below_threshold(self, detector: GapDetector, alerts: list) -> None:
        detector.record_malformed("binance:exchange")
        await detector._check_malformed_rates()
        assert len(alerts) == 0

    @pytest.mark.asyncio
    async def test_alert_above_threshold(self, detector: GapDetector, alerts: list) -> None:
        for _ in range(5):
            detector.record_malformed("binance:exchange")
        await detector._check_malformed_rates()
        assert len(alerts) == 1
        assert alerts[0].alert_type == "MALFORMED_SPIKE"
        assert alerts[0].severity.value == "LOW"


class TestGapDetectorLifecycle:
    @pytest.mark.asyncio
    async def test_start_stop(self, detector: GapDetector) -> None:
        await detector.start()
        assert detector._running is True
        await detector.stop()
        assert detector._running is False
