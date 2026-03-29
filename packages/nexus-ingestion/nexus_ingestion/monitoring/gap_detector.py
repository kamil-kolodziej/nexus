"""Data gap detector with per-asset last-event timers."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from nexus_common.schemas.enums import Severity
from nexus_common.schemas.health_alert import HealthAlert

logger = logging.getLogger(__name__)


class GapDetector:
    """Monitors data freshness per asset and emits health alerts on gaps."""

    def __init__(
        self,
        *,
        gap_threshold: int = 60,
        malformed_rate_threshold: int = 2,
        check_interval: float = 10.0,
        health_callback: Callable[[HealthAlert], Any] | None = None,
    ) -> None:
        self._gap_threshold = gap_threshold
        self._malformed_rate_threshold = malformed_rate_threshold
        self._check_interval = check_interval
        self._health_callback = health_callback
        self._last_event_times: dict[str, datetime] = {}
        self._malformed_counts: dict[str, list[datetime]] = {}
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the gap detection background loop."""
        self._running = True
        self._task = asyncio.create_task(self._check_loop(), name="gap-detector")
        logger.info("GapDetector started (threshold=%ds)", self._gap_threshold)

    async def stop(self) -> None:
        """Stop the gap detection loop."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def record_event(self, adapter_id: str, asset: str) -> None:
        """Update the last-event timestamp for an adapter+asset pair."""
        key = f"{adapter_id}:{asset}"
        self._last_event_times[key] = datetime.now(UTC)

    def record_malformed(self, adapter_id: str) -> None:
        """Track a malformed event for rate monitoring."""
        now = datetime.now(UTC)
        if adapter_id not in self._malformed_counts:
            self._malformed_counts[adapter_id] = []
        self._malformed_counts[adapter_id].append(now)

    async def _check_loop(self) -> None:
        """Periodically check for gaps and malformed spikes."""
        while self._running:
            try:
                await asyncio.sleep(self._check_interval)
                await self._check_gaps()
                await self._check_malformed_rates()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.error("Gap detector check error", exc_info=True)

    async def _check_gaps(self) -> None:
        """Check all tracked assets for data gaps."""
        now = datetime.now(UTC)
        for key, last_time in list(self._last_event_times.items()):
            gap_seconds = (now - last_time).total_seconds()
            if gap_seconds > self._gap_threshold:
                parts = key.rsplit(":", 1)
                adapter_id = parts[0] if len(parts) > 1 else key
                asset = parts[1] if len(parts) > 1 else None

                alert = HealthAlert(
                    alert_type="DATA_GAP",
                    adapter_id=adapter_id,
                    asset=asset,
                    severity=Severity.HIGH,
                    timestamp=now,
                    message=(
                        f"No {asset} events from {adapter_id} for {gap_seconds:.0f}s "
                        f"(threshold: {self._gap_threshold}s)"
                    ),
                )
                await self._emit_alert(alert)

    async def _check_malformed_rates(self) -> None:
        """Check malformed event rates per adapter."""
        now = datetime.now(UTC)
        for adapter_id, timestamps in list(self._malformed_counts.items()):
            # Keep only last 60 seconds
            cutoff = now.timestamp() - 60
            recent = [t for t in timestamps if t.timestamp() > cutoff]
            self._malformed_counts[adapter_id] = recent

            rate_per_min = len(recent)
            if rate_per_min > self._malformed_rate_threshold:
                alert = HealthAlert(
                    alert_type="MALFORMED_SPIKE",
                    adapter_id=adapter_id,
                    severity=Severity.LOW,
                    timestamp=now,
                    message=(
                        f"{adapter_id} malformed event rate {rate_per_min}/min "
                        f"exceeds threshold {self._malformed_rate_threshold}/min"
                    ),
                )
                await self._emit_alert(alert)

    async def _emit_alert(self, alert: HealthAlert) -> None:
        """Send alert to the health callback."""
        if self._health_callback:
            result = self._health_callback(alert)
            if asyncio.iscoroutine(result):
                await result
