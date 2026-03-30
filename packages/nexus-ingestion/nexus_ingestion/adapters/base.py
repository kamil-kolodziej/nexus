"""Base adapter abstract class for ingestion adapters."""

from __future__ import annotations

import abc
import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from nexus_common.schemas.enums import AdapterStatus
from nexus_common.schemas.health_alert import AdapterHealth, HealthAlert
from nexus_common.schemas.market_event import MarketEvent


class BaseAdapter(abc.ABC):
    """Abstract base class for all ingestion adapters."""

    def __init__(self, adapter_id: str, adapter_type: str) -> None:
        self._adapter_id = adapter_id
        self._adapter_type = adapter_type
        self._status = AdapterStatus.CONNECTED
        self._last_event_at: datetime | None = None
        self._event_count = 0
        self._error_count = 0
        self._malformed_count = 0
        self._event_callback: Callable[[MarketEvent], Any] | None = None
        self._health_callback: Callable[[HealthAlert], Any] | None = None

    @property
    def adapter_id(self) -> str:
        return self._adapter_id

    @property
    def adapter_type(self) -> str:
        return self._adapter_type

    @property
    def status(self) -> AdapterStatus:
        return self._status

    @status.setter
    def status(self, value: AdapterStatus) -> None:
        self._status = value

    @property
    def health(self) -> AdapterHealth:
        """Current health snapshot for this adapter."""
        return AdapterHealth(
            adapter_id=self._adapter_id,
            adapter_type=self._adapter_type,
            status=self._status,
            last_event_at=self._last_event_at,
            event_count=self._event_count,
            error_count=self._error_count,
            malformed_count=self._malformed_count,
        )

    def record_event(self) -> None:
        """Mark that a valid event was published."""
        self._event_count += 1
        self._last_event_at = datetime.now(UTC)

    def record_error(self) -> None:
        """Mark that an error occurred."""
        self._error_count += 1

    def record_malformed(self) -> None:
        """Mark that a malformed event was dropped."""
        self._malformed_count += 1

    async def _emit_event(self, event: MarketEvent) -> None:
        """Record and forward an event to the registered callback."""
        self.record_event()
        if self._event_callback:
            result = self._event_callback(event)
            if asyncio.iscoroutine(result):
                await result

    @abc.abstractmethod
    async def connect(self) -> None:
        """Establish connection to the data source."""

    @abc.abstractmethod
    async def subscribe(self) -> None:
        """Subscribe to configured data channels."""

    @abc.abstractmethod
    async def run(self) -> None:
        """Main event loop — fetch, normalize, and yield events."""

    @abc.abstractmethod
    async def stop(self) -> None:
        """Gracefully shut down the adapter."""
