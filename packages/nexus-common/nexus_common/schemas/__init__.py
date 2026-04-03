"""Shared schemas for the Nexus trading platform."""

from nexus_common.schemas.enums import AdapterStatus, EventType, Severity
from nexus_common.schemas.health_alert import AdapterHealth, HealthAlert
from nexus_common.schemas.market_event import (
    Candle,
    MarketEvent,
    NewsArticle,
    OrderBookUpdate,
    SentimentScore,
    Tick,
    Trade,
)

__all__ = [
    "AdapterHealth",
    "AdapterStatus",
    "Candle",
    "EventType",
    "HealthAlert",
    "MarketEvent",
    "NewsArticle",
    "OrderBookUpdate",
    "SentimentScore",
    "Severity",
    "Tick",
    "Trade",
]
