"""Shared enums for the Nexus trading platform."""

from enum import StrEnum


class EventType(StrEnum):
    """Event type discriminator for MarketEvent payloads."""

    TICK = "TICK"
    ORDER_BOOK_UPDATE = "ORDER_BOOK_UPDATE"
    TRADE = "TRADE"
    CANDLE = "CANDLE"
    NEWS_ARTICLE = "NEWS_ARTICLE"
    SENTIMENT_SCORE = "SENTIMENT_SCORE"


class AdapterStatus(StrEnum):
    """Runtime status of an ingestion adapter."""

    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"
    DOWN = "DOWN"


class Severity(StrEnum):
    """Alert severity levels."""

    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
