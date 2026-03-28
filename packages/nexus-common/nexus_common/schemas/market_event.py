"""MarketEvent envelope and payload models."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from nexus_common.schemas.enums import EventType

_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")


# --- Payload models ---


class Tick(BaseModel):
    """Best bid/ask snapshot from an exchange."""

    bid: float = Field(gt=0)
    ask: float = Field(gt=0)
    last: float = Field(gt=0)
    volume_24h: float = Field(ge=0)

    @model_validator(mode="after")
    def ask_gte_bid(self) -> Tick:
        if self.ask < self.bid:
            msg = f"ask ({self.ask}) must be >= bid ({self.bid})"
            raise ValueError(msg)
        return self


class OrderBookUpdate(BaseModel):
    """Order book depth snapshot or delta."""

    bids: list[list[float]] = Field(min_length=0)
    asks: list[list[float]] = Field(min_length=0)
    depth: int = Field(gt=0)

    @field_validator("bids")
    @classmethod
    def bids_descending(cls, v: list[list[float]]) -> list[list[float]]:
        for entry in v:
            if len(entry) != 2 or entry[0] <= 0 or entry[1] < 0:
                msg = "Each bid must be [price > 0, quantity >= 0]"
                raise ValueError(msg)
        prices = [entry[0] for entry in v]
        if prices != sorted(prices, reverse=True):
            msg = "Bids must be sorted descending by price"
            raise ValueError(msg)
        return v

    @field_validator("asks")
    @classmethod
    def asks_ascending(cls, v: list[list[float]]) -> list[list[float]]:
        for entry in v:
            if len(entry) != 2 or entry[0] <= 0 or entry[1] < 0:
                msg = "Each ask must be [price > 0, quantity >= 0]"
                raise ValueError(msg)
        prices = [entry[0] for entry in v]
        if prices != sorted(prices):
            msg = "Asks must be sorted ascending by price"
            raise ValueError(msg)
        return v


class Trade(BaseModel):
    """Individual trade execution on the exchange."""

    trade_id: str = Field(min_length=1)
    price: float = Field(gt=0)
    amount: float = Field(gt=0)
    side: Literal["buy", "sell"]
    taker_or_maker: Literal["taker", "maker"] | None = None


class Candle(BaseModel):
    """OHLCV candlestick data."""

    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0)
    timeframe: str = Field(min_length=1)

    @model_validator(mode="after")
    def high_low_bounds(self) -> Candle:
        if self.high < self.open or self.high < self.close:
            msg = "high must be >= open and >= close"
            raise ValueError(msg)
        if self.low > self.open or self.low > self.close:
            msg = "low must be <= open and <= close"
            raise ValueError(msg)
        return self


class NewsArticle(BaseModel):
    """A news article fetched from an RSS feed or news API."""

    headline: str = Field(min_length=1)
    body_summary: str = Field(max_length=1000)
    url: str = Field(min_length=1)
    source_name: str = Field(min_length=1)
    published_at: datetime
    related_assets: list[str] = Field(default_factory=list)


# Map from EventType to payload class
PAYLOAD_TYPE_MAP: dict[EventType, type[BaseModel]] = {
    EventType.TICK: Tick,
    EventType.ORDER_BOOK_UPDATE: OrderBookUpdate,
    EventType.TRADE: Trade,
    EventType.CANDLE: Candle,
    EventType.NEWS_ARTICLE: NewsArticle,
}


# --- Envelope ---


class MarketEvent(BaseModel):
    """Normalized envelope for all ingested data."""

    source: str = Field(min_length=1)
    asset: str | None = None
    timestamp: datetime
    event_type: EventType
    schema_version: str = "1.0.0"
    payload: dict

    @field_validator("schema_version")
    @classmethod
    def valid_semver(cls, v: str) -> str:
        if not _SEMVER_RE.match(v):
            msg = f"schema_version must be semver (e.g. '1.0.0'), got '{v}'"
            raise ValueError(msg)
        return v

    @field_validator("timestamp")
    @classmethod
    def ensure_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v

    def validated_payload(self) -> BaseModel:
        """Parse and validate the payload dict against the expected type."""
        payload_cls = PAYLOAD_TYPE_MAP.get(self.event_type)
        if payload_cls is None:
            msg = f"Unknown event_type: {self.event_type}"
            raise ValueError(msg)
        return payload_cls.model_validate(self.payload)

    def to_redis_fields(self) -> dict[str, str]:
        """Serialize to flat key-value map for Redis Stream XADD."""
        import json

        return {
            "source": self.source,
            "asset": self.asset or "",
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type.value,
            "schema_version": self.schema_version,
            "payload": json.dumps(self.payload),
        }

    @classmethod
    def from_redis_fields(cls, fields: dict[str, str]) -> MarketEvent:
        """Deserialize from Redis Stream entry fields."""
        import json

        return cls(
            source=fields["source"],
            asset=fields["asset"] or None,
            timestamp=datetime.fromisoformat(fields["timestamp"]),
            event_type=EventType(fields["event_type"]),
            schema_version=fields["schema_version"],
            payload=json.loads(fields["payload"]),
        )
