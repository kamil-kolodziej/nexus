"""HealthAlert and AdapterHealth models."""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field, field_validator

from nexus_common.schemas.enums import AdapterStatus, Severity


class HealthAlert(BaseModel):
    """Health alert published to nexus:ingestion-health-events stream."""

    alert_type: str = Field(min_length=1)
    adapter_id: str = Field(min_length=1)
    asset: str | None = None
    severity: Severity
    timestamp: datetime
    message: str = Field(min_length=1)

    @field_validator("timestamp")
    @classmethod
    def ensure_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            return v.replace(tzinfo=UTC)
        return v

    def to_redis_fields(self) -> dict[str, str]:
        """Serialize to flat key-value map for Redis Stream XADD."""
        return {
            "alert_type": self.alert_type,
            "adapter_id": self.adapter_id,
            "asset": self.asset or "",
            "severity": self.severity.value,
            "timestamp": self.timestamp.isoformat(),
            "message": self.message,
        }


class AdapterHealth(BaseModel):
    """Runtime status per adapter, exposed via GET /health."""

    adapter_id: str
    adapter_type: str
    status: AdapterStatus = AdapterStatus.CONNECTED
    last_event_at: datetime | None = None
    event_count: int = 0
    error_count: int = 0
    malformed_count: int = 0
