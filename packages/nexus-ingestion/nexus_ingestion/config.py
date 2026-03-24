"""Ingestion-specific configuration."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class NewsSourceConfig(BaseSettings):
    """Configuration for a single news source."""

    name: str = ""
    type: str = "rss"
    url: str = ""


class IngestionConfig(BaseSettings):
    """Configuration for the nexus-ingestion service."""

    model_config = SettingsConfigDict(
        env_prefix="NEXUS_",
        env_nested_delimiter="__",
    )

    # Exchange
    exchange_id: str = "binance"
    exchange_sandbox: bool = True
    exchange_api_key: str = ""
    exchange_api_secret: str = ""
    subscribed_assets: list[str] = Field(default=["BTC/USDT"])

    # Redis
    redis_url: str = "redis://localhost:6379"
    redis_buffer_max: int = 10000
    market_events_stream: str = "nexus:market-events"
    news_events_stream: str = "nexus:news-events"
    health_events_stream: str = "nexus:ingestion-health-events"
    market_events_maxlen: int = 100000
    news_events_maxlen: int = 10000
    health_events_maxlen: int = 5000

    # TimescaleDB
    timescaledb_dsn: str = "postgresql://nexus:nexus_dev@localhost:5432/nexus"
    batch_size: int = 500
    flush_interval: float = 5.0
    queue_maxsize: int = 50000

    # News
    news_poll_interval: int = 300
    news_sources: list[NewsSourceConfig] = Field(default_factory=list)

    # Monitoring
    health_port: int = 8080
    gap_threshold: int = 60
    timestamp_tolerance: int = 60
    malformed_rate_threshold: int = 2

    # Supervisor
    max_restart_attempts: int = 10
    restart_backoff_base: float = 1.0
    restart_backoff_max: float = 60.0
