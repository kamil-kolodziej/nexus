"""Ingestion-specific configuration."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 fallback
    import tomli as tomllib


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
    exchange_api_key: SecretStr = SecretStr("")
    exchange_api_secret: SecretStr = SecretStr("")
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
    health_host: str = "127.0.0.1"
    health_port: int = 8080
    gap_threshold: int = 60
    timestamp_tolerance: int = 60
    malformed_rate_threshold: int = 2

    # Supervisor
    max_restart_attempts: int = 10
    restart_backoff_base: float = 1.0
    restart_backoff_max: float = 60.0

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: Any,
        env_settings: Any,
        dotenv_settings: Any,
        file_secret_settings: Any,
    ) -> tuple[Any, ...]:
        # Precedence: explicit init args > env vars > config.toml > defaults.
        return (
            init_settings,
            env_settings,
            cls._toml_config_settings_source,
            dotenv_settings,
            file_secret_settings,
        )

    @classmethod
    def _toml_config_settings_source(cls, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        """Load settings from config.toml-like file into flat IngestionConfig fields."""
        config_path = Path(os.getenv("NEXUS_CONFIG_FILE", "config.toml"))
        if not config_path.exists() or not config_path.is_file():
            return {}

        with config_path.open("rb") as fh:
            raw = tomllib.load(fh)

        if not isinstance(raw, dict):
            return {}

        settings: dict[str, Any] = {}

        exchange = raw.get("exchange")
        if isinstance(exchange, dict):
            if "api_key" in exchange or "api_secret" in exchange:
                import logging as _log
                _log.getLogger(__name__).warning(
                    "Credentials found in TOML config are ignored. "
                    "Set NEXUS_EXCHANGE_API_KEY and NEXUS_EXCHANGE_API_SECRET "
                    "via environment variables only (SRC-003)."
                )
            if "id" in exchange:
                settings["exchange_id"] = exchange["id"]
            if "sandbox" in exchange:
                settings["exchange_sandbox"] = exchange["sandbox"]
            if "subscribed_assets" in exchange:
                settings["subscribed_assets"] = exchange["subscribed_assets"]

        redis = raw.get("redis")
        if isinstance(redis, dict):
            redis_fields = {
                "url": "redis_url",
                "buffer_max": "redis_buffer_max",
                "market_events_stream": "market_events_stream",
                "news_events_stream": "news_events_stream",
                "health_events_stream": "health_events_stream",
                "market_events_maxlen": "market_events_maxlen",
                "news_events_maxlen": "news_events_maxlen",
                "health_events_maxlen": "health_events_maxlen",
            }
            for key, target in redis_fields.items():
                if key in redis:
                    settings[target] = redis[key]

        timescaledb = raw.get("timescaledb")
        if isinstance(timescaledb, dict):
            timescaledb_fields = {
                "dsn": "timescaledb_dsn",
                "batch_size": "batch_size",
                "flush_interval": "flush_interval",
                "queue_maxsize": "queue_maxsize",
            }
            for key, target in timescaledb_fields.items():
                if key in timescaledb:
                    settings[target] = timescaledb[key]

        news = raw.get("news")
        if isinstance(news, dict):
            if "poll_interval" in news:
                settings["news_poll_interval"] = news["poll_interval"]
            if "sources" in news:
                settings["news_sources"] = news["sources"]

        monitoring = raw.get("monitoring")
        if isinstance(monitoring, dict):
            monitoring_fields = {
                "health_host": "health_host",
                "health_port": "health_port",
                "gap_threshold": "gap_threshold",
                "timestamp_tolerance": "timestamp_tolerance",
                "malformed_rate_threshold": "malformed_rate_threshold",
            }
            for key, target in monitoring_fields.items():
                if key in monitoring:
                    settings[target] = monitoring[key]

        supervisor = raw.get("supervisor")
        if isinstance(supervisor, dict):
            supervisor_fields = {
                "max_restart_attempts": "max_restart_attempts",
                "restart_backoff_base": "restart_backoff_base",
                "restart_backoff_max": "restart_backoff_max",
            }
            for key, target in supervisor_fields.items():
                if key in supervisor:
                    settings[target] = supervisor[key]

        return settings
