"""Sentiment service configuration."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 fallback
    import tomli as tomllib  # type: ignore[no-redef]


class SentimentConfig(BaseSettings):
    """Configuration for the nexus-sentiment service."""

    model_config = SettingsConfigDict(
        env_prefix="NEXUS_",
        env_nested_delimiter="__",
    )

    # Processor
    processor_type: str = "vader"

    # Redis
    redis_url: str = "redis://localhost:6379"
    input_stream: str = "nexus:news-events"
    output_stream: str = "nexus:sentiment-events"
    health_stream: str = "nexus:sentiment-health-events"
    consumer_group: str = "nexus-sentiment-group"
    block_timeout: int = 5000
    output_maxlen: int = 50000
    health_maxlen: int = 5000

    # Consumer group
    pending_claim_threshold: int = 300
    claim_sweep_interval: int = 60

    # Asset extraction
    active_assets: list[str] = Field(default=["BTC/USDT"])
    asset_dictionary_path: str = "data/asset_dictionary.yaml"
    max_fan_out: int = 50

    # TimescaleDB
    timescaledb_dsn: str = "postgresql://nexus:nexus_dev@localhost:5432/nexus"
    batch_size: int = 500
    flush_interval: float = 5.0

    # Monitoring
    health_host: str = "127.0.0.1"
    health_port: int = 8081
    log_env: str = "production"

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: Any,
        env_settings: Any,
        dotenv_settings: Any,
        file_secret_settings: Any,
    ) -> tuple[Any, ...]:
        return (
            init_settings,
            env_settings,
            cls._toml_config_settings_source,
            dotenv_settings,
            file_secret_settings,
        )

    @classmethod
    def _toml_config_settings_source(cls, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        """Load settings from config.toml [sentiment] section."""
        config_path = Path(os.getenv("NEXUS_CONFIG_FILE", "config.toml"))
        if not config_path.exists() or not config_path.is_file():
            return {}

        with config_path.open("rb") as fh:
            raw = tomllib.load(fh)

        settings: dict[str, Any] = {}

        sentiment = raw.get("sentiment")
        if isinstance(sentiment, dict):
            sentiment_fields = {
                "processor_type": "processor_type",
                "input_stream": "input_stream",
                "output_stream": "output_stream",
                "health_stream": "health_stream",
                "consumer_group": "consumer_group",
                "block_timeout": "block_timeout",
                "pending_claim_threshold": "pending_claim_threshold",
                "claim_sweep_interval": "claim_sweep_interval",
                "active_assets": "active_assets",
                "asset_dictionary_path": "asset_dictionary_path",
                "output_maxlen": "output_maxlen",
                "health_maxlen": "health_maxlen",
                "batch_size": "batch_size",
                "flush_interval": "flush_interval",
                "health_host": "health_host",
                "health_port": "health_port",
                "max_fan_out": "max_fan_out",
                "log_env": "log_env",
            }
            for key, target in sentiment_fields.items():
                if key in sentiment:
                    settings[target] = sentiment[key]

        redis = raw.get("redis")
        if isinstance(redis, dict):
            if "url" in redis:
                settings.setdefault("redis_url", redis["url"])

        timescaledb = raw.get("timescaledb")
        if isinstance(timescaledb, dict):
            if "dsn" in timescaledb:
                settings.setdefault("timescaledb_dsn", timescaledb["dsn"])

        return settings
