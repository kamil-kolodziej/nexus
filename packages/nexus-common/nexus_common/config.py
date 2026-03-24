"""Shared configuration base for Nexus services."""

from __future__ import annotations

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisSettings(BaseSettings):
    """Redis connection settings."""

    model_config = SettingsConfigDict(env_prefix="NEXUS_REDIS_")

    url: str = "redis://localhost:6379"
    buffer_max: int = 10000


class TimescaleDBSettings(BaseSettings):
    """TimescaleDB connection settings."""

    model_config = SettingsConfigDict(env_prefix="NEXUS_TIMESCALEDB_")

    dsn: SecretStr = SecretStr("postgresql://nexus:nexus_dev@localhost:5432/nexus")
    batch_size: int = 500
    flush_interval: float = 5.0
    queue_maxsize: int = 50000


class ExchangeSettings(BaseSettings):
    """Exchange connection settings."""

    model_config = SettingsConfigDict(env_prefix="NEXUS_EXCHANGE_")

    id: str = "binance"
    sandbox: bool = True
    api_key: SecretStr = SecretStr("")
    api_secret: SecretStr = SecretStr("")
    subscribed_assets: list[str] = ["BTC/USDT"]


class NexusBaseSettings(BaseSettings):
    """Base settings shared across all Nexus services."""

    model_config = SettingsConfigDict(
        env_prefix="NEXUS_",
        env_nested_delimiter="__",
    )

    redis: RedisSettings = RedisSettings()
    timescaledb: TimescaleDBSettings = TimescaleDBSettings()
    exchange: ExchangeSettings = ExchangeSettings()
