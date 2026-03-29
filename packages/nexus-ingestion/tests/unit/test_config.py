"""Unit tests for IngestionConfig source precedence and TOML loading."""

from __future__ import annotations

from pathlib import Path

import pydantic
import pytest
from nexus_ingestion.adapters.exchange_adapter import ExchangeAdapter
from nexus_ingestion.config import IngestionConfig, NewsSourceConfig

# Fixture covering every TOML-mappable field with non-default values.
_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
_ALL_FIELDS_TOML = _FIXTURES_DIR / "config_all_fields.toml"


@pytest.fixture(autouse=True)
def clear_nexus_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear relevant env vars so each test controls precedence explicitly."""
    keys = [
        "NEXUS_CONFIG_FILE",
        "NEXUS_EXCHANGE_ID",
        "NEXUS_EXCHANGE_SANDBOX",
        "NEXUS_SUBSCRIBED_ASSETS",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)


def _write_config(path: Path, exchange_id: str = "kraken") -> None:
    path.write_text(
        f"""
[exchange]
id = "{exchange_id}"
sandbox = false
subscribed_assets = ["ETH/USDT", "BTC/USDT"]
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_toml_overrides_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_file = tmp_path / "config.toml"
    _write_config(config_file, exchange_id="kraken")
    monkeypatch.setenv("NEXUS_CONFIG_FILE", str(config_file))

    config = IngestionConfig()

    assert config.exchange_id == "kraken"
    assert config.exchange_sandbox is False
    assert config.subscribed_assets == ["ETH/USDT", "BTC/USDT"]


def test_env_overrides_toml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_file = tmp_path / "config.toml"
    _write_config(config_file, exchange_id="kraken")
    monkeypatch.setenv("NEXUS_CONFIG_FILE", str(config_file))
    monkeypatch.setenv("NEXUS_EXCHANGE_ID", "coinbase")

    config = IngestionConfig()

    assert config.exchange_id == "coinbase"


def test_init_overrides_env_and_toml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_file = tmp_path / "config.toml"
    _write_config(config_file, exchange_id="kraken")
    monkeypatch.setenv("NEXUS_CONFIG_FILE", str(config_file))
    monkeypatch.setenv("NEXUS_EXCHANGE_ID", "coinbase")

    config = IngestionConfig(exchange_id="bybit")

    assert config.exchange_id == "bybit"


def test_toml_credentials_ignored_and_warned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """SRC-003: api_key/api_secret in TOML must not load into config and must warn."""
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        '[exchange]\napi_key = "secret"\napi_secret = "topsecret"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("NEXUS_CONFIG_FILE", str(config_file))

    import logging

    with caplog.at_level(logging.WARNING):
        config = IngestionConfig()

    assert config.exchange_api_key.get_secret_value() == ""
    assert config.exchange_api_secret.get_secret_value() == ""
    assert "ignored" in caplog.text
    assert "SRC-003" in caplog.text


def test_invalid_news_source_type_raises() -> None:
    """Unsupported source type must fail at parse time, not silently do nothing."""
    with pytest.raises(pydantic.ValidationError):
        NewsSourceConfig(type="invalid")


def test_exchange_adapter_keeps_explicit_empty_assets() -> None:
    adapter = ExchangeAdapter("binance", assets=[])
    assert adapter._assets == []


def test_toml_all_sections_map_correctly(monkeypatch: pytest.MonkeyPatch) -> None:
    """Drift protection: every TOML-mappable field must round-trip from the fixture file.

    If a new field is added to IngestionConfig but forgotten in _toml_config_settings_source,
    its assertion here will fail because the loader returns the default instead of the
    non-default fixture value.
    """
    monkeypatch.setenv("NEXUS_CONFIG_FILE", str(_ALL_FIELDS_TOML))
    c = IngestionConfig()

    # exchange
    assert c.exchange_id == "kraken"
    assert c.exchange_sandbox is False
    assert c.subscribed_assets == ["ETH/USDT"]

    # redis
    assert c.redis_url == "redis://redis-host:6380"
    assert c.redis_buffer_max == 500
    assert c.market_events_stream == "test:market"
    assert c.news_events_stream == "test:news"
    assert c.health_events_stream == "test:health"
    assert c.market_events_maxlen == 50000
    assert c.news_events_maxlen == 2000
    assert c.health_events_maxlen == 1000

    # timescaledb
    assert c.timescaledb_dsn == "postgresql://user:pass@db:5433/testdb"
    assert c.batch_size == 100
    assert c.flush_interval == 2.5
    assert c.queue_maxsize == 1000

    # news
    assert c.news_poll_interval == 60

    # monitoring
    assert c.health_host == "0.0.0.0"
    assert c.health_port == 9090
    assert c.gap_threshold == 30
    assert c.timestamp_tolerance == 120
    assert c.malformed_rate_threshold == 5

    # supervisor
    assert c.max_restart_attempts == 5
    assert c.restart_backoff_base == 2.0
    assert c.restart_backoff_max == 30.0
