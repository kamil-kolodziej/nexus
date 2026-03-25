"""Unit tests for IngestionConfig source precedence and TOML loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from nexus_ingestion.adapters.exchange_adapter import ExchangeAdapter
from nexus_ingestion.config import IngestionConfig


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


def test_exchange_adapter_keeps_explicit_empty_assets() -> None:
    adapter = ExchangeAdapter("binance", assets=[])
    assert adapter._assets == []
