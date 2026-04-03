"""Unit tests for SentimentConfig."""

from __future__ import annotations

try:
    import tomllib
except ModuleNotFoundError:
    pass


class TestSentimentConfig:
    """Tests for SentimentConfig TOML loading and env var precedence."""

    def test_defaults(self, monkeypatch):
        monkeypatch.setenv("NEXUS_CONFIG_FILE", "/nonexistent/config.toml")
        from nexus_sentiment.config import SentimentConfig

        config = SentimentConfig()
        assert config.processor_type == "vader"
        assert config.redis_url == "redis://localhost:6379"
        assert config.input_stream == "nexus:news-events"
        assert config.output_stream == "nexus:sentiment-events"
        assert config.health_port == 8081
        assert config.max_fan_out == 50
        assert config.pending_claim_threshold == 300
        assert config.claim_sweep_interval == 60

    def test_toml_overrides_defaults(self, monkeypatch, tmp_path):
        toml_content = """
[sentiment]
processor_type = "finbert"
health_port = 9090
max_fan_out = 10
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(toml_content)
        monkeypatch.setenv("NEXUS_CONFIG_FILE", str(config_file))

        from nexus_sentiment.config import SentimentConfig

        config = SentimentConfig()
        assert config.processor_type == "finbert"
        assert config.health_port == 9090
        assert config.max_fan_out == 10

    def test_env_overrides_toml(self, monkeypatch, tmp_path):
        toml_content = """
[sentiment]
processor_type = "finbert"
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(toml_content)
        monkeypatch.setenv("NEXUS_CONFIG_FILE", str(config_file))
        monkeypatch.setenv("NEXUS_PROCESSOR_TYPE", "vader")

        from nexus_sentiment.config import SentimentConfig

        config = SentimentConfig()
        assert config.processor_type == "vader"

    def test_init_overrides_env(self, monkeypatch):
        monkeypatch.setenv("NEXUS_CONFIG_FILE", "/nonexistent/config.toml")
        monkeypatch.setenv("NEXUS_PROCESSOR_TYPE", "finbert")

        from nexus_sentiment.config import SentimentConfig

        config = SentimentConfig(processor_type="vader")
        assert config.processor_type == "vader"

    def test_redis_url_from_redis_section(self, monkeypatch, tmp_path):
        toml_content = """
[redis]
url = "redis://custom:6380"
"""
        config_file = tmp_path / "config.toml"
        config_file.write_text(toml_content)
        monkeypatch.setenv("NEXUS_CONFIG_FILE", str(config_file))

        from nexus_sentiment.config import SentimentConfig

        config = SentimentConfig()
        assert config.redis_url == "redis://custom:6380"
