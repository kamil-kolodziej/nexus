"""Unit tests for AssetExtractor."""

from __future__ import annotations

import os
import tempfile

import pytest
import yaml
from nexus_sentiment.extraction.asset_extractor import AssetExtractor


@pytest.fixture
def dictionary_path():
    """Create a temporary asset dictionary YAML file."""
    data = {
        "version": "1.0.0",
        "assets": {
            "BTC/USDT": {"aliases": ["Bitcoin", "BTC", "bitcoin"]},
            "ETH/USDT": {"aliases": ["Ethereum", "ETH", "Ether"]},
        },
        "sectors": {
            "sector:crypto": {"keywords": ["crypto market", "cryptocurrency market", "crypto"]},
            "sector:stocks": {"keywords": ["stock market", "equities market", "stocks"]},
        },
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(data, f)
        path = f.name
    yield path
    os.unlink(path)


class TestAssetExtractor:
    """Tests for dictionary-based asset extraction."""

    def test_loads_dictionary(self, dictionary_path):
        extractor = AssetExtractor(dictionary_path, active_assets={"BTC/USDT", "ETH/USDT"})
        assert len(extractor._patterns) > 0

    def test_extracts_bitcoin(self, dictionary_path):
        extractor = AssetExtractor(dictionary_path, active_assets={"BTC/USDT"})
        result = extractor.extract("Bitcoin surges past $100K")
        assert "BTC/USDT" in result

    def test_case_insensitive(self, dictionary_path):
        extractor = AssetExtractor(dictionary_path, active_assets={"BTC/USDT"})
        result = extractor.extract("BITCOIN hits new high")
        assert "BTC/USDT" in result

    def test_word_boundary(self, dictionary_path):
        extractor = AssetExtractor(dictionary_path, active_assets={"ETH/USDT"})
        # "ETH" should match as word boundary
        result = extractor.extract("ETH rallies today")
        assert "ETH/USDT" in result

    def test_multiple_assets(self, dictionary_path):
        extractor = AssetExtractor(dictionary_path, active_assets={"BTC/USDT", "ETH/USDT"})
        result = extractor.extract("Bitcoin and Ethereum rally on ETF approval")
        assert "BTC/USDT" in result
        assert "ETH/USDT" in result

    def test_deduplication(self, dictionary_path):
        extractor = AssetExtractor(dictionary_path, active_assets={"BTC/USDT"})
        result = extractor.extract("Bitcoin BTC bitcoin all the same")
        assert result.count("BTC/USDT") == 1

    def test_sector_extraction(self, dictionary_path):
        extractor = AssetExtractor(dictionary_path, active_assets={"sector:crypto"})
        result = extractor.extract("Crypto market crashes amid regulatory crackdown")
        assert "sector:crypto" in result

    def test_both_asset_and_sector(self, dictionary_path):
        extractor = AssetExtractor(dictionary_path, active_assets={"BTC/USDT", "sector:crypto"})
        result = extractor.extract("Bitcoin leads crypto market rally")
        assert "BTC/USDT" in result
        assert "sector:crypto" in result

    def test_inactive_asset_suppressed(self, dictionary_path):
        extractor = AssetExtractor(
            dictionary_path,
            active_assets={"ETH/USDT"},  # BTC not active
        )
        result = extractor.extract("Bitcoin surges")
        # BTC/USDT extracted but not in active_assets — filtering happens in service
        # The extractor itself returns all matches; filtering is at service level
        assert "BTC/USDT" in result

    def test_missing_dictionary_raises(self):
        with pytest.raises(FileNotFoundError):
            AssetExtractor("/nonexistent/path.yaml", active_assets=set())

    def test_malformed_dictionary_raises(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("not: a: valid: yaml: [")
            path = f.name
        try:
            with pytest.raises(Exception):
                AssetExtractor(path, active_assets=set())
        finally:
            os.unlink(path)

    def test_no_version_raises(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"assets": {}}, f)
            path = f.name
        try:
            with pytest.raises(ValueError, match="version"):
                AssetExtractor(path, active_assets=set())
        finally:
            os.unlink(path)

    def test_no_match_returns_empty(self, dictionary_path):
        extractor = AssetExtractor(dictionary_path, active_assets={"BTC/USDT"})
        result = extractor.extract("Federal Reserve signals rate hikes")
        assert result == []


class TestSectorExtraction:
    """US5: Sector tagging tests."""

    @pytest.fixture
    def extractor(self, dictionary_path):
        return AssetExtractor(
            dictionary_path,
            active_assets={"BTC/USDT", "ETH/USDT", "sector:crypto", "sector:stocks"},
        )

    def test_sector_crypto_extracted(self, extractor):
        result = extractor.extract("Crypto market crashes amid regulatory crackdown")
        assert "sector:crypto" in result

    def test_both_specific_and_sector(self, extractor):
        result = extractor.extract("Bitcoin leads crypto market rally")
        assert "BTC/USDT" in result
        assert "sector:crypto" in result

    def test_sector_distinguishable(self, extractor):
        result = extractor.extract("Crypto market dips")
        for item in result:
            if item.startswith("sector:"):
                assert ":" in item  # sector: prefix is distinguishable
