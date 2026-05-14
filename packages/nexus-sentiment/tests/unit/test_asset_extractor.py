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
        assert "BTC/USDT" not in result

    def test_no_active_filter_returns_all_matches(self, dictionary_path):
        extractor = AssetExtractor(dictionary_path, active_assets=set())
        result = extractor.extract("Bitcoin and Ethereum rally")
        assert "BTC/USDT" in result
        assert "ETH/USDT" in result

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


class TestAssetExtractorCompanies:
    """Companies section + non-dict entries are tolerated."""

    @pytest.fixture
    def dictionary_path(self):
        data = {
            "version": "1.0.0",
            "assets": {
                "BTC/USDT": {"aliases": ["Bitcoin"]},
                "BAD_ASSET": "not-a-dict",  # exercises the `not isinstance(info, dict)` skip
            },
            "sectors": {
                "sector:crypto": {"keywords": ["crypto"]},
                "BAD_SECTOR": "also-bad",
            },
            "companies": {
                "company:COIN": {"aliases": ["Coinbase", "COIN"]},
                "BAD_COMPANY": 42,
            },
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(data, f)
            path = f.name
        yield path
        os.unlink(path)

    def test_company_alias_extracted(self, dictionary_path):
        extractor = AssetExtractor(dictionary_path, active_assets={"company:COIN"})
        result = extractor.extract("Coinbase reports record quarterly revenue")
        assert "company:COIN" in result

    def test_non_dict_entries_skipped_not_crash(self, dictionary_path):
        # The loader must tolerate stray non-dict values inside assets/sectors/companies.
        extractor = AssetExtractor(
            dictionary_path,
            active_assets={"BTC/USDT", "sector:crypto", "company:COIN"},
        )
        # Patterns from the valid entries are loaded, bad ones are skipped.
        canonical_ids = {cid for cid, _ in extractor._patterns}
        assert canonical_ids == {"BTC/USDT", "sector:crypto", "company:COIN"}

    def test_assets_value_is_not_a_dict(self):
        """If `assets:` itself is not a dict (e.g. a list), loader skips silently."""
        data = {
            "version": "1.0.0",
            "assets": ["this", "is", "wrong"],
            "sectors": ["bad"],
            "companies": "string",
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(data, f)
            path = f.name
        try:
            extractor = AssetExtractor(path, active_assets=set())
            assert extractor._patterns == []
        finally:
            os.unlink(path)
