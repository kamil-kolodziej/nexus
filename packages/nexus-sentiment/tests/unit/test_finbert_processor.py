"""Tests for FinBertProcessor."""

from __future__ import annotations

import pytest


class TestFinBertProcessor:
    """Tests for FinBERT sentiment processor."""

    def test_import_error_without_transformers(self):
        """Verify clear error when transformers/torch not installed."""
        try:
            import transformers

            pytest.skip("transformers is installed — cannot test ImportError guard")
        except ImportError:
            pass

        from nexus_sentiment.processors.finbert_processor import FinBertProcessor

        with pytest.raises(ImportError, match="transformers"):
            FinBertProcessor()

    def test_finbert_import_succeeds_with_transformers(self):
        """If transformers is installed, FinBertProcessor should instantiate."""
        try:
            import transformers
        except ImportError:
            pytest.skip("transformers not installed")

        from nexus_sentiment.processors.finbert_processor import FinBertProcessor

        proc = FinBertProcessor()
        assert "finbert:" in proc.model_id
