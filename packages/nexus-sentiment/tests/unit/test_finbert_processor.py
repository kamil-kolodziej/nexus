"""Tests for FinBertProcessor."""

from __future__ import annotations

import pytest


class TestFinBertProcessor:
    """Tests for FinBERT sentiment processor."""

    def test_import_error_without_transformers(self):
        """Verify clear error when transformers/torch not installed."""
        try:
            import transformers  # noqa: F401

            pytest.skip("transformers is installed — cannot test ImportError guard")
        except ImportError:
            pass

        from nexus_sentiment.processors.finbert_processor import FinBertProcessor

        with pytest.raises(ImportError, match="transformers"):
            FinBertProcessor()

    def test_finbert_import_succeeds_with_transformers(self):
        """If transformers is installed, FinBertProcessor should instantiate."""
        try:
            import transformers  # noqa: F401
        except ImportError:
            pytest.skip("transformers not installed")

        from nexus_sentiment.processors.finbert_processor import FinBertProcessor

        proc = FinBertProcessor()
        assert "finbert:" in proc.model_id


class TestFinBertAnalyze:
    """Argmax label selection and score computation, with the pipeline stubbed."""

    @pytest.fixture
    def processor(self):
        # Bypass the transformers-required __init__ so this test runs without the
        # optional finbert extras installed.
        from nexus_sentiment.processors.finbert_processor import FinBertProcessor

        proc = FinBertProcessor.__new__(FinBertProcessor)
        proc._pipeline = None
        return proc

    def _stub(self, processor, pos, neg, neutral):
        def fake_pipeline(_text):
            return [
                {"label": "positive", "score": pos},
                {"label": "negative", "score": neg},
                {"label": "neutral", "score": neutral},
            ]

        processor._pipeline = fake_pipeline

    def test_positive_argmax(self, processor):
        self._stub(processor, pos=0.7, neg=0.2, neutral=0.1)
        result = processor.analyze("Great earnings beat")
        assert result.label == "positive"
        assert result.score == pytest.approx(0.5)
        assert result.confidence == pytest.approx(0.7)

    def test_negative_argmax(self, processor):
        self._stub(processor, pos=0.1, neg=0.8, neutral=0.1)
        result = processor.analyze("Missed revenue target")
        assert result.label == "negative"
        assert result.score == pytest.approx(-0.7)

    def test_argmax_over_narrow_margin(self, processor):
        # VADER-style `pos - neg >= 0.05` would label this "positive" even though
        # pos is the clear argmax — argmax and VADER happen to agree here.
        self._stub(processor, pos=0.40, neg=0.35, neutral=0.25)
        result = processor.analyze("Company announces reorganization")
        assert result.label == "positive"

    def test_neutral_when_neutral_dominates(self, processor):
        # VADER-style would label this "neutral" too, but via the wrong logic.
        # Argmax picks neutral because it has the highest probability.
        self._stub(processor, pos=0.20, neg=0.20, neutral=0.60)
        result = processor.analyze("Board meeting scheduled for Tuesday")
        assert result.label == "neutral"
        assert result.score == pytest.approx(0.0)

    def test_neutral_picked_over_narrow_directional_lead(self, processor):
        # Regression for the old VADER-threshold bug: pos - neg = 0.05 used to
        # falsely label "positive" even when neutral was the argmax.
        self._stub(processor, pos=0.40, neg=0.35, neutral=0.45)
        result = processor.analyze("Mixed signals from the latest report")
        assert result.label == "neutral"

    def test_raises_if_not_loaded(self, processor):
        processor._pipeline = None
        with pytest.raises(RuntimeError, match="not loaded"):
            processor.analyze("anything")
