"""Tests for FinBertProcessor."""

from __future__ import annotations

from unittest.mock import MagicMock

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
        captured = {}

        def fake_pipeline(text, **kwargs):
            captured["text"] = text
            captured["kwargs"] = kwargs
            return [
                {"label": "positive", "score": pos},
                {"label": "negative", "score": neg},
                {"label": "neutral", "score": neutral},
            ]

        processor._pipeline = fake_pipeline
        processor._captured = captured

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

    def test_pos_neg_tie_resolves_to_neutral(self, processor):
        # pos == neg with both above neutral: model is split between positive
        # and negative — honest answer is "neutral", not an arbitrary pick.
        self._stub(processor, pos=0.40, neg=0.40, neutral=0.20)
        result = processor.analyze("Report is mixed")
        assert result.label == "neutral"

    def test_perfect_three_way_tie_resolves_to_neutral(self, processor):
        # CHK044 edge case.
        self._stub(processor, pos=1 / 3, neg=1 / 3, neutral=1 / 3)
        result = processor.analyze("Ambiguous")
        assert result.label == "neutral"

    def test_truncation_passed_at_call_time(self, processor):
        # Regression guard: transformers issue #25994 — `truncation=True` at
        # pipeline construction is silently dropped, so it must be passed on
        # every call. Without this, FinBERT raises on inputs > 512 tokens.
        self._stub(processor, pos=0.5, neg=0.3, neutral=0.2)
        processor.analyze("some long article text")
        assert processor._captured["kwargs"].get("truncation") is True

    def test_raises_if_not_loaded(self, processor):
        processor._pipeline = None
        with pytest.raises(RuntimeError, match="not loaded"):
            processor.analyze("anything")

    def test_nested_list_result_is_unwrapped(self, processor):
        """transformers pipelines with top_k may return a wrapped list."""
        captured = {}

        def fake_pipeline(text, **kwargs):
            captured["text"] = text
            return [
                [
                    {"label": "positive", "score": 0.7},
                    {"label": "negative", "score": 0.2},
                    {"label": "neutral", "score": 0.1},
                ]
            ]

        processor._pipeline = fake_pipeline
        result = processor.analyze("Earnings beat estimates")
        assert result.label == "positive"
        assert result.score == pytest.approx(0.5)

    def test_empty_results_default_to_neutral(self, processor):
        """If the pipeline returns nothing usable, label is neutral with zero confidence."""
        processor._pipeline = lambda text, **kwargs: []
        result = processor.analyze("anything")
        assert result.label == "neutral"
        assert result.score == 0.0
        assert result.confidence == 0.0


class TestFinBertGuards:
    """ImportError guard and load()/close() lifecycle."""

    def test_init_raises_when_transformers_missing(self, monkeypatch):
        import nexus_sentiment.processors.finbert_processor as fb_mod

        monkeypatch.setattr(fb_mod, "_HAS_TRANSFORMERS", False)
        with pytest.raises(ImportError, match="transformers"):
            fb_mod.FinBertProcessor()

    async def test_close_releases_pipeline(self):
        try:
            import transformers  # noqa: F401
        except ImportError:
            pytest.skip("transformers not installed")
        from nexus_sentiment.processors.finbert_processor import FinBertProcessor

        proc = FinBertProcessor()
        proc._pipeline = MagicMock()
        await proc.close()
        assert proc._pipeline is None
