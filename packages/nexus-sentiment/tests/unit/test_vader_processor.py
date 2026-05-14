"""Property-based and unit tests for VaderProcessor."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from nexus_sentiment.processors.vader_processor import VaderProcessor


class TestVaderProcessor:
    """Tests for VADER sentiment processor."""

    @pytest.fixture
    async def processor(self):
        proc = VaderProcessor()
        await proc.load()
        yield proc
        await proc.close()

    async def test_load_creates_analyzer(self, processor):
        assert processor.model_id.startswith("vader:")

    @given(text=st.text(min_size=1, max_size=500))
    @settings(max_examples=100)
    def test_score_in_range(self, text):
        """SC-005: score must be in [-1.0, +1.0] for all inputs."""
        proc = VaderProcessor()
        proc._load_sync()
        result = proc.analyze(text)
        assert -1.0 <= result.score <= 1.0

    @given(text=st.text(min_size=1, max_size=500))
    @settings(max_examples=100)
    def test_confidence_in_range(self, text):
        """SC-005: confidence must be in [0.0, 1.0] for all inputs."""
        proc = VaderProcessor()
        proc._load_sync()
        result = proc.analyze(text)
        assert 0.0 <= result.confidence <= 1.0

    @given(text=st.text(min_size=1, max_size=500))
    @settings(max_examples=100)
    def test_label_threshold_correctness(self, text):
        """VADER thresholds: compound >= 0.05 -> positive, <= -0.05 -> negative, else neutral."""
        proc = VaderProcessor()
        proc._load_sync()
        result = proc.analyze(text)
        if result.score >= 0.05:
            assert result.label == "positive"
        elif result.score <= -0.05:
            assert result.label == "negative"
        else:
            assert result.label == "neutral"

    async def test_positive_text(self, processor):
        result = processor.analyze("This is absolutely wonderful and amazing news!")
        assert result.score > 0
        assert result.label == "positive"

    async def test_negative_text(self, processor):
        result = processor.analyze("This is terrible and awful news, very disappointing.")
        assert result.score < 0
        assert result.label == "negative"

    async def test_neutral_text(self, processor):
        result = processor.analyze("The meeting is scheduled for tomorrow at 3pm.")
        assert result.label == "neutral"

    async def test_model_id_format(self, processor):
        assert "vader:" in processor.model_id

    def test_analyze_before_load_raises(self):
        proc = VaderProcessor()
        with pytest.raises(RuntimeError, match="not loaded"):
            proc.analyze("anything")
