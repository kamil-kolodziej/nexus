"""VADER sentiment processor implementation."""

from __future__ import annotations

from importlib.metadata import version as _pkg_version

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from nexus_sentiment.processors.base import BaseSentimentProcessor, SentimentResult


class VaderProcessor(BaseSentimentProcessor):
    """Rule-based sentiment analysis using VADER."""

    def __init__(self) -> None:
        self._analyzer: SentimentIntensityAnalyzer | None = None

    async def load(self) -> None:
        """Load VADER analyzer."""
        self._load_sync()

    def _load_sync(self) -> None:
        """Synchronous load for use in property-based tests."""
        self._analyzer = SentimentIntensityAnalyzer()

    def analyze(self, text: str) -> SentimentResult:
        """Run VADER inference on text."""
        if self._analyzer is None:
            msg = "VaderProcessor not loaded — call load() first"
            raise RuntimeError(msg)

        scores = self._analyzer.polarity_scores(text)
        compound = scores["compound"]

        if compound >= 0.05:
            label = "positive"
        elif compound <= -0.05:
            label = "negative"
        else:
            label = "neutral"

        return SentimentResult(
            label=label,
            score=compound,
            confidence=abs(compound),
        )

    async def close(self) -> None:
        """Release resources."""
        self._analyzer = None

    @property
    def model_id(self) -> str:
        """Return VADER version identifier."""
        return f"vader:{_pkg_version('vaderSentiment')}"
