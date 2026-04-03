"""FinBERT sentiment processor implementation."""

from __future__ import annotations

from nexus_sentiment.processors.base import BaseSentimentProcessor, SentimentResult

try:
    import transformers

    _HAS_TRANSFORMERS = True
except ImportError:
    _HAS_TRANSFORMERS = False


class FinBertProcessor(BaseSentimentProcessor):
    """Transformer-based financial sentiment analysis using ProsusAI/finbert."""

    def __init__(self) -> None:
        if not _HAS_TRANSFORMERS:
            msg = (
                "FinBERT requires 'transformers' and 'torch'. "
                "Install with: pip install nexus-sentiment[finbert]"
            )
            raise ImportError(msg)
        self._pipeline = None

    async def load(self) -> None:
        """Load FinBERT model via transformers pipeline."""
        from transformers import pipeline

        self._pipeline = pipeline(
            "text-classification",
            model="ProsusAI/finbert",
            top_k=3,
        )

    def analyze(self, text: str) -> SentimentResult:
        """Run FinBERT inference on text."""
        if self._pipeline is None:
            msg = "FinBertProcessor not loaded — call load() first"
            raise RuntimeError(msg)

        results = self._pipeline(text[:512])  # FinBERT max 512 tokens
        if isinstance(results, list) and results and isinstance(results[0], list):
            results = results[0]

        probs = {}
        for item in results:
            probs[item["label"]] = item["score"]

        pos = probs.get("positive", 0.0)
        neg = probs.get("negative", 0.0)

        score = pos - neg
        confidence = max(probs.values()) if probs else 0.0

        if score >= 0.05:
            label = "positive"
        elif score <= -0.05:
            label = "negative"
        else:
            label = "neutral"

        return SentimentResult(
            label=label,
            score=max(-1.0, min(1.0, score)),
            confidence=max(0.0, min(1.0, confidence)),
        )

    async def close(self) -> None:
        """Release resources."""
        self._pipeline = None

    @property
    def model_id(self) -> str:
        """Return FinBERT version identifier."""
        return f"finbert:{transformers.__version__}"
