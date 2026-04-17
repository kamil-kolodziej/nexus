"""FinBERT sentiment processor implementation."""

from __future__ import annotations

from typing import Any

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
        self._pipeline: Any = None

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

        # truncation must be passed at call time — the pipeline constructor
        # silently drops it (transformers issue #25994).
        results = self._pipeline(text, truncation=True)
        if isinstance(results, list) and results and isinstance(results[0], list):
            results = results[0]

        probs = {}
        for item in results:
            probs[item["label"]] = item["score"]

        pos = probs.get("positive", 0.0)
        neg = probs.get("negative", 0.0)
        neutral = probs.get("neutral", 0.0)

        # Label via softmax argmax over all three classes. Strict `>` so ties
        # fall through to "neutral" — avoids emitting a directional signal when
        # the model is genuinely undecided (matches CHK044 in spec).
        if pos > neg and pos > neutral:
            label = "positive"
        elif neg > pos and neg > neutral:
            label = "negative"
        else:
            label = "neutral"

        # Signed magnitude for downstream aggregation
        score = pos - neg
        confidence = max(probs.values()) if probs else 0.0

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
