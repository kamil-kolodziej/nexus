"""Base sentiment processor ABC."""

from __future__ import annotations

import abc
from typing import NamedTuple


class SentimentResult(NamedTuple):
    """Result of NLP sentiment inference."""

    label: str  # "positive", "negative", "neutral"
    score: float  # [-1.0, +1.0]
    confidence: float  # [0.0, 1.0]


class BaseSentimentProcessor(abc.ABC):
    """Abstract base class for NLP sentiment processors."""

    @abc.abstractmethod
    async def load(self) -> None:
        """Load model/resources. Raises on failure."""

    @abc.abstractmethod
    def analyze(self, text: str) -> SentimentResult:
        """Synchronous inference. Called in thread pool."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Release resources."""

    @property
    @abc.abstractmethod
    def model_id(self) -> str:
        """Processor + version identifier."""
