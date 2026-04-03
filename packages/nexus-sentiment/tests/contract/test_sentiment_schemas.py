"""Contract snapshot tests for SentimentScore serialization stability."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from nexus_common.schemas.enums import EventType
from nexus_common.schemas.market_event import MarketEvent, SentimentScore

FIXED_TS = datetime(2026, 4, 1, 12, 0, 0, 0, tzinfo=UTC)


class TestSentimentScoreContracts:
    """Snapshot tests verifying Redis serialization format for SentimentScore."""

    def test_sentiment_score_json_roundtrip(self) -> None:
        score = SentimentScore(
            article_url="https://example.com/article/btc-rally",
            asset="BTC/USDT",
            score=0.75,
            confidence=0.85,
            sentiment_label="positive",
            model_id="vader:3.3.2",
        )
        data = score.model_dump()
        restored = SentimentScore.model_validate(data)
        assert restored == score

    def test_sentiment_score_none_asset_roundtrip(self) -> None:
        score = SentimentScore(
            article_url="https://example.com/macro-news",
            asset=None,
            score=-0.3,
            confidence=0.6,
            sentiment_label="negative",
            model_id="vader:3.3.2",
        )
        data = score.model_dump()
        restored = SentimentScore.model_validate(data)
        assert restored == score
        assert restored.asset is None

    def test_sentiment_score_redis_serialization(self, snapshot) -> None:
        event = MarketEvent(
            source="nexus-sentiment:vader",
            asset="BTC/USDT",
            timestamp=FIXED_TS,
            event_type=EventType.SENTIMENT_SCORE,
            schema_version="1.0.0",
            payload={
                "article_url": "https://example.com/article/btc-rally",
                "asset": "BTC/USDT",
                "score": 0.75,
                "confidence": 0.85,
                "sentiment_label": "positive",
                "model_id": "vader:3.3.2",
            },
        )
        fields = event.to_redis_fields()
        fields["payload"] = json.dumps(json.loads(fields["payload"]), sort_keys=True)
        assert fields == snapshot

    def test_sentiment_score_redis_roundtrip(self) -> None:
        event = MarketEvent(
            source="nexus-sentiment:vader",
            asset="BTC/USDT",
            timestamp=FIXED_TS,
            event_type=EventType.SENTIMENT_SCORE,
            schema_version="1.0.0",
            payload={
                "article_url": "https://example.com/article/btc-rally",
                "asset": "BTC/USDT",
                "score": 0.75,
                "confidence": 0.85,
                "sentiment_label": "positive",
                "model_id": "vader:3.3.2",
            },
        )
        fields = event.to_redis_fields()
        restored = MarketEvent.from_redis_fields(fields)
        assert restored.event_type == EventType.SENTIMENT_SCORE
        assert restored.source == "nexus-sentiment:vader"
        payload = restored.validated_payload()
        assert isinstance(payload, SentimentScore)
        assert payload.score == 0.75
        assert payload.asset == "BTC/USDT"

    def test_sentiment_score_none_asset_redis_roundtrip(self) -> None:
        event = MarketEvent(
            source="nexus-sentiment:vader",
            asset=None,
            timestamp=FIXED_TS,
            event_type=EventType.SENTIMENT_SCORE,
            schema_version="1.0.0",
            payload={
                "article_url": "https://example.com/macro",
                "asset": None,
                "score": -0.1,
                "confidence": 0.5,
                "sentiment_label": "neutral",
                "model_id": "vader:3.3.2",
            },
        )
        fields = event.to_redis_fields()
        restored = MarketEvent.from_redis_fields(fields)
        assert restored.asset is None
        payload = restored.validated_payload()
        assert isinstance(payload, SentimentScore)
        assert payload.asset is None
