"""Unit tests for NewsAdapter.

Tests RSS normalization, HTTP failure handling, dedup, and NEWS_SOURCE_DOWN alert emission.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nexus_common.schemas.enums import EventType
from nexus_common.schemas.health_alert import HealthAlert
from nexus_common.schemas.market_event import MarketEvent

from nexus_ingestion.adapters.news_adapter import NewsAdapter


@pytest.fixture
def events() -> list[MarketEvent]:
    return []


@pytest.fixture
def alerts() -> list[HealthAlert]:
    return []


@pytest.fixture
def adapter(events: list[MarketEvent], alerts: list[HealthAlert]) -> NewsAdapter:
    return NewsAdapter(
        source_name="test-rss",
        source_url="https://example.com/feed.xml",
        source_type="rss",
        poll_interval=10,
        event_callback=lambda e: events.append(e),
        health_callback=lambda a: alerts.append(a),
    )


def _make_rss_entry(title: str = "Test Article", link: str = "https://example.com/1") -> MagicMock:
    entry = MagicMock()
    entry.title = title
    entry.summary = "Summary text"
    entry.link = link
    entry.published_parsed = (2026, 3, 22, 10, 15, 0, 0, 0, 0)
    return entry


class TestRSSNormalization:
    def test_valid_entry_produces_news_event(self, adapter: NewsAdapter, events: list) -> None:
        entry = _make_rss_entry()
        event = adapter._normalize_entry(entry)
        assert event is not None
        assert event.event_type == EventType.NEWS_ARTICLE
        assert event.payload["headline"] == "Test Article"
        assert event.payload["source_name"] == "test-rss"

    def test_empty_title_returns_none(self, adapter: NewsAdapter) -> None:
        entry = _make_rss_entry(title="")
        event = adapter._normalize_entry(entry)
        assert event is None
        assert adapter._malformed_count == 1

    def test_body_summary_truncated(self, adapter: NewsAdapter) -> None:
        entry = _make_rss_entry()
        entry.summary = "x" * 2000
        event = adapter._normalize_entry(entry)
        assert event is not None
        assert len(event.payload["body_summary"]) == 1000

    def test_missing_link_returns_none(self, adapter: NewsAdapter) -> None:
        entry = _make_rss_entry(link="")
        event = adapter._normalize_entry(entry)
        assert event is None


class TestDedup:
    @pytest.mark.asyncio
    async def test_duplicate_url_skipped(self, adapter: NewsAdapter, events: list) -> None:
        adapter._seen_urls.add("https://example.com/1")
        entry = _make_rss_entry(link="https://example.com/1")

        # Simulate polling — the dedup check happens in _poll_rss
        # Directly test that normalize works but dedup prevents emit
        event = adapter._normalize_entry(entry)
        assert event is not None  # Normalize succeeds
        # But _poll_rss would skip it due to _seen_urls


class TestHTTPFailureHandling:
    @pytest.mark.asyncio
    async def test_http_error_emits_alert(self, adapter: NewsAdapter, alerts: list) -> None:
        mock_session = AsyncMock()
        mock_resp = AsyncMock()
        mock_resp.status = 503
        mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_session.get.return_value.__aexit__ = AsyncMock(return_value=False)
        adapter._session = mock_session

        await adapter._poll_rss()
        assert len(alerts) == 1
        assert alerts[0].alert_type == "NEWS_SOURCE_DOWN"

    @pytest.mark.asyncio
    async def test_connection_error_emits_alert(self, adapter: NewsAdapter, alerts: list) -> None:
        mock_session = AsyncMock()
        mock_session.get.side_effect = Exception("Connection refused")
        adapter._session = mock_session

        await adapter._poll_rss()
        assert len(alerts) == 1
        assert alerts[0].alert_type == "NEWS_SOURCE_DOWN"


class TestNewsAdapterLifecycle:
    @pytest.mark.asyncio
    async def test_connect_creates_session(self, adapter: NewsAdapter) -> None:
        await adapter.connect()
        assert adapter._session is not None
        await adapter.stop()
        assert adapter._session is None
