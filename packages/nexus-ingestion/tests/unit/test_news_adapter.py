"""Unit tests for NewsAdapter.

Tests RSS normalization, HTTP failure handling, dedup, and NEWS_SOURCE_DOWN alert emission.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nexus_common.schemas.enums import EventType
from nexus_common.schemas.health_alert import HealthAlert
from nexus_common.schemas.market_event import MarketEvent
from nexus_ingestion.adapters.news_adapter import NewsAdapter
from nexus_ingestion.config import NewsSourceType


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
        source_type=NewsSourceType.RSS,
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


def _make_rss_session(status: int = 200, body: str = "<rss/>") -> MagicMock:
    """Return a mock aiohttp session whose get() supports `async with`."""
    mock_resp = AsyncMock()
    mock_resp.status = status
    mock_resp.text = AsyncMock(return_value=body)
    mock_session = MagicMock()
    mock_session.get.return_value.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_session.get.return_value.__aexit__ = AsyncMock(return_value=False)
    return mock_session


class TestDedup:
    @pytest.mark.asyncio
    async def test_duplicate_url_not_emitted(self, adapter: NewsAdapter, events: list) -> None:
        """_poll_rss must not emit the same URL twice across two polls."""
        feed = MagicMock()
        feed.entries = [_make_rss_entry(link="https://example.com/1")]
        adapter._session = _make_rss_session()

        with patch("nexus_ingestion.adapters.news_adapter.feedparser.parse", return_value=feed):
            await adapter._poll_rss()
            await adapter._poll_rss()

        assert len(events) == 1  # second poll must not re-emit the same URL

    @pytest.mark.asyncio
    async def test_lru_eviction_allows_reemit_after_cap(
        self, adapter: NewsAdapter, events: list
    ) -> None:
        """Once the cap is exceeded, the oldest URL is evicted and may be re-emitted."""
        adapter._seen_urls_max = 2

        # Fill to cap with two different URLs
        adapter._seen_urls["https://example.com/old1"] = None
        adapter._seen_urls["https://example.com/old2"] = None

        # A third entry triggers eviction of old1 (oldest)
        feed = MagicMock()
        feed.entries = [_make_rss_entry(link="https://example.com/new")]
        adapter._session = _make_rss_session()

        with patch("nexus_ingestion.adapters.news_adapter.feedparser.parse", return_value=feed):
            await adapter._poll_rss()

        assert len(adapter._seen_urls) == 2
        assert "https://example.com/old1" not in adapter._seen_urls
        assert "https://example.com/new" in adapter._seen_urls


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


class TestAlertTransitions:
    @pytest.mark.asyncio
    async def test_source_down_emitted_once_on_first_failure(
        self, adapter: NewsAdapter, alerts: list
    ) -> None:
        """NEWS_SOURCE_DOWN must fire exactly once; repeated failures are silent."""
        mock_session = _make_rss_session(status=503)
        adapter._session = mock_session

        await adapter._poll_rss()
        await adapter._poll_rss()
        await adapter._poll_rss()

        assert len([a for a in alerts if a.alert_type == "NEWS_SOURCE_DOWN"]) == 1

    @pytest.mark.asyncio
    async def test_source_recovered_emitted_after_failure(
        self, adapter: NewsAdapter, alerts: list
    ) -> None:
        """NEWS_SOURCE_RECOVERED must fire exactly once when fetch succeeds after a failure."""
        feed = MagicMock()
        feed.entries = []

        # First poll — success (source starts up, no alert)
        adapter._session = _make_rss_session(status=200)
        with patch("nexus_ingestion.adapters.news_adapter.feedparser.parse", return_value=feed):
            await adapter._poll_rss()
        assert len(alerts) == 0

        # Second poll — failure (DOWN alert emitted once)
        adapter._session = _make_rss_session(status=503)
        await adapter._poll_rss()
        assert len([a for a in alerts if a.alert_type == "NEWS_SOURCE_DOWN"]) == 1

        # Third poll — success (RECOVERED alert emitted)
        adapter._session = _make_rss_session(status=200)
        with patch("nexus_ingestion.adapters.news_adapter.feedparser.parse", return_value=feed):
            await adapter._poll_rss()
        assert len([a for a in alerts if a.alert_type == "NEWS_SOURCE_RECOVERED"]) == 1

    @pytest.mark.asyncio
    async def test_no_alert_on_continued_success(self, adapter: NewsAdapter, alerts: list) -> None:
        """No health alerts must be emitted when the source remains up across polls."""
        feed = MagicMock()
        feed.entries = []
        adapter._session = _make_rss_session(status=200)

        with patch("nexus_ingestion.adapters.news_adapter.feedparser.parse", return_value=feed):
            await adapter._poll_rss()
            await adapter._poll_rss()
            await adapter._poll_rss()

        assert len(alerts) == 0


class TestNewsAdapterLifecycle:
    @pytest.mark.asyncio
    async def test_connect_creates_session(self, adapter: NewsAdapter) -> None:
        await adapter.connect()
        assert adapter._session is not None
        await adapter.stop()
        assert adapter._session is None
