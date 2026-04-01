"""News adapter for RSS/NewsAPI polling."""

from __future__ import annotations

import asyncio
import re
from calendar import timegm
from collections import OrderedDict
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import aiohttp
import feedparser
import structlog
from nexus_common.schemas.enums import EventType, Severity
from nexus_common.schemas.health_alert import HealthAlert
from nexus_common.schemas.market_event import MarketEvent

from nexus_ingestion.adapters.base import BaseAdapter
from nexus_ingestion.config import NewsSourceType

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


class NewsAdapter(BaseAdapter):
    """Adapter for news articles from RSS feeds and NewsAPI."""

    def __init__(
        self,
        source_name: str,
        source_url: str,
        source_type: NewsSourceType = NewsSourceType.RSS,
        *,
        poll_interval: int = 300,
        event_callback: Callable[[MarketEvent], Any] | None = None,
        health_callback: Callable[[HealthAlert], Any] | None = None,
    ) -> None:
        super().__init__(
            adapter_id=f"{source_name}:news",
            adapter_type="news",
        )
        self._source_name = source_name
        self._source_url = source_url
        self._source_type = source_type
        self._poll_interval = poll_interval
        self._event_callback = event_callback
        self._health_callback = health_callback
        self._session: aiohttp.ClientSession | None = None
        # LRU deduplication set: prevents re-emitting articles that reappear in the feed across
        # polls. Capped at 10,000 entries (oldest evicted first) so memory stays bounded
        # regardless of how long the service runs. An RSS feed's own retention window is
        # typically 24-72 hours; 10k entries covers years of even a prolific feed, so in
        # practice nothing is evicted while still live in any feed.
        self._seen_urls: OrderedDict[str, None] = OrderedDict()
        self._seen_urls_max = 10_000
        self._running = False
        self._source_was_up: bool = True
        self._logger = structlog.get_logger().bind(adapter_id=self.adapter_id)

    async def connect(self) -> None:
        """Create the aiohttp client session."""
        self._session = aiohttp.ClientSession()
        self._logger.info(
            "news_adapter_connected", source_name=self._source_name, source_url=self._source_url
        )

    async def subscribe(self) -> None:
        """No subscription needed for polling adapter."""

    async def run(self) -> None:
        """Main polling loop."""
        self._running = True
        while self._running:
            try:
                await self._poll()
            except asyncio.CancelledError:
                break
            except Exception:
                self._logger.error("news_poll_error", source_name=self._source_name, exc_info=True)
                self.record_error()
                await self._emit_source_down_alert()

            await asyncio.sleep(self._poll_interval)

    async def stop(self) -> None:
        """Close the HTTP session."""
        self._running = False
        if self._session:
            await self._session.close()
            self._session = None

    async def _poll(self) -> None:
        """Fetch and parse news from the source."""
        if not self._session:
            return

        if self._source_type == NewsSourceType.RSS:
            await self._poll_rss()

    async def _poll_rss(self) -> None:
        """Fetch and parse an RSS feed."""
        assert self._session is not None
        try:
            async with self._session.get(
                self._source_url, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    self._logger.warning(
                        "rss_fetch_failed", status=resp.status, url=self._source_url
                    )
                    self.record_error()
                    await self._emit_source_down_alert()
                    return
                body = await resp.text()
                await self._emit_source_recovered_alert()
        except Exception as e:
            self._logger.warning("rss_fetch_error", source_name=self._source_name, error=str(e))
            self.record_error()
            await self._emit_source_down_alert()
            return

        # Parse RSS in thread to avoid blocking
        feed = await asyncio.to_thread(feedparser.parse, body)

        for entry in feed.entries:
            url = getattr(entry, "link", "")
            if not url or url in self._seen_urls:
                continue

            self._seen_urls[url] = None
            if len(self._seen_urls) > self._seen_urls_max:
                self._seen_urls.popitem(last=False)
            event = self._normalize_entry(entry)
            if event:
                await self._emit_event(event)

    def _normalize_entry(self, entry: Any) -> MarketEvent | None:
        """Normalize a feedparser entry to a MarketEvent."""
        try:
            headline = getattr(entry, "title", "")
            if not headline:
                self.record_malformed()
                return None

            summary = getattr(entry, "summary", "") or ""
            if not summary:
                content_list = getattr(entry, "content", None)
                if content_list:
                    raw = content_list[0].get("value", "") or ""
                    if raw and "<" in raw:
                        raw = _WHITESPACE_RE.sub(" ", _HTML_TAG_RE.sub(" ", raw)).strip()
                    summary = raw
            if len(summary) > 1000:
                summary = summary[:1000]

            url = getattr(entry, "link", "")
            if not url:
                self.record_malformed()
                return None

            # Parse published date
            published_parsed = getattr(entry, "published_parsed", None)
            if published_parsed:
                published_at = datetime.fromtimestamp(timegm(published_parsed), tz=UTC)
            else:
                published_at = datetime.now(UTC)

            return MarketEvent(
                source=self.adapter_id,
                asset=None,
                timestamp=datetime.now(UTC),
                event_type=EventType.NEWS_ARTICLE,
                payload={
                    "headline": headline,
                    "body_summary": summary,
                    "url": url,
                    "source_name": self._source_name,
                    "published_at": published_at.isoformat(),
                    "related_assets": [],
                },
            )
        except Exception:
            self.record_malformed()
            self._logger.debug("normalize_rss_entry_failed", exc_info=True)
            return None

    async def _emit_source_down_alert(self) -> None:
        """Emit NEWS_SOURCE_DOWN once on the up→down transition; silent if already down."""
        if not self._source_was_up:
            return
        self._source_was_up = False
        if not self._health_callback:
            return
        alert = HealthAlert(
            alert_type="NEWS_SOURCE_DOWN",
            adapter_id=self.adapter_id,
            severity=Severity.LOW,
            timestamp=datetime.now(UTC),
            message=f"{self.adapter_id} fetch failed. Retrying next interval.",
        )
        result = self._health_callback(alert)
        if asyncio.iscoroutine(result):
            await result

    async def _emit_source_recovered_alert(self) -> None:
        """Emit NEWS_SOURCE_RECOVERED once on the down→up transition; silent if already up."""
        if self._source_was_up:
            return
        self._source_was_up = True
        if not self._health_callback:
            return
        alert = HealthAlert(
            alert_type="NEWS_SOURCE_RECOVERED",
            adapter_id=self.adapter_id,
            severity=Severity.LOW,
            timestamp=datetime.now(UTC),
            message=f"{self.adapter_id} fetch succeeded after previous failure.",
        )
        result = self._health_callback(alert)
        if asyncio.iscoroutine(result):
            await result
