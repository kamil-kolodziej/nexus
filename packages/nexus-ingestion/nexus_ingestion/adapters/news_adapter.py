"""News adapter for RSS/NewsAPI polling."""

from __future__ import annotations

import asyncio
import logging
from calendar import timegm
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Callable

import aiohttp
import feedparser

from nexus_common.schemas.enums import EventType, Severity
from nexus_common.schemas.health_alert import HealthAlert
from nexus_common.schemas.market_event import MarketEvent

from nexus_ingestion.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class NewsAdapter(BaseAdapter):
    """Adapter for news articles from RSS feeds and NewsAPI."""

    def __init__(
        self,
        source_name: str,
        source_url: str,
        source_type: str = "rss",
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

    async def connect(self) -> None:
        """Create the aiohttp client session."""
        self._session = aiohttp.ClientSession()
        logger.info("NewsAdapter connected: %s (%s)", self._source_name, self._source_url)

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
                logger.error("News poll error for %s", self._source_name, exc_info=True)
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

        if self._source_type == "rss":
            await self._poll_rss()

    async def _poll_rss(self) -> None:
        """Fetch and parse an RSS feed."""
        try:
            async with self._session.get(self._source_url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    logger.warning("RSS fetch failed: HTTP %d from %s", resp.status, self._source_url)
                    self.record_error()
                    await self._emit_source_down_alert()
                    return
                body = await resp.text()
        except Exception as e:
            logger.warning("RSS fetch error for %s: %s", self._source_name, e)
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
            if len(summary) > 1000:
                summary = summary[:1000]

            url = getattr(entry, "link", "")
            if not url:
                self.record_malformed()
                return None

            # Parse published date
            published_parsed = getattr(entry, "published_parsed", None)
            if published_parsed:
                published_at = datetime.fromtimestamp(timegm(published_parsed), tz=timezone.utc)
            else:
                published_at = datetime.now(timezone.utc)

            return MarketEvent(
                source=self.adapter_id,
                asset=None,
                timestamp=datetime.now(timezone.utc),
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
            logger.debug("Failed to normalize RSS entry", exc_info=True)
            return None

    async def _emit_event(self, event: MarketEvent) -> None:
        """Send event to the registered callback."""
        self.record_event()
        if self._event_callback:
            result = self._event_callback(event)
            if asyncio.iscoroutine(result):
                await result

    async def _emit_source_down_alert(self) -> None:
        """Emit a NEWS_SOURCE_DOWN health alert."""
        if not self._health_callback:
            return

        alert = HealthAlert(
            alert_type="NEWS_SOURCE_DOWN",
            adapter_id=self.adapter_id,
            severity=Severity.LOW,
            timestamp=datetime.now(timezone.utc),
            message=f"{self.adapter_id} fetch failed. Retrying next interval.",
        )
        result = self._health_callback(alert)
        if asyncio.iscoroutine(result):
            await result
