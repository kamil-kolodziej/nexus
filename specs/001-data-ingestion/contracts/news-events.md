# Contract: News Events Stream

**Stream Name**: `nexus:news-events`
**Producer**: `nexus-ingestion` (sole producer)
**Consumers**: `nexus-sentiment`, `nexus-strategies` (optional)
**Schema Version**: `1.0.0`

## Stream Configuration

| Property | Value | Rationale |
|----------|-------|-----------|
| MAXLEN | ~10,000 (approximate) | News arrives far less frequently than market data |
| Trimming | Approximate (`~`) | Consistent with market-events pattern |
| ID Strategy | Auto-generated (`*`) | Redis auto-generates monotonic IDs |

## Message Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | ✓ | `"{source_name}:news"` (e.g., `"newsapi:news"`, `"coindesk-rss:news"`) |
| `asset` | string | | Detected related asset symbol, or empty string if undetermined |
| `timestamp` | string | ✓ | ISO-8601 UTC (article fetch time) |
| `event_type` | string | ✓ | Always `NEWS_ARTICLE` |
| `schema_version` | string | ✓ | Semantic version (e.g., `"1.0.0"`) |
| `payload` | string (JSON) | ✓ | JSON-encoded NewsArticle payload |

## Payload Schema

### NEWS_ARTICLE

```json
{
  "headline": "Bitcoin Surges Past $70K as ETF Inflows Accelerate",
  "body_summary": "Bitcoin prices reached a new all-time high...",
  "url": "https://example.com/article/bitcoin-surges",
  "source_name": "coindesk",
  "published_at": "2026-03-22T10:15:00Z",
  "related_assets": ["BTC/USDT", "ETH/USDT"]
}
```

## Consumer Group Convention

```
XGROUP CREATE nexus:news-events {service-name}-group $ MKSTREAM
```

Primary consumer:
- `nexus-sentiment-group` — processes articles and produces `SentimentScore` events

## Backward Compatibility Rules

Same rules as market-events contract (SBC-002):
- New optional payload fields: backward-compatible
- Field removal or type change: MAJOR version bump
- `related_assets` is best-effort; consumers must handle empty lists
