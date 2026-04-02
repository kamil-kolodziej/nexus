# Contract: Sentiment Events Stream

**Stream Name**: `nexus:sentiment-events`
**Producer**: `nexus-sentiment` (sole producer)
**Consumers**: `nexus-strategies` (future), `nexus-backtester` (future)
**Schema Version**: `1.0.0`

## Stream Configuration

| Property | Value | Rationale |
|----------|-------|-----------|
| MAXLEN | ~50,000 (approximate) | Higher than health stream; sentiment scores consumed by strategy processes |
| Trimming | Approximate (`~`) | Avoids O(N) exact counting on every XADD |
| ID Strategy | Auto-generated (`*`) | Redis auto-generates monotonic IDs |

## Message Schema

Each message is a flat key-value map (Redis Stream entry fields), following the standard `MarketEvent` envelope:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | ✓ | `"nexus-sentiment:{processor_type}"` (e.g., `"nexus-sentiment:vader"`, `"nexus-sentiment:finbert"`) |
| `asset` | string | ✓ | ccxt unified symbol (e.g., `"BTC/USDT"`), sector group with `sector:` prefix (e.g., `"sector:crypto"`), or empty string (encodes `None` = general market) |
| `timestamp` | string | ✓ | ISO-8601 UTC (e.g., `"2026-04-02T14:30:00.123Z"`) |
| `event_type` | string | ✓ | Always `"SENTIMENT_SCORE"` |
| `schema_version` | string | ✓ | Semantic version (e.g., `"1.0.0"`) |
| `payload` | string (JSON) | ✓ | JSON-encoded `SentimentScore` payload (see below) |

## Payload Schema: SENTIMENT_SCORE

```json
{
  "article_url": "https://www.coindesk.com/bitcoin-surges-100k",
  "asset": "BTC/USDT",
  "score": 0.85,
  "confidence": 0.92,
  "sentiment_label": "positive",
  "model_id": "vader:3.3.2"
}
```

### Field Definitions

| Field | Type | Required | Constraints | Description |
|-------|------|----------|-------------|-------------|
| `article_url` | string | ✓ | Non-empty | URL of the source news article (pass-through from NewsArticle) |
| `asset` | string \| null | ✓ | ccxt symbol, `sector:` prefix, or null | The asset this score applies to. `null` for general market sentiment. |
| `score` | float | ✓ | [-1.0, +1.0] | Sentiment polarity. Positive = bullish, negative = bearish. |
| `confidence` | float | ✓ | [0.0, 1.0] | Model certainty in the classification. |
| `sentiment_label` | string | ✓ | `"positive"` \| `"negative"` \| `"neutral"` | Discrete classification. |
| `model_id` | string | ✓ | Non-empty, format `"{processor}:{version}"` | Identifies the NLP model that produced this score. |

### Asset Field Convention

Consumers MUST distinguish between three asset field patterns:

| Pattern | Example | Meaning |
|---------|---------|---------|
| ccxt symbol | `"BTC/USDT"` | Sentiment specific to a trading pair |
| `sector:` prefix | `"sector:crypto"` | Sector-wide sentiment |
| `null` (empty string in Redis) | `""` → deserialized as `None` | General market sentiment (no specific asset or sector) |

## Fan-out Semantics

A single `NewsArticle` event may produce **multiple** `SentimentScore` messages:

- If the effective asset list has N entries → N messages, each with a different `asset` value but the same `score`, `confidence`, `sentiment_label`, and `model_id`.
- If the effective asset list is empty → 1 message with `asset=null`.
- Deduplication: the effective asset list is deduplicated before fan-out. No two messages from the same article share the same `asset` value.

## Delivery Guarantees

- **At-least-once**: If the producer crashes after publishing some but not all fan-out messages, the source `NewsArticle` is not acknowledged. On restart, all fan-out messages for that article are re-published. Consumers MUST handle duplicates.
- **Ordering**: Messages within the stream are strictly ordered by Redis Stream ID. Messages from different articles may interleave. No cross-article ordering guarantee.

## Consumer Group Usage

Not defined by `nexus-sentiment` (it is the producer). Downstream consumers (e.g., `nexus-strategies`) will define their own consumer groups on this stream.

## Examples

### Single-asset article (BTC)

```
source: "nexus-sentiment:vader"
asset: "BTC/USDT"
timestamp: "2026-04-02T14:30:00.123456+00:00"
event_type: "SENTIMENT_SCORE"
schema_version: "1.0.0"
payload: '{"article_url":"https://example.com/btc-surges","asset":"BTC/USDT","score":0.85,"confidence":0.85,"sentiment_label":"positive","model_id":"vader:3.3.2"}'
```

### Multi-asset article (BTC + ETH) — produces 2 messages

Message 1:
```
source: "nexus-sentiment:vader"
asset: "BTC/USDT"
timestamp: "2026-04-02T14:30:00.123456+00:00"
event_type: "SENTIMENT_SCORE"
schema_version: "1.0.0"
payload: '{"article_url":"https://example.com/crypto-rally","asset":"BTC/USDT","score":0.72,"confidence":0.72,"sentiment_label":"positive","model_id":"vader:3.3.2"}'
```

Message 2:
```
source: "nexus-sentiment:vader"
asset: "ETH/USDT"
timestamp: "2026-04-02T14:30:00.123456+00:00"
event_type: "SENTIMENT_SCORE"
schema_version: "1.0.0"
payload: '{"article_url":"https://example.com/crypto-rally","asset":"ETH/USDT","score":0.72,"confidence":0.72,"sentiment_label":"positive","model_id":"vader:3.3.2"}'
```

### General market article (no asset)

```
source: "nexus-sentiment:finbert"
asset: ""
timestamp: "2026-04-02T14:30:00.123456+00:00"
event_type: "SENTIMENT_SCORE"
schema_version: "1.0.0"
payload: '{"article_url":"https://example.com/fed-rate-hike","asset":null,"score":-0.65,"confidence":0.89,"sentiment_label":"negative","model_id":"finbert:4.40.0"}'
```

### Sector-level article

```
source: "nexus-sentiment:vader"
asset: "sector:crypto"
timestamp: "2026-04-02T14:30:00.123456+00:00"
event_type: "SENTIMENT_SCORE"
schema_version: "1.0.0"
payload: '{"article_url":"https://example.com/crypto-crash","asset":"sector:crypto","score":-0.91,"confidence":0.91,"sentiment_label":"negative","model_id":"vader:3.3.2"}'
```
