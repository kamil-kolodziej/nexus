# Contract: Sentiment Health Events Stream

**Stream Name**: `nexus:sentiment-health-events`
**Producer**: `nexus-sentiment` (sole producer)
**Consumers**: monitoring dashboard (future), alerting (future)
**Schema Version**: `1.0.0`

## Stream Configuration

| Property | Value | Rationale |
|----------|-------|-----------|
| MAXLEN | ~5,000 (approximate) | Matches `nexus:ingestion-health-events` convention |
| Trimming | Approximate (`~`) | Avoids O(N) exact counting on every XADD |
| ID Strategy | Auto-generated (`*`) | Redis auto-generates monotonic IDs |

## Message Schema

Uses the same `HealthAlert` model from `nexus-common` as `nexus-ingestion`:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `alert_type` | string | ✓ | Alert category (see Alert Types below) |
| `adapter_id` | string | ✓ | Always `"nexus-sentiment"` |
| `asset` | string | ✗ | Empty string for service-level alerts; asset symbol for per-asset errors |
| `severity` | string | ✓ | `"HIGH"`, `"MEDIUM"`, or `"LOW"` |
| `timestamp` | string | ✓ | ISO-8601 UTC |
| `message` | string | ✓ | Human-readable description |

## Alert Types

| Alert Type | Severity | Trigger | Description |
|------------|----------|---------|-------------|
| `MODEL_INFERENCE_ERROR` | HIGH | NLP inference fails for a single article | Logged, counted; consumer loop continues. |
| `MODEL_LOAD_FAILURE` | HIGH | NLP model fails to load at startup | Service exits immediately after emitting this alert. Non-recoverable. |
| `REDIS_DISCONNECT` | HIGH | Lost connection to Redis | **Not yet emitted** — `RedisPublisher` detects disconnect (sets `_connected=False`) but does not produce a `HealthAlert`. Tracked in `docs/plans/TODO.md` (cross-package) alongside the buffer-flush/reconnect bug. Would publish on reconnection (the alert itself requires connectivity). |
| `PERSISTENCE_ERROR` | MEDIUM | TimescaleDB batch write fails after retries | Events are still published to Redis; only persistence is affected. |
| `DEAD_LETTER_CLAIMED` | MEDIUM | Pending message exceeded `pending_claim_threshold` | Message acknowledged and logged; was unprocessable. |

## Buffering Behavior

`HealthPublisher` does **not** buffer alerts when Redis is unavailable. Alerts are fire-and-forget — dropped if Redis is disconnected. This avoids circular dependencies (a Redis disconnect alert requiring Redis to publish).

## Examples

### Model inference error

```
alert_type: "MODEL_INFERENCE_ERROR"
adapter_id: "nexus-sentiment"
asset: ""
severity: "HIGH"
timestamp: "2026-04-02T14:30:00.123456+00:00"
message: "NLP inference failed for article https://example.com/article: ValueError('empty text')"
```

### Dead-letter claim

```
alert_type: "DEAD_LETTER_CLAIMED"
adapter_id: "nexus-sentiment"
asset: ""
severity: "MEDIUM"
timestamp: "2026-04-02T14:35:00.000000+00:00"
message: "Claimed pending message 1712345678901-0 after 312s (threshold: 300s)"
```

### Persistence error

```
alert_type: "PERSISTENCE_ERROR"
adapter_id: "nexus-sentiment"
asset: ""
severity: "MEDIUM"
timestamp: "2026-04-02T14:40:00.000000+00:00"
message: "TimescaleDB batch write failed: connection refused. Queue depth: 150"
```
