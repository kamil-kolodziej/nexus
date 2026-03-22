# Contract: Health Events Stream

**Stream Name**: `nexus:ingestion-health-events`
**Producer**: `nexus-ingestion`
**Consumers**: `nexus-risk` (primary), `nexus-api` (dashboard)
**Schema Version**: `1.0.0`

## Stream Configuration

| Property | Value | Rationale |
|----------|-------|-----------|
| MAXLEN | ~5,000 (approximate) | Health alerts are infrequent; retain for incident review |
| Trimming | Approximate (`~`) | Consistent with other streams |
| ID Strategy | Auto-generated (`*`) | Redis auto-generates monotonic IDs |

## Message Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `alert_type` | string | ✓ | Alert category (see Alert Types below) |
| `adapter_id` | string | ✓ | Adapter that triggered the alert (e.g., `"binance:exchange"`) |
| `asset` | string | | Affected asset symbol, or empty for adapter-level alerts |
| `severity` | string | ✓ | `HIGH`, `MEDIUM`, or `LOW` |
| `timestamp` | string | ✓ | ISO-8601 UTC |
| `message` | string | ✓ | Human-readable description |

## Alert Types

| `alert_type` | Severity | Description | Example `message` |
|-------------|----------|-------------|-------------------|
| `DATA_GAP` | HIGH | No events for asset within threshold window | `"No BTC/USDT events from binance:exchange for 120s (threshold: 60s)"` |
| `ADAPTER_DOWN` | HIGH | Adapter exhausted max reconnection attempts | `"binance:exchange entered DOWN state after 10 reconnection attempts"` |
| `ADAPTER_RECONNECTING` | MEDIUM | Adapter lost connection, reconnecting | `"binance:exchange WebSocket disconnected, starting reconnection"` |
| `PERSISTENCE_ERROR` | MEDIUM | TimescaleDB write failed, retrying | `"TimescaleDB batch write failed: connection refused. Queue depth: 1500"` |
| `MALFORMED_SPIKE` | LOW | Malformed event rate exceeded threshold | `"binance:exchange malformed event rate 5/min exceeds threshold 2/min"` |
| `NEWS_SOURCE_DOWN` | LOW | News source fetch failed | `"newsapi:news fetch failed: HTTP 503. Retrying next interval."` |
| `ADAPTER_RECOVERED` | LOW | Adapter successfully reconnected | `"binance:exchange reconnected after 15s downtime"` |

## Consumer Group Convention

```
XGROUP CREATE nexus:ingestion-health-events {service-name}-group $ MKSTREAM
```

Primary consumer groups:
- `nexus-risk-group` — Risk Manager evaluates alerts and may halt trading on affected assets
- `nexus-api-group` — Dashboard displays health status

## Risk Manager Contract

The Risk Manager MUST:
1. Consume `DATA_GAP` alerts with severity `HIGH` and evaluate whether to halt trading on the affected asset
2. Consume `ADAPTER_DOWN` alerts and consider halting all assets from that adapter
3. NOT treat `ADAPTER_RECONNECTING` as a halt trigger (brief disconnects are expected)

The ingestion service MUST NOT:
1. Directly invoke Risk Manager methods or APIs
2. Make trading decisions based on health alerts
3. Block event publishing while waiting for alert delivery

## Backward Compatibility Rules

- New `alert_type` values: MINOR version bump; consumers must handle unknown types (log and skip)
- New fields: backward-compatible (consumers ignore unknown fields)
- Removal of `alert_type` values or field changes: MAJOR version bump
