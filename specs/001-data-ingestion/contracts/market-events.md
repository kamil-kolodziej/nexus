# Contract: Market Events Stream

**Stream Name**: `nexus:market-events`
**Producer**: `nexus-ingestion` (sole producer)
**Consumers**: `nexus-strategies`, `nexus-aggregator`, `nexus-backtester`
**Schema Version**: `1.0.0`

## Stream Configuration

| Property | Value | Rationale |
|----------|-------|-----------|
| MAXLEN | ~100,000 (approximate) | Bounds memory; consumers track via consumer groups |
| Trimming | Approximate (`~`) | Avoids O(N) exact counting on every XADD |
| ID Strategy | Auto-generated (`*`) | Redis auto-generates monotonic IDs |

## Message Schema

Each message is a flat key-value map (Redis Stream entry fields):

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | ✓ | `"{exchange_id}:exchange"` (e.g., `"binance:exchange"`) |
| `asset` | string | ✓ | ccxt unified symbol (e.g., `"BTC/USDT"`) |
| `timestamp` | string | ✓ | ISO-8601 UTC (e.g., `"2026-03-22T14:30:00.123Z"`) |
| `event_type` | string | ✓ | One of: `TICK`, `ORDER_BOOK_UPDATE`, `TRADE`, `CANDLE` |
| `schema_version` | string | ✓ | Semantic version (e.g., `"1.0.0"`) |
| `payload` | string (JSON) | ✓ | JSON-encoded payload per event_type |

## Payload Schemas by Event Type

### TICK

```json
{
  "bid": 67234.50,
  "ask": 67235.10,
  "last": 67234.80,
  "volume_24h": 12345.67
}
```

### ORDER_BOOK_UPDATE

```json
{
  "bids": [[67234.50, 1.5], [67234.00, 2.3]],
  "asks": [[67235.10, 0.8], [67235.50, 1.2]],
  "depth": 10
}
```

### TRADE

```json
{
  "trade_id": "123456789",
  "price": 67234.80,
  "amount": 0.15,
  "side": "buy",
  "taker_or_maker": "taker"
}
```

### CANDLE

```json
{
  "open": 67200.00,
  "high": 67300.00,
  "low": 67150.00,
  "close": 67234.80,
  "volume": 456.78,
  "timeframe": "1m"
}
```

## Consumer Group Convention

Consumers create their own consumer groups:

```
XGROUP CREATE nexus:market-events {service-name}-group $ MKSTREAM
```

Example groups:
- `nexus-strategies-group`
- `nexus-aggregator-group`
- `nexus-backtester-group`

Each consumer reads with `XREADGROUP` and acknowledges with `XACK`.

## Backward Compatibility Rules

Per SBC-002:
- **New optional fields** in payload: backward-compatible (consumers ignore unknown fields)
- **Field removal**: MAJOR version bump required; coordinate with all consumers
- **Field type change**: MAJOR version bump required
- **New event_type value**: MINOR version bump; consumers must handle unknown types gracefully (log and skip)
