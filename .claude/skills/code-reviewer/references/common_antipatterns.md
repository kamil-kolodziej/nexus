# Common Antipatterns — Nexus

Real antipatterns found or likely in this codebase, with fixes.

---

## 1. Blocking the Event Loop

**Problem**: Synchronous calls in async context freeze all concurrent tasks.

```python
# BAD — blocks all adapters while parsing
feed = feedparser.parse(body)

# GOOD — offload to thread
feed = await asyncio.to_thread(feedparser.parse, body)
```

**Also watch for**: `time.sleep()` (use `asyncio.sleep()`), synchronous HTTP libraries, CPU-heavy computation.

---

## 2. Unbounded Collections

**Problem**: Data structures that grow indefinitely cause memory leaks over long-running services.

```python
# BAD — grows forever
self._seen_urls: set[str] = set()

# GOOD — bounded LRU or periodic pruning
from collections import OrderedDict
class LRUSet:
    def __init__(self, maxsize: int = 10000):
        self._data: OrderedDict = OrderedDict()
        self._maxsize = maxsize
    def add(self, item):
        self._data[item] = None
        self._data.move_to_end(item)
        if len(self._data) > self._maxsize:
            self._data.popitem(last=False)
```

**Also watch for**: `_last_event_times` dicts, `_malformed_counts` lists that only prune on check, deques without `maxlen`.

---

## 3. String Comparison for Numeric Values

**Problem**: JSON `->>'field'` returns text. Comparing text numerically is wrong.

```sql
-- BAD — lexicographic: '2' > '10'
WHERE payload->>'volume_24h' >= '10'

-- GOOD — explicit cast
WHERE (payload->>'volume_24h')::float >= 10
```

---

## 4. Empty String Instead of None

**Problem**: Using `""` for absent values passes model validation but breaks downstream logic.

```python
# BAD — gap detector checks `if event.asset` which is truthy for ""... wait, "" is falsy
# But it's semantically wrong and inconsistent with `asset: str | None`
asset=""

# GOOD
asset=None
```

---

## 5. Dead / Unused Code

**Problem**: Code defined but never called confuses readers and rots.

Examples found:
- `validated_payload()` on `MarketEvent` — use this to get a typed payload object (`Tick`, `Candle`, etc.) in consumers; do not access `event.payload` as a raw dict in business logic

**Fix**: Either wire it in or remove it. Dead code is a lie about what the system does.

---

## 6. Duplicated Patterns Without Shared Base

**Problem**: Same logic copy-pasted across files.

```python
# Both ExchangeAdapter and NewsAdapter have identical _emit_event:
async def _emit_event(self, event):
    self.record_event()
    if self._event_callback:
        result = self._event_callback(event)
        if asyncio.iscoroutine(result):
            await result
```

**Fix**: Move to `BaseAdapter`. Same applies to the `iscoroutine` callback pattern used in 4+ places.

---

## 7. Binding to 0.0.0.0 in Dev

**Problem**: Health endpoint binds all interfaces by default, exposing ports to the network.

```python
# BAD for local dev
host="0.0.0.0"

# GOOD — configurable, default to loopback
host=config.health_host  # default "127.0.0.1", override to "0.0.0.0" for Docker
```

---

## 8. Logging Credentials

**Problem**: Connection URLs often contain passwords.

```python
# BAD
logger.info("Redis connection established: %s", url)
# Output: Redis connection established: redis://:secretpass@localhost:6379

# GOOD — sanitize
from urllib.parse import urlparse, urlunparse
def _sanitize_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.password:
        replaced = parsed._replace(netloc=f"{parsed.hostname}:{parsed.port}")
        return urlunparse(replaced)
    return url
```

---

## 9. Manual Config Field Mapping Drift

**Problem**: TOML→flat field mapping in `IngestionConfig._toml_config_settings_source` is maintained by hand. Adding a config field requires updating the mapper too — easy to forget.

**Fix**: Add a test that compares TOML section keys against `IngestionConfig` field names, failing if a mapping is missing.

---

## 10. No Test Isolation Between Tests

**Problem**: Tests sharing database state cause order-dependent failures.

```python
# BAD — table retains rows from prior test
@pytest.fixture
async def setup_schema(dsn):
    conn = await asyncpg.connect(dsn)
    await conn.execute(SCHEMA_SQL)
    await conn.close()

# GOOD — clean slate
@pytest.fixture
async def setup_schema(dsn):
    conn = await asyncpg.connect(dsn)
    await conn.execute(SCHEMA_SQL)
    await conn.execute("TRUNCATE market_events")
    await conn.close()
```

---

## 11. Missing Pre-commit Catches

**Problem**: Code merged without formatting/linting leads to noisy diffs later when hooks are added.

**Symptoms**:
- Inconsistent quote styles
- Unsorted imports
- Unused imports lingering
- Missing type annotations on public functions

**Fix**: Configure `.pre-commit-config.yaml` with black + isort + flake8. Run `pre-commit run --all-files` once to normalize the existing codebase before enforcing on commits.
