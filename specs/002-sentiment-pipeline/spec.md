# Feature Specification: Sentiment Analysis Pipeline

**Feature Branch**: `002-sentiment-pipeline`
**Created**: 2026-04-01
**Status**: Draft
**Input**: A standalone Python asyncio service that consumes NewsArticle events from the nexus:news-events Redis Stream, runs NLP inference to produce SentimentScore events, and publishes them to nexus:sentiment-events.

## Clarifications

### Session 2026-04-01

- Q: Where should the asset/sector extraction dictionary live? → A: Store it in a dedicated versioned file in the repo, loaded at service startup.
- Q: Should extraction emit only configured/tradable assets or any asset found in the dictionary? → A: Only emit assets and sectors that are currently configured or tradable in Nexus.
- Q: How strict should alias matching be for ambiguous tickers and common words? → A: Match only curated exact aliases and strict word-boundary regexes; ignore ambiguous loose matches.
- Q: If both specific assets and a broader sector are matched, should both be emitted? → A: Yes. Emit both specific assets and sector tags when both are matched.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Sentiment Scores Flowing From News Articles (Priority: P1)

As a strategy developer, I want every news article that enters the system to be automatically scored for sentiment so that sentiment-based strategies receive quantified signals alongside price data without any manual intervention.

**Why this priority**: Without sentiment scores, the entire sentiment strategy vertical is non-functional. This is the core value proposition of the service — transforming unstructured news text into structured, actionable signals that strategies can consume. Delivering this first provides an independently observable pipeline that can be validated end-to-end.

**Independent Test**: Publish a `NewsArticle` event to `nexus:news-events` with a headline like "Bitcoin surges past $100K on institutional demand" and `related_assets: ["BTC/USDT"]`. Observe that a `SentimentScore` event appears in `nexus:sentiment-events` within the latency target, with a positive `score`, valid `confidence`, `sentiment_label` of `"positive"`, and the correct `asset` and `article_url` fields populated.

**Acceptance Scenarios**:

1. **Given** the sentiment service is running with a loaded NLP model, **When** a `NewsArticle` event with `related_assets: ["BTC/USDT"]` appears in `nexus:news-events`, **Then** exactly one `SentimentScore` event with `asset="BTC/USDT"` is published to `nexus:sentiment-events` within 1 second (VADER) or 2 seconds (FinBERT CPU).
2. **Given** a `NewsArticle` event is consumed, **When** the `SentimentScore` is published, **Then** the `score` is in `[-1.0, +1.0]`, `confidence` is in `[0.0, 1.0]`, `sentiment_label` is one of `"positive"`, `"negative"`, or `"neutral"`, and `model_id` is non-empty and matches the configured processor.
3. **Given** the sentiment service is running, **When** a `SentimentScore` event is published, **Then** it is wrapped in a `MarketEvent` envelope with `event_type=SENTIMENT_SCORE` and `source="nexus-sentiment:{processor_type}"`.

---

### User Story 2 — Per-Asset Scoring for Multi-Asset Articles (Priority: P1)

As a strategy developer, I want articles that mention multiple assets to produce one sentiment score per mentioned asset so that each asset's strategy receives its own signal and can react independently.

**Why this priority**: Many financial news articles mention multiple assets simultaneously. Without per-asset fan-out, strategies would either miss signals or receive ambiguous scores that don't map to a specific trading pair. This is essential for correct strategy operation.

**Independent Test**: Publish a `NewsArticle` with `related_assets: []` and headline "Bitcoin and Ethereum rally on ETF approval". Observe that the service extracts `["BTC/USDT", "ETH/USDT"]` from the text and publishes exactly 2 `SentimentScore` events, each tagged with the correct `asset`.

**Acceptance Scenarios**:

1. **Given** a `NewsArticle` arrives with `related_assets: ["BTC/USDT", "ETH/USDT"]`, **When** the service processes it, **Then** exactly 2 `SentimentScore` events are published — one with `asset="BTC/USDT"` and one with `asset="ETH/USDT"`.
2. **Given** a `NewsArticle` arrives with `related_assets: []` and headline mentioning "Bitcoin", **When** the service runs asset extraction, **Then** it detects `BTC/USDT` from the text and publishes a `SentimentScore` with `asset="BTC/USDT"`.
3. **Given** an article with N total assets (from `related_assets` merged with extracted assets), **When** processed, **Then** all N `SentimentScore` events share the same `score`, `confidence`, and `sentiment_label` values from a single `analyze()` call (inference runs once per article, result is reused for fan-out).

---

### User Story 3 — Asset Extraction From Article Text (Priority: P1)

As a strategy developer, I want the sentiment service to automatically detect which assets and sectors an article is about — even when `related_assets` is empty — so that per-asset sentiment scoring works regardless of whether the upstream news adapter tagged the article.

**Why this priority**: The ingestion layer (`nexus-ingestion`) publishes `NewsArticle` events with `related_assets: []` because RSS feeds do not provide structured asset tags. Without asset extraction, every article produces only `asset=None` general market sentiment, making per-asset scoring (User Story 2) and sector tagging (User Story 5) non-functional. This is a prerequisite for useful sentiment signals.

**Independent Test**: Publish a `NewsArticle` with `related_assets: []` and headline "Crypto market surges as Bitcoin breaks $100K and Ethereum hits new highs". Observe that the service extracts `["BTC/USDT", "ETH/USDT", "sector:crypto"]` and publishes 3 `SentimentScore` events with the correct asset tags.

**Acceptance Scenarios**:

1. **Given** a `NewsArticle` arrives with `related_assets: []` and headline containing "Bitcoin", **When** the service runs the asset extraction step, **Then** `BTC/USDT` is detected and included in the effective asset list for fan-out.
2. **Given** a `NewsArticle` arrives with `related_assets: []` and headline containing "crypto market", **When** the service runs asset extraction, **Then** `sector:crypto` is detected and included in the effective asset list.
3. **Given** a `NewsArticle` arrives with `related_assets: ["BTC/USDT"]` and headline also mentioning "Ethereum", **When** the service runs asset extraction, **Then** the extracted `ETH/USDT` is merged with the pre-populated `BTC/USDT`, producing 2 `SentimentScore` events.
4. **Given** a `NewsArticle` with no recognizable asset or sector mentions and empty `related_assets`, **When** the service runs asset extraction, **Then** the effective asset list remains empty and one `SentimentScore` with `asset=None` (general market sentiment) is published.

---

### User Story 4 — General Market Sentiment for Non-Asset Articles (Priority: P2)

As a strategy developer, I want articles with no specific asset mentions (e.g., macro news, regulatory announcements) to still produce a sentiment score tagged as general market sentiment so that broad market strategies and risk monitoring can factor in overall news tone.

**Why this priority**: Not all market-moving news targets a specific asset. Federal reserve announcements, regulatory changes, and macro-economic reports affect the entire market. Strategies and risk monitoring need these signals to adjust overall market exposure.

**Independent Test**: Publish a `NewsArticle` with `related_assets: []` and headline "Federal Reserve signals aggressive rate hikes". Observe that exactly 1 `SentimentScore` event is published with `asset=None`.

**Acceptance Scenarios**:

1. **Given** a `NewsArticle` arrives with `related_assets: []`, **When** the service processes it, **Then** exactly 1 `SentimentScore` event is published with `asset=None` (general market sentiment).
2. **Given** a general market sentiment score is published, **When** the `MarketEvent` envelope is inspected, **Then** `MarketEvent.asset` is also `None`.

---

### User Story 5 — Asset-Group Tagging for Sector-Wide Articles (Priority: P2)

As a strategy developer, I want articles that reference an entire asset class (e.g., "crypto market crashes", "tech stocks rally") to be tagged with a recognizable group identifier so that strategies can distinguish between sentiment for a specific trading pair and sentiment for an entire sector.

**Why this priority**: When a headline says "Crypto market plunges 20%", that affects all crypto assets, not just one. Without a group tag, this would be treated as generic market sentiment and lose the sector-specific signal. Strategies for crypto assets need to know this sentiment applies specifically to their sector.

**Independent Test**: Publish a `NewsArticle` with `related_assets: []` and headline "Crypto market crashes amid regulatory crackdown". Observe that the service extracts `sector:crypto` from the text and publishes a `SentimentScore` event with `asset="sector:crypto"`.

**Acceptance Scenarios**:

1. **Given** a `NewsArticle` arrives with `related_assets: ["sector:crypto"]`, **When** the service processes it, **Then** exactly 1 `SentimentScore` event is published with `asset="sector:crypto"`.
2. **Given** a `NewsArticle` with `related_assets: ["sector:stocks", "AAPL/USD"]`, **When** the service processes it, **Then** 2 `SentimentScore` events are published — one with `asset="sector:stocks"` and one with `asset="AAPL/USD"`.
3. **Given** any `SentimentScore` event, **When** a consumer inspects the `asset` field, **Then** it can distinguish individual assets (ccxt symbol format like `"BTC/USDT"`) from sector groups (prefixed with `"sector:"`) by checking for the `sector:` prefix.

---

### User Story 6 — Resilience to Inference Errors (Priority: P2)

As a platform operator, I want the sentiment service to continue operating when a single article causes a model inference failure so that one bad input does not disrupt the entire sentiment pipeline.

**Why this priority**: In production, malformed text, unexpected character encodings, or transient model issues will occur. The service must degrade gracefully — logging the error, alerting monitoring, and continuing to process subsequent articles.

**Independent Test**: Mock the NLP processor to raise an exception on a specific article (or publish a `NewsArticle` with text containing characters known to trigger an encoding error in the processor). Observe that the service logs the error, emits a `MODEL_INFERENCE_ERROR` health alert, does not acknowledge the source message, and then successfully processes the next valid article.

**Acceptance Scenarios**:

1. **Given** a `NewsArticle` causes an NLP inference failure, **When** the error is caught, **Then** the source message is NOT acknowledged (remains pending in the consumer group for retry or claim), a `MODEL_INFERENCE_ERROR` health alert is emitted, and the consumer loop continues processing the next message.
2. **Given** repeated inference failures occur, **When** the failure count is inspected, **Then** the error counter increments correctly and the health endpoint reflects the degraded state.
3. **Given** a message has been pending beyond the claim threshold (default 300 seconds), **When** the claim sweep runs, **Then** the message is claimed, logged as a dead-letter, and acknowledged with a health alert.

---

### User Story 7 — Pluggable NLP Model Selection (Priority: P3)

As a platform operator, I want to choose between a lightweight rule-based model (VADER) and a transformer-based model (FinBERT) via configuration so that I can trade off between inference speed and accuracy depending on available hardware and accuracy requirements.

**Why this priority**: Different deployment environments have different constraints. Development and testing benefit from VADER's zero-dependency speed, while production with GPU access benefits from FinBERT's superior financial text understanding. Making this configurable avoids code changes for deployment variations.

**Independent Test**: Start the service with `processor_type="vader"` and verify `model_id` in output events contains `"vader"`. Restart with `processor_type="finbert"` and verify `model_id` contains `"finbert"`. Both produce valid `SentimentScore` events from the same input article.

**Acceptance Scenarios**:

1. **Given** the service is configured with `processor_type="vader"`, **When** an article is processed, **Then** the `SentimentScore.model_id` identifies the VADER model and version.
2. **Given** the service is configured with `processor_type="finbert"`, **When** an article is processed, **Then** the `SentimentScore.model_id` identifies the FinBERT model and version.
3. **Given** the FinBERT model dependencies are not installed, **When** the service starts with `processor_type="finbert"`, **Then** the service exits immediately with a clear error message indicating the missing optional dependency.

---

### Edge Cases

- What happens when a `NewsArticle` has an empty `headline` and empty `body_summary`?
  → The service logs a warning with the article URL, publishes a `SentimentScore` with `score=0.0`, `confidence=0.0`, and `sentiment_label="neutral"` (a zero-signal rather than an error), and acknowledges the message.
- What happens when `related_assets` contains duplicate entries (e.g., `["BTC/USDT", "BTC/USDT"]`)?
  → The service deduplicates the list before fan-out; only one `SentimentScore` per unique asset is published.
- What happens when ingestion pre-populates `related_assets` with an asset that the extraction step also detects from the text?
  → The merged effective asset list is deduplicated. If `related_assets: ["BTC/USDT"]` and extraction also finds `BTC/USDT`, only one `SentimentScore` for `BTC/USDT` is published.
- What happens when the article mentions a ticker or asset name that is not in the configured extraction dictionary?
  → Unrecognized mentions are ignored. Only configured asset aliases and sector keywords produce matches.
- What happens when an alias is ambiguous or would require fuzzy/substring matching to infer an asset?
  → The match is ignored. Extraction fails closed and only uses curated exact aliases or strict word-boundary regexes from the versioned dictionary.
- What happens when the dictionary contains an alias for an asset that Nexus is not currently configured to trade?
  → The match is suppressed. Only currently configured or tradable assets and sectors are allowed into the effective asset list.
- What happens when the Redis connection to `nexus:sentiment-events` (output stream) is lost mid-publish during a fan-out of N scores?
  → The source message (`nexus:news-events`) is NOT acknowledged. The partially-published scores may be duplicated when the message is reprocessed after reconnection. Consumers must handle at-least-once delivery. The service emits a `REDIS_DISCONNECT` health alert and attempts reconnection with backoff.
- What happens when the `nexus:news-events` stream is empty and no articles arrive?
  → The consumer blocks on `XREADGROUP` with a configurable timeout (default 5 seconds), then loops. No errors, no health alerts. The health endpoint reports the service as healthy with `events_processed=0`.
- What happens when the NLP model fails to load at startup?
  → The service exits immediately with a non-zero exit code and a clear error message. It does NOT start the consumer loop or health endpoint in a degraded state. This is a non-recoverable failure.
- What happens when the same article is redelivered (e.g., after a crash before XACK)?
  → The service processes it again and publishes duplicate `SentimentScore` events. This is expected behavior under at-least-once delivery semantics. Downstream deduplication is the consumer's responsibility.
- What happens when an article's `url` field is malformed or missing?
  → The `article_url` field in `SentimentScore` is set to whatever value was in the source `NewsArticle`. The service does not validate URL format — it passes through the value as-is. Malformed article payloads that fail schema validation are logged and dropped (not acknowledged).
- What happens when `related_assets` contains a mix of individual assets and sector groups?
  → Each entry produces one `SentimentScore`. For example, `["sector:crypto", "BTC/USDT"]` produces 2 events — one with `asset="sector:crypto"` and one with `asset="BTC/USDT"`.
- What happens when extraction finds both specific assets and a broader sector in the same article?
  → Both are emitted. Sector tags are additive and are not suppressed by the presence of specific asset matches.
- What happens when `related_assets` contains entries that are not valid ccxt symbols or recognized `sector:` tags (e.g., arbitrary strings like `"foo"`)?
  → Entries from `related_assets` that are not present in the `active_assets` configuration list are silently filtered out during the active-universe filtering step (FR-019). If an entry passes the active-universe filter (i.e., it is explicitly listed in `active_assets`), it is used as-is.
- What happens when `related_assets` or the extracted asset list contains an extremely large number of entries?
  → Fan-out is bounded: the effective asset list is capped at a configurable `max_fan_out` (default 50). If the deduplicated list exceeds this cap, the excess entries are dropped (by insertion order) and a warning is logged. This prevents a single article from producing unbounded Redis publishes.
- What happens when the Redis connection drops during the `XACK` call (after all publishes succeed but before acknowledgment)?
  → The message remains pending and will be reprocessed on restart or claimed by the pending sweep. This results in duplicate `SentimentScore` events — expected under at-least-once semantics.
- What happens when the service receives SIGTERM or SIGINT?
  → The service sets a stop flag, waits for the current in-flight message (if any) to finish processing and publishing, then shuts down the health endpoint, flushes the TimescaleDB writer queue, and exits. Messages not yet read are left in the stream for the next startup.
- What happens if Redis or TimescaleDB is unreachable at startup?
  → Redis is required: if the Redis connection fails at startup, the service exits immediately with a non-zero exit code. TimescaleDB is best-effort: if the TimescaleDB connection fails at startup, the service logs a warning and starts without persistence (events are still published to Redis). The TimescaleDB writer retries connection in the background.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The service MUST consume `NewsArticle` events from the `nexus:news-events` Redis Stream using `XREADGROUP` with consumer group `nexus-sentiment-group`. At startup, the service MUST create the consumer group via `XGROUP CREATE ... $ MKSTREAM`, ignoring `BUSYGROUP` errors if the group already exists. If the stream does not exist, `MKSTREAM` creates it. Messages are processed sequentially (one at a time per consumer instance).
- **FR-002**: The service MUST run NLP inference on each article's combined text to produce a sentiment classification. The combined text is `headline + ". " + body_summary` when `body_summary` is non-empty, or just `headline` when `body_summary` is empty (no trailing period or space).
- **FR-003**: For each article with a non-empty effective asset list (see FR-018), the service MUST publish one `SentimentScore` event per unique asset to `nexus:sentiment-events`.
- **FR-004**: For each article with an empty effective asset list, the service MUST publish exactly one `SentimentScore` event with `asset=None` (general market sentiment) to `nexus:sentiment-events`.
- **FR-005**: All `SentimentScore` events MUST be wrapped in a `MarketEvent` envelope with `event_type=SENTIMENT_SCORE` and `source="nexus-sentiment:{processor_type}"` (e.g., `"nexus-sentiment:vader"`, `"nexus-sentiment:finbert"`).
- **FR-006**: The source message on `nexus:news-events` MUST be acknowledged (`XACK`) only after all corresponding `SentimentScore` events for that article are successfully published. If any publish fails, the message MUST remain pending.
- **FR-007**: The service MUST support two pluggable NLP processors, selectable via configuration:
  - `vader` — rule-based sentiment analysis (`vaderSentiment` library), lightweight, no GPU required. This is the default processor.
  - `finbert` — transformer-based financial sentiment analysis (`ProsusAI/finbert` via `transformers` + `torch`), higher accuracy, optional install.
- **FR-008**: NLP inference MUST run in a thread pool (`asyncio.run_in_executor`) to avoid blocking the asyncio event loop.
- **FR-009**: The consumer loop and health endpoint MUST run as independent `asyncio.create_task` tasks with `add_done_callback` for failure handling. No `asyncio.TaskGroup` — a failure in one MUST NOT cancel the other. This follows the same isolation pattern as `nexus-ingestion` (FR-004 in 001-data-ingestion).
- **FR-010**: The service MUST expose a health endpoint at `GET /health` reporting processor status, model identifier, events processed count, and error count. Response time MUST be under 200 milliseconds.
- **FR-011**: Health alerts MUST be published to `nexus:sentiment-health-events` for: model load failure, inference errors, and Redis disconnects. Health alerts use the same `HealthAlert` schema as `nexus-ingestion`.
- **FR-012**: Configuration MUST follow the same precedence chain as `nexus-ingestion`: explicit kwargs → `NEXUS_*` environment variables → `config.toml [sentiment]` section → defaults. Environment variables take precedence over TOML.
- **FR-013**: The service MUST persist `SentimentScore` events to TimescaleDB asynchronously using the same pattern as the `nexus-ingestion` TimescaleDB writer. Events are published to Redis immediately without waiting for persistence.
- **FR-014**: Malformed or unparseable `NewsArticle` payloads (i.e., messages that fail `MarketEvent` or `NewsArticle` schema validation) MUST be acknowledged and dropped (logged as a warning) without crashing the consumer loop. This is distinct from NLP inference failures on valid payloads, which leave the source message unacknowledged (see SRC-004, User Story 6).
- **FR-015**: Messages pending beyond the configurable `pending_claim_threshold` (default 300 seconds) MUST be claimed via `XAUTOCLAIM`, logged as dead-letters, and acknowledged with a `DEAD_LETTER_CLAIMED` health alert. The claim sweep MUST run periodically at a configurable `claim_sweep_interval` (default 60 seconds).
- **FR-016**: Sector and asset-group references in `related_assets` (entries with the `sector:` prefix, e.g., `"sector:crypto"`, `"sector:stocks"`) MUST be treated identically to individual asset entries for fan-out purposes — one `SentimentScore` per entry. The `sector:` prefix convention allows consumers to distinguish group-level sentiment from individual asset sentiment.
- **FR-017**: Before fan-out, the service MUST run asset extraction against article text (using the same combined text as FR-002) via dictionary/regex matching to detect known assets and sectors. The extraction dictionary MUST live in a dedicated versioned file in the repository and be loaded at service startup. If the dictionary file is missing or malformed at startup, the service MUST exit immediately with a clear error message (non-recoverable, same as model load failure). The dictionary is loaded once at startup and is NOT reloaded at runtime; changes require a service restart.
- **FR-018**: The effective asset list MUST be the deduplicated union of incoming `related_assets` and assets detected by FR-017. After merging, the effective asset list MUST be filtered against `active_assets` (FR-019) — entries not in `active_assets`, whether from extraction or `related_assets`, are silently dropped. If the filtered effective asset list is empty, FR-004 applies.
- **FR-019**: The extraction step MUST emit only assets and sectors that are present in the `active_assets` configuration list (loaded via `config.toml [sentiment] active_assets` or `NEXUS_ACTIVE_ASSETS` env var). Dictionary entries for unsupported instruments MAY exist for future use, but they MUST NOT produce published `SentimentScore` events until the corresponding asset or sector is added to `active_assets`.
- **FR-020**: The extraction step MUST fail closed on ambiguity. It MUST match only curated exact aliases and strict word-boundary regex patterns from the versioned dictionary. Fuzzy matching, loose substring matching, and heuristic expansion of ambiguous tokens are out of scope for the initial implementation.
- **FR-021**: When extraction identifies both specific assets and broader sector tags in the same article, both MUST be included in the effective asset list. Sector tags are additive context and MUST NOT be suppressed solely because one or more specific assets were also matched.

### Safety & Risk Constraints *(mandatory)*

- **SRC-001**: `nexus-sentiment` MUST NOT execute, submit, modify, or cancel any order. It is a read/transform/publish pipeline only. This service MUST NOT make or influence any trading decision.
- **SRC-002**: `nexus-sentiment` MUST NOT directly invoke the Risk Manager, Strategy Engine, or Execution Engine. All output is exclusively via Redis Streams.
- **SRC-003**: `SentimentScore` events MUST NOT contain exchange API credentials, personally identifiable information, or model weight data. The `model_id` field contains only a version string (e.g., `"finbert:1.0.0"`), never a file path or token.
- **SRC-004**: A model inference failure MUST NOT crash the consumer loop. Errors are counted, alerted, and the loop continues with the next message.
- **SRC-005**: The NLP model is loaded once at startup and MUST NOT be changed at runtime. A configuration change requires a service restart. If model loading fails, the service MUST exit immediately (non-recoverable) rather than silently producing no scores.

### Service Boundary & Contract Impact *(mandatory)*

- **SBC-001**: `nexus-sentiment` sits between `nexus-ingestion` (upstream) and `nexus-strategies` (downstream). It consumes from `nexus:news-events` and publishes to `nexus:sentiment-events`. It has no dependency on any other Nexus service except Redis and `nexus-common`.
- **SBC-002**: The `SentimentScore` payload type and `SENTIMENT_SCORE` event type value MUST be added to `nexus-common` (shared schemas and `EventType` enum). This is a backward-compatible addition (new enum value, new payload type).
- **SBC-003**: The `sector:` prefix convention affects downstream consumers in `nexus-strategies` (which must handle `sector:`-prefixed asset fields). `nexus-ingestion` is not required to populate `related_assets`; extraction is owned by `nexus-sentiment` (FR-017).
- **SBC-004**: Health alerts published to `nexus:sentiment-health-events` follow the same `HealthAlert` schema used by `nexus-ingestion`. No schema changes required.

### Key Entities

- **SentimentScore**: The output of NLP inference on a news article. Contains `article_url` (link to the source article), `asset` (specific trading pair, sector group with `sector:` prefix, or `None` for general market), `score` (float in `[-1.0, +1.0]` representing sentiment polarity), `confidence` (float in `[0.0, 1.0]` representing model certainty), `sentiment_label` (`"positive"`, `"negative"`, or `"neutral"`), and `model_id` (identifier of the NLP model that produced the score).
- **SentimentHealth**: Runtime status of the sentiment service exposed via `GET /health`. Contains overall service status, processor type and state, model identifier, events processed count, and error count.
- **BaseSentimentProcessor**: The processor abstraction that both VADER and FinBERT implement. Defines a lifecycle of `load()` → `analyze(text)` → `close()`. The `analyze` method returns a tuple of `(label, score, confidence)`.
- **AssetDictionary**: Versioned file in the repository containing the mapping and regex patterns used to detect asset and sector mentions from article text. Maps aliases and keywords to canonical identifiers such as `BTC/USDT` and `sector:crypto`. Loaded at service startup.
- **ActiveAssetUniverse**: The set of assets and sectors currently enabled in Nexus configuration. Asset extraction may only emit identifiers that are present in this active universe.

## Assumptions

- The `nexus-ingestion` service (001-data-ingestion) is deployed and producing `NewsArticle` events to `nexus:news-events` before this service starts consuming. The service handles an empty stream gracefully (blocks on `XREADGROUP`).
- The `vaderSentiment` library is a required dependency. The `transformers` and `torch` libraries for FinBERT are optional install extras (`pip install nexus-sentiment[finbert]`).
- VADER score mapping: compound score ≥ 0.05 → positive; ≤ -0.05 → negative; else neutral. `score = compound`.
- FinBERT score mapping: softmax over `[negative, neutral, positive]`; `score = probs[positive] - probs[negative]`; `confidence = max(probs)`.
- The `sector:` prefix convention for tagging asset groups is established by extraction logic in `nexus-sentiment`, not ingestion.
- Redis Streams consumer group semantics provide at-least-once delivery. Downstream consumers of `nexus:sentiment-events` must handle potential duplicate scores (same article processed twice after crash recovery).
- TimescaleDB schema for `SentimentScore` events will be added to `docker/timescaledb/init.sql`.
- The NLP model is loaded once at startup into memory. For FinBERT, this means the model weights (~440MB) are downloaded and cached on first run; subsequent starts use the cache.
- Asset extraction uses deterministic dictionary/regex matching as a lightweight pre-processing step before sentiment inference.
- The extraction dictionary is stored in a dedicated versioned file in the repository rather than in `config.toml`, Redis, or inline Python constants.
- Extraction results are filtered against the active Nexus asset universe so unsupported assets do not generate downstream sentiment events.
- Extraction prioritizes precision over recall. Ambiguous aliases are intentionally ignored unless they are explicitly represented by strict dictionary rules.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Within 1 second (VADER) or 2 seconds (FinBERT on CPU) of a `NewsArticle` appearing in `nexus:news-events`, a corresponding `SentimentScore` event appears in `nexus:sentiment-events`.
- **SC-002**: An article with N unique entries in its effective asset list (the deduplicated union of `related_assets` and extracted assets per FR-018) produces exactly N `SentimentScore` events in the output stream. An article with an empty effective asset list produces exactly 1 event with `asset=None`.
- **SC-003**: After a simulated inference error on a single article, the service continues processing subsequent articles without interruption. The pending message count does not grow unboundedly.
- **SC-004**: The `GET /health` endpoint responds within 200 milliseconds and accurately reports the processor state, events processed, and error count.
- **SC-005**: For all valid inputs, `score` is always within `[-1.0, +1.0]` and `confidence` is always within `[0.0, 1.0]`. Verified by property-based tests.
- **SC-006**: No API credentials, model weights, or file paths appear in any event payload, log line, or health alert output.
- **SC-007**: The consumer loop and health endpoint operate independently — a failure in the health endpoint does not stop sentiment processing, and vice versa.
- **SC-008**: Articles with empty `related_assets` but known asset mentions in text produce asset-tagged `SentimentScore` events via extraction, not only `asset=None`.
- **SC-009**: Articles mentioning assets that exist in the extraction dictionary but are not currently enabled in Nexus do not produce published `SentimentScore` events for those unsupported assets.
- **SC-010**: Ambiguous tokens that are not covered by strict dictionary aliases or word-boundary regex rules do not produce extracted assets or sectors.
- **SC-011**: Articles that match both specific assets and a broader sector produce published `SentimentScore` events for both the specific assets and the sector tag.
