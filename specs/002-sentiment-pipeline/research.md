# Research: Sentiment Analysis Pipeline

**Date**: 2026-04-02 | **Branch**: `002-sentiment-pipeline`

## 1. VADER Sentiment Integration

**Decision**: Use `vaderSentiment.vaderSentiment.SentimentIntensityAnalyzer` with compound score mapping. Load once at startup. Call `polarity_scores(text)` per article synchronously in a thread pool executor.

**Rationale**: VADER is a rule-based lexicon approach designed for social media text. It requires no model download, no GPU, and produces results in <1ms per text. The compound score (range [-1.0, +1.0]) directly maps to our `score` field. Confidence is derived from the magnitude of the compound score: `confidence = abs(compound)`. Label thresholds per VADER conventions: compound ≥ 0.05 → positive; ≤ -0.05 → negative; else neutral (spec assumption confirmed). `model_id` format: `"vader:{vaderSentiment.__version__}"`.

**Alternatives Considered**:
- TextBlob — simpler API but less accurate on financial text, no compound score
- NLTK SentimentAnalyzer — requires separate corpus download and more setup
- Custom lexicon — unnecessary engineering when VADER exists and is widely validated

## 2. FinBERT Transformer Integration

**Decision**: Use `ProsusAI/finbert` via Hugging Face `transformers` pipeline (`text-classification`). Load model and tokenizer once at startup. Run inference in thread pool executor using `asyncio.run_in_executor()`. Optional install via `pip install nexus-sentiment[finbert]`.

**Rationale**: FinBERT is specifically fine-tuned on financial text (Reuters TRC2 + Financial PhraseBank). It outputs softmax probabilities over `[positive, negative, neutral]`. Score mapping per spec: `score = probs[positive] - probs[negative]` (range [-1.0, +1.0]). `confidence = max(probs)` (range [0.0, 1.0]). Model weights ~440MB, downloaded and cached on first run via Hugging Face hub. Inference ~50-200ms per text on CPU. `model_id` format: `"finbert:{transformers.__version__}"`.

Text truncation: FinBERT has a 512-token context window. Combined text (`headline + ". " + body_summary`) is truncated at tokenization via `truncation=True`, **passed at `__call__` time** (`self._pipeline(text, truncation=True)`) rather than at pipeline construction — constructor-time `truncation=True` is silently dropped by the transformers text-classification pipeline (HF issue [#25994](https://github.com/huggingface/transformers/issues/25994)). The `body_summary` field is already capped at 1000 chars (NewsArticle model constraint), so most inputs fit within limits.

**Alternatives Considered**:
- DistilBERT-base with fine-tuning — requires collecting training data and training; FinBERT is pre-trained on financial text
- GPT-based sentiment via API — adds external dependency, latency, cost; violates self-contained service principle
- FinBERT via ONNX Runtime — faster inference but adds complexity; save for future optimization if CPU latency is insufficient

## 3. Thread Pool Executor Sizing for NLP Inference (FR-008)

**Decision**: Use default `asyncio` thread pool executor (None — delegates to `concurrent.futures.ThreadPoolExecutor` default). Do not create a custom-sized pool. VADER is fast enough (<1ms) that thread pool overhead dominates. FinBERT holds the GIL during tokenization but releases it during PyTorch inference. One concurrent inference at a time is sufficient for expected throughput (≤100 articles/minute).

**Rationale**: At ≤100 articles/minute (~1.6/second), even FinBERT at 200ms/inference processes well within throughput requirements. A single-thread-at-a-time approach avoids GPU/VRAM contention (if later upgraded to GPU). The default thread pool allows OS scheduling. If throughput needs increase, a dedicated `ThreadPoolExecutor(max_workers=N)` can be introduced without changing the calling code.

**Alternatives Considered**:
- `ProcessPoolExecutor` — avoids GIL but costs process spawn overhead and complicates model sharing (would require loading model in each process)
- Custom thread pool with fixed workers — premature optimization; default pool adequate for target throughput
- In-loop synchronous inference — would block the event loop, violating FR-008

## 4. Redis Stream Consumer Group Pattern

**Decision**: Use `XREADGROUP GROUP nexus-sentiment-group consumer-{hostname} COUNT 1 BLOCK 5000` in a while loop. Process one message at a time. `XACK` only after all fan-out `SentimentScore` events are successfully published (FR-006). Use `XAUTOCLAIM` for pending message sweep with configurable threshold (default 300s, FR-015).

**Rationale**: Single-message processing (COUNT 1) simplifies transactional guarantees — each message's fan-out publishes must all succeed before acknowledgment. `BLOCK 5000` (5 seconds) matches spec edge case (empty stream blocks on XREADGROUP with configurable timeout). `XAUTOCLAIM` (Redis 6.2+) is preferred over `XCLAIM` + `XPENDING` because it atomically finds and claims stale messages in one command.

Consumer group creation: `XGROUP CREATE nexus:news-events nexus-sentiment-group $ MKSTREAM` at service startup. `$` means only new messages (no replay of historical data on first start). If the group already exists, catch and ignore the BUSYGROUP error.

Dead-letter handling: After claiming a message that has exceeded `pending_claim_threshold`, log it as a dead-letter warning, emit a `MODEL_INFERENCE_ERROR` health alert, and acknowledge it. Do not attempt reprocessing — the message already failed on a previous consumer.

**Alternatives Considered**:
- COUNT > 1 batch reads — complicates partial-failure semantics (which messages to XACK if some fan-outs fail?)
- `>` (read new) only, no pending sweep — pending messages would accumulate indefinitely after crashes
- Separate dead-letter stream — over-engineering for initial implementation; logging + health alert sufficient

## 5. Asset Extraction Dictionary Design (FR-017, FR-020)

**Decision**: YAML file at `data/asset_dictionary.yaml` with three sections: `assets` (mapping canonical identifiers to aliases), `sectors` (mapping sector tags to keyword lists), and `regex_patterns` (optional strict word-boundary regex overrides). Loaded once at service startup. Filtered against active asset universe at extraction time.

**Rationale**: YAML is human-readable and version-controllable. Separating assets from sectors allows independent maintenance. Strict word-boundary matching (`\b`) prevents false positives (e.g., "BIT" matching "Bitcoin"). The dictionary is the single source of truth for what tokens can be extracted — nothing is inferred or fuzzy-matched (FR-020).

Dictionary structure:
```yaml
# data/asset_dictionary.yaml
version: "1.0.0"

assets:
  BTC/USDT:
    aliases: ["Bitcoin", "BTC", "bitcoin"]
  ETH/USDT:
    aliases: ["Ethereum", "ETH", "Ether", "ethereum"]
  # ... more assets

sectors:
  "sector:crypto":
    keywords: ["crypto market", "cryptocurrency market", "crypto"]
  "sector:stocks":
    keywords: ["stock market", "equities market", "stocks"]
  # ... more sectors
```

Matching algorithm:
1. Combine `headline + ". " + body_summary` into a single text
2. For each asset in dictionary: check if any alias matches using `\b{alias}\b` (case-insensitive) regex
3. For each sector in dictionary: check if any keyword matches using `\b{keyword}\b` (case-insensitive) regex
4. Filter results against active asset universe (FR-019)
5. Merge with incoming `related_assets`, deduplicate

**Alternatives Considered**:
- Python dict inline in code — not versioned separately, harder to maintain; violates FR-017
- JSON file — less readable than YAML for nested alias lists
- NLP entity extraction (spaCy NER) — over-engineering; dictionary matching is sufficient for known asset names and avoids false positives
- Database-backed dictionary — unnecessary complexity; file is versioned with code, loaded once at startup
- Fuzzy matching (fuzzywuzzy/rapidfuzz) — explicitly out of scope per FR-020 (fail closed on ambiguity)

## 6. Active Asset Universe Filtering (FR-019)

**Decision**: Load the set of currently configured/tradable assets from the `SentimentConfig.active_assets` configuration field, which reads from `config.toml [sentiment] active_assets` or `NEXUS_ACTIVE_ASSETS` env var. The asset extractor receives this set at initialization and filters extraction results against it.

**Rationale**: The active asset universe must be configurable independent of the extraction dictionary. The dictionary may contain entries for future assets. Only assets in `active_assets` produce published scores. This decouples dictionary maintenance from operational configuration.

**Alternatives Considered**:
- Query exchange adapter for tradable pairs at startup — creates dependency on `nexus-ingestion` or exchange; violates service boundary
- Redis key listing — adds Redis dependency for config; pydantic-settings config chain is simpler and consistent with existing patterns
- Filter at publish time instead of extraction time — equivalent result, but filtering early reduces unnecessary NLP inference fan-out

## 7. SentimentScore Payload Model & MarketEvent Integration (SBC-002)

**Decision**: Add `SENTIMENT_SCORE` to `EventType` enum in `nexus-common`. Add `SentimentScore` Pydantic model to `nexus_common.schemas.market_event`. Register in `PAYLOAD_TYPE_MAP`. This is a backward-compatible addition.

**Rationale**: The `MarketEvent` envelope is the standard event wrapper across all Nexus services. Adding `SENTIMENT_SCORE` as a new `EventType` variant and `SentimentScore` as the corresponding payload type follows the existing discriminated-union pattern. Existing consumers that don't handle `SENTIMENT_SCORE` will simply skip unknown event types.

SentimentScore fields:
- `article_url: str` — source article URL (pass-through from NewsArticle)
- `asset: str | None` — trading pair, sector tag, or None (general market)
- `score: float` — sentiment polarity [-1.0, +1.0]
- `confidence: float` — model certainty [0.0, 1.0]
- `sentiment_label: str` — "positive", "negative", or "neutral"
- `model_id: str` — processor identifier (e.g., "vader:3.3.2", "finbert:4.40.0")

**Alternatives Considered**:
- Separate schema module for sentiment — adds fragmentation; the PAYLOAD_TYPE_MAP pattern centralizes all payload types
- Protocol buffer definitions — adds compilation step; Pydantic JSON serialization is the established pattern
- Separate Redis message format (not MarketEvent) — would break the uniform envelope contract that downstream consumers expect

## 8. TimescaleDB Schema for Sentiment Scores (FR-013)

**Decision**: Add a `sentiment_scores` hypertable to `docker/timescaledb/init.sql`. Use the same async batch writer pattern as `nexus-ingestion`'s `TimescaleWriter`, adapted for sentiment-specific columns.

**Rationale**: Sentiment scores have a different columnar structure than market events (asset, score, confidence, label, model_id, article_url). A dedicated hypertable enables efficient time-series queries for backtesting and analytics. The writer follows the same `asyncio.Queue` → `copy_records_to_table` pattern.

Schema:
```sql
CREATE TABLE IF NOT EXISTS sentiment_scores (
    time            TIMESTAMPTZ NOT NULL,
    source          TEXT NOT NULL,
    asset           TEXT,
    article_url     TEXT NOT NULL,
    score           DOUBLE PRECISION NOT NULL,
    confidence      DOUBLE PRECISION NOT NULL,
    sentiment_label TEXT NOT NULL,
    model_id        TEXT NOT NULL,
    schema_version  TEXT NOT NULL DEFAULT '1.0.0'
);

SELECT create_hypertable('sentiment_scores', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_sentiment_scores_asset
    ON sentiment_scores (asset, time DESC);
```

**Alternatives Considered**:
- Store in `market_events` table using JSONB payload — possible but loses queryability; dedicated columns enable efficient WHERE/GROUP BY on score, asset, model_id
- Skip TimescaleDB persistence initially — spec requires it (FR-013); same pattern exists in `nexus-ingestion`

## 9. Publisher Reuse vs. Duplication

**Decision**: Copy `RedisPublisher` and `HealthPublisher` into `nexus-sentiment` rather than extracting to `nexus-common`. Each service maintains its own publisher implementation.

**Rationale**: The publishers are small (~60 lines each) and may diverge between services (e.g., different buffering strategies, monitoring hooks). Extracting to `nexus-common` creates coupling for a ~120-line utility. When/if a third service needs the same pattern, refactor to a shared module at that point. This follows YAGNI.

**Alternatives Considered**:
- Extract to `nexus-common` now — premature; only two services exist
- Import directly from `nexus-ingestion` — creates cross-service import dependency; violates service boundary (SBC-001)

## 10. Health Endpoint Design (FR-010)

**Decision**: FastAPI app with single `GET /health` returning JSON with processor status, model ID, events processed, and error count. Same `HealthEndpoint` wrapper pattern as `nexus-ingestion`. Response time target: <200ms.

**Rationale**: Health probes need one endpoint. FastAPI is already used in `nexus-ingestion` and is a required dependency. The endpoint reads in-memory counters only — no I/O, so <200ms is trivially achieved.

Response format:
```json
{
  "status": "ok",
  "processor": {
    "type": "vader",
    "state": "loaded",
    "model_id": "vader:3.3.2"
  },
  "events_processed": 1234,
  "errors": 5
}
```

**Alternatives Considered**:
- Prometheus metrics endpoint — adds dependency; health probe is sufficient for initial monitoring
- gRPC health check — not needed; HTTP is standard for Docker/Kubernetes health probes
