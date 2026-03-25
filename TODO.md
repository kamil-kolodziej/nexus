# TODO

## nexus-ingestion

- [ ] Dynamic `subscribed_assets` — currently read once at startup and baked into the adapter. Explore options: config file watching, Redis-backed asset list, or a web UI/API to add/remove assets at runtime without restarting the service.
- [ ] Adapter plugin/registry pattern — `main.py` wiring is hardcoded per adapter type. Introduce a registry so new adapters (e.g., IB, Alpaca) can be declared in config and auto-discovered without editing `main.py`.
- [ ] Structured multi-adapter config — `IngestionConfig` is a flat model. Support a list of typed adapter configs so multiple adapters of different types can be declared (e.g., crypto + stocks side by side).
- [ ] Interactive Brokers adapter — implement `BaseAdapter` subclass using `ib_insync` / TWS API, normalizing IB market data into the `MarketEvent` envelope.
- [ ] TimescaleDB data lifecycle policy — add retention/compression strategy for `market_events` (e.g., drop old chunks after N days, enable compression for older data, and optionally downsample long-term history).
