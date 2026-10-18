# market-data-service

## Purpose

Owns the market-data read model: the public trade tape, pre-aggregated candles and the WebSocket fan-out for priceboards and charts. It also serves ticker and book reads by forwarding them to the matching engine. It does not match orders, own positions or produce trades.

## Domain knowledge

Executed trades arrive as messages and are deduplicated by trade identifier before anything else happens. A duplicate is dropped, while a new trade is appended to the durable tick store, to a bounded tape and to every supported candle interval. Intervals are one second, one minute, five minutes, one hour and one day, with one minute as the fallback for an unknown interval.

Candle roll-ups are atomic: within a bucket the high and low extend, the close moves and volume accumulates, and crossing a bucket boundary closes the previous candle and starts a new one. The tape keeps two thousand trades and each interval keeps one thousand completed candles, so both are bounded windows rather than full history. Public trade views are anonymized and never expose account identifiers.

The WebSocket hub sends a snapshot on connect and then ticker, candle and trade events. The hub polls the matching engine once per subscribed symbol and folds the last price into a live one-minute candle, which keeps charts moving even before the first trade event arrives. Candle state is merged so authoritative volume and extremes are not lost.

## Decisions and consequences

**Materialized view in Redis.** Tape and candles are precomputed read models rather than query-time aggregations. This gives predictable latency and lets any replica serve any client, at the cost of Redis being mandatory.

**Idempotent consumer with a time-bounded dedupe set.** Trade identifiers are remembered for twenty-four hours, which absorbs at-least-once delivery. The window is the limit: a replay older than that would be ingested again.

**Time-series hypertable for raw ticks.** TimescaleDB is the durable append-only log, which is the right shape for tick data. It is optional for local runs and is currently write-only, so it cannot yet rebuild the Redis read model.

**Pub/Sub fan-out across replicas.** Ingesting replicas publish updates and every replica pushes to its own WebSocket clients. This makes the hub stateless, but Pub/Sub has no replay, so a disconnected client can only resynchronize through a fresh snapshot.

**Lua candle roll-up.** Atomic scripts avoid read-modify-write races between replicas and keep OHLCV consistent. The cost is that candle updates concentrate on a small number of Redis keys.

## Operations and known limits

Redis is required and the service exits without it; TimescaleDB is optional and a failure only disables durable ticks. Message consumption is optional for local UI work, because the ticker poll still produces live one-minute candles. The WebSocket endpoint is public at the edge, so connection count and message rate are the capacity signals to watch.

Losing Redis loses the tape and candle history, and there is no replay path from TimescaleDB today. The dedupe set and bounded lists mean the service is not a historical archive. The ticker poll interval controls upstream load and chart freshness, and the hub depends on the matching engine being reachable for ticker and book reads.

The service listens on port 8085 and exposes health, metrics, tape, candles, ticker, book and the WebSocket route. Raw ticks are written asynchronously, so a tick store outage does not block trade ingestion or tape updates, but it does create a durability gap.

## Future direction

Business: longer history and additional intervals are the likely requests, and both depend on continuous aggregates rather than bounded in-memory windows.

Technical: Timescale continuous aggregates, a backfill or replay tool, and a dedicated streaming gateway for WebSocket scaling. Binary WebSocket payloads are a later optimization.

Boundaries: the WebSocket hub is a candidate for a separate edge service once connection volume outgrows the current read model.
