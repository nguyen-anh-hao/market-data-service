# Market Data Service (`market-data-service`)

> **Time-Series Market Data Service** built with Go — owns the read-side market
> data pipeline: raw trade persistence, OHLCV aggregation and the public trade
> tape.

---

## 1. Primary Responsibility

The Market Data Service decouples market-data reads from the API Gateway. It:

1. **Consumes trade events** from NATS JetStream (`market.trades.executed`)
   published by the API Gateway after each matched order.
2. **Persists raw ticks** into TimescaleDB (append-only `ticks` hypertable) as the
   durable source of truth.
3. **Pre-aggregates OHLCV candles** (`1s/1m/5m/1h/1d`) in memory as trades arrive.
4. **Maintains the public trade tape** (in memory, mirrored to Redis).
5. **Serves market-data endpoints** and forwards `ticker`/`book` reads to the
   matching engine.

---

## 2. Boundaries & I/O

- **Ingress (async):** NATS JetStream subject `market.trades.executed` (stream `TRADES`).
- **Ingress (sync):** `api-gateway` proxies `GET /ticker`, `/book`, `/trades-tape`, `/ohlcv`.
- **Downstream:** `matching-engine` (`/ticker`, `/book`).
- **Data Stores:**
  - **TimescaleDB** — durable raw ticks (`market_data` DB, port `5433`).
  - **Redis** — recent trade tape mirror.

---

## 3. Quick Start

```bash
export NATS_ADDR="localhost:4222"
export REDIS_ADDR="localhost:6379"
export TIMESCALE_DSN="postgres://market_data:market_data@localhost:5433/market_data?sslmode=disable"
export MATCHING_ENGINE_ADDR="localhost:9001"

go run ./cmd/main.go
```

Verify:

```bash
curl http://localhost:8085/health
curl "http://localhost:8085/ohlcv?symbol=AAPL&interval=1m&limit=100"
```

---

## 4. Engineering Roadmap

| Version | Phase | Focus |
| :--- | :--- | :--- |
| **v1.0.0** *(Current)* | **Baseline** | NATS JetStream consumer, in-memory OHLCV + tape, TimescaleDB tick persistence. |
| **v1.1.0** | **WebSocket Streaming** | Serve `orderbook@100ms` + `trades` streams (currently dead code in the gateway). |
| **v1.2.0** | **TimescaleDB Continuous Aggregates** | Replace in-memory candle roll-ups with `continuous_aggregate` views for long history. |
| **v2.0.0** | **Multi-replica Redis read model** | Make Redis the authoritative read model so the service scales horizontally. |
