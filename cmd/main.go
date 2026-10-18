package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/nguyen-anh-hao/market-data-service/internal/client"
	"github.com/nguyen-anh-hao/market-data-service/internal/domain"
	"github.com/nguyen-anh-hao/market-data-service/internal/eventbus"
	"github.com/nguyen-anh-hao/market-data-service/internal/handler"
	"github.com/nguyen-anh-hao/market-data-service/internal/metrics"
	"github.com/nguyen-anh-hao/market-data-service/internal/middleware"
	"github.com/nguyen-anh-hao/market-data-service/internal/service"
	"github.com/nguyen-anh-hao/market-data-service/internal/storage"
	"github.com/nguyen-anh-hao/market-data-service/internal/stream"
	"github.com/nguyen-anh-hao/market-data-service/internal/telemetry"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func main() {
	ctx := context.Background()

	// Durable raw tick store (TimescaleDB). Nil/error keeps the service runnable.
	ts, err := storage.NewTimescale(ctx, env("TIMESCALE_DSN", ""))
	if err != nil {
		slog.Warn("timescale unavailable, running without durable ticks", "err", err)
		ts = nil
	}
	defer ts.Close()

	// Redis is the authoritative read model. It is required: without it a
	// replica would have to keep tape/candles in process memory.
	rdb := storage.NewRedis(env("REDIS_ADDR", ""))
	if rdb == nil {
		slog.Error("REDIS_ADDR is required for stateless market-data-service operation")
		os.Exit(1)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	pingErr := rdb.Ping(pingCtx).Err()
	cancel()
	if pingErr != nil {
		slog.Error("redis unavailable; market-data-service requires shared state", "err", pingErr)
		os.Exit(1)
	}

	engine := client.NewEngineClient(env("MATCHING_ENGINE_ADDR", "localhost:9001"))
	svc := service.New(ts, rdb)
	hub := stream.NewHub(engine, svc, time.Duration(envInt("WS_TICKER_POLL_MS", 500))*time.Millisecond)
	go hub.Run(ctx)
	h := handler.New(svc, engine, hub)

	// Cross-replica fan-out: the ingesting replica publishes to Redis Pub/Sub and
	// every replica (including this one) pushes the update to its WS clients.
	go func() {
		for u := range svc.SubscribeUpdates(ctx) {
			if u.Trade != nil {
				hub.BroadcastTradeView(*u.Trade)
			}
			if u.Candle != nil {
				hub.BroadcastCandle(u.Symbol, u.Interval, *u.Candle)
			}
		}
	}()

	// Consume trade events from order-service via NATS JetStream.
	sub := eventbus.NewSubscriber(env("NATS_ADDR", ""), func(tr domain.Trade) {
		if !svc.Ingest(context.Background(), tr) {
			return
		}
		tv := service.TradeView{
			TradeID: tr.TradeID, Symbol: tr.Symbol, Price: tr.Price,
			Quantity: tr.Quantity, Timestamp: tr.Timestamp,
		}
		u := service.Update{Symbol: tr.Symbol, Trade: &tv}
		if c, ok := svc.CurrentCandle(context.Background(), tr.Symbol, "1m"); ok {
			u.Interval = "1m"
			u.Candle = &c
		}
		svc.PublishUpdate(context.Background(), u)
	})
	sub.Start()

	// 1. Structured logging (slog -> OTLP gRPC -> Loki, plus JSON stdout).
	logShutdown, err := telemetry.InitLogging(env("OTEL_SERVICE_NAME", "market-data-service"), env("OTEL_EXPORTER_OTLP_ENDPOINT", ""))
	if err != nil {
		slog.Warn("OTLP logging disabled", "err", err)
		logShutdown = func(context.Context) error { return nil }
	}
	defer func() { _ = logShutdown(context.Background()) }()

	// 2. OpenTelemetry tracing (OTLP -> collector -> Jaeger).
	tpShutdown, err := telemetry.Init(env("OTEL_SERVICE_NAME", "market-data-service"), env("OTEL_EXPORTER_OTLP_ENDPOINT", ""))
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
		tpShutdown = func(context.Context) error { return nil }
	}
	defer func() { _ = tpShutdown(context.Background()) }()

	root := http.NewServeMux()
	root.Handle("GET /metrics", metrics.Default.Handler())
	root.Handle("/", telemetry.NewHandler(middleware.Metrics(h), "market-data-service"))

	addr := ":" + env("MARKET_DATA_PORT", "8085")
	slog.Info("market-data-service listening", "addr", addr)
	if err := http.ListenAndServe(addr, root); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
