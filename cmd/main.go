package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/nguyen-anh-hao/market-data-service/internal/client"
	"github.com/nguyen-anh-hao/market-data-service/internal/domain"
	"github.com/nguyen-anh-hao/market-data-service/internal/eventbus"
	"github.com/nguyen-anh-hao/market-data-service/internal/handler"
	"github.com/nguyen-anh-hao/market-data-service/internal/metrics"
	"github.com/nguyen-anh-hao/market-data-service/internal/service"
	"github.com/nguyen-anh-hao/market-data-service/internal/storage"
	"github.com/nguyen-anh-hao/market-data-service/internal/telemetry"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
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

	// Cache mirror (Redis). Nil keeps the service runnable standalone.
	rdb := storage.NewRedis(env("REDIS_ADDR", ""))

	engine := client.NewEngineClient(env("MATCHING_ENGINE_ADDR", "localhost:9001"))
	svc := service.New(ts, rdb)
	h := handler.New(svc, engine)

	// Consume trade events from the gateway via NATS JetStream.
	sub := eventbus.NewSubscriber(env("NATS_ADDR", ""), func(tr domain.Trade) {
		svc.Ingest(context.Background(), tr)
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
	root.Handle("/", telemetry.NewHandler(h, "market-data-service"))

	addr := ":" + env("MARKET_DATA_PORT", "8085")
	slog.Info("market-data-service listening", "addr", addr)
	if err := http.ListenAndServe(addr, root); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
