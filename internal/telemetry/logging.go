package telemetry

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
)

// InitLogging configures the process-wide slog default logger with:
//   - a JSON handler writing to stdout (local/dev fallback)
//   - an OTLP gRPC log exporter (logs -> otel-collector -> Loki) when endpoint != ""
//
// Every record carries service_name + environment; request-scoped records also
// carry trace_id/span_id (resolved from the request context). The level is read
// from LOG_LEVEL (debug|info|warn|error, default info) and the environment from
// APP_ENV (default dev).
func InitLogging(serviceName, endpoint string) (func(context.Context) error, error) {
	level := new(slog.LevelVar)
	level.Set(parseLevel(os.Getenv("LOG_LEVEL")))
	environment := envOr(os.Getenv("APP_ENV"), "dev")

	handlers := []slog.Handler{
		newStdoutHandler(serviceName, environment, level),
	}
	shutdown := func(context.Context) error { return nil }
	var setupErr error

	if endpoint != "" {
		exp, err := newOTLPLogExporter(endpoint)
		if err != nil {
			setupErr = err
		} else {
			res, err := resource.Merge(
				resource.Default(),
				resource.NewSchemaless(
					attribute.String("service.name", serviceName),
					attribute.String("deployment.environment", environment),
				),
			)
			if err == nil {
				provider := sdklog.NewLoggerProvider(
					sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)),
					sdklog.WithResource(res),
				)
				handlers = append(handlers, otelslog.NewHandler(serviceName, otelslog.WithLoggerProvider(provider)))
				shutdown = provider.Shutdown
			} else {
				setupErr = err
			}
		}
	}

	slog.SetDefault(slog.New(multiHandler{handlers: handlers}))
	return shutdown, setupErr
}

// newOTLPLogExporter builds an OTLP gRPC log exporter. The endpoint accepts
// either "host:port" or "http(s)://host:port".
func newOTLPLogExporter(endpoint string) (*otlploggrpc.Exporter, error) {
	host := endpoint
	if i := strings.Index(host, "://"); i >= 0 {
		host = host[i+3:]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return otlploggrpc.New(ctx,
		otlploggrpc.WithEndpoint(host),
		otlploggrpc.WithInsecure(),
	)
}

// newStdoutHandler returns a JSON stdout handler that emits the standard fields
// (timestamp, level, message, service_name, environment) plus trace_id/span_id
// when a valid span context is present.
func newStdoutHandler(serviceName, environment string, level slog.Leveler) slog.Handler {
	json := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.TimeKey:
				a.Key = "timestamp"
				a.Value = slog.StringValue(a.Value.Time().UTC().Format(time.RFC3339Nano))
			case slog.MessageKey:
				a.Key = "message"
			}
			return a
		},
	})
	return enrichHandler{next: json, serviceName: serviceName, environment: environment}
}

// enrichHandler stamps every record with service_name, environment and (when the
// context holds a span) trace_id/span_id.
type enrichHandler struct {
	next        slog.Handler
	serviceName string
	environment string
}

func (h enrichHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l)
}

func (h enrichHandler) Handle(ctx context.Context, r slog.Record) error {
	r.AddAttrs(
		slog.String("service_name", h.serviceName),
		slog.String("environment", h.environment),
	)
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.next.Handle(ctx, r)
}

func (h enrichHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return enrichHandler{next: h.next.WithAttrs(attrs), serviceName: h.serviceName, environment: h.environment}
}

func (h enrichHandler) WithGroup(name string) slog.Handler {
	return enrichHandler{next: h.next.WithGroup(name), serviceName: h.serviceName, environment: h.environment}
}

// parseLevel maps a LOG_LEVEL string to a slog.Level, defaulting to info.
func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// envOr returns v, or fallback when v is empty.
func envOr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

// multiHandler fans a log record out to several slog.Handlers.
type multiHandler struct {
	handlers []slog.Handler
}

func (m multiHandler) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range m.handlers {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (m multiHandler) Handle(ctx context.Context, r slog.Record) error {
	var firstErr error
	for _, h := range m.handlers {
		if h.Enabled(ctx, r.Level) {
			if err := h.Handle(ctx, r.Clone()); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (m multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	hs := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		hs[i] = h.WithAttrs(attrs)
	}
	return multiHandler{handlers: hs}
}

func (m multiHandler) WithGroup(name string) slog.Handler {
	hs := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		hs[i] = h.WithGroup(name)
	}
	return multiHandler{handlers: hs}
}
