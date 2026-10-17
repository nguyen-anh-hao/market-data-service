// Package telemetry configures OpenTelemetry tracing with an OTLP gRPC
// exporter so the synchronous call graph between services can be reconstructed
// in Jaeger / the Grafana Node Graph dashboard.
package telemetry

import (
	"context"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Init configures the global tracer provider with an OTLP gRPC exporter and a
// W3C TraceContext propagator. It returns a shutdown function. If endpoint is
// empty, tracing is a no-op and the returned shutdown func is safe to call.
//
// serviceName is stamped into every span's resource as `service.name`, which is
// what Jaeger / the Grafana Node Graph uses to draw service boundaries.
func Init(serviceName, endpoint string) (func(context.Context) error, error) {
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	// Accept either "host:port" or "http://host:port".
	host := endpoint
	if i := strings.Index(host, "://"); i >= 0 {
		host = host[i+3:]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(host),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	if serviceName == "" {
		serviceName = "unknown_service"
	}
	// Merge the default resource (process/sdk/env attributes) with an explicit
	// service.name so the Node Graph can tell services apart even without
	// OTEL_SERVICE_NAME being set.
	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(attribute.String("service.name", serviceName)),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return tp.Shutdown, nil
}

// NewHandler wraps an http.Handler, emitting a server span per request and
// extracting any inbound W3C traceparent context.
func NewHandler(next http.Handler, name string) http.Handler {
	return otelhttp.NewHandler(next, name)
}
