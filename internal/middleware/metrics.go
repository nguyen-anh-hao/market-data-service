// Package middleware contains process-level HTTP middleware for market-data-service.
package middleware

import (
	"bufio"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/nguyen-anh-hao/market-data-service/internal/metrics"
)

var (
	httpRequestsTotal = metrics.Default.Counter(
		"http_requests_total", "Total HTTP requests.", "method", "path", "status")
	httpRequestDuration = metrics.Default.Histogram(
		"http_request_duration_seconds", "HTTP request latency in seconds.",
		[]float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}, "method", "path")
)

type recorder struct {
	http.ResponseWriter
	status int
}

func (r *recorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *recorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return r.ResponseWriter.(http.Hijacker).Hijack()
}

// Metrics records RED metrics for the market-data HTTP/WebSocket surface.
func Metrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &recorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		elapsed := time.Since(start)
		httpRequestsTotal.Inc(r.Method, r.URL.Path, strconv.Itoa(rec.status))
		httpRequestDuration.Observe(elapsed.Seconds(), r.Method, r.URL.Path)
	})
}
