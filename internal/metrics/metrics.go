// Package metrics exposes a minimal Prometheus-compatible /metrics endpoint
// using the text exposition format (0.0.4) with zero external dependencies.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Default is the process-wide registry used by the services.
var Default = New()

type Registry struct {
	mu         sync.Mutex
	counters   map[string]*Counter
	histograms map[string]*Histogram
	gauges     map[string]*Gauge
}

func New() *Registry {
	return &Registry{
		counters:   map[string]*Counter{},
		histograms: map[string]*Histogram{},
		gauges:     map[string]*Gauge{},
	}
}

type Counter struct {
	name       string
	help       string
	labelNames []string
	mu         sync.Mutex
	values     map[string]float64
}

func (r *Registry) Counter(name, help string, labelNames ...string) *Counter {
	c := &Counter{name: name, help: help, labelNames: labelNames, values: map[string]float64{}}
	r.mu.Lock()
	r.counters[name] = c
	r.mu.Unlock()
	return c
}

func (c *Counter) Inc(labels ...string) {
	c.Add(1, labels...)
}

// Add increments the counter by delta. Useful for bulk updates where the
// upstream system already provides a delta (for example GC cycle counts).
func (c *Counter) Add(delta float64, labels ...string) {
	k := strings.Join(labels, "\x00")
	c.mu.Lock()
	c.values[k] += delta
	c.mu.Unlock()
}

type Histogram struct {
	name       string
	help       string
	labelNames []string
	buckets    []float64
	mu         sync.Mutex
	counts     map[string][]uint64
	sums       map[string]float64
}

func (r *Registry) Histogram(name, help string, buckets []float64, labelNames ...string) *Histogram {
	h := &Histogram{
		name: name, help: help, labelNames: labelNames, buckets: buckets,
		counts: map[string][]uint64{}, sums: map[string]float64{},
	}
	r.mu.Lock()
	r.histograms[name] = h
	r.mu.Unlock()
	return h
}

func (h *Histogram) Observe(v float64, labels ...string) {
	k := strings.Join(labels, "\x00")
	h.mu.Lock()
	if h.counts[k] == nil {
		h.counts[k] = make([]uint64, len(h.buckets)+1)
	}
	for i, b := range h.buckets {
		if v <= b {
			h.counts[k][i]++
		}
	}
	h.counts[k][len(h.buckets)]++ // +Inf (total)
	h.sums[k] += v
	h.mu.Unlock()
}

type Gauge struct {
	name       string
	help       string
	labelNames []string
	mu         sync.Mutex
	values     map[string]float64
}

func (r *Registry) Gauge(name, help string, labelNames ...string) *Gauge {
	g := &Gauge{name: name, help: help, labelNames: labelNames, values: map[string]float64{}}
	r.mu.Lock()
	r.gauges[name] = g
	r.mu.Unlock()
	return g
}

func (g *Gauge) Set(v float64, labels ...string) {
	k := strings.Join(labels, "\x00")
	g.mu.Lock()
	g.values[k] = v
	g.mu.Unlock()
}

// Add adds delta to the gauge. It is used for in-flight counters.
func (g *Gauge) Add(delta float64, labels ...string) {
	k := strings.Join(labels, "\x00")
	g.mu.Lock()
	g.values[k] += delta
	g.mu.Unlock()
}

func (g *Gauge) write(b *strings.Builder) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s gauge\n", g.name, g.help, g.name)
	keys := make([]string, 0, len(g.values))
	for k := range g.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vals := strings.Split(k, "\x00")
		fmt.Fprintf(b, "%s%s %s\n", g.name, labelStr(g.labelNames, vals),
			strconv.FormatFloat(g.values[k], 'g', -1, 64))
	}
}

// Handler serves the text exposition format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var b strings.Builder
		r.writeAll(&b)
		_, _ = w.Write([]byte(b.String()))
	})
}

func (r *Registry) writeAll(b *strings.Builder) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cn := make([]string, 0, len(r.counters))
	for n := range r.counters {
		cn = append(cn, n)
	}
	sort.Strings(cn)
	for _, n := range cn {
		r.counters[n].write(b)
	}

	hn := make([]string, 0, len(r.histograms))
	for n := range r.histograms {
		hn = append(hn, n)
	}
	sort.Strings(hn)
	for _, n := range hn {
		r.histograms[n].write(b)
	}

	gn := make([]string, 0, len(r.gauges))
	for n := range r.gauges {
		gn = append(gn, n)
	}
	sort.Strings(gn)
	for _, n := range gn {
		r.gauges[n].write(b)
	}
}

func (c *Counter) write(b *strings.Builder) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s counter\n", c.name, c.help, c.name)
	keys := make([]string, 0, len(c.values))
	for k := range c.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vals := strings.Split(k, "\x00")
		fmt.Fprintf(b, "%s%s %s\n", c.name, labelStr(c.labelNames, vals),
			strconv.FormatFloat(c.values[k], 'g', -1, 64))
	}
}

func (h *Histogram) write(b *strings.Builder) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
	keys := make([]string, 0, len(h.counts))
	for k := range h.counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vals := strings.Split(k, "\x00")
		total := h.counts[k][len(h.buckets)]
		for i, ub := range h.buckets {
			le := strconv.FormatFloat(ub, 'f', -1, 64)
			fmt.Fprintf(b, "%s_bucket%s %d\n", h.name, labelStrAppend(h.labelNames, vals, "le", le), h.counts[k][i])
		}
		fmt.Fprintf(b, "%s_bucket%s %d\n", h.name, labelStrAppend(h.labelNames, vals, "le", "+Inf"), total)
		fmt.Fprintf(b, "%s_sum%s %s\n", h.name, labelStr(h.labelNames, vals),
			strconv.FormatFloat(h.sums[k], 'g', -1, 64))
		fmt.Fprintf(b, "%s_count%s %d\n", h.name, labelStr(h.labelNames, vals), total)
	}
}

func labelStr(names, values []string) string {
	if len(names) == 0 {
		return ""
	}
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = n + `="` + escape(values[i]) + `"`
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func labelStrAppend(names, values []string, extraName, extraValue string) string {
	n2 := append(append([]string{}, names...), extraName)
	v2 := append(append([]string{}, values...), extraValue)
	return labelStr(n2, v2)
}

func escape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}
