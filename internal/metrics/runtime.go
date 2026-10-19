package metrics

import (
	"runtime"
	"runtime/debug"
	"time"
)

// Go runtime metrics are intentionally exposed from the same dependency-free
// registry as the business metrics. They give every Go service the same
// goroutine/heap/GC view without pulling in the official Prometheus client.
var (
	goGoroutines = Default.Gauge(
		"go_goroutines", "Number of goroutines that currently exist.")
	goHeapInuse = Default.Gauge(
		"go_memstats_heap_inuse_bytes", "Bytes in in-use heap spans.")
	goHeapAlloc = Default.Gauge(
		"go_memstats_heap_alloc_bytes", "Bytes of allocated heap objects.")
	goNextGC = Default.Gauge(
		"go_memstats_next_gc_bytes", "Heap size target for the next GC cycle.")
	goGCCPUFraction = Default.Gauge(
		"go_memstats_gc_cpu_fraction", "Fraction of CPU time used by the GC.")
	goSysBytes = Default.Gauge(
		"go_memstats_sys_bytes", "Bytes obtained from the OS by the Go runtime.")
	goGCCycles = Default.Counter(
		"go_gc_cycles_total", "Completed GC cycles since process start.")
	goGCPause = Default.Histogram(
		"go_gc_pause_seconds", "Stop-the-world GC pause duration in seconds.",
		[]float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1})
)

func init() {
	go func() {
		var lastNumGC int64
		collect := func() {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			goGoroutines.Set(float64(runtime.NumGoroutine()))
			goHeapInuse.Set(float64(m.HeapInuse))
			goHeapAlloc.Set(float64(m.HeapAlloc))
			goNextGC.Set(float64(m.NextGC))
			goGCCPUFraction.Set(m.GCCPUFraction)
			goSysBytes.Set(float64(m.Sys))

			var stats debug.GCStats
			debug.ReadGCStats(&stats)
			if stats.NumGC > lastNumGC {
				n := int(stats.NumGC - lastNumGC)
				if n > len(stats.Pause) {
					n = len(stats.Pause)
				}
				for i := 0; i < n; i++ {
					goGCPause.Observe(stats.Pause[i].Seconds())
				}
				goGCCycles.Add(float64(stats.NumGC - lastNumGC))
				lastNumGC = stats.NumGC
			}
		}

		collect()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			collect()
		}
	}()
}
