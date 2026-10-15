package metrics

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/pransuu69/meritual-harness-webhook/internal/models"
)

// Collector records ultra-low overhead request latencies and memory statistics.
type Collector struct {
	mu sync.RWMutex

	hist *hdrhistogram.Histogram

	totalReceived   atomic.Uint64
	validProcessed  atomic.Uint64
	droppedForged   atomic.Uint64
	invalidPayloads atomic.Uint64

	maxHeapAllocMB atomic.Uint64 // stored as uint64 bits or scaled
}

// NewCollector initializes an HDR histogram recording latencies from 1µs to 60s.
func NewCollector() *Collector {
	c := &Collector{
		// 1 microsecond to 60,000,000 microseconds (60s), 3 significant figures
		hist: hdrhistogram.New(1, 60000000, 3),
	}
	return c
}

// RecordLatency records a payload processing duration in microseconds.
func (c *Collector) RecordLatency(d time.Duration) {
	micros := d.Microseconds()
	if micros < 1 {
		micros = 1
	}

	c.mu.Lock()
	_ = c.hist.RecordValue(micros)
	c.mu.Unlock()

	c.updateMemoryTracker()
}

// IncReceived increments total incoming requests.
func (c *Collector) IncReceived() {
	c.totalReceived.Add(1)
}

// IncValid increments successfully processed valid webhooks.
func (c *Collector) IncValid() {
	c.validProcessed.Add(1)
}

// IncDropped increments invalid/forged signature payloads dropped by the sieve.
func (c *Collector) IncDropped() {
	c.droppedForged.Add(1)
}

// IncInvalid increments unparseable or bad payloads.
func (c *Collector) IncInvalid() {
	c.invalidPayloads.Add(1)
}

func (c *Collector) updateMemoryTracker() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	allocMB := m.Alloc / (1024 * 1024)

	for {
		currMax := c.maxHeapAllocMB.Load()
		if allocMB <= currMax {
			break
		}
		if c.maxHeapAllocMB.CompareAndSwap(currMax, allocMB) {
			break
		}
	}
}

// Snapshot returns the current aggregated telemetry statistics.
func (c *Collector) Snapshot(totalTreeCommits int) models.IngestStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	currentHeapMB := float64(m.Alloc) / (1024 * 1024)
	maxHeapMB := float64(c.maxHeapAllocMB.Load())
	if currentHeapMB > maxHeapMB {
		maxHeapMB = currentHeapMB
	}

	// HDR histogram values are in microseconds, convert to milliseconds
	toMs := func(micros int64) float64 {
		return float64(micros) / 1000.0
	}

	return models.IngestStats{
		TotalReceived:    c.totalReceived.Load(),
		ValidProcessed:   c.validProcessed.Load(),
		DroppedForged:    c.droppedForged.Load(),
		InvalidPayloads:  c.invalidPayloads.Load(),
		CurrentHeapMB:    currentHeapMB,
		MaxHeapMB:        maxHeapMB,
		P50LatencyMs:     toMs(c.hist.ValueAtQuantile(50.0)),
		P90LatencyMs:     toMs(c.hist.ValueAtQuantile(90.0)),
		P95LatencyMs:     toMs(c.hist.ValueAtQuantile(95.0)),
		P99LatencyMs:     toMs(c.hist.ValueAtQuantile(99.0)),
		P999LatencyMs:    toMs(c.hist.ValueAtQuantile(99.9)),
		MaxLatencyMs:     toMs(c.hist.Max()),
		TotalCommitsTree: totalTreeCommits,
	}
}

// Reset clears recorded statistics for a fresh benchmark run.
func (c *Collector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.hist.Reset()
	c.totalReceived.Store(0)
	c.validProcessed.Store(0)
	c.droppedForged.Store(0)
	c.invalidPayloads.Store(0)
	c.maxHeapAllocMB.Store(0)
}
