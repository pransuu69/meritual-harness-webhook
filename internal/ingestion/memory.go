package ingestion

import (
	"log"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// MicroVMGuard manages memory allocations and GC pacing to ensure strict adherence
// to the 64MB microVM heap ceiling.
type MicroVMGuard struct {
	maxAllowedBytes uint64
	stopChan        chan struct{}
	trippedOOMRisk  atomic.Bool
	peakHeapBytes   atomic.Uint64
}

// NewMicroVMGuard configures Go runtime memory pacing for Firecracker microVM environments.
// maxMB is typically 64 for this challenge.
func NewMicroVMGuard(maxMB int) *MicroVMGuard {
	if maxMB <= 0 {
		maxMB = 64
	}

	maxAllowedBytes := uint64(maxMB) * 1024 * 1024
	// Configure Go runtime soft memory limit to 75% of microVM physical ceiling (e.g. 48MB for 64MB VM)
	softLimitBytes := int64(float64(maxAllowedBytes) * 0.75)
	debug.SetMemoryLimit(softLimitBytes)

	// Set more aggressive GC pacing (50% growth threshold instead of default 100%)
	debug.SetGCPercent(50)

	guard := &MicroVMGuard{
		maxAllowedBytes: maxAllowedBytes,
		stopChan:        make(chan struct{}),
	}

	go guard.watchdogLoop()

	return guard
}

func (g *MicroVMGuard) watchdogLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			// Track peak heap allocation
			for {
				curr := g.peakHeapBytes.Load()
				if m.Alloc <= curr {
					break
				}
				if g.peakHeapBytes.CompareAndSwap(curr, m.Alloc) {
					break
				}
			}

			// If heap exceeds 78% of ceiling (50MB in 64MB VM), force synchronous GC
			threshold := uint64(float64(g.maxAllowedBytes) * 0.78)
			if m.Alloc > threshold {
				log.Printf("[MicroVMGuard WARN] High heap allocation detected: %.2f MB / %d MB limit. Invoking emergency GC.",
					float64(m.Alloc)/(1024*1024), g.maxAllowedBytes/(1024*1024))
				runtime.GC()
			}

			// Invariant verification: Did heap ever breach 64MB ceiling?
			if m.Alloc >= g.maxAllowedBytes {
				g.trippedOOMRisk.Store(true)
				log.Printf("[MicroVMGuard FATAL] Memory boundary breached! Alloc: %d bytes >= %d bytes limit",
					m.Alloc, g.maxAllowedBytes)
			}

		case <-g.stopChan:
			return
		}
	}
}

// PeakHeapMB returns the highest observed heap usage in megabytes.
func (g *MicroVMGuard) PeakHeapMB() float64 {
	return float64(g.peakHeapBytes.Load()) / (1024 * 1024)
}

// CurrentHeapMB returns instantaneous heap allocation in megabytes.
func (g *MicroVMGuard) CurrentHeapMB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / (1024 * 1024)
}

// HasBreachedCeiling returns true if memory ever breached the microVM ceiling.
func (g *MicroVMGuard) HasBreachedCeiling() bool {
	return g.trippedOOMRisk.Load()
}

// Close stops the watchdog.
func (g *MicroVMGuard) Close() {
	close(g.stopChan)
}
