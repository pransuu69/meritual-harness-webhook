package gitdag

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/pransuu69/meritual-harness-webhook/internal/models"
)

func TestLinearHistoryReconstruction(t *testing.T) {
	resolver := NewResolver(1024)
	defer resolver.Close()

	// Generate 100 sequential commits
	total := 100
	commits := make([]models.Commit, total)
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	prevSHA := "0000000000000000000000000000000000000000"
	for i := 0; i < total; i++ {
		sha := fmt.Sprintf("%040x", i+1)
		commits[i] = models.Commit{
			SHA:       sha,
			ParentSHA: prevSHA,
			Timestamp: baseTime.Add(time.Duration(i) * time.Minute),
			Author:    "staff-eng@meritual.io",
			Message:   fmt.Sprintf("commit %d", i+1),
		}
		prevSHA = sha
	}

	// Shuffle commits to simulate out-of-order webhook delivery
	shuffled := make([]models.Commit, total)
	copy(shuffled, commits)
	r := rand.New(rand.NewSource(42))
	r.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	// Ingest out-of-order
	for _, c := range shuffled {
		resolver.IngestAsync(c)
	}

	// Resolve history
	history, root, tip := resolver.ResolveHistory()

	if len(history) != total {
		t.Fatalf("expected %d commits in history, got %d", total, len(history))
	}

	expectedRoot := commits[0].SHA
	expectedTip := commits[total-1].SHA

	if root != expectedRoot {
		t.Errorf("expected root %s, got %s", expectedRoot, root)
	}
	if tip != expectedTip {
		t.Errorf("expected tip %s, got %s", expectedTip, tip)
	}

	// Verify exact chronological sequence
	for i := 0; i < total; i++ {
		if history[i] != commits[i].SHA {
			t.Fatalf("history mismatch at index %d: expected %s, got %s", i, commits[i].SHA, history[i])
		}
	}
}

func Benchmark10KCommitResolution(b *testing.B) {
	const totalCommits = 10000

	// Generate 10,000 commits
	commits := make([]models.Commit, totalCommits)
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	prevSHA := ""
	for i := 0; i < totalCommits; i++ {
		sha := fmt.Sprintf("%040x", i+1)
		commits[i] = models.Commit{
			SHA:       sha,
			ParentSHA: prevSHA,
			Timestamp: baseTime.Add(time.Duration(i) * time.Second),
		}
		prevSHA = sha
	}

	// Shuffle
	r := rand.New(rand.NewSource(1337))
	shuffled := make([]models.Commit, totalCommits)
	copy(shuffled, commits)
	r.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		resolver := NewResolver(totalCommits * 2)

		var m1, m2 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m1)

		// Ingest 10,000 out-of-order commits
		for _, c := range shuffled {
			resolver.IngestAsync(c)
		}

		history, root, tip := resolver.ResolveHistory()

		runtime.ReadMemStats(&m2)
		heapUsedMB := float64(m2.Alloc-m1.Alloc) / (1024 * 1024)

		if len(history) != totalCommits {
			b.Fatalf("expected %d commits, got %d", totalCommits, len(history))
		}
		if root != commits[0].SHA || tip != commits[totalCommits-1].SHA {
			b.Fatalf("root/tip mismatch")
		}

		b.Logf("10k commits heap allocated: %.2f MB", heapUsedMB)
		resolver.Close()
	}
}
