package models

import "time"

// Commit represents a normalized Git commit extracted from incoming push webhooks.
type Commit struct {
	SHA       string    `json:"sha"`
	ParentSHA string    `json:"parent_sha"`
	Timestamp time.Time `json:"timestamp"`
	Author    string    `json:"author,omitempty"`
	Message   string    `json:"message,omitempty"`
}

// IngestStats provides real-time telemetry on webhook ingestion.
type IngestStats struct {
	TotalReceived    uint64  `json:"total_received"`
	ValidProcessed   uint64  `json:"valid_processed"`
	DroppedForged    uint64  `json:"dropped_forged"`
	InvalidPayloads  uint64  `json:"invalid_payloads"`
	CurrentHeapMB    float64 `json:"current_heap_mb"`
	MaxHeapMB        float64 `json:"max_heap_mb"`
	P50LatencyMs     float64 `json:"p50_latency_ms"`
	P90LatencyMs     float64 `json:"p90_latency_ms"`
	P95LatencyMs     float64 `json:"p95_latency_ms"`
	P99LatencyMs     float64 `json:"p99_latency_ms"`
	P999LatencyMs    float64 `json:"p999_latency_ms"`
	MaxLatencyMs     float64 `json:"max_latency_ms"`
	TotalCommitsTree int     `json:"total_commits_tree"`
}

// HistoryResponse represents the resolved chronological git history.
type HistoryResponse struct {
	Status        string   `json:"status"`
	TotalCommits  int      `json:"total_commits"`
	RootCommit    string   `json:"root_commit,omitempty"`
	TipCommit     string   `json:"tip_commit,omitempty"`
	CommitHistory []string `json:"commit_history"`
}
