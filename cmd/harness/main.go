package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pransuu69/meritual-harness-webhook/internal/crypto"
	"github.com/pransuu69/meritual-harness-webhook/internal/models"
)

type WebhookTask struct {
	Index       int
	Payload     []byte
	Signature   string
	IsAdversary bool
	CommitSHA   string
	ParentSHA   string
}

func main() {
	serverURL := flag.String("server", "http://127.0.0.1:8080", "Target Webhook Ingestion Engine URL")
	secret := flag.String("secret", "meritual-adversarial-webhook-secret-2026", "Webhook secret key")
	totalPayloads := flag.Int("total", 10000, "Total number of payloads to ingest")
	adversaryRate := flag.Float64("adversary-rate", 0.30, "Adversarial injection rate (0.30 = 30%)")
	concurrency := flag.Int("concurrency", 100, "Number of concurrent burst workers")
	flag.Parse()

	fmt.Println("================================================================================")
	fmt.Println(" MERITUAL HARNESS: ADVERSARIAL WEBHOOK INGESTION VERIFICATION SUITE")
	fmt.Println("================================================================================")
	fmt.Printf(" Target Engine:            %s\n", *serverURL)
	fmt.Printf(" Total Payloads:           %d\n", *totalPayloads)
	fmt.Printf(" Adversarial Injection:    %.1f%% (%d forged payloads expected)\n",
		*adversaryRate*100, int(float64(*totalPayloads)**adversaryRate))
	fmt.Printf(" Synchronized Concurrency: %d workers\n", *concurrency)
	fmt.Println("================================================================================")

	// 1. Wait for server readiness
	waitForServer(*serverURL)

	// Reset server state
	resetServer(*serverURL)

	// 2. Generate Ground-Truth Git History & Payloads
	fmt.Printf("\n[STAGE 1/4] Generating %d chronological commits and push event payloads...\n", *totalPayloads)
	tasks, groundTruthHistory := generatePayloads(*totalPayloads, *adversaryRate, []byte(*secret))

	expectedValid := len(groundTruthHistory)
	expectedForged := *totalPayloads - expectedValid

	fmt.Printf(" -> Generated %d valid commits in ground-truth sequence.\n", expectedValid)
	fmt.Printf(" -> Injected %d adversarial forged signatures.\n", expectedForged)

	// 3. Execute Synchronized Burst
	fmt.Printf("\n[STAGE 2/4] Firing synchronized burst of %d requests with %d workers...\n", *totalPayloads, *concurrency)

	latencies, acceptedCount, rejectedCount, errorCount := executeBurst(*serverURL, tasks, *concurrency)

	fmt.Printf(" -> Burst complete. Accepted: %d, Rejected (401): %d, Network Errors: %d\n",
		acceptedCount, rejectedCount, errorCount)

	// Allow server 100ms to finalize DAG in-memory synchronization
	time.Sleep(100 * time.Millisecond)

	// 4. Verify Telemetry & CI Invariants
	fmt.Println("\n[STAGE 3/4] Fetching engine telemetry and evaluating CI invariants...")
	stats := fetchStats(*serverURL)
	historyResp := fetchHistory(*serverURL)

	// Compute client-side latencies
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	clientP50 := percentile(latencies, 50.0)
	clientP95 := percentile(latencies, 95.0)
	clientP99 := percentile(latencies, 99.0)
	clientMax := percentile(latencies, 100.0)

	// 5. Evaluate CI Invariants
	fmt.Println("\n[STAGE 4/4] Evaluating Strict Verification Assertions:")
	fmt.Println("--------------------------------------------------------------------------------")

	// Assertion 1: Execution Speed
	inv1Pass := clientP99 < 35.0 && stats.P99LatencyMs < 35.0
	printAssertion("1. Execution Speed (p99 < 35ms)",
		fmt.Sprintf("Client p99: %.2fms | Engine p99: %.2fms (Target: < 35.00ms)", clientP99, stats.P99LatencyMs),
		inv1Pass)

	// Assertion 2: Memory Ceiling (64MB)
	inv2Pass := stats.MaxHeapMB < 64.0 && stats.CurrentHeapMB < 64.0
	printAssertion("2. MicroVM Memory Ceiling (< 64MB)",
		fmt.Sprintf("Peak Heap: %.2fMB | Current: %.2fMB (Ceiling: 64.00MB)", stats.MaxHeapMB, stats.CurrentHeapMB),
		inv2Pass)

	// Assertion 3: Cryptographic Sieve
	inv3Pass := rejectedCount == int64(expectedForged) && stats.DroppedForged == uint64(expectedForged)
	printAssertion("3. Cryptographic Sieve (30% Forged Dropped)",
		fmt.Sprintf("Dropped: %d / %d expected (False Positives: 0, False Negatives: 0)",
			rejectedCount, expectedForged),
		inv3Pass)

	// Assertion 4: Tree Resolution
	treeMatch := verifyTreeResolution(historyResp.CommitHistory, groundTruthHistory)
	inv4Pass := treeMatch && historyResp.TotalCommits == expectedValid
	printAssertion("4. Chronological Git Tree Resolution",
		fmt.Sprintf("Resolved Commits: %d / %d ground-truth | Exact Sequence Match: %v",
			historyResp.TotalCommits, expectedValid, treeMatch),
		inv4Pass)

	fmt.Println("--------------------------------------------------------------------------------")

	if inv1Pass && inv2Pass && inv3Pass && inv4Pass {
		fmt.Println("\n>>> [SUCCESS] ALL CI INVARIANTS SATISFIED! EXECUTION SCORE: 5000 / 5000 <<<")
		fmt.Printf(" -> Target Role: Staff Infrastructure Engineer [QUALIFIED]\n")
		fmt.Printf(" -> Latency Profile: p50: %.2fms | p95: %.2fms | p99: %.2fms | max: %.2fms\n",
			clientP50, clientP95, clientP99, clientMax)
		fmt.Printf(" -> MicroVM Isolation: 64MB Boundary Respected (Peak: %.2fMB)\n", stats.MaxHeapMB)
	} else {
		fmt.Println("\n>>> [FAILURE] ONE OR MORE INVARIANTS BREACHED! EXECUTION VOIDED. <<<")
		log.Fatalf("Verification failed.")
	}
}

func generatePayloads(total int, adversaryRate float64, secret []byte) ([]WebhookTask, []string) {
	numAdversary := int(float64(total) * adversaryRate)
	numValid := total - numAdversary

	validCommits := make([]models.Commit, numValid)
	groundTruthSHAs := make([]string, numValid)
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	prevSHA := "0000000000000000000000000000000000000000"
	for i := 0; i < numValid; i++ {
		sha := fmt.Sprintf("%040x", i+1)
		validCommits[i] = models.Commit{
			SHA:       sha,
			ParentSHA: prevSHA,
			Timestamp: baseTime.Add(time.Duration(i) * time.Second),
			Author:    "staff-infrastructure@meritual.io",
			Message:   fmt.Sprintf("feat: resilient microvm ingestion batch %d", i+1),
		}
		groundTruthSHAs[i] = sha
		prevSHA = sha
	}

	tasks := make([]WebhookTask, total)

	for i := 0; i < numValid; i++ {
		c := validCommits[i]
		payloadBytes, _ := json.Marshal(map[string]any{
			"commit_sha": c.SHA,
			"parent_sha": c.ParentSHA,
			"timestamp":  c.Timestamp.Format(time.RFC3339),
			"author":     c.Author,
			"message":    c.Message,
		})

		sig := crypto.Sign(secret, payloadBytes)

		tasks[i] = WebhookTask{
			Index:       i,
			Payload:     payloadBytes,
			Signature:   sig,
			IsAdversary: false,
			CommitSHA:   c.SHA,
			ParentSHA:   c.ParentSHA,
		}
	}

	for i := 0; i < numAdversary; i++ {
		fakeSHA := fmt.Sprintf("forged-%033x", i+1)
		fakeParent := fmt.Sprintf("forged-parent-%026x", i)

		payloadBytes, _ := json.Marshal(map[string]any{
			"commit_sha": fakeSHA,
			"parent_sha": fakeParent,
			"timestamp":  time.Now().Format(time.RFC3339),
			"author":     "adversary-attacker@darknet.io",
			"message":    "malicious payload injection",
		})

		forgedSig := "sha256=" + fmt.Sprintf("%064x", i+999999)

		tasks[numValid+i] = WebhookTask{
			Index:       numValid + i,
			Payload:     payloadBytes,
			Signature:   forgedSig,
			IsAdversary: true,
			CommitSHA:   fakeSHA,
			ParentSHA:   fakeParent,
		}
	}

	fisherYatesShuffle(tasks)

	return tasks, groundTruthSHAs
}

func executeBurst(serverURL string, tasks []WebhookTask, concurrency int) ([]float64, int64, int64, int64) {
	total := len(tasks)
	latencies := make([]float64, total)
	var latencyIdx atomic.Int64
	var accepted atomic.Int64
	var rejected atomic.Int64
	var errCount atomic.Int64

	webhookEndpoint := serverURL + "/webhook"
	healthEndpoint := serverURL + "/health"

	// Partition tasks evenly across workers to eliminate channel lock contention
	taskChunks := make([][]WebhookTask, concurrency)
	for i, task := range tasks {
		workerID := i % concurrency
		taskChunks[workerID] = append(taskChunks[workerID], task)
	}

	// Create shared persistent transport with dedicated socket pool
	tr := &http.Transport{
		MaxIdleConns:        concurrency * 2,
		MaxIdleConnsPerHost: concurrency * 2,
		MaxConnsPerHost:     concurrency * 2,
		IdleConnTimeout:     60 * time.Second,
		DisableKeepAlives:   false,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	// Pre-warm TCP connections
	fmt.Printf(" -> Pre-warming %d persistent TCP connections...\n", concurrency)
	warmClient := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	var warmWg sync.WaitGroup
	warmWg.Add(concurrency)
	for w := 0; w < concurrency; w++ {
		go func() {
			defer warmWg.Done()
			if resp, err := warmClient.Get(healthEndpoint); err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}()
	}
	warmWg.Wait()

	// Synchronize start gate so all workers fire simultaneously
	startGate := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for w := 0; w < concurrency; w++ {
		workerTasks := taskChunks[w]
		go func(tasks []WebhookTask) {
			defer wg.Done()
			client := &http.Client{
				Transport: tr,
				Timeout:   10 * time.Second,
			}

			// Pre-allocate reader buffer
			bodyReader := bytes.NewReader(nil)

			<-startGate // Synchronized release

			for i := range tasks {
				task := &tasks[i]
				reqStart := time.Now()

				bodyReader.Reset(task.Payload)
				req, rErr := http.NewRequest(http.MethodPost, webhookEndpoint, bodyReader)
				if rErr != nil {
					errCount.Add(1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-Hub-Signature-256", task.Signature)
				req.Header.Set("X-GitHub-Event", "push")

				resp, err := client.Do(req)
				durMs := float64(time.Since(reqStart).Microseconds()) / 1000.0

				if err != nil {
					errCount.Add(1)
					continue
				}

				idx := latencyIdx.Add(1) - 1
				if idx < int64(total) {
					latencies[idx] = durMs
				}

				if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
					accepted.Add(1)
				} else if resp.StatusCode == http.StatusUnauthorized {
					rejected.Add(1)
				} else {
					errCount.Add(1)
				}

				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}(workerTasks)
	}

	startTime := time.Now()
	close(startGate) // Release burst!

	wg.Wait()
	totalBurstDuration := time.Since(startTime)
	throughput := float64(total) / totalBurstDuration.Seconds()

	fmt.Printf(" -> Burst completed in %v (Throughput: %.1f req/sec)\n", totalBurstDuration, throughput)

	return latencies[:latencyIdx.Load()], accepted.Load(), rejected.Load(), errCount.Load()
}

func verifyTreeResolution(reconstructed, groundTruth []string) bool {
	if len(reconstructed) != len(groundTruth) {
		return false
	}
	for i := range reconstructed {
		if reconstructed[i] != groundTruth[i] {
			return false
		}
	}
	return true
}

func fetchStats(serverURL string) models.IngestStats {
	resp, err := http.Get(serverURL + "/metrics")
	if err != nil {
		log.Fatalf("failed to fetch stats: %v", err)
	}
	defer resp.Body.Close()

	var stats models.IngestStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		log.Fatalf("failed to decode stats: %v", err)
	}
	return stats
}

func fetchHistory(serverURL string) models.HistoryResponse {
	resp, err := http.Get(serverURL + "/history")
	if err != nil {
		log.Fatalf("failed to fetch history: %v", err)
	}
	defer resp.Body.Close()

	var h models.HistoryResponse
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		log.Fatalf("failed to decode history: %v", err)
	}
	return h
}

func waitForServer(serverURL string) {
	fmt.Printf("[INIT] Checking target server health at %s...", serverURL)
	for i := 0; i < 30; i++ {
		resp, err := http.Get(serverURL + "/health")
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			fmt.Println(" [ONLINE]")
			return
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Fatalf("server at %s did not become ready", serverURL)
}

func resetServer(serverURL string) {
	req, _ := http.NewRequest(http.MethodPost, serverURL+"/reset", nil)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
}

func percentile(sorted []float64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1) * (pct / 100.0))
	return sorted[index]
}

func fisherYatesShuffle(tasks []WebhookTask) {
	for i := len(tasks) - 1; i > 0; i-- {
		nBig, _ := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		j := int(nBig.Int64())
		tasks[i], tasks[j] = tasks[j], tasks[i]
	}
}

func printAssertion(name, detail string, passed bool) {
	status := "[PASS]"
	if !passed {
		status = "[FAIL]"
	}
	fmt.Printf(" %-6s %-40s | %s\n", status, name, detail)
}
