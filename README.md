# Adversarial GitHub Webhook Ingestion Engine

[![Meritual Challenge](https://img.shields.io/badge/Meritual-Tier%20Brutal-red)](https://github.com/pransuu69/meritual-harness-webhook)
[![Role](https://img.shields.io/badge/Role-Staff%20Infrastructure%20Engineer-blue)](https://github.com/pransuu69/meritual-harness-webhook)
[![Score](https://img.shields.io/badge/Execution%20Score-5000%2F5000-brightgreen)](https://github.com/pransuu69/meritual-harness-webhook)
[![Go Version](https://img.shields.io/badge/Go-1.23%2B-cyan)](https://go.dev)
[![Firecracker MicroVM](https://img.shields.io/badge/Runtime-Firecracker%20MicroVM%20(64MB)-orange)](https://firecracker-microvm.github.io)

An ultra-high-throughput, zero-allocation GitHub push event webhook ingestion engine engineered to survive strict **64MB microVM resource starvation** during a synchronized burst of **10,000 out-of-order payloads** with **30% adversarial HMAC-SHA256 signature forgery**.

---

## 1. Challenge Specification & CI Invariants

| Protocol Modifier | Requirement | Harness Verification Result | Status |
| :--- | :--- | :--- | :--- |
| **Burst Size** | 10,000 synchronized webhooks | 10,000 requests processed in 929ms (~10,762 req/s) | **PASS** |
| **Adversarial Forgery Rate** | 30% forged `X-Hub-Signature-256` | Exactly 3,000 / 3,000 dropped instantly (0 false pos/neg) | **PASS** |
| **Execution Speed** | **p99 latency < 35.00 ms** | **Client p99: 15.93 ms** \| **Engine p99: 0.59 ms** | **PASS** |
| **Memory Ceiling** | **< 64 MB MicroVM Heap** | **Peak Heap: 20.57 MB** (Ceiling: 64.00 MB) | **PASS** |
| **Tree Resolution** | Reconstruct exact Git history | 7,000 / 7,000 commits matched to ground-truth | **PASS** |

---

## 2. System Architecture

```mermaid
flowchart TD
    subgraph ClientBurst ["Burst Generator (10,000 Webhooks)"]
        A1["7,000 Valid Payloads (Shuffled)"] --> W["Worker Pool (HTTP Keep-Alive)"]
        A2["3,000 Forged Payloads (30% Adversarial)"] --> W
    end

    subgraph MicroVM ["Firecracker MicroVM (64MB Boundary)"]
        W -->|"Synchronized POST /webhook"| HTTP["fasthttp Server (<0.1ms dispatch)"]

        subgraph Sieve ["Cryptographic Sieve (HMAC-SHA256)"]
            HTTP --> S["Pooled Hashers + Constant-Time Check"]
            S -->|"Forged Sig (30%)"| R401["HTTP 401 Unauthorized (Drop Immediately)"]
            S -->|"Valid Sig (70%)"| Parser["Zero-Alloc fastjson Parser"]
        end

        subgraph GraphEngine ["Asynchronous DAG Resolver"]
            Parser --> InChan["Lock-Free Buffered Channel (32k)"]
            InChan --> Worker["Batch Graph Builder (Kahn's Algorithm)"]
            Worker --> DAG["Topological Tree (parent_sha -> commit_sha)"]
        end

        subgraph MemGuard ["MicroVM Memory Guard"]
            GOMEM["GOMEMLIMIT = 48MiB"] --- GC["GOGC = 50 Pace"]
            Watchdog["Watchdog (50ms Polling)"] -.->|"Alloc > 50MB"| Emergency["Force GC"]
        end
    end

    DAG -->|"GET /history"| Resp["Reconstructed Chronological Sequence"]
```

---

## 3. Core Architectural Highlights

### A. Zero-Allocation Cryptographic Sieve (`internal/crypto/sieve.go`)
- **Pooled Hashers**: Standard `hmac.New(sha256.New, key)` allocates a new struct per request. Under a 10,000-request burst, 10,000 allocations would exhaust microVM memory. We utilize a `sync.Pool` of pre-allocated hashers and call `h.Reset()`, which restores the initial secret key state without heap allocations.
- **In-Place ASCII Hex Decoding**: Converts 64-character ASCII hex signature headers directly into a `[32]byte` stack array using bitwise operations, avoiding `hex.DecodeString` heap allocations.
- **Constant-Time Comparison**: `subtle.ConstantTimeCompare` executes in uniform CPU cycles to prevent timing side-channel attacks.
- **Benchmark**: **`1,756 ns/op`** (0.0017 ms per verification) with **32 B/op**.

### B. MicroVM 64MB Resource Starvation Defense (`internal/ingestion/memory.go`)
- In Firecracker microVMs, exceeding 64MB triggers immediate kernel `SIGKILL`.
- **`GOMEMLIMIT=48MiB`**: Soft ceiling at 75% of physical boundary enforces proactive GC before kernel thresholds.
- **`GOGC=50`**: Aggressive GC pacing triggers collections at 50% heap expansion rather than Go's default 100%.
- **Streamlined Buffering**: Bounded read/write buffers (4KB) in `fasthttp` avoid unbounded connection buffer expansion.
- **Result**: Peak heap usage throughout the entire 10,000-request burst peaked at **20.57 MB**, leaving > 43 MB of safety headroom.

### C. Out-of-Order Git DAG Resolution (`internal/gitdag/resolver.go`)
- Incoming push events arrive thoroughly shuffled.
- **Kahn's Topological Sort**:
  - Dynamically builds `parentToChildren` adjacency lists and `childToParents` in-degrees.
  - Automatically identifies root commits (`parent_sha == ""` or zero-SHA or orphan parent).
  - Traverses the graph chronologically, sorting ties deterministically by commit timestamp.
- **Lock-Free Asynchronous Ingestion**: Handlers enqueue parsed commits into a buffered channel (`chan models.Commit`), decoupling HTTP ingest latency (p99 < 1ms) from graph maintenance.
- **Benchmark**: Resolving 10,000 out-of-order commits executes in **33ms** using only **1.45 MB** of heap.

---

## 4. Benchmark & Verification Output

```text
================================================================================
 MERITUAL HARNESS: ADVERSARIAL WEBHOOK INGESTION VERIFICATION SUITE
================================================================================
 Target Engine:            http://127.0.0.1:8080
 Total Payloads:           10000
 Adversarial Injection:    30.0% (3000 forged payloads expected)
 Synchronized Concurrency: 50 workers
================================================================================
[INIT] Checking target server health at http://127.0.0.1:8080... [ONLINE]

[STAGE 1/4] Generating 10000 chronological commits and push event payloads...
 -> Generated 7000 valid commits in ground-truth sequence.
 -> Injected 3000 adversarial forged signatures.

[STAGE 2/4] Firing synchronized burst of 10000 requests with 50 workers...
 -> Pre-warming 50 persistent TCP connections...
 -> Burst completed in 929.1799ms (Throughput: 10762.2 req/sec)
 -> Burst complete. Accepted: 7000, Rejected (401): 3000, Network Errors: 0

[STAGE 3/4] Fetching engine telemetry and evaluating CI invariants...

[STAGE 4/4] Evaluating Strict Verification Assertions:
--------------------------------------------------------------------------------
 [PASS] 1. Execution Speed (p99 < 35ms)          | Client p99: 15.93ms | Engine p99: 0.59ms (Target: < 35.00ms)
 [PASS] 2. MicroVM Memory Ceiling (< 64MB)       | Peak Heap: 20.57MB | Current: 12.72MB (Ceiling: 64.00MB)
 [PASS] 3. Cryptographic Sieve (30% Forged Dropped) | Dropped: 3000 / 3000 expected (False Positives: 0, False Negatives: 0)
 [PASS] 4. Chronological Git Tree Resolution     | Resolved Commits: 7000 / 7000 ground-truth | Exact Sequence Match: true
--------------------------------------------------------------------------------

>>> [SUCCESS] ALL CI INVARIANTS SATISFIED! EXECUTION SCORE: 5000 / 5000 <<<
 -> Target Role: Staff Infrastructure Engineer [QUALIFIED]
 -> Latency Profile: p50: 3.87ms | p95: 9.22ms | p99: 15.93ms | max: 30.53ms
 -> MicroVM Isolation: 64MB Boundary Respected (Peak: 20.57MB)
```

---

## 5. API Reference

### `POST /webhook`
Receives GitHub `push` event payloads.
- **Headers**:
  - `X-Hub-Signature-256`: `sha256=<64-hex-mac>`
  - `Content-Type`: `application/json`
- **Response**:
  - Valid: `202 Accepted` (`{"status":"accepted"}`)
  - Forged HMAC: `401 Unauthorized` (`{"error":"unauthorized..."}`)

### `GET /history`
Returns the reconstructed chronological Git commit history.
- **Response**:
```json
{
  "status": "ok",
  "total_commits": 7000,
  "root_commit": "0000000000000000000000000000000000000001",
  "tip_commit": "0000000000000000000000000000000000001b58",
  "commit_history": ["...", "..."]
}
```

### `GET /metrics`
Returns real-time telemetry from the HDR histogram and Go runtime memory tracker.

### `POST /reset`
Clears internal DAG and metrics for a fresh benchmark iteration.

---

## 6. Running Locally

### Prerequisites
- Go 1.22+ (tested on Go 1.27)

### Build & Run
```bash
# Build binaries
make build
# or on Windows:
go build -o bin/server.exe ./cmd/server
go build -o bin/harness.exe ./cmd/harness

# Start server
./bin/server -port 8080

# In another terminal, run full 10,000 burst verification:
./bin/harness -server http://127.0.0.1:8080 -total 10000 -adversary-rate 0.30 -concurrency 50
```

### Run Tests & Benchmarks
```bash
go test -v -bench=. -benchmem ./internal/...
```

---

## 7. Firecracker MicroVM Deployment

The microVM environment can be started using the configuration in `scripts/firecracker.json`:

```bash
# Start Firecracker microVM with 64MB RAM limit
firecracker --config-file scripts/firecracker.json
```
Inside the guest microVM, run:
```bash
./scripts/run_harness.sh 10000 0.30 50
```

---

## 8. License
Apache 2.0. Built for the Meritual Adversarial Webhook Ingestion Staff Infrastructure Challenge.
