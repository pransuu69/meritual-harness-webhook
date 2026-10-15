package ingestion

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/pransuu69/meritual-harness-webhook/internal/crypto"
	"github.com/pransuu69/meritual-harness-webhook/internal/gitdag"
	"github.com/pransuu69/meritual-harness-webhook/internal/metrics"
	"github.com/pransuu69/meritual-harness-webhook/internal/models"
	"github.com/valyala/fasthttp"
)

// EngineConfig configures the Webhook Ingestion Engine.
type EngineConfig struct {
	Port           int
	WebhookSecret  []byte
	MaxMemoryMB    int
	MaxLatencyP99Ms int
	Concurrency    int
}

// Engine encapsulates the HTTP server, cryptographic sieve, and Git DAG resolver.
type Engine struct {
	cfg      EngineConfig
	sieve    *crypto.Sieve
	resolver *gitdag.Resolver
	parser   *Parser
	metrics  *metrics.Collector
	guard    *MicroVMGuard
	server   *fasthttp.Server
	ln       net.Listener
}

// NewEngine initializes the high-throughput ingestion engine.
func NewEngine(cfg EngineConfig) *Engine {
	if cfg.Port <= 0 {
		cfg.Port = 8080
	}
	if len(cfg.WebhookSecret) == 0 {
		cfg.WebhookSecret = []byte("meritual-harness-default-secret-key-1337")
	}
	if cfg.MaxMemoryMB <= 0 {
		cfg.MaxMemoryMB = 64
	}
	if cfg.MaxLatencyP99Ms <= 0 {
		cfg.MaxLatencyP99Ms = 35
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10000
	}

	e := &Engine{
		cfg:      cfg,
		sieve:    crypto.NewSieve(cfg.WebhookSecret),
		resolver: gitdag.NewResolver(32768),
		parser:   NewParser(),
		metrics:  metrics.NewCollector(),
		guard:    NewMicroVMGuard(cfg.MaxMemoryMB),
	}

	e.server = &fasthttp.Server{
		Handler:            e.HandleRequest,
		Concurrency:        cfg.Concurrency,
		ReadBufferSize:     4096,
		WriteBufferSize:    4096,
		MaxRequestBodySize: 2 * 1024 * 1024, // 2MB max payload (typical webhook is ~2KB-10KB)
		DisableKeepalive:   false,
		TCPKeepalive:       true,
		TCPKeepalivePeriod: 30 * time.Second,
		Name:               "Meritual-Ingest-Engine/1.0",
	}

	return e
}

// HandleRequest routes incoming HTTP requests with sub-millisecond dispatch.
func (e *Engine) HandleRequest(ctx *fasthttp.RequestCtx) {
	path := string(ctx.Path())
	method := string(ctx.Method())

	switch {
	case method == fasthttp.MethodPost && (path == "/webhook" || path == "/events" || path == "/" || path == "/api/v1/webhook"):
		e.handleWebhook(ctx)

	case method == fasthttp.MethodGet && (path == "/history" || path == "/commits" || path == "/tree"):
		e.handleHistory(ctx)

	case method == fasthttp.MethodGet && (path == "/metrics" || path == "/stats"):
		e.handleMetrics(ctx)

	case method == fasthttp.MethodPost && path == "/reset":
		e.handleReset(ctx)

	case method == fasthttp.MethodGet && (path == "/health" || path == "/healthz"):
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetContentType("application/json")
		ctx.SetBodyString(`{"status":"healthy","engine":"meritual-webhook-ingestion"}`)

	default:
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		ctx.SetContentType("application/json")
		ctx.SetBodyString(`{"error":"not found"}`)
	}
}

func (e *Engine) handleWebhook(ctx *fasthttp.RequestCtx) {
	start := time.Now()
	e.metrics.IncReceived()

	// 1. Extract X-Hub-Signature-256 header
	sig := ctx.Request.Header.Peek("X-Hub-Signature-256")
	if len(sig) == 0 {
		sig = ctx.Request.Header.Peek("x-hub-signature-256")
	}

	body := ctx.PostBody()

	// 2. Cryptographic Sieve: verify HMAC-SHA256 in constant time
	if len(sig) == 0 || !e.sieve.Verify(sig, body) {
		// Drop forged or unsigned payloads instantly
		e.metrics.IncDropped()
		e.metrics.RecordLatency(time.Since(start))

		ctx.SetStatusCode(fasthttp.StatusUnauthorized)
		ctx.SetContentType("application/json")
		ctx.SetBodyString(`{"error":"unauthorized: forged or invalid signature dropped by cryptographic sieve"}`)
		return
	}

	// 3. Extract commit information using zero-allocation fast parser
	commits, err := e.parser.ParseCommits(body)
	if err != nil || len(commits) == 0 {
		e.metrics.IncInvalid()
		e.metrics.RecordLatency(time.Since(start))

		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetContentType("application/json")
		ctx.SetBodyString(`{"error":"malformed push payload"}`)
		return
	}

	// 4. Ingest valid commits into out-of-order DAG resolver
	for _, c := range commits {
		e.resolver.IngestAsync(c)
	}

	e.metrics.IncValid()
	e.metrics.RecordLatency(time.Since(start))

	ctx.SetStatusCode(fasthttp.StatusAccepted)
	ctx.SetContentType("application/json")
	ctx.SetBodyString(`{"status":"accepted"}`)
}

func (e *Engine) handleHistory(ctx *fasthttp.RequestCtx) {
	history, root, tip := e.resolver.ResolveHistory()

	resp := models.HistoryResponse{
		Status:        "ok",
		TotalCommits:  len(history),
		RootCommit:    root,
		TipCommit:     tip,
		CommitHistory: history,
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	if data, err := json.Marshal(resp); err == nil {
		ctx.SetBody(data)
	} else {
		ctx.SetBodyString(`{"error":"failed to serialize history"}`)
	}
}

func (e *Engine) handleMetrics(ctx *fasthttp.RequestCtx) {
	stats := e.metrics.Snapshot(e.resolver.TotalCommits())
	// Enrich with memory guard peak
	if peak := e.guard.PeakHeapMB(); peak > stats.MaxHeapMB {
		stats.MaxHeapMB = peak
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	if data, err := json.Marshal(stats); err == nil {
		ctx.SetBody(data)
	} else {
		ctx.SetBodyString(`{"error":"failed to serialize metrics"}`)
	}
}

func (e *Engine) handleReset(ctx *fasthttp.RequestCtx) {
	e.resolver.Reset()
	e.metrics.Reset()

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.SetBodyString(`{"status":"reset complete"}`)
}

// Start begins listening and serving traffic.
func (e *Engine) Start() error {
	addr := fmt.Sprintf(":%d", e.cfg.Port)
	ln, err := net.Listen("tcp4", addr)
	if err != nil {
		return err
	}
	e.ln = ln

	return e.server.Serve(ln)
}

// Stop gracefully terminates the server and background workers.
func (e *Engine) Stop() error {
	var err error
	if e.ln != nil {
		err = e.ln.Close()
	}
	e.resolver.Close()
	e.guard.Close()
	return err
}

// GetMetrics returns the current metrics collector.
func (e *Engine) GetMetrics() *metrics.Collector {
	return e.metrics
}

// GetResolver returns the internal Git DAG resolver.
func (e *Engine) GetResolver() *gitdag.Resolver {
	return e.resolver
}

// GetGuard returns the MicroVM memory guard.
func (e *Engine) GetGuard() *MicroVMGuard {
	return e.guard
}
