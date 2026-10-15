package ingestion

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/pransuu69/meritual-harness-webhook/internal/crypto"
	"github.com/pransuu69/meritual-harness-webhook/internal/models"
	"github.com/valyala/fasthttp"
)

func TestEngineWebhookHandling(t *testing.T) {
	secret := []byte("test-webhook-secret-999")
	cfg := EngineConfig{
		Port:            9999,
		WebhookSecret:   secret,
		MaxMemoryMB:     64,
		MaxLatencyP99Ms: 35,
		Concurrency:     100,
	}

	engine := NewEngine(cfg)
	defer engine.Stop()

	// 1. Valid payload test
	validPayload := []byte(`{"commit_sha":"c001","parent_sha":"","timestamp":"2026-01-01T00:00:00Z","message":"initial"}`)
	validSig := crypto.Sign(secret, validPayload)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(fasthttp.MethodPost)
	ctx.Request.SetRequestURI("/webhook")
	ctx.Request.Header.Set("X-Hub-Signature-256", validSig)
	ctx.Request.SetBody(validPayload)

	engine.HandleRequest(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusAccepted {
		t.Fatalf("expected 202 Accepted for valid payload, got %d", ctx.Response.StatusCode())
	}

	// 2. Forged signature test
	forgedSig := "sha256=0000000000000000000000000000000000000000000000000000000000000000"
	ctxForge := &fasthttp.RequestCtx{}
	ctxForge.Request.Header.SetMethod(fasthttp.MethodPost)
	ctxForge.Request.SetRequestURI("/webhook")
	ctxForge.Request.Header.Set("X-Hub-Signature-256", forgedSig)
	ctxForge.Request.SetBody(validPayload)

	engine.HandleRequest(ctxForge)

	if ctxForge.Response.StatusCode() != fasthttp.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for forged payload, got %d", ctxForge.Response.StatusCode())
	}

	// 3. Missing signature test
	ctxNoSig := &fasthttp.RequestCtx{}
	ctxNoSig.Request.Header.SetMethod(fasthttp.MethodPost)
	ctxNoSig.Request.SetRequestURI("/webhook")
	ctxNoSig.Request.SetBody(validPayload)

	engine.HandleRequest(ctxNoSig)

	if ctxNoSig.Response.StatusCode() != fasthttp.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for missing signature, got %d", ctxNoSig.Response.StatusCode())
	}

	// 4. Ingest second child commit
	childPayload := []byte(`{"commit_sha":"c002","parent_sha":"c001","timestamp":"2026-01-01T00:01:00Z","message":"child"}`)
	childSig := crypto.Sign(secret, childPayload)

	ctxChild := &fasthttp.RequestCtx{}
	ctxChild.Request.Header.SetMethod(fasthttp.MethodPost)
	ctxChild.Request.SetRequestURI("/webhook")
	ctxChild.Request.Header.Set("X-Hub-Signature-256", childSig)
	ctxChild.Request.SetBody(childPayload)

	engine.HandleRequest(ctxChild)

	if ctxChild.Response.StatusCode() != fasthttp.StatusAccepted {
		t.Fatalf("expected 202 Accepted for child commit, got %d", ctxChild.Response.StatusCode())
	}

	// Wait briefly for worker to drain
	time.Sleep(10 * time.Millisecond)

	// 5. Query /history
	ctxHist := &fasthttp.RequestCtx{}
	ctxHist.Request.Header.SetMethod(fasthttp.MethodGet)
	ctxHist.Request.SetRequestURI("/history")

	engine.HandleRequest(ctxHist)

	if ctxHist.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected 200 OK for /history, got %d", ctxHist.Response.StatusCode())
	}

	var histResp models.HistoryResponse
	if err := json.Unmarshal(ctxHist.Response.Body(), &histResp); err != nil {
		t.Fatalf("failed to parse /history response: %v", err)
	}

	if histResp.TotalCommits != 2 {
		t.Fatalf("expected 2 commits in history, got %d", histResp.TotalCommits)
	}
	if histResp.RootCommit != "c001" || histResp.TipCommit != "c002" {
		t.Fatalf("expected root c001 and tip c002, got root=%s, tip=%s", histResp.RootCommit, histResp.TipCommit)
	}

	// 6. Query /metrics
	ctxMetrics := &fasthttp.RequestCtx{}
	ctxMetrics.Request.Header.SetMethod(fasthttp.MethodGet)
	ctxMetrics.Request.SetRequestURI("/metrics")

	engine.HandleRequest(ctxMetrics)

	if ctxMetrics.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("expected 200 OK for /metrics, got %d", ctxMetrics.Response.StatusCode())
	}

	var stats models.IngestStats
	if err := json.Unmarshal(ctxMetrics.Response.Body(), &stats); err != nil {
		t.Fatalf("failed to parse /metrics response: %v", err)
	}

	if stats.ValidProcessed != 2 {
		t.Fatalf("expected 2 valid processed, got %d", stats.ValidProcessed)
	}
	if stats.DroppedForged != 2 { // 1 forged + 1 missing
		t.Fatalf("expected 2 dropped forged, got %d", stats.DroppedForged)
	}
}
