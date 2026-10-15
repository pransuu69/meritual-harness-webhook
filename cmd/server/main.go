package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/pransuu69/meritual-harness-webhook/internal/ingestion"
)

func main() {
	portFlag := flag.Int("port", 8080, "HTTP server listening port")
	secretFlag := flag.String("secret", "meritual-adversarial-webhook-secret-2026", "HMAC SHA-256 webhook secret")
	maxMemFlag := flag.Int("max-mem-mb", 64, "Strict Firecracker MicroVM heap boundary in MB")
	p99Flag := flag.Int("p99-latency-ms", 35, "Target maximum p99 latency assertion in ms")
	flag.Parse()

	// Environment variable overrides
	port := *portFlag
	if p := os.Getenv("PORT"); p != "" {
		if val, err := strconv.Atoi(p); err == nil {
			port = val
		}
	}

	secret := *secretFlag
	if s := os.Getenv("WEBHOOK_SECRET"); s != "" {
		secret = s
	}

	maxMemMB := *maxMemFlag
	if m := os.Getenv("MAX_MEMORY_MB"); m != "" {
		if val, err := strconv.Atoi(m); err == nil {
			maxMemMB = val
		}
	}

	maxP99Ms := *p99Flag
	if l := os.Getenv("MAX_LATENCY_P99_MS"); l != "" {
		if val, err := strconv.Atoi(l); err == nil {
			maxP99Ms = val
		}
	}

	fmt.Println("================================================================================")
	fmt.Println(" MERITUAL HARNESS: ADVERSARIAL GITHUB WEBHOOK INGESTION ENGINE")
	fmt.Println(" Target Role: Staff Infrastructure Engineer | Tier: brutal | Score: 5000")
	fmt.Println("================================================================================")
	fmt.Printf(" Listening Port:         :%d\n", port)
	fmt.Printf(" Firecracker Memory Cap: %d MB (Hard MicroVM SIGKILL Boundary)\n", maxMemMB)
	fmt.Printf(" Target P99 Latency:     < %d ms\n", maxP99Ms)
	fmt.Printf(" Cryptographic Sieve:    HMAC-SHA256 (30%% Adversarial Drop Expected)\n")
	fmt.Println("================================================================================")

	cfg := ingestion.EngineConfig{
		Port:            port,
		WebhookSecret:   []byte(secret),
		MaxMemoryMB:     maxMemMB,
		MaxLatencyP99Ms: maxP99Ms,
		Concurrency:     10000,
	}

	engine := ingestion.NewEngine(cfg)

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\n[SHUTDOWN] Intercepted termination signal. Stopping ingestion engine gracefully...")
		if err := engine.Stop(); err != nil {
			log.Printf("[SHUTDOWN ERROR] %v\n", err)
		}
		os.Exit(0)
	}()

	log.Printf("[READY] Server initialized on :%d. Ready to ingest burst traffic.\n", port)
	if err := engine.Start(); err != nil {
		log.Fatalf("[FATAL] Server terminated with error: %v\n", err)
	}
}
