#!/usr/bin/env bash
set -euo pipefail

TOTAL=${1:-10000}
RATE=${2:-0.30}
CONCURRENCY=${3:-50}
PORT=${4:-8080}

echo ">>> Building server and harness binaries..."
go build -ldflags="-s -w" -o bin/server ./cmd/server
go build -ldflags="-s -w" -o bin/harness ./cmd/harness

echo ">>> Starting Ingestion Engine (Port: $PORT)..."
./bin/server -port "$PORT" &
SERVER_PID=$!

cleanup() {
    echo ">>> Stopping Ingestion Engine (PID: $SERVER_PID)..."
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

sleep 1

echo ">>> Firing Adversarial Verification Burst..."
./bin/harness -server "http://127.0.0.1:$PORT" -total "$TOTAL" -adversary-rate "$RATE" -concurrency "$CONCURRENCY"
