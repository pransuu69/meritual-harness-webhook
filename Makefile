.PHONY: all build test bench verify clean

all: build test

build:
	@mkdir -p bin
	go build -ldflags="-s -w" -o bin/server ./cmd/server
	go build -ldflags="-s -w" -o bin/harness ./cmd/harness

test:
	go test -v -race ./internal/...

bench:
	go test -v -bench=. -benchmem ./internal/...

verify: build
	@echo "Running Meritual Adversarial Ingestion Harness..."
	./scripts/run_harness.sh 10000 0.30 50

clean:
	rm -rf bin/
