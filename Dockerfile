# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/harness ./cmd/harness

# Minimal runtime stage
FROM alpine:3.20

RUN apk --no-cache add ca-certificates curl tzdata
WORKDIR /app

COPY --from=builder /bin/server /app/server
COPY --from=builder /bin/harness /app/harness

# Enforce microVM memory environment variables
ENV GOMEMLIMIT=48MiB
ENV GOGC=50
ENV MAX_MEMORY_MB=64
ENV MAX_LATENCY_P99_MS=35
ENV PORT=8080

EXPOSE 8080
ENTRYPOINT ["/app/server"]
