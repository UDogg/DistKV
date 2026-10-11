SHELL := /bin/bash
GO      ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LDFLAGS := -s -w \
	-X github.com/UDogg/DistKV/internal/version.Version=$(VERSION) \
	-X github.com/UDogg/DistKV/internal/version.Commit=$(COMMIT)
IMAGE   ?= distkv:$(VERSION)

.PHONY: all build test test-short race chaos bench fuzz lint fmt vet cover cluster cluster-stop docker compose-up compose-down clean help

all: lint test build ## Lint, test and build

build: ## Build all binaries into ./bin
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/ ./cmd/...

test: ## Run all tests (including chaos tests)
	$(GO) test -count=1 -timeout 15m ./...

test-short: ## Run fast unit tests only
	$(GO) test -short -count=1 ./...

race: ## Run all tests with the race detector
	$(GO) test -race -count=1 -timeout 20m ./...

chaos: ## Run the end-to-end fault-injection suite verbosely
	$(GO) test -count=1 -v -run Chaos ./test/e2e/

bench: build ## Benchmark a local 3-node cluster at 10k ops/s
	scripts/bench.sh

microbench: ## Run Go micro-benchmarks
	$(GO) test -run '^$$' -bench . -benchmem ./internal/...

fuzz: ## Fuzz the Raft wire codec for 30s
	$(GO) test -run '^$$' -fuzz FuzzUnmarshalAppendEntries -fuzztime 30s ./internal/transport/

fmt: ## Format code
	gofmt -s -w .

vet: ## go vet
	$(GO) vet ./...

lint: vet ## Check formatting and vet
	@test -z "$$(gofmt -s -l . | tee /dev/stderr)" || (echo "run 'make fmt'"; exit 1)

cover: ## Unit test coverage report
	$(GO) test -short -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1

cluster: build ## Start a local 3-node cluster (16 shards)
	scripts/cluster.sh start 3 16

cluster-stop: ## Stop the local cluster
	scripts/cluster.sh stop

docker: ## Build the container image
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) -t $(IMAGE) .

compose-up: ## Run a 3-node cluster with Prometheus and Grafana via docker compose
	docker compose -f deploy/docker-compose.yml up -d --build

compose-down: ## Tear down the docker compose cluster
	docker compose -f deploy/docker-compose.yml down -v

clean: ## Remove build output and local cluster data
	rm -rf bin dist coverage.out
	scripts/cluster.sh clean >/dev/null 2>&1 || true

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'
