.PHONY: help build run test bench race clean docker-build docker-run

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the application
	@echo "Building..."
	@go build -o bin/crypto-stream-engine cmd/server/main.go
	@echo "Build complete: bin/crypto-stream-engine"

run: ## Run the application
	@go run cmd/server/main.go

test: ## Run unit tests
	@echo "Running tests..."
	@go test -v -cover ./...

bench: ## Run benchmarks
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem ./...

race: ## Run race detector
	@echo "Running race detector..."
	@go test -race ./...
	@echo "✓ No race conditions detected"

clean: ## Clean build artifacts
	@rm -rf bin/
	@echo "Cleaned build artifacts"

deps: ## Download dependencies
	@go mod download
	@go mod tidy

lint: ## Run linter (requires golangci-lint)
	@golangci-lint run

fmt: ## Format code
	@go fmt ./...

vet: ## Run go vet
	@go vet ./...

metrics: ## View Prometheus metrics (requires running server)
	@curl -s http://localhost:8080/metrics | grep crypto_stream

docker-build: ## Build Docker image
	@docker build -t crypto-stream-engine:latest .

docker-run: ## Run Docker container
	@docker run -p 8080:8080 crypto-stream-engine:latest

all: fmt vet test build ## Run fmt, vet, test, and build