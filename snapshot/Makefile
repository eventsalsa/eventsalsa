.PHONY: help check test test-unit test-integration lint fmt fmt-check vet gosec govulncheck build

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'

check: fmt-check vet lint test-unit test-integration gosec govulncheck ## Run all checks (lint, unit tests, integration tests, security)

test: test-unit ## Run all tests

test-unit: ## Run unit tests
	go test -v -race -count=1 -coverprofile=coverage.out ./...

test-integration: ## Run integration tests (requires Docker daemon for testcontainers)
	go test -p 1 -v -race -count=1 -tags=integration ./...

vet: ## Run go vet
	go vet ./...

lint: ## Run linter
	golangci-lint run --config=../.golangci.yml --timeout=5m ./...

govulncheck: ## Run vulnerability checker
	govulncheck ./...

gosec: ## Run gosec security scanner
	@tmp=$$(mktemp --suffix=.sarif); \
	gosec -fmt sarif -out "$$tmp" ./... || { rm -f "$$tmp"; exit 1; }; \
	rm -f "$$tmp"

fmt: ## Format code
	gofmt -w -s .
	goimports -w -local eventsalsa.dev .

fmt-check: ## Check code formatting
	@test -z "$$(gofmt -l .)" || { gofmt -l .; echo "run make fmt to fix"; exit 1; }

build: ## Build all packages
	go build -v ./...
