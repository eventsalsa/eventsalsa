.PHONY: help check test test-unit test-integration lint fmt fmt-check vet gosec govulncheck build

SUBMODULES := examples

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

check: fmt-check vet lint test-unit test-integration gosec govulncheck ## Run all checks

test: test-unit ## Run unit tests

test-unit: ## Run unit tests across cqrs and examples
	go test -v -race -count=1 -coverprofile=coverage.out ./...
	@for mod in $(SUBMODULES); do \
		go test -C "$$mod" -v -race -count=1 ./... || exit 1; \
	done

test-integration: ## Run in-process CQRS pipeline integration tests
	go test -p 1 -v -race -count=1 -tags=integration ./integration_test/...

vet: ## Run go vet across cqrs and examples
	go vet ./...
	@for mod in $(SUBMODULES); do \
		go vet -C "$$mod" ./... || exit 1; \
	done

lint: ## Run linter across cqrs and examples
	golangci-lint run --config=../.golangci.yml --timeout=5m ./...
	@for mod in $(SUBMODULES); do \
		(cd "$$mod" && golangci-lint run --config=../../.golangci.yml --timeout=5m ./...) || exit 1; \
	done

fmt: ## Format code across cqrs and examples
	gofmt -w -s .

fmt-check: ## Check code formatting across cqrs and examples
	@test -z "$$(gofmt -l .)" || { gofmt -l .; echo "run make fmt to fix"; exit 1; }

gosec: ## Run gosec security scanner (published module only)
	@tmp=$$(mktemp --suffix=.sarif); \
	gosec -exclude-dir=examples -fmt sarif -out "$$tmp" ./... || { rm -f "$$tmp"; exit 1; }; \
	rm -f "$$tmp"

govulncheck: ## Run vulnerability checker (published module only)
	govulncheck ./...

build: ## Build all packages across cqrs and examples
	go build -v ./...
	@for mod in $(SUBMODULES); do \
		go build -C "$$mod" -v ./... || exit 1; \
	done
