GO_MODULES := store snapshot projector encryption encryption/kek/awskms encryption/kek/gcpkms encryption/examples
INTEGRATION_MODULES := store snapshot projector encryption

.PHONY: help check fmt fmt-check vet lint build test test-unit test-integration gosec vuln docs-build \
	check-store check-snapshot check-projector check-encryption

help: ## Show available targets
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

check: fmt-check vet lint test-unit test-integration gosec vuln docs-build ## Run full local CI checks across all modules and docs

fmt: ## Format Go files across all modules
	@for mod in $(GO_MODULES); do \
		(cd "$$mod" && gofmt -w -s . && goimports -w -local eventsalsa.dev .); \
	done

fmt-check: ## Verify formatting across all modules
	@unformatted=""; \
	for mod in $(GO_MODULES); do \
		out=$$(gofmt -l "$$mod"); \
		if [ -n "$$out" ]; then \
			unformatted="$$unformatted$$out\n"; \
		fi; \
	done; \
	if [ -n "$$unformatted" ]; then \
		printf "Unformatted files:\n%b" "$$unformatted"; \
		echo "Run 'make fmt' to fix"; \
		exit 1; \
	fi

vet: ## Run go vet across all Go modules
	@for mod in $(GO_MODULES); do \
		echo "==> go vet ($$mod)"; \
		go vet -C "$$mod" ./... || exit 1; \
	done

lint: ## Run golangci-lint across all Go modules using root config
	@for mod in $(GO_MODULES); do \
		echo "==> golangci-lint ($$mod)"; \
		(cd "$$mod" && golangci-lint run --config="$(CURDIR)/.golangci.yml" --timeout=5m ./...) || exit 1; \
	done

build: ## Build all Go packages across all modules
	@for mod in $(GO_MODULES); do \
		echo "==> go build ($$mod)"; \
		go build -C "$$mod" ./... || exit 1; \
	done

test: test-unit ## Alias for test-unit

test-unit: ## Run race-enabled unit tests across all Go modules
	@for mod in $(GO_MODULES); do \
		echo "==> unit tests ($$mod)"; \
		go test -C "$$mod" -race -count=1 ./... || exit 1; \
	done

test-integration: ## Run testcontainers-go integration tests (requires Docker)
	@for mod in $(INTEGRATION_MODULES); do \
		echo "==> integration tests ($$mod)"; \
		go test -C "$$mod" -p 1 -race -count=1 -tags=integration -timeout=300s ./... || exit 1; \
	done

gosec: ## Run gosec across all Go modules
	@for mod in $(GO_MODULES); do \
		echo "==> gosec ($$mod)"; \
		tmp=$$(mktemp --suffix=.sarif); \
		extra=""; \
		if [ "$$mod" = "encryption" ]; then \
			extra="-exclude-dir=kek/awskms -exclude-dir=kek/gcpkms -exclude-dir=examples"; \
		fi; \
		(cd "$$mod" && gosec $$extra -fmt sarif -out "$$tmp" ./...) || { rm -f "$$tmp"; exit 1; }; \
		rm -f "$$tmp"; \
	done

vuln: ## Run govulncheck across all Go modules
	@for mod in $(GO_MODULES); do \
		echo "==> govulncheck ($$mod)"; \
		govulncheck -C "$$mod" ./... || exit 1; \
	done

docs-build: ## Type-check Cloudflare middleware and build Astro documentation site
	@echo "==> docs type-check & build"
	(cd docs && ./node_modules/.bin/tsc --noEmit --skipLibCheck --strict --target es2022 --module esnext --lib es2022,dom functions/_middleware.ts)
	npm --prefix docs run build

check-store: ## Run full Go check suite for store
	$(MAKE) -C store check

check-snapshot: ## Run full Go check suite for snapshot
	$(MAKE) -C snapshot check

check-projector: ## Run full Go check suite for projector
	$(MAKE) -C projector check

check-encryption: ## Run full Go check suite for encryption and KMS/example submodules
	$(MAKE) -C encryption check
