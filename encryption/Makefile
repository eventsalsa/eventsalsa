.PHONY: build check fmt fmt-check golangci govulncheck gosec lint test test-integration vet

SUBMODULES := kek/awskms kek/gcpkms examples
SECURITY_SUBMODULES := kek/awskms kek/gcpkms
ROOT_CONFIG := $(abspath $(CURDIR)/../.golangci.yml)

## Run the full local check suite mirrored by CI and the security workflow.
check: fmt-check vet golangci test build test-integration gosec govulncheck

## Run go vet across root module and sub-modules
vet:
	go vet ./...
	@for mod in $(SUBMODULES); do \
		go vet -C "$$mod" ./...; \
	done

## Run golangci-lint across root module and sub-modules
golangci:
	golangci-lint run --config="$(ROOT_CONFIG)" --timeout=5m ./...
	@for mod in $(SUBMODULES); do \
		(cd "$$mod" && golangci-lint run --config="$(ROOT_CONFIG)" --timeout=5m ./...); \
	done

## Run all tests with race detector and coverage across root module and sub-modules
test:
	tmp=$$(mktemp); \
	trap 'rm -f "$$tmp"' EXIT; \
	go test -race -count=1 -coverprofile="$$tmp" ./...
	@for mod in $(SUBMODULES); do \
		go test -C "$$mod" -race -count=1 ./...; \
	done

## Run integration-tagged tests (requires Docker when Postgres integration tests are present)
test-integration:
	go test -p 1 -race -count=1 -tags=integration ./...

## Build all packages across root module and sub-modules
build:
	go build ./...
	@for mod in $(SUBMODULES); do \
		go build -C "$$mod" ./...; \
	done

## Format code across root module and sub-modules
fmt:
	gofmt -w -s .
	goimports -w -local eventsalsa.dev .

## Check formatting (fails if files need formatting)
fmt-check:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; echo "run make fmt to fix"; exit 1; }

## Run formatting, vet, and golangci-lint
lint: fmt-check vet golangci

## Run gosec across root module and sub-modules
gosec:
	tmp=$$(mktemp --suffix=.sarif); \
	gosec -exclude-dir=kek/awskms -exclude-dir=kek/gcpkms -exclude-dir=examples -fmt sarif -out "$$tmp" ./... || { rm -f "$$tmp"; exit 1; }; \
	rm -f "$$tmp"
	@for mod in $(SECURITY_SUBMODULES); do \
		tmp=$$(mktemp --suffix=.sarif); \
		(cd "$$mod" && gosec -fmt sarif -out "$$tmp" ./...) || { rm -f "$$tmp"; exit 1; }; \
		rm -f "$$tmp"; \
	done

## Run govulncheck across root module and published sub-modules
govulncheck:
	govulncheck ./...
	@for mod in $(SECURITY_SUBMODULES); do \
		govulncheck -C "$$mod" ./...; \
	done
