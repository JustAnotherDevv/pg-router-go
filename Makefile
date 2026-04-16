.PHONY: build test test-unit test-integration lint clean cover run help

BINARY := pgrouter
PKG    := ./...
BIN    := bin/$(BINARY)
GO     := go

# Default
help:
	@echo "Targets:"
	@echo "  build            - compile $(BINARY) binary into bin/"
	@echo "  test             - run all tests (unit + race)"
	@echo "  test-unit        - run unit tests (short, no integration)"
	@echo "  test-integration - run integration tests (needs Postgres)"
	@echo "  lint             - run golangci-lint"
	@echo "  cover            - generate coverage report"
	@echo "  run              - build and run with default config"
	@echo "  clean            - remove build artifacts"

build:
	@mkdir -p bin
	$(GO) build -o $(BIN) ./cmd/pgrouter

test: test-unit

test-unit:
	$(GO) test -race -short $(PKG)

test-integration:
	$(GO) test -race -tags=integration $(PKG)

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "golangci-lint not installed"; exit 1; }
	golangci-lint run

cover:
	$(GO) test -race -coverprofile=coverage.out -covermode=atomic $(PKG)
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "open coverage.html"

run: build
	$(BIN) --config configs/pgrouter.yaml

clean:
	rm -rf bin coverage.out coverage.html
