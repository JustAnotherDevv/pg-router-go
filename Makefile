.PHONY: build build-all test test-unit test-integration test-pg15 test-pg16 \
        lint clean cover run help fmt deps-up deps-down

BINARY  := pgrouter
PKG     := ./...
BIN_DIR := bin
GO      := go

# Default
help:
	@echo "Targets:"
	@echo "  build              compile pgrouter binary into bin/"
	@echo "  build-all          compile all command + tool binaries"
	@echo "  test               run unit + race tests"
	@echo "  test-unit          run unit tests (short, no integration)"
	@echo "  test-integration   run integration tests (needs Postgres on PGROUTER_DSN)"
	@echo "  test-pg15          run integration tests against deps/pg15 (port 25515)"
	@echo "  test-pg16          run integration tests against deps/pg16 (port 25516)"
	@echo "  deps-up            start PG 15 + PG 16 test containers"
	@echo "  deps-down          stop + remove PG test containers"
	@echo "  lint               run golangci-lint"
	@echo "  fmt                run gofmt + goimports"
	@echo "  cover              generate coverage report (coverage.html)"
	@echo "  run                build and run with default config"
	@echo "  clean              remove build artifacts"

build:
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/$(BINARY) ./cmd/pgrouter

build-all:
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/$(BINARY)  ./cmd/pgrouter
	$(GO) build -o $(BIN_DIR)/handshake  ./test/handshake
	$(GO) build -o $(BIN_DIR)/poke       ./test/poke
	$(GO) build -o $(BIN_DIR)/bench      ./test/bench

test: test-unit

test-unit:
	$(GO) test -race -short $(PKG)

test-integration:
	$(GO) test -race -tags=integration ./test/integration/...

test-pg15:
	PGROUTER_DSN="postgres://test@127.0.0.1:25515/test?sslmode=disable" \
	  $(GO) test -race -tags=integration ./test/integration/...

test-pg16:
	PGROUTER_DSN="postgres://test@127.0.0.1:25516/test?sslmode=disable" \
	  $(GO) test -race -tags=integration ./test/integration/...

deps-up:
	docker compose -f deploy/docker-compose.test.yml up -d
	@echo "PG 15: 127.0.0.1:25515 (test/test/test, trust auth)"
	@echo "PG 16: 127.0.0.1:25516 (test/test/test, trust auth)"

deps-down:
	docker compose -f deploy/docker-compose.test.yml down -v

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
	  echo "golangci-lint not installed. See: https://golangci-lint.run/welcome/install/"; \
	  exit 1; }
	golangci-lint run

fmt:
	gofmt -w -s .
	@command -v goimports >/dev/null 2>&1 && goimports -w -local github.com/JustAnotherDevv/pgrouter . || true

cover:
	$(GO) test -race -coverprofile=coverage.out -covermode=atomic $(PKG)
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "open coverage.html"

run: build
	$(BIN_DIR)/$(BINARY) --config configs/pgrouter.yaml

clean:
	rm -rf $(BIN_DIR) coverage.out coverage.html
