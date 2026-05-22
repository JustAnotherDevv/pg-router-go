.PHONY: build build-all build-linux build-pgo pgo-profile test test-unit \
        test-integration test-pg15 test-pg16 lint lint-compose clean cover \
        run help fmt deps-up deps-down

BINARY  := pgrouter
PKG     := ./...
BIN_DIR := bin
GO      := go
LDFLAGS := -s -w

# Default
help:
	@echo "Targets:"
	@echo "  build              compile pgrouter binary into bin/"
	@echo "  build-all          compile all command + tool binaries"
	@echo "  build-linux        cross-compile linux/amd64 binary (for Docker/deploy)"
	@echo "  build-pgo          build with PGO profile (default.pgo, requires prior profile)"
	@echo "  pgo-profile        full PGO cycle: build base → run under load → rebuild with profile"
	@echo "  test               run unit + race tests"
	@echo "  test-unit          run unit tests (short, no integration)"
	@echo "  test-integration   run integration tests (needs Postgres on PGROUTER_DSN)"
	@echo "  test-pg15          run integration tests against deps/pg15 (port 25515)"
	@echo "  test-pg16          run integration tests against deps/pg16 (port 25516)"
	@echo "  deps-up            start PG 15 + PG 16 test containers"
	@echo "  deps-down          stop + remove PG test containers"
	@echo "  lint               run golangci-lint"
	@echo "  lint-compose       run docker-compose security lint (no public DB ports)"
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

build-linux:
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	  $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN_DIR)/$(BINARY)-linux ./cmd/pgrouter

build-pgo: default.pgo
	@mkdir -p $(BIN_DIR)
	$(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN_DIR)/$(BINARY) ./cmd/pgrouter

# Full PGO cycle: build base → profile under load → rebuild with profile.
# Requires PGROUTER_DSN pointing to a running pgrouter instance, or runs
# the binary directly if PGROUTER_BIN is set.
pgo-profile: build-linux
	@echo "==> building base binary for profiling..."
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	  $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o $(BIN_DIR)/$(BINARY)-pgo-base ./cmd/pgrouter
	@echo "==> base binary: $(BIN_DIR)/$(BINARY)-pgo-base"
	@echo ""
	@echo "==> To capture profile on a running instance:"
	@echo "     curl -o default.pgo http://HOST:PORT/debug/pprof/profile?seconds=30"
	@echo "     Then run: make build-pgo"
	@echo ""
	@echo "==> Or deploy the base binary, run pgbench, then capture:"
	@echo "     scp $(BIN_DIR)/$(BINARY)-pgo-base HOST:/tmp/pgrouter"
	@echo "     ssh HOST 'pgbench -h 127.0.0.1 -p PORT -U postgres -d postgres -S -M extended -c 32 -j 8 -T 30 &' "
	@echo "     curl -o default.pgo http://HOST:9090/debug/pprof/profile?seconds=30"
	@echo "     make build-pgo"

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

lint-compose:
	@command -v python3 >/dev/null 2>&1 || { echo "python3 required"; exit 1; }
	bash scripts/check-compose-security.sh

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

# Profile file — stale profiles hurt more than no profile.
# Re-capture after significant code changes.
default.pgo:
	@echo "No default.pgo found. Run 'make pgo-profile' to generate one."
	@exit 1
