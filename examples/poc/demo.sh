#!/usr/bin/env bash
# pgrouter PoC end-to-end demo.
#
# This script exercises the PRODUCTION code path: `pgrouter run --config`
# routed through PooledHandler + pool.Manager. The legacy
# `pgrouter --listen --backend` PoC-style flags are still supported but
# don't reflect how real users deploy the binary.
#
# Prerequisites (checked at the top):
#   - Go 1.26+
#   - Docker daemon reachable (used to start PG 16 ephemerally)
#
# Cross-platform: on Git Bash / MSYS / Cygwin we transparently append
# `.exe` to binary names so the same script works on Windows hosts.
set -euo pipefail

cd "$(dirname "$0")/../.."

# --------------------------------------------------------------------
# Prerequisites
# --------------------------------------------------------------------
if ! command -v go >/dev/null 2>&1; then
    echo "ERROR: 'go' not found on PATH. Install Go 1.26+: https://go.dev/dl/" >&2
    exit 1
fi
if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: 'docker' not found on PATH." >&2
    echo "       Install Docker Desktop (Win/macOS) or docker engine (Linux)." >&2
    echo "       https://docs.docker.com/get-docker/" >&2
    exit 1
fi
if ! docker info >/dev/null 2>&1; then
    echo "ERROR: docker daemon is not reachable." >&2
    echo "       Start Docker Desktop, or run 'sudo systemctl start docker'." >&2
    exit 1
fi

# --------------------------------------------------------------------
# Portable .exe suffix for Windows shells (Git Bash / MSYS / Cygwin).
# --------------------------------------------------------------------
EXE=""
case "${OSTYPE:-}" in
    msys*|cygwin*|win32*) EXE=".exe" ;;
esac
if [[ -z "$EXE" && -n "${WINDIR:-}" ]]; then EXE=".exe"; fi

PG_NAME=pgrouter-poc-pg
PG_PORT=25432
PR_PORT=16432
DEMO_CONFIG="$(pwd)/.pgrouter-demo.yaml"
PR_PID=""

cleanup() {
    echo
    echo "==> cleanup"
    if [[ -n "$PR_PID" ]]; then
        kill "$PR_PID" 2>/dev/null || true
        wait "$PR_PID" 2>/dev/null || true
    fi
    docker rm -f "$PG_NAME" >/dev/null 2>&1 || true
    rm -f "$DEMO_CONFIG"
}
trap cleanup EXIT

# --------------------------------------------------------------------
# Build the four binaries the demo drives.
# --------------------------------------------------------------------
echo "==> building binaries"
go build -o "bin/pgrouter${EXE}"  ./cmd/pgrouter
go build -o "bin/handshake${EXE}" ./test/handshake
go build -o "bin/bench${EXE}"     ./test/bench

# --------------------------------------------------------------------
# Spin up Postgres 16 in Docker with trust auth.
# --------------------------------------------------------------------
echo "==> starting Postgres 16 in Docker (trust auth on :$PG_PORT)"
docker run -d --name "$PG_NAME" --rm \
    -e POSTGRES_PASSWORD=testpw \
    -e POSTGRES_USER=test \
    -e POSTGRES_DB=test \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -p "$PG_PORT:5432" postgres:16 >/dev/null

echo "  waiting for Postgres..."
for i in {1..30}; do
    if docker exec "$PG_NAME" pg_isready -U test >/dev/null 2>&1; then
        echo "  ready"
        break
    fi
    sleep 1
done

# --------------------------------------------------------------------
# Write a config that points at our docker pg, then run pgrouter via the
# production `run --config` path (NOT the legacy --listen --backend).
# --------------------------------------------------------------------
cat > "$DEMO_CONFIG" <<EOF
server:
  listen_addr: 127.0.0.1
  listen_port: $PR_PORT
  max_client_conn: 100

pool:
  mode: transaction
  default_pool_size: 5
  query_wait_timeout: 30s

auth:
  type: trust

metrics:
  listen: 127.0.0.1:0  # ephemeral; off-by-default for demo
  path: /metrics

databases:
  test:
    host: 127.0.0.1
    port: $PG_PORT
    dbname: test
EOF

echo "==> validating config"
"./bin/pgrouter${EXE}" validate "$DEMO_CONFIG"

echo "==> starting pgrouter run --config (production path)"
"./bin/pgrouter${EXE}" run --config "$DEMO_CONFIG" &
PR_PID=$!
sleep 0.7

# --------------------------------------------------------------------
# Drive the demo: handshake → integration tests → bench.
# --------------------------------------------------------------------
echo
echo "==> handshake demo (no psql required)"
"./bin/handshake${EXE}" "127.0.0.1:$PR_PORT" test test

echo
echo "==> integration tests through proxy"
PGROUTER_DSN="postgres://test@127.0.0.1:$PR_PORT/test?sslmode=disable" \
    go test -tags=integration -v ./test/integration/...

echo
echo "==> mini bench (c=1 t=500 extended)"
"./bin/bench${EXE}" \
    -dsn "postgres://test@127.0.0.1:$PR_PORT/test?sslmode=disable" \
    -c 1 -t 500 -mode extended

echo
echo "==> demo complete"
