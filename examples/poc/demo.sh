#!/usr/bin/env bash
# PoC end-to-end demo. Requires Docker + Go.
# Spins up Postgres, starts pgrouter, runs handshake + integration tests.
set -euo pipefail

cd "$(dirname "$0")/../.."

PG_NAME=pgrouter-poc-pg
PG_PORT=25432
PR_PORT=16432

cleanup() {
    echo
    echo "==> cleanup"
    if [[ -n "${PR_PID:-}" ]]; then
        kill "$PR_PID" 2>/dev/null || true
    fi
    docker rm -f "$PG_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> building binaries"
go build -o bin/pgrouter   ./cmd/pgrouter
go build -o bin/handshake  ./test/handshake
go build -o bin/bench      ./test/bench

echo "==> starting Postgres 16 in Docker (trust auth on :$PG_PORT)"
docker run -d --name "$PG_NAME" --rm \
    -e POSTGRES_PASSWORD=testpw \
    -e POSTGRES_USER=test \
    -e POSTGRES_DB=test \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -p "$PG_PORT:5432" postgres:16 >/dev/null

echo "  waiting for Postgres..."
for i in {1..20}; do
    if docker exec "$PG_NAME" pg_isready -U test >/dev/null 2>&1; then
        echo "  ready"
        break
    fi
    sleep 1
done

echo "==> starting pgrouter on :$PR_PORT -> :$PG_PORT"
./bin/pgrouter --listen "127.0.0.1:$PR_PORT" --backend "127.0.0.1:$PG_PORT" &
PR_PID=$!
sleep 0.5

echo
echo "==> handshake demo (no psql required)"
./bin/handshake "127.0.0.1:$PR_PORT" test test

echo
echo "==> integration tests through proxy"
PGROUTER_DSN="postgres://test@127.0.0.1:$PR_PORT/test?sslmode=disable" \
    go test -tags=integration -v ./test/integration/...

echo
echo "==> mini bench (c=1 t=500 extended)"
./bin/bench -dsn "postgres://test@127.0.0.1:$PR_PORT/test?sslmode=disable" \
            -c 1 -t 500 -mode extended

echo
echo "==> demo complete"
