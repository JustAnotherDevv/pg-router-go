# pgrouter

[![CI](https://github.com/JustAnotherDevv/pg-router-go/actions/workflows/ci.yml/badge.svg)](https://github.com/JustAnotherDevv/pg-router-go/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/JustAnotherDevv/pg-router-go)](https://github.com/JustAnotherDevv/pg-router-go/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/JustAnotherDevv/pg-router-go)](https://github.com/JustAnotherDevv/pg-router-go/blob/main/go.mod)
[![License](https://img.shields.io/github/license/JustAnotherDevv/pg-router-go)](https://github.com/JustAnotherDevv/pg-router-go/blob/main/LICENSE)

pgrouter is a PostgreSQL connection pooler written in Go. It speaks the
PostgreSQL wire protocol on both sides, accepts normal Postgres clients, and
routes them through per-database/per-user backend pools.

It is built for low-overhead transaction pooling with practical production
features: SCRAM/MD5/trust/peer/cert/HBA auth, TLS, cancel routing, prepared
statement reuse, GUC replay, Prometheus metrics, graceful shutdown, and live
config/userlist reload.

## Features

- Transaction, session, and statement pool modes
- Per `(database, user)` pool isolation, pool sizing, reserve pools, and global
  database/user connection caps
- SCRAM-SHA-256, MD5, trust, peer, cert, HBA, userlist.txt, and auth_query
- Client-side and server-side TLS
- CancelRequest routing through synthetic client-facing BackendKeyData
- GUC tracking and replay on backend acquire
- Cross-client prepared statement cache with deterministic backend statement
  names
- Read-replica routing with lag/health checks and sticky reads after writes
- Prometheus `/metrics`, `/healthz`, `/readyz`, and admin HTTP endpoints
- PgBouncer-style SQL admin console on the `pgbouncer` virtual database
- Graceful SIGTERM drain and SIGHUP config/userlist reload

## Quick Start

Build the binary:

```sh
make build
```

Create a config:

```yaml
server:
  listen_addr: 0.0.0.0
  listen_port: 6432

pool:
  mode: transaction
  default_pool_size: 20

auth:
  type: trust

databases:
  appdb:
    host: 127.0.0.1
    port: 5432
    dbname: appdb
```

Validate and run:

```sh
bin/pgrouter validate examples/configs/basic.yaml
bin/pgrouter run --config examples/configs/basic.yaml
```

Connect through pgrouter:

```sh
psql "postgres://alice@127.0.0.1:6432/appdb?sslmode=disable"
```

Metrics are exposed on `:9090/metrics` by default.

## Configuration

pgrouter uses strict YAML. Unknown fields fail validation so typos are caught
at startup.

Useful examples:

- `examples/configs/basic.yaml` - single primary, transaction pooling
- `examples/configs/session-mode.yaml` - session pooling
- `examples/configs/multi-pool.yaml` - multiple databases, TLS, per-user
  overrides

Common sections:

- `server` - listeners, Unix sockets, client limits, worker count, runtime
  knobs
- `pool` - pool mode, sizes, timeouts, reset query, global caps
- `auth` - trust, SCRAM, MD5, peer, cert, HBA, userlist, auth_query
- `tls` - client-facing and backend-facing TLS
- `databases` - upstream hosts and per-database overrides
- `users` - per-user pool/limit overrides
- `metrics` and `logging` - observability and log output

## Build

Requires Go 1.26+.

```sh
make build      # bin/pgrouter
make build-all  # pgrouter plus local test tools
make test-unit  # short unit suite
```

Build directly:

```sh
go build -o bin/pgrouter ./cmd/pgrouter
```

## Test

```sh
go test -short ./...
go test -race -short ./...
```

Integration tests need a real Postgres:

```sh
docker compose -f test/integration/docker-compose.yml up -d
go test -tags integration -count=1 ./test/integration/...
docker compose -f test/integration/docker-compose.yml down -v
```

The integration harness builds a temporary pgrouter binary, starts it on a free
local port, and runs pgx, GORM, sqlx, lib/pq, cancel-routing, and edge-case
tests through it.

## Benchmark

Side-by-side pool benchmarks live under `test/bench/compare`:

```sh
cd test/bench/compare
docker compose up -d --build
./run.sh
docker compose down -v
```

The matrix compares direct Postgres, PgBouncer, PgCat, and pgrouter with
pgbench plus a Go pgx workload. Results are written under
`test/bench/compare/results/`.

## Deploy

- Dockerfiles: `deploy/Dockerfile`, `deploy/Dockerfile.release`
- systemd unit: `deploy/pgrouter.service`
- Helm chart: `deploy/helm/pgrouter`

Container image:

```sh
docker pull ghcr.io/justanotherdevv/pgrouter:1.0
```

Release artifacts can be verified with cosign:

```sh
cosign verify-blob \
  --certificate-identity-regexp 'https://github.com/JustAnotherDevv/pg-router-go/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --signature checksums.txt.sig \
  --certificate checksums.txt.pem \
  checksums.txt
```

## Operator Guide

Minimum production checklist:

- Put `auth.type`, TLS mode, pool sizes, `max_client_conn`, and admin token in
  config explicitly. Do not rely on defaults you have not reviewed.
- Bind Postgres and pgrouter only where intended. Keep test-only trust auth and
  public listeners out of production.
- Enable metrics scraping before first traffic and alert on process restarts,
  readiness failures, query timeouts, waiters, and backend dial errors.
- Keep `query_timeout`, `query_wait_timeout`, and backend connect timeouts set
  to finite values.

Startup and health:

- Validate config before rollout: `bin/pgrouter validate <config>`
- Start with `bin/pgrouter run --config <config>`
- Check `/readyz` for readiness, `/healthz` for liveness, and `/metrics` for
  saturation or dial failures.
- Smoke test a real client query through pgrouter before sending full traffic.

Reloads and rollouts:

- Use `SIGHUP` for config and userlist reloads.
- Change pool sizes gradually and watch waiters, dial errors, and query
  timeouts during the rollout.
- Prefer staged rollout with a canary slice before full cutover.

Failure handling:

- Backend restart: expect in-flight work to fail and new acquires to recover
  once Postgres is reachable again.
- Slow query: expect SQLSTATE `57014` when `query_timeout` fires.
- Pool exhaustion: expect acquires to fail after `query_wait_timeout`; scale
  pool capacity or reduce concurrency instead of letting waiters grow without
  bound.
- Unexpected backend disconnect: discard the affected client/backend pair and
  confirm fresh connections succeed before reopening traffic.

## Architecture

The short path:

```text
client
  -> listener.Listener
  -> client.PooledHandler
  -> pool.Manager / pool.Pool
  -> backend.Conn
  -> PostgreSQL
```

Core packages:

- `cmd/pgrouter` - CLI, process lifecycle, signals, metrics/admin binding
- `internal/wire` - shared runtime wiring for cmd and library mode
- `internal/client` - startup handling, pooling dispatcher, SQL observation
- `internal/pool` - backend pool and per-key manager
- `internal/backend` - upstream dial/auth/reset and backend state
- `internal/auth` - client auth, HBA, userlist, auth_query
- `internal/listener` - TCP/Unix listeners, TLS helpers, PROXY protocol
- `internal/stats` - Prometheus and admin HTTP API
- `pkg/pgrouter` - embeddable library API

## Contributing

Before sending changes:

```sh
gofmt -w .
go test -short ./...
```

For changes that touch pooling, auth, wire forwarding, or shutdown behavior,
also run the race suite and the integration tests when Postgres is available.

## License

Apache-2.0. See [LICENSE](LICENSE).
