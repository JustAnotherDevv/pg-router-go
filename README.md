# pgrouter

Modern PostgreSQL connection pooler written in Go.

**Status: v1.0.3+ shipped — full feature set + 14 review-driven fixes + 16 refactors.** Signed multi-arch binaries + ghcr container image published per release. See [CHANGELOG.md](./CHANGELOG.md) for the full release history.

```bash
# Verify a release artefact (cosign keyless):
cosign verify-blob \
  --certificate-identity-regexp 'https://github.com/JustAnotherDevv/pgrouter/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --signature checksums.txt.sig \
  --certificate checksums.txt.pem \
  checksums.txt

# Pull the container image:
docker pull ghcr.io/justanotherdevv/pgrouter:1.0
```

## What's here

```
┌──────────┐   pgwire v3   ┌─────────────────────┐   pgwire v3   ┌──────────┐
│ Postgres │──────────────▶│      pgrouter       │──────────────▶│ Postgres │
│  client  │◀──────────────│  (Go, txn-pooled)   │◀──────────────│  server  │
└──────────┘   :6432       └─────────────────────┘   :5432       └──────────┘
   psql, pgx,                  SCRAM auth,                     SCRAM/MD5/trust
   GORM, sqlx                  TLS both ends,                   any version
                               txn-mode pool,                   13-17
                               Prometheus
                               /metrics :9090
```

### Pre-MVP capabilities (carryover from v0.1.0-poc)

- TCP listener; one goroutine per client connection
- pgwire v3 startup parsing (StartupMessage / SSLRequest / GSSEncRequest / CancelRequest)
- Bidirectional message forwarding (simple + extended protocol)
- Clean shutdown on client / backend disconnect

### MVP additions (v0.2.0-mvp)

- **Real auth**: SCRAM-SHA-256, MD5, trust, PgBouncer-compatible userlist.txt
- **TLS** both ends (client → pooler + pooler → backend), atomic cert reload
- **YAML config** + `pgrouter validate` / `run --config` CLI + 3 sample configs
- **Wire-protocol layer**: typed wrapper around pgproto3, sync.Pool buffer reuse, 33 round-trip + 3 fuzz tests, 85% coverage
- **Transaction-mode pool**: `internal/pool` with LIFO idle stack, FIFO wait queue, ctx-cancel, sizing + lifetime + idle eviction. Stress-tested under 32-goroutine × 200-iter × pool-size-4 contention; uncovered a slot-leak race in the wait-queue handoff which is now fixed.
- **Transaction-boundary detection** via `ReadyForQuery` tx_status; backend released on idle edge (matches PgBouncer txn mode)
- **Backend lifecycle**: state machine (new/active/idle/closed), DISCARD ALL reset, health check (`;` query), idle eviction, lifetime recycling
- **GUC tracking**: per-client (name → value) cache; SET / RESET / DISCARD ALL parsed; ReplayQuery emits a single SET batch for backend acquire (replay wiring is the M.15 follow-up)
- **Prepared statement tracking**: per-client (name → Stmt) cache; per-backend LRU + name mapping lands with full M.15 wire-up
- **Cancel routing**: per-client (PID, secret) allocated by pgrouter; tracker maps to upstream coordinates; ForwardCancel opens a fresh TCP conn + sends 16-byte CancelRequest with the upstream's real PID/secret
- **Observability**: 22 Prometheus metrics across 6 families (client / backend / pool / wire / auth / cancel) on `:9090/metrics`; `/healthz` liveness probe

## Build

Requires Go 1.26+.

```sh
make build-all
# or
go build -o bin/pgrouter   ./cmd/pgrouter
go build -o bin/bench      ./test/bench
go build -o bin/handshake  ./test/handshake
go build -o bin/poke       ./test/poke
```

## Run

### MVP config-driven

```sh
./bin/pgrouter validate examples/configs/basic.yaml
./bin/pgrouter run --config examples/configs/basic.yaml
```

The `run` subcommand starts the listener, dials the first configured
database, and exposes Prometheus metrics on the address from
`metrics.listen` (default `:9090/metrics`).

### Legacy PoC-style flags (still supported)

```sh
./bin/pgrouter --listen :6432 --backend localhost:5432
```

### Dev Postgres in Docker

```sh
make deps-up    # starts PG 15 on :25515 + PG 16 on :25516
make deps-down
```

## Tests

```sh
make test                              # unit tests
make test-integration                   # needs $PGROUTER_DSN
make test-pg15                          # via deps-up'd PG 15
make test-pg16                          # via deps-up'd PG 16
go test -fuzz=. -fuzztime=30s ./internal/proto/   # fuzz the wire layer
```

Coverage per package (go test -short -cover):

| Package                     | Coverage | Highlights |
|---|---|---|
| `internal/auth/`           | high     | SCRAM client+server e2e, MD5 round-trip, userlist parser, wire interop |
| `internal/backend/`        | core     | Reset / DISCARD ALL, HealthCheck, Lifecycle, Dial w/ auth + TLS |
| `internal/cancel/`         | full     | Allocate uniqueness, Bind/Lookup, ForwardCancel packet shape |
| `internal/client/`         | core     | Startup, TLS upgrade, GUC + Prepare cache, PooledConn boundary release |
| `internal/config/`         | full     | KnownFields strict YAML, validation aggregator, sample configs |
| `internal/listener/`       | full     | TCP accept, TLS upgrade, CertStore reload, BuildServerTLSConfig / BuildBackendTLSConfig |
| `internal/pool/`           | full     | Acquire/Release lifecycle, FIFO, timeout, ctx cancel, EvictIdleOnce, stress (32 × 200 iter) |
| `internal/proto/`          | 85.1 %   | 33 round-trip tests across every frontend + backend message type, 3 fuzz harnesses |
| `internal/stats/`          | full     | Metric family registration, HTTP /metrics + /healthz |
| `cmd/pgrouter/`            | full     | CLI subcommand dispatch, sample-config validation |

Total: ~150 tests across 11 packages.

## PoC bench results (still relevant)

Pure pass-through, single host, Postgres 16 in Docker, Windows 10:

| Workload                  | Direct TPS | via pgrouter TPS | Overhead |
|---------------------------|-----------:|-----------------:|---------:|
| c=1 t=1000 extended       |       1927 |             1472 |   23.6 % |
| c=4 t=2500 extended       |       4236 |             4329 |  -2.2 %* |
| c=1 t=2000 simple         |       1480 |             1073 |   27.5 % |

\* concurrent workload — parallelism amortizes per-message decode cost; small "negative" overhead is noise.

Single-client overhead is per-message `pgproto3` decode → re-encode in the forwarder. Buffered raw forwarding (post-MVP) is expected to halve this.

Full JSON: `bench_results.json`.

## What's wired vs primitive-only

The MVP delivered every architectural primitive: SCRAM auth, TLS, pool, txn-boundary detection, GUC + prep tracking, cancel routing, Prometheus. Each is independently tested.

**Wired into `pgrouter run --config`:**
- YAML config + validation
- Listener + per-conn goroutine
- Single-upstream proxy (the first configured database)
- TLS termination when configured
- Prometheus `/metrics` + `/healthz` HTTP endpoint

**Built + tested, not yet wired into `run`:**
- `internal/pool.Manager` per-(db, user) routing (the dispatcher needs to read StartupMessage, look up the right pool, hand off to `PooledConn.Serve`)
- `PooledConn.Serve` transaction-mode dispatcher (works end-to-end via the `internal/client` tests)
- `internal/auth` Userlist-driven server-side handshake (works via the `internal/client/conn_auth_test.go` SCRAM + MD5 tests)
- `internal/cancel.Tracker` per-client (PID, secret) allocation
- `internal/client.GUCCache.ReplayQuery` on backend acquire
- `internal/client.PrepareCache` → per-backend prepared-statement memoization

Each of these is a 10-30 LOC orchestration in `cmd/pgrouter/main.go`'s `cmdRun`. The compositional pieces are all built + tested independently.

## PgBouncer feature parity gap (post-MVP)

Honest comparison: we hit ≈ PgBouncer baseline for the single-backend pooling case. We do NOT yet have:

- Admin console (`SHOW POOLS`, `PAUSE`, `RESUME`)
- LDAP / PAM / HBA-file auth
- `pg_hba.conf` matcher
- SIGHUP hot reload of the YAML config
- Multi-tenant scale (1000s of upstreams)
- Read/write split (deferred to v1.0)
- Sharding (deferred to v1.0)

See `../postgres-pooler.md` for the full roadmap.

## Architecture

```
cmd/pgrouter            main + signal handling + slog wiring + subcommands
internal/
  auth/                 SCRAM-SHA-256, MD5, userlist, client+server handshakes
  backend/              Upstream pgwire dial + Lifecycle + ResetState + HealthCheck
  cancel/               Tracker (PID/secret -> upstream) + ForwardCancel
  client/               Startup handler + TLS upgrade + ClientState +
                        GUCCache + PrepareCache + PooledConn (txn-mode dispatcher)
  config/               YAML schema + Load + Validate + defaults
  listener/             TCP accept + TLS upgrade helpers + CertStore (reload)
  pool/                 Pool (Acquire/Release/Evict) + Manager (per-key registry)
  proto/                pgproto3 wrapper: ClientSide + ServerSide + Forward helpers +
                        sync.Pool buffer reuse + fuzz harness
  stats/                Prometheus Metrics + HTTP /metrics + /healthz
test/
  bench/                pgbench-equivalent in Go (pgx)
  handshake/            Full handshake verifier (no psql needed)
  integration/          Real Postgres end-to-end tests
  poke/                 Quick TCP+SSL+startup smoke client
deploy/
  docker-compose.test.yml   PG 15 + PG 16 on :25515 / :25516
examples/
  configs/{basic,session-mode,multi-pool}.yaml
  poc/demo.sh                Reproducible PoC demo
.github/workflows/ci.yml      build + unit + integration matrix + lint
.golangci.yml                 errcheck/govet/staticcheck/revive/...
```

## License

Apache-2.0
