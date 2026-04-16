# pgrouter

Modern PostgreSQL connection pooler written in Go.

**Status: v0.1.0 — PoC** (pass-through proxy). Not production-ready.

## What this PoC does

```
┌──────────┐   pgwire v3   ┌───────────┐   pgwire v3   ┌──────────┐
│ Postgres │──────────────▶│  pgrouter │──────────────▶│ Postgres │
│  client  │◀──────────────│   (Go)    │◀──────────────│  server  │
└──────────┘   :6432       └───────────┘   :5432       └──────────┘
   psql, pgx,                  this PoC                 trust auth
   GORM, sqlx                                           required
```

- Accepts TCP on `--listen`
- Declines SSL/GSS cleanly
- Parses startup
- Opens upstream backend on demand (trust auth only)
- Bidirectional message forwarding (simple + extended protocol)
- Cleanly closes both sides

Per-client backend (no pool yet). One Go goroutine per client connection,
one per backend connection.

## What it does NOT do yet

- Real authentication (SCRAM, MD5 — MVP M.5)
- TLS / SSL (MVP M.4)
- Connection pooling (MVP M.6–M.9)
- Prepared statement cache (MVP M.11)
- GUC session tracking (MVP M.10)
- Cancel routing (MVP M.12)
- Observability — Prometheus / OTel (MVP M.13)
- Hot config reload, admin console, multi-tenant, R/W split (v1.0)

Full plan in `../postgres-pooler.md`.

## Build

Requires Go 1.26+.

```sh
go build -o bin/pgrouter ./cmd/pgrouter
go build -o bin/bench    ./test/bench
go build -o bin/handshake ./test/handshake
go build -o bin/poke     ./test/poke
```

## Run

Start any Postgres on `:5432` with `trust` auth (or use the dev container
below), then:

```sh
./bin/pgrouter --listen :6432 --backend localhost:5432
```

In another terminal:

```sh
psql -h localhost -p 6432 -U test -d test
# or any pgx/GORM/sqlx app pointed at :6432
```

### Trust-auth dev Postgres in Docker

```sh
docker run -d --name pgrouter-pg --rm \
  -e POSTGRES_PASSWORD=testpw -e POSTGRES_USER=test -e POSTGRES_DB=test \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -p 25432:5432 postgres:16

./bin/pgrouter --listen :6432 --backend 127.0.0.1:25432
```

## Demo

End-to-end test using the bundled `handshake` client (no `psql` required):

```sh
./bin/handshake :6432 test test
```

Expected output (subset):

```
[ssl decline] = 'N'
[AuthenticationOk]
[ParameterStatus] server_version = "16.4 (...)"
[ParameterStatus] client_encoding = "UTF8"
...
[BackendKeyData] pid=... secret_hex=...
[ReadyForQuery] tx_status='I'
HANDSHAKE COMPLETE
```

## Tests

```sh
# Unit tests (no Postgres required).
go test -short ./...

# Integration tests (need Postgres on PGROUTER_DSN; default :25432).
PGROUTER_DSN="postgres://test@127.0.0.1:25432/test?sslmode=disable" \
  go test -tags=integration ./test/integration/...

# Through proxy:
PGROUTER_DSN="postgres://test@127.0.0.1:6432/test?sslmode=disable" \
  go test -tags=integration ./test/integration/...
```

Integration covers: `SimpleSelect`, `ParameterizedQuery`, `PreparedStatement`,
`MultiRow`, `Transaction`.

## PoC bench results

Pure pass-through. Postgres 16 in Docker, single host, Windows 10
(clock resolution ~1 ms — sub-ms p50 readings get clamped).

| Workload                  | Direct TPS | via pgrouter TPS | Overhead |
|---------------------------|-----------:|-----------------:|---------:|
| c=1 t=1000 extended       |       1927 |             1472 |   23.6 % |
| c=4 t=2500 extended       |       4236 |             4329 |  -2.2 %* |
| c=1 t=2000 simple         |       1480 |             1073 |   27.5 % |

\* concurrent workload — parallelism amortizes per-message decode cost; small
"negative" overhead is noise.

Single-client overhead is the per-message `pgproto3` decode → re-encode
round trip in the forwarder. MVP **M.2** replaces this with buffered raw
forwarding (expected to cut single-client overhead in half).

Re-run:

```sh
./bin/bench -dsn "postgres://test@127.0.0.1:25432/test?sslmode=disable" \
            -c 1 -t 1000 -mode extended
```

Full JSON: `bench_results.json`.

## PoC verification checklist

| Capability | Verified by |
|---|---|
| TCP listen + per-conn goroutine        | `internal/listener/*_test.go`, live `bin/poke` |
| Parse `StartupMessage`                 | `TestStartupMessageParsed`, live `bin/handshake` |
| Decline SSL / GSS                      | `TestSSLRequestDeclinedThenStartup`, `TestGSSEncRequestDeclined` |
| Synthesize Auth handshake (trust)      | `TestStartupResponseSequence`, live handshake |
| Cancel request parse                   | `TestCancelRequestLogged` |
| Open upstream backend                  | `TestDialTrust` |
| Bidirectional message forwarding       | `test/integration/poc_proxy_test.go` × 5 |
| Extended protocol (Parse/Bind/Execute) | `TestPreparedStatement`, `TestParameterizedQuery` |
| pgbench-equivalent throughput          | `bench_results.json` |

## Architecture (PoC)

```
cmd/pgrouter            main + signal handling + slog wiring
internal/
  listener/             TCP accept loop, per-conn goroutine
  handler/              PoC per-client handler (startup + proxy loop)
  backend/              Upstream pgwire dial (trust auth)
  depcheck/             Test-only: keeps deps direct
test/
  poke/                 Quick "TCP+SSL+startup" smoke client
  handshake/            Full handshake verifier (no psql needed)
  bench/                pgbench-equivalent in Go (pgx)
  integration/          Real Postgres / pgrouter end-to-end tests
```

## License

Apache-2.0
