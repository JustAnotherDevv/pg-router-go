#!/usr/bin/env bash
# Side-by-side bench runner: pgbench + pgx-bench × {direct, pgbouncer, pgcat, pgrouter}
# × concurrency {1, 8, 32} × pgbench modes {-S simple, -S extended}.
#
# This is the Phase A splice variant of run.sh. Differences from
# test/bench/compare/run.sh:
#   - container names for docker stats are env-configurable
#     (BENCH_PG, BENCH_PGBOUNCER, BENCH_PGCAT, BENCH_PGROUTER) so
#     the same script works with the COMPOSE_PROJECT_NAME=phasea stack
#     (containers named e.g. "phasea-postgres-1") or any other project.
#   - port defaults unchanged (5432/6432/6433/6434 on 127.0.0.1).
#
# Run on a Linux host with docker + postgresql-client installed.
# Outputs results to results/$(date)/results.jsonl + results.md.
#
# Usage:
#   COMPOSE_PROJECT_NAME=phasea ./run.sh
#   DURATION=10 COMPOSE_PROJECT_NAME=phasea ./run.sh
#   SCALE=10 COMPOSE_PROJECT_NAME=phasea ./run.sh
set -uo pipefail

DURATION=${DURATION:-20}
SCALE=${SCALE:-10}
ROUNDS=${ROUNDS:-3}  # runs per (mode,conc,pool) tuple; aggregate keeps all
WARMUP=${WARMUP:-5}

# Endpoint matrix. host=127.0.0.1 from the bench-runner's perspective.
# Defaults match the new isolated project (compare-phasea/docker-compose.yml):
#   25432  postgres   (host port 5432 is taken by VPS's systemd postgres)
#   26432  pgbouncer
#   26433  pgcat
#   26434  pgrouter
: "${PORT_DIRECT:=25432}"
: "${PORT_PGBOUNCER:=26432}"
: "${PORT_PGCAT:=26433}"
: "${PORT_PGROUTER:=26434}"
declare -A PORTS=(
  [direct]="$PORT_DIRECT"
  [pgbouncer]="$PORT_PGBOUNCER"
  [pgcat]="$PORT_PGCAT"
  [pgrouter]="$PORT_PGROUTER"
)
POOLS=(direct pgbouncer pgcat pgrouter)
CONCS=(1 8 32)
MODES=("simple" "extended")  # pgbench -M flag

# Container names for docker stats. Phase A project uses
# COMPOSE_PROJECT_NAME=phasea which produces names like
# "phasea-postgres-1", "phasea-pgbouncer-1", "phasea-pgcat-1",
# "phasea-pgrouter-1". Override these via env to match your project.
: "${BENCH_PROJECT_PREFIX:=${COMPOSE_PROJECT_NAME:-phasea}}"
: "${BENCH_PG:=${BENCH_PROJECT_PREFIX}-postgres-1}"
: "${BENCH_PGBOUNCER:=${BENCH_PROJECT_PREFIX}-pgbouncer-1}"
: "${BENCH_PGCAT:=${BENCH_PROJECT_PREFIX}-pgcat-1}"
: "${BENCH_PGROUTER:=${BENCH_PROJECT_PREFIX}-pgrouter-1}"
BENCH_CONTAINERS=("$BENCH_PG" "$BENCH_PGBOUNCER" "$BENCH_PGCAT" "$BENCH_PGROUTER")

OUT_DIR="results/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT_DIR"
JSONL="$OUT_DIR/results.jsonl"
MD="$OUT_DIR/results.md"
LOG="$OUT_DIR/raw.log"
: > "$JSONL"
: > "$LOG"

echo "=== bench config ==="
echo "  duration:  ${DURATION}s per run"
echo "  scale:     $SCALE (~$((SCALE * 15)) MB)"
echo "  warmup:    ${WARMUP}s"
echo "  pools:     ${POOLS[*]}"
echo "  concs:     ${CONCS[*]}"
echo "  modes:     ${MODES[*]}"
echo "  project:   ${BENCH_PROJECT_PREFIX}"
echo "  postgres:  $BENCH_PG"
echo "  pgbouncer: $BENCH_PGBOUNCER"
echo "  pgcat:     $BENCH_PGCAT"
echo "  pgrouter:  $BENCH_PGROUTER"
echo "  output:    $OUT_DIR"
echo

# Wait for all endpoints to accept connections.
echo "=== waiting for endpoints ==="
for p in "${POOLS[@]}"; do
  port=${PORTS[$p]}
  for i in {1..60}; do
    if pg_isready -h 127.0.0.1 -p "$port" -U postgres -d postgres -q 2>/dev/null; then
      echo "  $p:$port  ready"
      break
    fi
    sleep 1
    if [[ $i -eq 60 ]]; then
      echo "  $p:$port  TIMEOUT" >&2
      exit 1
    fi
  done
done

# pgbench -i once via direct connection (initializes pgbench_* tables in postgres).
# Skip via SKIP_PGBENCH=1 if tables already initialized.
if [ "${SKIP_PGBENCH:-0}" != "1" ]; then
  echo
  echo "=== pgbench -i (scale=$SCALE) via direct ==="
  PGPASSWORD=postgres pgbench -h 127.0.0.1 -p "${PORTS[direct]}" -U postgres -d postgres \
    -i -s "$SCALE" -q 2>&1 | tee -a "$LOG"
fi

# Helper: append a JSON object to JSONL (uses jq).
emit() {
  jq -nc \
    --arg tool "$1" \
    --arg mode "$2" \
    --arg pool "$3" \
    --argjson conc "$4" \
    --argjson tps "$5" \
    --argjson lat_avg_ms "$6" \
    --argjson lat_p50_ms "${7:-0}" \
    --argjson lat_p95_ms "${8:-0}" \
    --argjson lat_p99_ms "${9:-0}" \
    --arg cmd "${10}" \
    '{tool:$tool,mode:$mode,pool:$pool,conc:$conc,tps:$tps,lat_avg_ms:$lat_avg_ms,lat_p50_ms:$lat_p50_ms,lat_p95_ms:$lat_p95_ms,lat_p99_ms:$lat_p99_ms,cmd:$cmd}' \
    >> "$JSONL"
}

# Parse pgbench output: extract tps + latency average.
parse_pgbench() {
  local out="$1"
  local tps lat
  tps=$(echo "$out" | awk -F'= ' '/^tps =/{gsub(/ .*/,"",$2); print $2}' | head -1)
  lat=$(echo "$out" | awk -F'= ' '/^latency average/{gsub(/ ms.*/,"",$2); print $2}' | head -1)
  echo "${tps:-0} ${lat:-0}"
}

# pgbench runs (SELECT-only, two protocol modes).
# Interleaved: for each (mode,conc), all 4 poolers run back-to-back to even
# out VPS drift. Each (mode,conc,pool) is repeated ROUNDS times; aggregate
# keeps all samples and reports median/mean.
echo
echo "=== pgbench matrix (interleaved poolers, $ROUNDS rounds) ==="
if [ "${SKIP_PGBENCH:-0}" != "1" ]; then
  for mode in "${MODES[@]}"; do
    for c in "${CONCS[@]}"; do
      j=$c   # threads = clients (1:1)
      for round in $(seq 1 "$ROUNDS"); do
        for pool in "${POOLS[@]}"; do
          port=${PORTS[$pool]}
          label="pgbench  pool=$pool  mode=$mode  c=$c  r=$round"
          cmd="pgbench -h 127.0.0.1 -p $port -U postgres -d postgres -S -M $mode -c $c -j $j -T $DURATION -P 5 --no-vacuum"
          printf '  %-50s ... ' "$label"
          out=$(PGPASSWORD=postgres $cmd 2>&1) || {
            echo "FAILED (exit $?): $(echo "$out" | tail -3)" | tee -a "$LOG"
            emit "pgbench" "select-$mode" "$pool" "$c" "0" "0" "0" "0" "0" "$cmd"
            continue
          }
          # Log only the final summary lines (skip per-5s progress to keep raw.log small).
          echo "$out" | awk '/^tps =|^latency average|^SQL|^number of transactions/' >> "$LOG"
          read tps lat <<<"$(parse_pgbench "$out")"
          printf 'tps=%-10s lat_avg=%-6sms\n' "$tps" "$lat"
          emit "pgbench" "select-$mode" "$pool" "$c" "$tps" "$lat" "0" "0" "0" "$cmd"
        done
      done
    done
  done
fi

# Build pgx-bench (Go tool, already in repo).
echo
echo "=== building pgx-bench ==="
( cd ../../.. && go build -o test/bench/compare-phasea/pgx-bench ./test/bench )

# Parse pgx-bench output: tps + p50/p95/p99.
parse_pgxbench() {
  local out="$1"
  local tps p50 p95 p99
  tps=$(echo "$out"  | awk '/^  tps /{print $2}')
  p50=$(echo "$out"  | awk '/^  p50 latency/{print $3}')
  p95=$(echo "$out"  | awk '/^  p95 latency/{print $3}')
  p99=$(echo "$out"  | awk '/^  p99 latency/{print $3}')
  # convert "1.234ms" / "456µs" / "1.2s" to milliseconds (numeric)
  ms() {
    local v="$1"
    case "$v" in
      *µs)  awk -v x="${v%µs}" 'BEGIN{printf "%.4f", x/1000}' ;;
      *ms)  awk -v x="${v%ms}" 'BEGIN{printf "%.4f", x}'      ;;
      *s)   awk -v x="${v%s}"  'BEGIN{printf "%.4f", x*1000}' ;;
      *)    echo "0" ;;
    esac
  }
  echo "${tps:-0} $(ms "$p50") $(ms "$p95") $(ms "$p99")"
}

echo
echo "=== pgx-bench matrix (extended, interleaved poolers, $ROUNDS rounds) ==="
TX_PER_CLIENT=2000
if [ "${SKIP_PGXBENCH:-0}" != "1" ]; then
  for c in "${CONCS[@]}"; do
    for round in $(seq 1 "$ROUNDS"); do
      for pool in "${POOLS[@]}"; do
        port=${PORTS[$pool]}
        label="pgxbench pool=$pool  c=$c  r=$round"
        cmd="./pgx-bench -dsn postgres://postgres@127.0.0.1:$port/postgres?sslmode=disable -c $c -t $TX_PER_CLIENT -mode extended -warmup 50"
        printf '  %-50s ... ' "$label"
        # PGPASSWORD=postgres required by pgcat (others use trust); harmless otherwise.
        out=$(PGPASSWORD=postgres $cmd 2>&1) || {
          echo "FAILED (exit $?): $(echo "$out" | tail -3)" | tee -a "$LOG"
          emit "pgxbench" "extended" "$pool" "$c" "0" "0" "0" "0" "0" "$cmd"
          continue
        }
        echo "$out" >> "$LOG"
        read tps p50 p95 p99 <<<"$(parse_pgxbench "$out")"
        printf 'tps=%-10s p95=%-8sms\n' "$tps" "$p95"
        emit "pgxbench" "extended" "$pool" "$c" "$tps" "0" "$p50" "$p95" "$p99" "$cmd"
      done
    done
  done
fi

# ──────────────────────────────────────────────────────────────
# Pooler-relevant benchmarks: contention, storm, memory
# ──────────────────────────────────────────────────────────────

# Contention: 200 clients, each runs 500 tx of SELECT pg_sleep(0.01).
# With pool_size=20, only 20 run at a time → 180 wait in queue.
echo
echo "=== contention matrix (200 clients, 500 tx each, $ROUNDS rounds) ==="
CONTENTION_CONC=200
CONTENTION_TX=500
for round in $(seq 1 "$ROUNDS"); do
  for pool in "${POOLS[@]}"; do
    port=${PORTS[$pool]}
    label="contention pool=$pool  c=$CONTENTION_CONC  r=$round"
    cmd="./pgx-bench -dsn postgres://postgres@127.0.0.1:$port/postgres?sslmode=disable -c $CONTENTION_CONC -t $CONTENTION_TX -mode contention"
    printf '  %-50s ... ' "$label"
    out=$(PGPASSWORD=postgres $cmd 2>&1) || {
      echo "FAILED (exit $?): $(echo "$out" | tail -3)" | tee -a "$LOG"
      emit "contention" "contention" "$pool" "$CONTENTION_CONC" "0" "0" "0" "0" "0" "$cmd"
      continue
    }
    echo "$out" >> "$LOG"
    read tps p50 p95 p99 <<<"$(parse_pgxbench "$out")"
    printf 'tps=%-10s p95=%-8sms\n' "$tps" "$p95"
    emit "contention" "contention" "$pool" "$CONTENTION_CONC" "$tps" "0" "$p50" "$p95" "$p99" "$cmd"
  done
done

# Storm: 50 clients × 100 reconnects each. Measures connection setup overhead.
echo
echo "=== storm matrix (50 clients, 100 reconnects each, $ROUNDS rounds) ==="
STORM_CONC=50
STORM_RECONNECTS=100
for round in $(seq 1 "$ROUNDS"); do
  for pool in "${POOLS[@]}"; do
    port=${PORTS[$pool]}
    label="storm pool=$pool  c=$STORM_CONC  r=$round"
    cmd="./pgx-bench -dsn postgres://postgres@127.0.0.1:$port/postgres?sslmode=disable -c $STORM_CONC -t $STORM_RECONNECTS -mode storm"
    printf '  %-50s ... ' "$label"
    out=$(PGPASSWORD=postgres $cmd 2>&1) || {
      echo "FAILED (exit $?): $(echo "$out" | tail -3)" | tee -a "$LOG"
      emit "storm" "storm" "$pool" "$STORM_CONC" "0" "0" "0" "0" "0" "$cmd"
      continue
    }
    echo "$out" >> "$LOG"
    read tps p50 p95 p99 <<<"$(parse_pgxbench "$out")"
    printf 'tps=%-10s p95=%-8sms\n' "$tps" "$p95"
    emit "storm" "storm" "$pool" "$STORM_CONC" "$tps" "0" "$p50" "$p95" "$p99" "$cmd"
  done
done

# Memory: 500 idle connections. Check RSS per container.
echo
echo "=== memory matrix (500 idle connections) ==="
MEM_CONC=500
for pool in "${POOLS[@]}"; do
  port=${PORTS[$pool]}
  label="memory pool=$pool  c=$MEM_CONC"
  cmd="./pgx-bench -dsn postgres://postgres@127.0.0.1:$port/postgres?sslmode=disable -c $MEM_CONC -t 0 -mode extended"
  printf '  %-50s ... ' "$label"
  out=$(PGPASSWORD=postgres $cmd 2>&1) || {
    echo "FAILED (exit $?): $(echo "$out" | tail -3)" | tee -a "$LOG"
    emit "memory" "idle" "$pool" "$MEM_CONC" "0" "0" "0" "0" "0" "$cmd"
    continue
  }
  echo "$out" >> "$LOG"
  peak_rss=$(echo "$out" | awk '/^  peak RSS/{print $3}')
  per_conn=$(echo "$out" | awk '/^  per-conn cost/{print $3}')
  printf 'peak_rss=%-10s per_conn=%s\n' "${peak_rss:-?}" "${per_conn:-?}"
  echo "  container memory with $MEM_CONC idle connections:"
  docker stats --no-stream --format "{{.Name}}: {{.MemUsage}}" "${BENCH_CONTAINERS[@]}" 2>/dev/null | tee -a "$LOG" || true
  echo
  emit "memory" "idle" "$pool" "$MEM_CONC" "0" "0" "0" "0" "0" "$cmd"
done

# Aggregate JSON → markdown.
echo
echo "=== aggregating ==="
./aggregate.sh "$JSONL" > "$MD"
cat "$MD"
echo
echo "Done. Results: $OUT_DIR/"
echo "  raw log:    $LOG"
echo "  jsonl:      $JSONL"
echo "  markdown:   $MD"
