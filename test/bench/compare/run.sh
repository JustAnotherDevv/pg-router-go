#!/usr/bin/env bash
# Side-by-side bench runner: pgbench + pgx-bench × {direct, pgbouncer, pgcat, pgrouter}
# × concurrency {1, 8, 32} × pgbench modes {-S simple, -S extended}.
#
# Run on a Linux host with docker + postgresql-client installed.
# Outputs results to results/$(date)/results.jsonl + results.md.
#
# Usage:
#   ./run.sh                         # full matrix, default duration 20s
#   DURATION=10 ./run.sh             # shorter run for smoke
#   SCALE=10 ./run.sh                # pgbench scale factor (default 10)
set -euo pipefail

DURATION=${DURATION:-20}
SCALE=${SCALE:-10}
WARMUP=${WARMUP:-5}

# Endpoint matrix. host=127.0.0.1 from the bench-runner's perspective;
# all four services bind to host ports.
declare -A PORTS=(
  [direct]=5432
  [pgbouncer]=6432
  [pgcat]=6433
  [pgrouter]=6434
)
POOLS=(direct pgbouncer pgcat pgrouter)
CONCS=(1 8 32)
MODES=("simple" "extended")  # pgbench -M flag

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
echo
echo "=== pgbench -i (scale=$SCALE) via direct ==="
PGPASSWORD=postgres pgbench -h 127.0.0.1 -p 5432 -U postgres -d postgres \
  -i -s "$SCALE" -q 2>&1 | tee -a "$LOG"

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
echo
echo "=== pgbench matrix ==="
for mode in "${MODES[@]}"; do
  for pool in "${POOLS[@]}"; do
    port=${PORTS[$pool]}
    for c in "${CONCS[@]}"; do
      j=$c   # threads = clients (1:1)
      label="pgbench  pool=$pool  mode=$mode  c=$c"
      cmd="pgbench -h 127.0.0.1 -p $port -U postgres -d postgres -S -M $mode -c $c -j $j -T $DURATION -P 5 --no-vacuum"
      printf '  %-50s ... ' "$label"
      out=$(PGPASSWORD=postgres $cmd 2>&1 | tee -a "$LOG")
      read tps lat <<<"$(parse_pgbench "$out")"
      printf 'tps=%-10s lat_avg=%-6sms\n' "$tps" "$lat"
      emit "pgbench" "select-$mode" "$pool" "$c" "$tps" "$lat" "0" "0" "0" "$cmd"
    done
  done
done

# Build pgx-bench (Go tool, already in repo).
echo
echo "=== building pgx-bench ==="
( cd ../../.. && go build -o test/bench/compare/pgx-bench ./test/bench )

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
echo "=== pgx-bench matrix (extended) ==="
TX_PER_CLIENT=2000
for pool in "${POOLS[@]}"; do
  port=${PORTS[$pool]}
  for c in "${CONCS[@]}"; do
    label="pgxbench pool=$pool  c=$c"
    cmd="./pgx-bench -dsn postgres://postgres@127.0.0.1:$port/postgres?sslmode=disable -c $c -t $TX_PER_CLIENT -mode extended -warmup 50"
    printf '  %-50s ... ' "$label"
    out=$($cmd 2>&1 | tee -a "$LOG")
    read tps p50 p95 p99 <<<"$(parse_pgxbench "$out")"
    printf 'tps=%-10s p95=%-8sms\n' "$tps" "$p95"
    emit "pgxbench" "extended" "$pool" "$c" "$tps" "0" "$p50" "$p95" "$p99" "$cmd"
  done
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
