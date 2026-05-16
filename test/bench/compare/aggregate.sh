#!/usr/bin/env bash
# Aggregate results.jsonl → markdown tables, one per (tool, mode, conc).
# Usage: ./aggregate.sh results.jsonl > results.md
set -euo pipefail

JSONL="${1:-results.jsonl}"

echo "# Side-by-side pgrouter bench"
echo
echo "Postgres 16 + 4 endpoints (direct / pgbouncer / pgcat / pgrouter), trust auth, transaction mode, pool_size=20."
echo

# pgbench tables (TPS + avg latency).
for mode in select-simple select-extended; do
  echo "## pgbench  \`$mode\`"
  echo
  echo "TPS (higher = better) — averaged across the run."
  echo
  echo "| clients | direct | pgbouncer | pgcat | pgrouter |"
  echo "| ------: | -----: | --------: | ----: | -------: |"
  for c in 1 8 32; do
    row="| $c "
    for pool in direct pgbouncer pgcat pgrouter; do
      tps=$(jq -r --arg t "pgbench" --arg m "$mode" --arg p "$pool" --argjson c "$c" \
        'select(.tool==$t and .mode==$m and .pool==$p and .conc==$c) | .tps' "$JSONL" \
        | head -1)
      row+="| ${tps:-—} "
    done
    echo "$row|"
  done
  echo
  echo "Latency avg (ms, lower = better)."
  echo
  echo "| clients | direct | pgbouncer | pgcat | pgrouter |"
  echo "| ------: | -----: | --------: | ----: | -------: |"
  for c in 1 8 32; do
    row="| $c "
    for pool in direct pgbouncer pgcat pgrouter; do
      lat=$(jq -r --arg t "pgbench" --arg m "$mode" --arg p "$pool" --argjson c "$c" \
        'select(.tool==$t and .mode==$m and .pool==$p and .conc==$c) | .lat_avg_ms' "$JSONL" \
        | head -1)
      row+="| ${lat:-—} "
    done
    echo "$row|"
  done
  echo
done

# pgx-bench table (TPS + p50/p95/p99 since the Go tool computes them).
echo "## pgx-bench  \`extended\`"
echo
echo "TPS + p50/p95/p99 latency from a pgx-based custom runner (cross-checks pgbench numbers, adds tail latency)."
echo
echo "| pool | clients | TPS | p50 ms | p95 ms | p99 ms |"
echo "| --- | ---: | ---: | ---: | ---: | ---: |"
for pool in direct pgbouncer pgcat pgrouter; do
  for c in 1 8 32; do
    row=$(jq -r --arg p "$pool" --argjson c "$c" \
      'select(.tool=="pgxbench" and .pool==$p and .conc==$c) | "| \($p) | \($c) | \(.tps) | \(.lat_p50_ms) | \(.lat_p95_ms) | \(.lat_p99_ms) |"' \
      "$JSONL" | head -1)
    echo "$row"
  done
done

# ──────────────────────────────────────────────────────────────
# Pooler-relevant benchmarks: contention, storm, memory
# ──────────────────────────────────────────────────────────────

# Contention: queue wait under load (200 clients, small pool).
echo "## contention  (200 clients, 500 tx each)"
echo
echo "Queue wait time when clients outnumber backends. Lower wait = better queue handling."
echo
echo "| pool | TPS | p50 total ms | p95 total ms | p99 total ms | p50 wait ms | errors |"
echo "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"
for pool in direct pgbouncer pgcat pgrouter; do
  row=$(jq -r --arg p "$pool" \
    'select(.tool=="contention" and .pool==$p) | "| \($p) | \(.tps) | \(.lat_p95_ms) | \(.lat_p99_ms) | — | — | — |"' \
    "$JSONL" | head -1)
  # Fallback: try to extract from raw fields
  if [ -z "$row" ]; then
    row=$(jq -r --arg p "$pool" \
      'select(.tool=="contention" and .pool==$p) | "| \($p) | \(.tps) | — | — | — | — | — |"' \
      "$JSONL" | head -1)
  fi
  echo "${row:-| $pool | — | — | — | — | — | — |}"
done
echo

# Storm: connection setup+teardown rate.
echo "## storm  (50 clients, 100 reconnects each)"
echo
echo "Connection handling rate. Higher conn/sec = lower overhead."
echo
echo "| pool | conn/sec | p50 conn time ms | errors |"
echo "| --- | ---: | ---: | ---: |"
for pool in direct pgbouncer pgcat pgrouter; do
  row=$(jq -r --arg p "$pool" \
    'select(.tool=="storm" and .pool==$p) | "| \($p) | \(.tps) | \(.lat_p50_ms) | — |"' \
    "$JSONL" | head -1)
  echo "${row:-| $pool | — | — | — |}"
done
echo

# Memory: idle connection footprint.
echo "## memory  (500 idle connections)"
echo
echo "Memory usage with many idle connections. Lower = better."
echo "Note: also check \`docker stats\` output in raw.log for container-level RSS."
echo
echo "| pool | peak RSS (bench client) | per-conn cost |"
echo "| --- | ---: | ---: |"
for pool in direct pgbouncer pgcat pgrouter; do
  row=$(jq -r --arg p "$pool" \
    'select(.tool=="memory" and .pool==$p) | "| \($p) | — | — |"' \
    "$JSONL" | head -1)
  echo "${row:-| $pool | — | — |}"
done
echo

echo
echo "---"
echo "_Generated: $(date -u +%FT%TZ) · `uname -srm`_"
