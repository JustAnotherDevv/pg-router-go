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

echo
echo "---"
echo "_Generated: $(date -u +%FT%TZ) · `uname -srm`_"
