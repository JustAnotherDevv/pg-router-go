#!/usr/bin/env bash
# Aggregate results.jsonl → markdown tables, one per (tool, mode, conc).
# When multiple rounds exist for the same (tool, mode, conc, pool), reports
# median (more robust to outliers than mean).
# Usage: ./aggregate.sh results.jsonl > results.md
set -euo pipefail

JSONL="${1:-results.jsonl}"

# Median via jq: sort, pick middle (or avg of two middles for even count).
# Returns "—" if no samples.
median() {
  local field="$1"; shift
  jq -r "$@" "$JSONL" \
    | jq -s --arg f "$field" 'map(select(. != null and . != "" and . != 0)) | sort | if length == 0 then "—" elif length % 2 == 1 then .[(length-1)/2] | tostring else (.[length/2-1] + .[length/2]) / 2 | tostring end'
}

echo "# Side-by-side pgrouter bench"
echo
echo "Postgres 16 + 4 endpoints (direct / pgbouncer / pgcat / pgrouter), trust auth, transaction mode, pool_size=20."
echo
echo "When multiple rounds exist, cells show **median** (robust to outliers). For raw per-round values see results.jsonl."
echo

# pgbench tables (TPS + avg latency).
for mode in select-simple select-extended; do
  echo "## pgbench  \`$mode\`"
  echo
  echo "TPS (higher = better)."
  echo
  echo "| clients | direct | pgbouncer | pgcat | pgrouter |"
  echo "| ------: | -----: | --------: | ----: | -------: |"
  for c in 1 8 32; do
    row="| $c "
    for pool in direct pgbouncer pgcat pgrouter; do
      v=$(jq -r --arg t "pgbench" --arg m "$mode" --arg p "$pool" --argjson c "$c" \
        'select(.tool==$t and .mode==$m and .pool==$p and .conc==$c) | .tps' "$JSONL" \
        | jq -s 'if length == 0 then "—" else sort | .[(length-1)/2] end')
      # strip quotes if string, format number to 1 decimal if numeric
      v=$(echo "$v" | tr -d '"' | awk '{ if ($1+0==$1 && $1!="") printf "%.1f", $1; else print }')
      row+="| ${v} "
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
      v=$(jq -r --arg t "pgbench" --arg m "$mode" --arg p "$pool" --argjson c "$c" \
        'select(.tool==$t and .mode==$m and .pool==$p and .conc==$c) | .lat_avg_ms' "$JSONL" \
        | jq -s 'if length == 0 then "—" else sort | .[(length-1)/2] end')
      v=$(echo "$v" | tr -d '"' | awk '{ if ($1+0==$1 && $1!="") printf "%.3f", $1; else print }')
      row+="| ${v} "
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
    tps=$(jq -r --arg p "$pool" --argjson c "$c" \
      'select(.tool=="pgxbench" and .pool==$p and .conc==$c) | .tps' "$JSONL" \
      | jq -s 'if length == 0 then "—" else sort | .[(length-1)/2] end' | tr -d '"' | awk '{ if ($1+0==$1 && $1!="") printf "%.1f", $1; else print }')
    p50=$(jq -r --arg p "$pool" --argjson c "$c" \
      'select(.tool=="pgxbench" and .pool==$p and .conc==$c) | .lat_p50_ms' "$JSONL" \
      | jq -s 'if length == 0 then "—" else sort | .[(length-1)/2] end' | tr -d '"' | awk '{ if ($1+0==$1 && $1!="") printf "%.3f", $1; else print }')
    p95=$(jq -r --arg p "$pool" --argjson c "$c" \
      'select(.tool=="pgxbench" and .pool==$p and .conc==$c) | .lat_p95_ms' "$JSONL" \
      | jq -s 'if length == 0 then "—" else sort | .[(length-1)/2] end' | tr -d '"' | awk '{ if ($1+0==$1 && $1!="") printf "%.3f", $1; else print }')
    p99=$(jq -r --arg p "$pool" --argjson c "$c" \
      'select(.tool=="pgxbench" and .pool==$p and .conc==$c) | .lat_p99_ms' "$JSONL" \
      | jq -s 'if length == 0 then "—" else sort | .[(length-1)/2] end' | tr -d '"' | awk '{ if ($1+0==$1 && $1!="") printf "%.3f", $1; else print }')
    echo "| $pool | $c | $tps | $p50 | $p95 | $p99 |"
  done
done

# ──────────────────────────────────────────────────────────────
# Pooler-relevant benchmarks: contention, storm, memory
# ──────────────────────────────────────────────────────────────

# Contention: queue wait under load (200 clients, small pool).
echo
echo "## contention  (200 clients, 500 tx each)"
echo
echo "Queue wait time when clients outnumber backends. Lower wait = better queue handling."
echo
echo "| pool | TPS | p95 total ms | p99 total ms | errors |"
echo "| --- | ---: | ---: | ---: | ---: |"
for pool in direct pgbouncer pgcat pgrouter; do
  tps=$(jq -r --arg p "$pool" \
    'select(.tool=="contention" and .pool==$p) | .tps' "$JSONL" \
    | jq -s 'if length == 0 then "—" else sort | .[(length-1)/2] end' | tr -d '"' | awk '{ if ($1+0==$1 && $1!="") printf "%.1f", $1; else print }')
  p95=$(jq -r --arg p "$pool" \
    'select(.tool=="contention" and .pool==$p) | .lat_p95_ms' "$JSONL" \
    | jq -s 'if length == 0 then "—" else sort | .[(length-1)/2] end' | tr -d '"' | awk '{ if ($1+0==$1 && $1!="") printf "%.3f", $1; else print }')
  p99=$(jq -r --arg p "$pool" \
    'select(.tool=="contention" and .pool==$p) | .lat_p99_ms' "$JSONL" \
    | jq -s 'if length == 0 then "—" else sort | .[(length-1)/2] end' | tr -d '"' | awk '{ if ($1+0==$1 && $1!="") printf "%.3f", $1; else print }')
  echo "| $pool | $tps | $p95 | $p99 | — |"
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
  v=$(jq -r --arg p "$pool" \
    'select(.tool=="storm" and .pool==$p) | .tps' "$JSONL" \
    | jq -s 'if length == 0 then "—" else sort | .[(length-1)/2] end' | tr -d '"' | awk '{ if ($1+0==$1 && $1!="") printf "%.1f", $1; else print }')
  p50=$(jq -r --arg p "$pool" \
    'select(.tool=="storm" and .pool==$p) | .lat_p50_ms' "$JSONL" \
    | jq -s 'if length == 0 then "—" else sort | .[(length-1)/2] end' | tr -d '"' | awk '{ if ($1+0==$1 && $1!="") printf "%.3f", $1; else print }')
  echo "| $pool | $v | $p50 | — |"
done
echo

# Memory: idle connection footprint.
echo "## memory  (500 idle connections)"
echo
echo "Memory usage with many idle connections. Lower = better."
echo "Note: also check \`docker stats\` output in raw.log for container-level RSS."
echo
echo "| pool | runs |"
echo "| --- | ---: |"
for pool in direct pgbouncer pgcat pgrouter; do
  n=$(jq -r --arg p "$pool" 'select(.tool=="memory" and .pool==$p) | .tps' "$JSONL" | wc -l)
  echo "| $pool | $n |"
done
echo

echo
echo "---"
echo "_Generated: $(date -u +%FT%TZ) · `uname -srm`_"
