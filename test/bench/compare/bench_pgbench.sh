#!/bin/bash
set -uo pipefail

DURATION=${DURATION:-20}
declare -A PORTS=([direct]=15432 [pgbouncer]=16432 [pgcat]=16433 [pgrouter]=16434)
POOLS=(direct pgbouncer pgcat pgrouter)
CONCS=(1 8 32)
MODES=(simple extended)

echo '{"results":[' > /tmp/results.jsonl
FIRST=true

for mode in "${MODES[@]}"; do
  for pool in "${POOLS[@]}"; do
    for conc in "${CONCS[@]}"; do
      port=${PORTS[$pool]}

      # Warmup 5s
      pgbench -h 127.0.0.1 -p "$port" -U postgres -c "$conc" -j "$conc" -S -M "$mode" --time 5 postgres >/dev/null 2>&1

      # Actual run
      OUT=$(pgbench -h 127.0.0.1 -p "$port" -U postgres -c "$conc" -j "$conc" -S -M "$mode" --time "$DURATION" postgres 2>&1)

      TPS=$(echo "$OUT" | grep 'tps = ' | head -1 | sed 's/.*tps = \([0-9.]*\).*/\1/')
      LATENCY=$(echo "$OUT" | grep 'latency average' | sed 's/.*= \([0-9.]*\).*/\1/')

      if [ "$FIRST" = true ]; then FIRST=false; else echo ',' >> /tmp/results.jsonl; fi
      echo "{\"pool\":\"$pool\",\"mode\":\"$mode\",\"conc\":$conc,\"tps\":$TPS,\"latency_ms\":$LATENCY}" >> /tmp/results.jsonl

      echo "$pool | $mode | c=$conc | TPS=$TPS | lat=$LATENCY ms"
    done
  done
done

echo ']' >> /tmp/results.jsonl
