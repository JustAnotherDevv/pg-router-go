#!/bin/bash
set -uo pipefail

DURATION=${DURATION:-20}
declare -A PORTS=([pgcat]=16433 [pgrouter]=16434)
POOLS=(pgcat pgrouter)
CONCS=(1 8 32)
MODES=(simple extended)

for mode in "${MODES[@]}"; do
  for pool in "${POOLS[@]}"; do
    for conc in "${CONCS[@]}"; do
      port=${PORTS[$pool]}

      # pgcat needs password auth
      AUTH=""
      if [ "$pool" = "pgcat" ]; then
        AUTH="PGPASSWORD=postgres"
      fi

      # Warmup 5s
      env $AUTH pgbench -h 127.0.0.1 -p "$port" -U postgres -c "$conc" -j "$conc" -S -M "$mode" --time 5 postgres >/dev/null 2>&1

      # Actual run
      OUT=$(env $AUTH pgbench -h 127.0.0.1 -p "$port" -U postgres -c "$conc" -j "$conc" -S -M "$mode" --time "$DURATION" postgres 2>&1)

      TPS=$(echo "$OUT" | grep 'tps = ' | head -1 | sed 's/.*tps = \([0-9.]*\).*/\1/')
      LATENCY=$(echo "$OUT" | grep 'latency average' | sed 's/.*= \([0-9.]*\).*/\1/')

      echo "$pool | $mode | c=$conc | TPS=$TPS | lat=$LATENCY ms"
    done
  done
done
