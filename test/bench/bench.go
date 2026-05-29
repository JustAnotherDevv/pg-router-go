// Command bench measures simple-query and extended-protocol throughput
// vs a Postgres backend. Stand-in for pgbench since we can't always
// reach pgbench cleanly on Windows.
//
// Each "client" goroutine opens its own connection, runs N iterations
// of "SELECT 1+1" (simple) or "SELECT $1::int" (extended), and records
// per-query latency. We report TPS + p50 / p95 / p99 latency.
//
// Usage:
//   bench -dsn "postgres://test@127.0.0.1:25432/test?sslmode=disable" -c 1 -t 1000 -mode extended
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	dsn := flag.String("dsn", "postgres://test@127.0.0.1:25432/test?sslmode=disable", "Postgres / pgrouter DSN")
	clients := flag.Int("c", 1, "concurrent clients (goroutines)")
	tx := flag.Int("t", 1000, "transactions per client")
	mode := flag.String("mode", "extended", "simple | extended")
	warmup := flag.Int("warmup", 50, "warmup iterations per client (not measured)")
	flag.Parse()

	fmt.Printf("dsn=%s clients=%d tx_per_client=%d mode=%s\n",
		*dsn, *clients, *tx, *mode)

	var (
		wg          sync.WaitGroup
		errCount    atomic.Int64
		latenciesMu sync.Mutex
		latencies   = make([]time.Duration, 0, (*clients)*(*tx))
	)

	start := time.Now()
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			conn, err := pgx.Connect(ctx, *dsn)
			if err != nil {
				fmt.Fprintf(os.Stderr, "client %d: connect: %v\n", id, err)
				errCount.Add(1)
				return
			}
			defer conn.Close(ctx)

			localLat := make([]time.Duration, 0, *tx)

			run := func(measure bool) {
				var n int
				var t0 time.Time
				if measure {
					t0 = time.Now()
				}
				switch *mode {
				case "simple":
					err = conn.QueryRow(ctx, "SELECT 1+1").Scan(&n)
				default: // extended
					arg := rand.Intn(1_000_000)
					err = conn.QueryRow(ctx, "SELECT $1::int", arg).Scan(&n)
				}
				if measure {
					localLat = append(localLat, time.Since(t0))
				}
				if err != nil {
					errCount.Add(1)
				}
			}

			for j := 0; j < *warmup; j++ {
				run(false)
			}
			for j := 0; j < *tx; j++ {
				run(true)
			}

			latenciesMu.Lock()
			latencies = append(latencies, localLat...)
			latenciesMu.Unlock()
		}(i)
	}
	wg.Wait()
	wall := time.Since(start)

	if len(latencies) == 0 {
		fmt.Fprintln(os.Stderr, "no successful queries")
		os.Exit(1)
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	pct := func(p float64) time.Duration {
		idx := int(float64(len(latencies)-1) * p)
		return latencies[idx]
	}

	total := int64(len(latencies))
	tps := float64(total) / wall.Seconds()
	fmt.Println()
	fmt.Printf("RESULTS\n")
	fmt.Printf("  wall          %v\n", wall.Round(time.Millisecond))
	fmt.Printf("  total tx      %d\n", total)
	fmt.Printf("  errors        %d\n", errCount.Load())
	fmt.Printf("  tps           %.1f\n", tps)
	fmt.Printf("  p50 latency   %v\n", pct(0.50).Round(time.Microsecond))
	fmt.Printf("  p95 latency   %v\n", pct(0.95).Round(time.Microsecond))
	fmt.Printf("  p99 latency   %v\n", pct(0.99).Round(time.Microsecond))
	fmt.Printf("  max latency   %v\n", latencies[len(latencies)-1].Round(time.Microsecond))
	fmt.Println()
}
