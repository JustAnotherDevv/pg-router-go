// Command bench measures throughput and pooler-relevant characteristics
// vs a Postgres backend or pooler.
//
// Modes:
//   simple    — SELECT 1+1 via simple protocol (default pgbench-like)
//   extended  — SELECT $1::int via extended protocol
//   contention — many clients, limited pool, measure queue wait time
//   storm     — rapid connect/disconnect cycles
//   idle      — hold N idle connections open, report memory
//
// Usage:
//   bench -dsn "postgres://test@127.0.0.1:25432/test?sslmode=disable" -c 1 -t 1000 -mode extended
//   bench -dsn "..." -c 200 -t 500 -mode contention
//   bench -dsn "..." -c 50 -t 100 -mode storm
//   bench -dsn "..." -c 1000 -hold 30 -mode idle
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
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
	mode := flag.String("mode", "extended", "simple | extended | contention | storm | idle")
	warmup := flag.Int("warmup", 50, "warmup iterations per client (not measured)")
	hold := flag.Int("hold", 30, "seconds to hold idle connections (idle mode)")
	flag.Parse()

	switch *mode {
	case "contention":
		runContention(*dsn, *clients, *tx)
	case "storm":
		runStorm(*dsn, *clients, *tx)
	case "idle":
		runIdle(*dsn, *clients, *hold)
	default:
		runThroughput(*dsn, *clients, *tx, *mode, *warmup)
	}
}

// runThroughput is the original simple/extended benchmark.
func runThroughput(dsn string, clients, tx int, mode string, warmup int) {
	fmt.Printf("dsn=%s clients=%d tx_per_client=%d mode=%s\n",
		dsn, clients, tx, mode)

	var (
		wg          sync.WaitGroup
		errCount    atomic.Int64
		latenciesMu sync.Mutex
		latencies   = make([]time.Duration, 0, clients*tx)
	)

	start := time.Now()
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				fmt.Fprintf(os.Stderr, "client %d: connect: %v\n", id, err)
				errCount.Add(1)
				return
			}
			defer conn.Close(ctx)

			localLat := make([]time.Duration, 0, tx)

			run := func(measure bool) {
				var n int
				var t0 time.Time
				if measure {
					t0 = time.Now()
				}
				switch mode {
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

			for j := 0; j < warmup; j++ {
				run(false)
			}
			for j := 0; j < tx; j++ {
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

// runContention spawns more clients than the backend pool can handle
// and measures how well the pooler queues requests.
//
// Each client runs tx queries of SELECT pg_sleep(0.01) (10ms query).
// With many clients and a small pool, most time is spent waiting in queue.
// Reports: TPS, p50/p95/p99 total query time (including queue wait),
// p50/p95/p99 queue wait estimate, errors/timeouts.
func runContention(dsn string, clients, tx int) {
	fmt.Printf("dsn=%s clients=%d tx_per_client=%d mode=contention\n",
		dsn, clients, tx)

	var (
		wg          sync.WaitGroup
		errCount    atomic.Int64
		latenciesMu sync.Mutex
		latencies   = make([]time.Duration, 0, clients*tx)
	)

	start := time.Now()
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				fmt.Fprintf(os.Stderr, "client %d: connect: %v\n", id, err)
				errCount.Add(1)
				return
			}
			defer conn.Close(ctx)

			localLat := make([]time.Duration, 0, tx)
			for j := 0; j < tx; j++ {
				t0 := time.Now()
				err = conn.QueryRow(ctx, "SELECT pg_sleep(0.01)").Scan()
				d := time.Since(t0)
				localLat = append(localLat, d)
				if err != nil {
					errCount.Add(1)
				}
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
	// Each query takes ~10ms of actual DB time. Queue wait = total latency - 10ms.
	dbTime := 10 * time.Millisecond
	waitP50 := pct(0.50) - dbTime
	waitP95 := pct(0.95) - dbTime
	waitP99 := pct(0.99) - dbTime
	if waitP50 < 0 {
		waitP50 = 0
	}
	if waitP95 < 0 {
		waitP95 = 0
	}
	if waitP99 < 0 {
		waitP99 = 0
	}

	fmt.Println()
	fmt.Printf("RESULTS\n")
	fmt.Printf("  wall            %v\n", wall.Round(time.Millisecond))
	fmt.Printf("  total tx        %d\n", total)
	fmt.Printf("  errors          %d\n", errCount.Load())
	fmt.Printf("  tps             %.1f\n", tps)
	fmt.Printf("  p50 total       %v\n", pct(0.50).Round(time.Microsecond))
	fmt.Printf("  p95 total       %v\n", pct(0.95).Round(time.Microsecond))
	fmt.Printf("  p99 total       %v\n", pct(0.99).Round(time.Microsecond))
	fmt.Printf("  max total       %v\n", latencies[len(latencies)-1].Round(time.Microsecond))
	fmt.Printf("  p50 queue wait  %v  (estimated)\n", waitP50.Round(time.Microsecond))
	fmt.Printf("  p95 queue wait  %v  (estimated)\n", waitP95.Round(time.Microsecond))
	fmt.Printf("  p99 queue wait  %v  (estimated)\n", waitP99.Round(time.Microsecond))
	fmt.Println()
}

// runStorm rapidly connects and disconnects to measure connection
// handling overhead. Each client opens a connection, runs one query,
// closes it, and repeats tx times.
func runStorm(dsn string, clients, tx int) {
	fmt.Printf("dsn=%s clients=%d iterations=%d mode=storm\n",
		dsn, clients, tx)

	var (
		wg          sync.WaitGroup
		errCount    atomic.Int64
		totalConns  atomic.Int64
		latenciesMu sync.Mutex
		latencies   = make([]time.Duration, 0, clients*tx)
	)

	start := time.Now()
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			localLat := make([]time.Duration, 0, tx)

			for j := 0; j < tx; j++ {
				t0 := time.Now()
				conn, err := pgx.Connect(ctx, dsn)
				if err != nil {
					errCount.Add(1)
					continue
				}
				var n int
				err = conn.QueryRow(ctx, "SELECT 1").Scan(&n)
				conn.Close(ctx)
				d := time.Since(t0)
				localLat = append(localLat, d)
				totalConns.Add(1)
				if err != nil {
					errCount.Add(1)
				}
			}

			latenciesMu.Lock()
			latencies = append(latencies, localLat...)
			latenciesMu.Unlock()
		}(i)
	}
	wg.Wait()
	wall := time.Since(start)

	if len(latencies) == 0 {
		fmt.Fprintln(os.Stderr, "no successful connections")
		os.Exit(1)
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	pct := func(p float64) time.Duration {
		idx := int(float64(len(latencies)-1) * p)
		return latencies[idx]
	}

	total := int64(len(latencies))
	connPerSec := float64(total) / wall.Seconds()
	fmt.Println()
	fmt.Printf("RESULTS\n")
	fmt.Printf("  wall            %v\n", wall.Round(time.Millisecond))
	fmt.Printf("  total conns     %d\n", total)
	fmt.Printf("  errors          %d\n", errCount.Load())
	fmt.Printf("  conn/sec        %.1f\n", connPerSec)
	fmt.Printf("  p50 conn time   %v\n", pct(0.50).Round(time.Microsecond))
	fmt.Printf("  p95 conn time   %v\n", pct(0.95).Round(time.Microsecond))
	fmt.Printf("  p99 conn time   %v\n", pct(0.99).Round(time.Microsecond))
	fmt.Printf("  max conn time   %v\n", latencies[len(latencies)-1].Round(time.Microsecond))
	fmt.Println()
}

// runIdle opens N connections and holds them open for holdSec seconds,
// reporting memory usage of the current process. Useful for comparing
// memory footprint of pooler vs direct Postgres with many idle clients.
func runIdle(dsn string, clients, holdSec int) {
	fmt.Printf("dsn=%s clients=%d hold=%ds mode=idle\n",
		dsn, clients, holdSec)

	ctx := context.Background()
	conns := make([]*pgx.Conn, 0, clients)
	var errCount int64

	// Phase 1: open all connections
	fmt.Printf("  opening %d connections... ", clients)
	openStart := time.Now()
	for i := 0; i < clients; i++ {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nclient %d: connect: %v\n", i, err)
			errCount++
			continue
		}
		conns = append(conns, conn)
	}
	openDur := time.Since(openStart)
	fmt.Printf("done (%d opened in %v, %d errors)\n", len(conns), openDur.Round(time.Millisecond), errCount)

	// Phase 2: hold idle, sample memory
	fmt.Printf("  holding %d idle connections for %ds...\n", len(conns), holdSec)
	var memSamples []uint64
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	holdStart := time.Now()
	done := make(chan struct{})
	go func() {
		for range ticker.C {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			memSamples = append(memSamples, m.Sys)
			elapsed := time.Since(holdStart).Seconds()
			fmt.Printf("    t=%.0fs  RSS=%.1fMB\n", elapsed, float64(m.Sys)/1024/1024)
			if elapsed >= float64(holdSec) {
				close(done)
				return
			}
		}
	}()
	<-done

	// Phase 3: close
	for _, c := range conns {
		c.Close(ctx)
	}

	// Report
	if len(memSamples) > 0 {
		var peak uint64
		var sum uint64
		for _, v := range memSamples {
			sum += v
			if v > peak {
				peak = v
			}
		}
		avg := sum / uint64(len(memSamples))
		fmt.Println()
		fmt.Printf("RESULTS\n")
		fmt.Printf("  connections     %d\n", len(conns))
		fmt.Printf("  hold duration   %ds\n", holdSec)
		fmt.Printf("  peak RSS        %.1f MB\n", float64(peak)/1024/1024)
		fmt.Printf("  avg RSS         %.1f MB\n", float64(avg)/1024/1024)
		fmt.Printf("  per-conn cost   %.1f KB\n", float64(peak)/float64(len(conns))/1024)
		fmt.Println()
	}
}
