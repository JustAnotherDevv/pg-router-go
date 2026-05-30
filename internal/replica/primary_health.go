// Per-database primary health monitor.
//
// PrimaryMonitor maintains its OWN dedicated backend conn for health
// probing — it does NOT borrow from the client-facing pool. This
// avoids two production hazards we hit in v1.0:
//
//   1. Under a client-traffic spike that exhausts the pool, the probe
//      would block on Acquire and time out, marking the primary
//      unhealthy. Failover would fire spuriously while the primary
//      was in fact perfectly fine.
//   2. Re-using pool.Manager.Get with a synthetic '_pgrouter_health_'
//      user leaked the probe pool into mgr.AllStats() — visible in
//      SHOW POOLS + Prometheus pgrouter_pool_* labels — confusing
//      operators with a fake tenant.
//
// The dedicated conn is opened on first probe and re-dialled if it
// errors out. State transitions (healthy ↔ unhealthy) are guarded by
// a mutex so the failure counter + the flag flip are atomic together
// (fixes the TOCTOU race in the original atomic.Bool/Int32 pair).

package replica

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
)

// PrimaryMonitor tracks health of one primary via a dedicated probe
// conn that lives outside the client pool.
type PrimaryMonitor struct {
	db   string
	dial func(ctx context.Context) (*backend.Conn, error)
	log  *slog.Logger

	probeQuery  string
	probeEvery  time.Duration
	maxFailures int

	// state guards the healthy flag + consecutive-failure counter
	// together so a concurrent recovery can't race with a failure
	// transition.
	state struct {
		mu      sync.Mutex
		healthy bool
		fails   int
	}

	// probeConn is the dedicated conn used by probe(). Re-dialled
	// after any failure that closes it. Guarded by probeMu so only
	// one probe runs at a time.
	probeMu   sync.Mutex
	probeConn *backend.Conn

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewPrimaryMonitor builds a monitor. `dial` is invoked to open the
// dedicated probe conn — pass the same backend.Dial-derived closure
// that the primary pool uses, so probes hit the same upstream.
//
// `maxFailures` is the consecutive failure count above which the
// primary is marked unhealthy.
func NewPrimaryMonitor(db string, dial func(context.Context) (*backend.Conn, error),
	every time.Duration, maxFailures int, probeQuery string, log *slog.Logger,
) *PrimaryMonitor {
	if log == nil {
		log = slog.Default()
	}
	if every <= 0 {
		every = 5 * time.Second
	}
	if maxFailures < 1 {
		maxFailures = 3
	}
	if probeQuery == "" {
		probeQuery = "SELECT 1"
	}
	pm := &PrimaryMonitor{
		db:          db,
		dial:        dial,
		log:         log,
		probeQuery:  probeQuery,
		probeEvery:  every,
		maxFailures: maxFailures,
		stopCh:      make(chan struct{}),
	}
	pm.state.healthy = true // optimistic — first probe will correct
	return pm
}

// Healthy returns the current primary health flag.
func (pm *PrimaryMonitor) Healthy() bool {
	pm.state.mu.Lock()
	defer pm.state.mu.Unlock()
	return pm.state.healthy
}

// Start spawns the probe goroutine.
func (pm *PrimaryMonitor) Start() {
	pm.wg.Add(1)
	go pm.loop()
}

// Stop terminates the probe goroutine + closes the probe conn.
func (pm *PrimaryMonitor) Stop() {
	pm.stopOnce.Do(func() { close(pm.stopCh) })
	pm.wg.Wait()
	pm.probeMu.Lock()
	if pm.probeConn != nil {
		_ = pm.probeConn.Close()
		pm.probeConn = nil
	}
	pm.probeMu.Unlock()
}

func (pm *PrimaryMonitor) loop() {
	defer pm.wg.Done()
	t := time.NewTicker(pm.probeEvery)
	defer t.Stop()
	pm.probe()
	for {
		select {
		case <-pm.stopCh:
			return
		case <-t.C:
			pm.probe()
		}
	}
}

// probe runs one health check using the dedicated probe conn. Re-dials
// on first call or after any conn-level error.
func (pm *PrimaryMonitor) probe() {
	pm.probeMu.Lock()
	defer pm.probeMu.Unlock()

	if pm.probeConn == nil {
		ctx, cancel := pm.probeCtx()
		defer cancel()
		c, err := pm.dial(ctx)
		if err != nil {
			pm.recordFailure("dial", err)
			return
		}
		pm.probeConn = c
	}

	if err := pingConn(pm.probeConn, pm.probeQuery); err != nil {
		_ = pm.probeConn.Close()
		pm.probeConn = nil
		pm.recordFailure("ping", err)
		return
	}
	pm.recordSuccess()
}

// probeCtx returns a context with the per-probe budget, cancelled
// early if Stop fires.
func (pm *PrimaryMonitor) probeCtx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	// Tie to stopCh so probe dials are cancelled on shutdown.
	go func() {
		select {
		case <-pm.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// recordSuccess atomically resets the failure counter and, if we were
// previously unhealthy, flips healthy=true and logs a recovery line.
func (pm *PrimaryMonitor) recordSuccess() {
	pm.state.mu.Lock()
	wasUnhealthy := !pm.state.healthy
	pm.state.fails = 0
	pm.state.healthy = true
	pm.state.mu.Unlock()
	if wasUnhealthy {
		pm.log.Info("primary recovered", "db", pm.db)
	}
}

// recordFailure atomically increments the failure counter and, if it
// crosses the threshold, flips healthy=false and logs the transition.
// The mutex eliminates the TOCTOU window the previous atomic.Bool +
// atomic.Int32 pair had (concurrent recordSuccess could end with
// healthy=false even after resetting fails=0).
func (pm *PrimaryMonitor) recordFailure(stage string, err error) {
	pm.state.mu.Lock()
	pm.state.fails++
	n := pm.state.fails
	tripped := n >= pm.maxFailures && pm.state.healthy
	if tripped {
		pm.state.healthy = false
	}
	pm.state.mu.Unlock()
	if tripped {
		pm.log.Warn("primary unhealthy (failover)",
			"db", pm.db, "consecutive_failures", n,
			"stage", stage, "err", err)
	}
}
