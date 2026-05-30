// Per-database primary health monitor.
//
// PrimaryMonitor periodically pings the primary pool (using the same
// server_check_query as replica health) and tracks consecutive
// failures. When the failure count exceeds the threshold, the
// primary is marked unhealthy; the dispatcher then fails writes
// fast with 08006 connection_failure and lets reads fall through to
// healthy replicas.
//
// Recovery is automatic: the first successful ping after a failure
// run flips the flag back.

package replica

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/pool"
)

// PrimaryMonitor tracks health of one primary pool.
type PrimaryMonitor struct {
	db   string
	pool *pool.Pool
	log  *slog.Logger

	probeQuery   string
	probeEvery   time.Duration
	maxFailures  int

	healthy atomic.Bool
	fails   atomic.Int32

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewPrimaryMonitor builds a monitor. `maxFailures` is the consecutive
// failure count above which the primary is marked unhealthy.
func NewPrimaryMonitor(db string, p *pool.Pool, every time.Duration, maxFailures int, probeQuery string, log *slog.Logger) *PrimaryMonitor {
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
		pool:        p,
		log:         log,
		probeQuery:  probeQuery,
		probeEvery:  every,
		maxFailures: maxFailures,
		stopCh:      make(chan struct{}),
	}
	pm.healthy.Store(true) // optimistic — first probe will correct
	return pm
}

// Healthy returns the current primary health flag.
func (pm *PrimaryMonitor) Healthy() bool { return pm.healthy.Load() }

// Start spawns the probe goroutine.
func (pm *PrimaryMonitor) Start() {
	pm.wg.Add(1)
	go pm.loop()
}

// Stop terminates the probe goroutine.
func (pm *PrimaryMonitor) Stop() {
	pm.stopOnce.Do(func() { close(pm.stopCh) })
	pm.wg.Wait()
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

func (pm *PrimaryMonitor) probe() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := pm.pool.Acquire(ctx)
	if err != nil {
		pm.handleFailure("acquire", err)
		return
	}
	err = pingConn(c, pm.probeQuery)
	pm.pool.Release(c, false)
	if err != nil {
		_ = c.Close()
		pm.handleFailure("ping", err)
		return
	}
	// Success.
	if pm.fails.Swap(0) > 0 && !pm.healthy.Load() {
		pm.healthy.Store(true)
		pm.log.Info("primary recovered", "db", pm.db)
	}
}

func (pm *PrimaryMonitor) handleFailure(stage string, err error) {
	n := pm.fails.Add(1)
	if int(n) >= pm.maxFailures && pm.healthy.Swap(false) {
		pm.log.Warn("primary unhealthy (failover)",
			"db", pm.db, "consecutive_failures", n,
			"stage", stage, "err", err)
	}
}
