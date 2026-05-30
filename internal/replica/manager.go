// Per-database replica registry + health/lag pollers.
//
// Each Database with replicas configured gets one Manager instance
// that owns:
//
//   - a pool.Pool per replica (so the same pgrouter Acquire/Release
//     machinery works for replica connections)
//   - one goroutine per replica running periodic SELECT 1 health pings
//     (consumed by #124)
//   - one goroutine per replica running periodic lag polls via
//     `pg_last_wal_replay_lsn()` vs the primary's
//     `pg_current_wal_lsn()` (consumed by #125)
//
// Pick(weighted-RR over healthy replicas under the lag cap) is the
// hook used by the request router (consumed by #126).

package replica

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/pool"
)

// ReplicaSpec is the static description supplied by config.
type ReplicaSpec struct {
	Host   string
	Port   int
	Weight int
}

// Replica holds the live state of one replica entry.
type Replica struct {
	Spec ReplicaSpec
	Pool *pool.Pool

	// healthy is set/cleared by the health-check goroutine.
	healthy atomic.Bool
	// lagBytes is updated by the lag-poll goroutine (#125).
	lagBytes atomic.Int64
}

// Healthy returns the current health flag (true after the first
// successful check; false on the first failure of a configured run).
func (r *Replica) Healthy() bool { return r.healthy.Load() }

// LagBytes returns the most recent WAL-bytes-behind reading.
func (r *Replica) LagBytes() int64 { return r.lagBytes.Load() }

// Manager owns the replica pool slice for one database.
type Manager struct {
	db       string
	log      *slog.Logger
	replicas []*Replica

	healthCheckEvery time.Duration
	checkQuery       string

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup

	// MaxLag, when > 0, excludes replicas whose lagBytes is above
	// this value from Pick's candidate set. Updated by SetMaxLag.
	maxLag atomic.Int64

	// rr is the round-robin counter used by Pick.
	rr atomic.Uint64
}

// SetMaxLag sets the per-Manager lag cap (units match Replica.lagBytes:
// seconds-of-replay-staleness in MVP). 0 = unbounded.
func (m *Manager) SetMaxLag(n int64) { m.maxLag.Store(n) }

// NewManager builds a Manager. Caller passes pool.Pools (already
// created via the standard backend.Dial flow) — this package doesn't
// know how to dial.
func NewManager(db string, replicas []*Replica, healthInterval time.Duration, checkQuery string, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	if healthInterval <= 0 {
		healthInterval = 5 * time.Second
	}
	if checkQuery == "" {
		checkQuery = "SELECT 1"
	}
	return &Manager{
		db:               db,
		log:              log,
		replicas:         replicas,
		healthCheckEvery: healthInterval,
		checkQuery:       checkQuery,
		stopCh:           make(chan struct{}),
	}
}

// Replicas returns the live replica slice (read-only).
func (m *Manager) Replicas() []*Replica { return m.replicas }

// Start spawns the per-replica health-check goroutines.
func (m *Manager) Start() {
	for _, r := range m.replicas {
		r := r
		m.wg.Add(1)
		go m.healthLoop(r)
	}
}

// Stop signals all goroutines to exit and waits for them.
func (m *Manager) Stop() {
	m.stopOnce.Do(func() { close(m.stopCh) })
	m.wg.Wait()
}

func (m *Manager) healthLoop(r *Replica) {
	defer m.wg.Done()
	t := time.NewTicker(m.healthCheckEvery)
	defer t.Stop()
	// One immediate probe so the first call to Pick doesn't have to
	// wait healthCheckEvery for the initial verdict.
	m.probe(r)
	for {
		select {
		case <-m.stopCh:
			return
		case <-t.C:
			m.probe(r)
		}
	}
}

// probe runs a single health check via Acquire + tiny pgwire round
// trip. Updates r.healthy.
func (m *Manager) probe(r *Replica) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := r.Pool.Acquire(ctx)
	if err != nil {
		if r.healthy.Swap(false) {
			m.log.Warn("replica unhealthy (acquire failed)",
				"db", m.db, "host", r.Spec.Host, "port", r.Spec.Port,
				"err", err)
		}
		return
	}
	// We borrow the conn for one Send/Receive trip. Errors → mark
	// unhealthy + close so the pool re-dials next time.
	err = pingConn(c, m.checkQuery)
	r.Pool.Release(c, false)
	if err != nil {
		_ = c.Close()
		if r.healthy.Swap(false) {
			m.log.Warn("replica unhealthy (ping failed)",
				"db", m.db, "host", r.Spec.Host, "port", r.Spec.Port,
				"err", err)
		}
		return
	}
	if !r.healthy.Swap(true) {
		m.log.Info("replica healthy",
			"db", m.db, "host", r.Spec.Host, "port", r.Spec.Port)
	}
}

// ErrNoHealthyReplica is returned by Pick when every replica fails
// the health gate (or the lag gate, once #126 wires it).
var ErrNoHealthyReplica = errors.New("replica: no healthy replica available")

// Pick returns the next replica to route a read to.
//
// Filters out: unhealthy replicas; replicas whose lagBytes is above
// the configured maxLag (when > 0). Weighted RR over the survivors.
func (m *Manager) Pick() (*Replica, error) {
	maxLag := m.maxLag.Load()
	var totalWeight int
	cands := make([]*Replica, 0, len(m.replicas))
	for _, r := range m.replicas {
		if !r.Healthy() {
			continue
		}
		if maxLag > 0 && r.LagBytes() > maxLag {
			continue
		}
		cands = append(cands, r)
		totalWeight += r.Spec.Weight
	}
	if len(cands) == 0 {
		return nil, ErrNoHealthyReplica
	}
	idx := int(m.rr.Add(1)-1) % totalWeight
	for _, r := range cands {
		if idx < r.Spec.Weight {
			return r, nil
		}
		idx -= r.Spec.Weight
	}
	return cands[0], nil
}

