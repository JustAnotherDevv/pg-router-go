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
	"slices"
	"sync/atomic"
	"time"

	"github.com/JustAnotherDevv/pg-router-go/internal/pool"
	"github.com/JustAnotherDevv/pg-router-go/internal/util"
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

	// Daemon owns the lifecycle (startOnce + stopOnce + stopCh + wg).
	// Shared with PrimaryMonitor + pkg/pgrouter.Server.
	util.Daemon

	// MaxLag, when > 0, excludes replicas whose lagBytes is above
	// this value from Pick's candidate set. Updated by SetMaxLag.
	maxLag atomic.Int64

	// rr is the round-robin counter used by Pick.
	rr atomic.Uint64

	// snapshot is the precomputed Pick candidate set. Rebuilt on
	// every health / lag / SetMaxLag transition; Pick reads it
	// lock-free.
	snapshot atomic.Pointer[pickSnapshot]
}

// SetMaxLag sets the per-Manager lag cap (units match Replica.lagBytes:
// seconds-of-replay-staleness in MVP). 0 = unbounded.
func (m *Manager) SetMaxLag(n int64) {
	m.maxLag.Store(n)
	m.rebuildSnapshot()
}

// NewManager builds a Manager. Caller passes pool.Pools (already
// created via the standard backend.Dial flow) â€” this package doesn't
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
	m := &Manager{
		db:               db,
		log:              log,
		replicas:         replicas,
		healthCheckEvery: healthInterval,
		checkQuery:       checkQuery,
	}
	// Seed the Pick snapshot from the initial healthy flags so unit
	// tests that don't call Start (which would also seed) can still
	// drive Pick directly.
	m.rebuildSnapshot()
	return m
}

// Replicas returns the live replica slice (read-only).
func (m *Manager) Replicas() []*Replica { return m.replicas }

// Start spawns the per-replica health-check goroutines. Also seeds
// the initial Pick snapshot (initially: every configured replica is
// optimistically healthy â€” first probe will correct).
//
// Idempotent: subsequent calls are no-ops. Without this guard a
// double-Start would spawn duplicate probe goroutines per replica and
// Stop's wg.Wait would count incorrectly.
func (m *Manager) Start() {
	m.Daemon.Start(func() {
		for _, r := range m.replicas {
			r.healthy.Store(true)
		}
		m.rebuildSnapshot()
		for _, r := range m.replicas {
			r := r
			m.Daemon.Run(func() { m.healthLoop(r) })
		}
	})
}

func (m *Manager) healthLoop(r *Replica) {
	t := time.NewTicker(m.healthCheckEvery)
	defer t.Stop()
	// One immediate probe so the first call to Pick doesn't have to
	// wait healthCheckEvery for the initial verdict.
	m.probe(r)
	for {
		select {
		case <-m.Daemon.StopCh():
			return
		case <-t.C:
			m.probe(r)
		}
	}
}

// probe runs a single health check via Acquire + tiny pgwire round
// trip. Updates r.healthy.
//
// The context is bounded by 2s OR cancelled when Manager.Stop fires
// â€” whichever comes first â€” so a probe mid-flight at shutdown
// doesn't extend the drain window.
func (m *Manager) probe(r *Replica) {
	ctx, cancel := m.probeCtx(2 * time.Second)
	defer cancel()
	c, err := r.Pool.Acquire(ctx)
	if err != nil {
		if r.healthy.Swap(false) {
			m.log.Warn("replica unhealthy (acquire failed)",
				"db", m.db, "host", r.Spec.Host, "port", r.Spec.Port,
				"err", err)
			m.rebuildSnapshot()
		}
		return
	}
	// We borrow the conn for one Send/Receive trip. Errors â†’ mark
	// unhealthy + close so the pool re-dials next time.
	err = pingConn(c, m.checkQuery)
	r.Pool.Release(c, false)
	if err != nil {
		_ = c.Close()
		if r.healthy.Swap(false) {
			m.log.Warn("replica unhealthy (ping failed)",
				"db", m.db, "host", r.Spec.Host, "port", r.Spec.Port,
				"err", err)
			m.rebuildSnapshot()
		}
		return
	}
	if !r.healthy.Swap(true) {
		m.log.Info("replica healthy",
			"db", m.db, "host", r.Spec.Host, "port", r.Spec.Port)
		m.rebuildSnapshot()
	}
}

// ErrNoHealthyReplica is returned by Pick when every replica fails
// the health gate (or the lag gate, once #126 wires it).
var ErrNoHealthyReplica = errors.New("replica: no healthy replica available")

// pickSnapshot is the precomputed candidate set served by Pick. It's
// cheap to atomic-swap on each health / lag transition so Pick can
// read lock-free even at 100k QPS.
//
// `expanded` is the weighted-round-robin ring: each replica appears
// in it exactly Spec.Weight times. Pick indexes directly into it, so
// the hot path is one atomic.Add + one mod + one slice read â€” no
// per-call O(n) scan. Build cost is paid once per health/lag
// transition (rare) instead of once per Pick (per-query).
type pickSnapshot struct {
	// cands holds the unique healthy replicas under the lag cap; nil
	// when the snapshot is empty.
	cands []*Replica
	// expanded is each replica replicated Spec.Weight times in
	// declaration order; cap is sum(weights).
	expanded []*Replica
}

// Pick returns the next replica to route a read to.
//
// Reads a precomputed candidate snapshot (atomic.Pointer) so the hot
// path doesn't allocate per call. Snapshots are rebuilt by the
// probe loops on every transition (healthy flip / lag update / start).
//
// O(1) â€” one atomic.Add + one modulo + one slice index. Previously
// O(n) with a per-call modular scan over weighted cands.
func (m *Manager) Pick() (*Replica, error) {
	snap := m.snapshot.Load()
	if snap == nil || len(snap.expanded) == 0 {
		return nil, ErrNoHealthyReplica
	}
	idx := int(m.rr.Add(1)-1) % len(snap.expanded)
	return snap.expanded[idx], nil
}

// probeCtx returns a context bounded by `timeout` AND tied to the
// Manager's stopCh â€” whichever fires first. Used by health/lag
// probes so Stop() doesn't have to wait for an in-flight 2s probe.
func (m *Manager) probeCtx(timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	go func() {
		select {
		case <-m.Daemon.StopCh():
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// rebuildSnapshot recomputes the candidate set + total weight under
// the current health + lag state, and atomically swaps it in. Called
// from probe success/failure transitions and lagProbe updates.
//
// Safe to call concurrently â€” atomic.Pointer.Store handles the
// publish; readers see either the old or new snapshot, never a
// torn one.
func (m *Manager) rebuildSnapshot() {
	maxLag := m.maxLag.Load()
	snap := &pickSnapshot{cands: make([]*Replica, 0, len(m.replicas))}
	totalWeight := 0
	for _, r := range m.replicas {
		if !r.Healthy() {
			continue
		}
		if maxLag > 0 && r.LagBytes() > maxLag {
			continue
		}
		snap.cands = append(snap.cands, r)
		totalWeight += normalizeWeight(r.Spec.Weight)
	}
	// Pre-expand into the weighted ring so Pick is O(1). Capacity
	// is exact â€” no resizes during the append loop. slices.Repeat
	// (Go 1.23+) replaces the inner counter loop.
	snap.expanded = make([]*Replica, 0, totalWeight)
	for _, r := range snap.cands {
		snap.expanded = append(snap.expanded,
			slices.Repeat([]*Replica{r}, normalizeWeight(r.Spec.Weight))...)
	}
	m.snapshot.Store(snap)
}

// normalizeWeight clamps Spec.Weight to â‰¥1 â€” Pool weight=0 means
// "default to 1" (declarative defaults are zero in YAML); negative
// weights are operator bugs and treated the same.
func normalizeWeight(w int) int {
	if w < 1 {
		return 1
	}
	return w
}
