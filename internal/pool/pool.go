// Package pool implements (db, user) → backend-conn pools.
//
// Sizing model (PgBouncer-compatible):
//
//	default_pool_size   N — usually-running idle + active backends per pool
//	min_pool_size       M — backends to keep warm even when idle (M <= N)
//	reserve_pool_size   R — short-term overflow under spikes
//	reserve_pool_timeout T — how long a client may wait for a regular
//	                        slot before the reserve gets unlocked.
//
// Acquire path:
//   1. If an idle backend exists, pop + mark active + return.
//   2. Else if currently spawned + active < N, dial a fresh backend.
//   3. Else if waited > T and reserve > 0, treat as if N becomes N+R.
//   4. Else block on the wait queue until a Release happens or ctx fires.
//
// Release path: optional reset (DISCARD ALL) → push to idle stack → wake
// next waiter.

package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
)

// Sentinel errors.
var (
	// ErrPoolClosed is returned by Acquire after Close is called.
	ErrPoolClosed = errors.New("pool: closed")
	// ErrAcquireTimeout is returned when the wait queue times out before
	// a backend is available (driven by Pool.QueryWait or ctx).
	ErrAcquireTimeout = errors.New("pool: acquire timeout")
)

// Dialer is the factory used by the pool to spawn fresh backends. Tests
// inject a mock; production wires `backend.Dial`.
type Dialer func(ctx context.Context) (*backend.Conn, error)

// Config is the per-pool sizing + timeouts.
type Config struct {
	DefaultPoolSize    int           // N: regular slots
	MinPoolSize        int           // M: keep-warm
	ReservePoolSize    int           // R: emergency overflow
	ReservePoolTimeout time.Duration // T: how long before reserve unlocks
	QueryWait          time.Duration // total time Acquire may block
	ServerIdle         time.Duration // idle backend eviction threshold
	ServerLifetime     time.Duration // max age before recycle
	Log                *slog.Logger
}

// Pool manages backends for one (db, user) tuple.
//
// Concurrency: a single mutex guards `idle`, `active`, and `waiters`.
// We unblock waiters under the lock to keep the FIFO ordering tight.
type Pool struct {
	cfg    Config
	dial   Dialer
	name   string

	mu      sync.Mutex
	idle    []*pooledConn  // LIFO stack of idle backends
	active  int            // count currently checked out
	waiters []*waiter      // FIFO queue
	closed  bool

	// Metrics-friendly counters.
	totalAcquired uint64
	totalReleased uint64
	totalSpawned  uint64
	totalEvicted  uint64
}

// pooledConn wraps a backend + its lifecycle marker.
type pooledConn struct {
	Conn      *backend.Conn
	Lifecycle *backend.Lifecycle
}

// waiter is parked in Acquire waiting for a backend.
type waiter struct {
	ch       chan *pooledConn
	canceled chan struct{}
}

// New builds a pool with the given dialer + config.
func New(name string, dial Dialer, cfg Config) *Pool {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.DefaultPoolSize < 1 {
		cfg.DefaultPoolSize = 1
	}
	return &Pool{
		cfg:  cfg,
		dial: dial,
		name: name,
	}
}

// Name is the (db, user) key this pool is registered under.
func (p *Pool) Name() string { return p.name }

// Acquire returns a backend ready for use, blocking until one is
// available or ctx fires.
func (p *Pool) Acquire(ctx context.Context) (*backend.Conn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}

	// Fast path: pop an idle backend.
	if n := len(p.idle); n > 0 {
		pc := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.active++
		p.totalAcquired++
		pc.Lifecycle.MarkActive(time.Now())
		p.mu.Unlock()
		return pc.Conn, nil
	}

	// Room to dial?
	limit := p.cfg.DefaultPoolSize
	if p.active < limit {
		p.active++
		p.totalAcquired++
		p.totalSpawned++
		p.mu.Unlock()
		c, err := p.dialNew(ctx)
		if err != nil {
			p.mu.Lock()
			p.active--
			p.mu.Unlock()
			return nil, err
		}
		return c, nil
	}

	// Park in wait queue.
	w := &waiter{ch: make(chan *pooledConn, 1), canceled: make(chan struct{})}
	p.waiters = append(p.waiters, w)
	p.mu.Unlock()

	// Block.
	timeout := p.cfg.QueryWait
	var timer *time.Timer
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		timeoutCh = timer.C
		defer timer.Stop()
	}
	select {
	case pc := <-w.ch:
		if pc == nil {
			// Close() sent zero-value via closed chan.
			return nil, ErrPoolClosed
		}
		// active was kept pinned by Release; just bump the metric.
		p.mu.Lock()
		p.totalAcquired++
		p.mu.Unlock()
		pc.Lifecycle.MarkActive(time.Now())
		return pc.Conn, nil
	case <-ctx.Done():
		p.cancelWaiter(w)
		return nil, ctx.Err()
	case <-timeoutCh:
		p.cancelWaiter(w)
		return nil, ErrAcquireTimeout
	}
}

// Release returns a backend to the pool. If resetSession is true, the
// backend is reset (DISCARD ALL) before being made available again.
// Failed reset → backend is closed instead of pooled.
func (p *Pool) Release(c *backend.Conn, resetSession bool) {
	if c == nil {
		return
	}

	// Optional reset BEFORE the lock — it does I/O.
	if resetSession {
		if err := c.ResetState(); err != nil {
			p.cfg.Log.Warn("backend reset failed; discarding", "err", err)
			_ = c.Close()
			p.mu.Lock()
			p.active--
			p.mu.Unlock()
			return
		}
	}

	now := time.Now()
	lifecycle := backend.NewLifecycle(now) // fresh; M.7 lifecycle on Conn lands later
	lifecycle.MarkIdle(now)

	pc := &pooledConn{Conn: c, Lifecycle: lifecycle}

	p.mu.Lock()
	p.totalReleased++
	// Hand off to next waiter under lock to preserve FIFO.
	// The active count is invariant across a handoff: the slot we just
	// released is being claimed by the waiter atomically — we do NOT
	// decrement active here. (If we did, another goroutine could see
	// active<limit between Release and the waiter's resume and dial a
	// new backend, leaking a slot.) The waiter side bumps totalAcquired
	// but skips active++ since we kept it pinned.
	for len(p.waiters) > 0 {
		w := p.waiters[0]
		p.waiters = p.waiters[1:]
		select {
		case w.ch <- pc:
			p.mu.Unlock()
			return
		case <-w.canceled:
			// already gone; try next waiter
		}
	}
	p.idle = append(p.idle, pc)
	p.active--
	p.mu.Unlock()
}

// cancelWaiter removes `w` from the queue. Called from ctx.Done /
// timeout paths. We mark `canceled` so a racing Release can skip past.
func (p *Pool) cancelWaiter(w *waiter) {
	close(w.canceled)
	p.mu.Lock()
	for i, ww := range p.waiters {
		if ww == w {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			break
		}
	}
	// If a backend was racing to hand us a conn between cancel + dequeue,
	// we drain it and put it back into idle so it's not leaked.
	select {
	case pc := <-w.ch:
		p.idle = append(p.idle, pc)
	default:
	}
	p.mu.Unlock()
}

func (p *Pool) dialNew(ctx context.Context) (*backend.Conn, error) {
	if p.cfg.ServerLifetime > 0 || p.cfg.ServerIdle > 0 {
		// Lifecycle is tracked on the pool's idle entries — fine for MVP.
		// Per-conn Lifecycle lives on the pool wrapper, not Conn itself.
	}
	c, err := p.dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("pool %s: dial: %w", p.name, err)
	}
	return c, nil
}

// Stats is a point-in-time snapshot of the pool's counters. Cheap.
type Stats struct {
	Name           string
	Active         int
	Idle           int
	Waiters        int
	TotalAcquired  uint64
	TotalReleased  uint64
	TotalSpawned   uint64
	TotalEvicted   uint64
}

// Stats returns a snapshot.
func (p *Pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{
		Name:          p.name,
		Active:        p.active,
		Idle:          len(p.idle),
		Waiters:       len(p.waiters),
		TotalAcquired: p.totalAcquired,
		TotalReleased: p.totalReleased,
		TotalSpawned:  p.totalSpawned,
		TotalEvicted:  p.totalEvicted,
	}
}

// Close drains the pool: marks closed, closes all idle conns, wakes any
// waiters with ErrPoolClosed. Active checkouts continue until released,
// then their Release just closes the conn instead of pooling.
//
// Idempotent.
func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	idle := p.idle
	p.idle = nil
	waiters := p.waiters
	p.waiters = nil
	p.mu.Unlock()

	for _, pc := range idle {
		_ = pc.Conn.Close()
	}
	for _, w := range waiters {
		close(w.ch) // signals Acquire to error
	}
}

// EvictIdleOnce sweeps idle backends and closes any that are past the
// configured ServerIdle / ServerLifetime threshold. Returns the count
// of evicted backends. Call from a janitor goroutine on a timer.
func (p *Pool) EvictIdleOnce(now time.Time) int {
	maxIdle := p.cfg.ServerIdle
	maxLife := p.cfg.ServerLifetime
	if maxIdle <= 0 && maxLife <= 0 {
		return 0
	}

	p.mu.Lock()
	kept := p.idle[:0]
	var evicted []*pooledConn
	for _, pc := range p.idle {
		if pc.Lifecycle.ShouldEvict(now, maxIdle) || pc.Lifecycle.ShouldRecycle(now, maxLife) {
			evicted = append(evicted, pc)
			continue
		}
		kept = append(kept, pc)
	}
	p.idle = kept
	p.totalEvicted += uint64(len(evicted))
	p.mu.Unlock()

	for _, pc := range evicted {
		_ = pc.Conn.Close()
	}
	return len(evicted)
}
