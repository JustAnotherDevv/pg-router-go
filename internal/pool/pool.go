// Package pool implements (db, user) → backend-conn pools.
//
// Sizing model (PgBouncer-compatible):
//
//	default_pool_size   N — regular checkout slots per pool
//	min_pool_size       M — backends kept warm even when idle (M <= N)
//	reserve_pool_size   R — emergency slots for traffic spikes
//	reserve_pool_timeout T — how long a client may wait for a regular
//	                        slot before the reserve becomes spendable.
//
// Acquire path:
//   1. If an idle backend exists, pop + mark active + return.
//   2. Else if active < N, dial a fresh backend (counted against N).
//   3. Else park in the FIFO wait queue. If we've waited longer than T
//      and active < N+R, dial a reserve backend (counted against R).
//   4. Else keep waiting until a Release fires or ctx/timeout fires.
//
// Release path: optional reset (DISCARD ALL) → push to idle stack OR
// hand off to next waiter, preserving FIFO and the active-slot invariant.
//
// Eviction (janitor):
//   - Drop idle backends past ServerIdle threshold, BUT never below
//     MinPoolSize backends total (active + idle).
//   - Drop any backend past ServerLifetime unconditionally — that's the
//     hard recycle cap.

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
	// ErrAcquireTimeout is returned when the wait queue times out
	// before a backend is available (driven by Pool.QueryWait or ctx).
	ErrAcquireTimeout = errors.New("pool: acquire timeout")
	// ErrDrainTimeout is returned by Close(ctx) when active checkouts
	// haven't released by ctx deadline.
	ErrDrainTimeout = errors.New("pool: drain timeout")
)

// Dialer is the factory used by the pool to spawn fresh backends. Tests
// inject a mock; production wires `backend.Dial`.
type Dialer func(ctx context.Context) (*backend.Conn, error)

// Callbacks lets callers observe Acquire wait time, dial events,
// eviction counts without coupling Pool to the metrics package.
// Any nil function is a no-op.
type Callbacks struct {
	OnAcquireWait func(pool string, d time.Duration)
	OnDial        func(pool string)
	OnDialError   func(pool string, err error)
	OnEvict       func(pool string, n int)
}

// Config is the per-pool sizing + timeouts.
type Config struct {
	DefaultPoolSize    int           // N: regular slots
	MinPoolSize        int           // M: keep-warm floor (<= N)
	ReservePoolSize    int           // R: emergency overflow slots
	ReservePoolTimeout time.Duration // T: wait before reserve unlocks
	QueryWait          time.Duration // total time Acquire may block
	ServerIdle         time.Duration // idle backend eviction threshold
	ServerLifetime     time.Duration // max age before recycle
	Log                *slog.Logger
	Callbacks          Callbacks
}

// Pool manages backends for one (db, user) tuple.
//
// Concurrency: a single mutex guards `idle`, `active`, `waiters` and
// counters. The mutex is unheld during dial + reset I/O.
type Pool struct {
	cfg  Config
	dial Dialer
	name string

	mu      sync.Mutex
	idle    []*pooledConn // LIFO stack of idle backends
	active  int           // count currently checked out
	waiters []*waiter     // FIFO queue
	closed  bool

	// Conditional variable used by drainAndClose to learn when the
	// active count has fallen to zero.
	cond *sync.Cond

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
	parkedAt time.Time
}

// New builds a pool with the given dialer + config.
func New(name string, dial Dialer, cfg Config) *Pool {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.DefaultPoolSize < 1 {
		cfg.DefaultPoolSize = 1
	}
	if cfg.MinPoolSize < 0 {
		cfg.MinPoolSize = 0
	}
	if cfg.MinPoolSize > cfg.DefaultPoolSize {
		cfg.MinPoolSize = cfg.DefaultPoolSize
	}
	if cfg.ReservePoolSize < 0 {
		cfg.ReservePoolSize = 0
	}
	if cfg.ReservePoolTimeout <= 0 {
		cfg.ReservePoolTimeout = 5 * time.Second
	}
	p := &Pool{cfg: cfg, dial: dial, name: name}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Name is the (db, user) key this pool is registered under.
func (p *Pool) Name() string { return p.name }

// Acquire returns a backend ready for use, blocking until one is
// available or ctx fires.
func (p *Pool) Acquire(ctx context.Context) (*backend.Conn, error) {
	start := time.Now()
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
		p.emitWait(start)
		return pc.Conn, nil
	}

	// Room to dial?
	if p.active < p.cfg.DefaultPoolSize {
		return p.dialWithLockHeld(ctx, start)
	}

	// Park in wait queue.
	w := &waiter{
		ch:       make(chan *pooledConn, 1),
		canceled: make(chan struct{}),
		parkedAt: time.Now(),
	}
	p.waiters = append(p.waiters, w)
	p.mu.Unlock()

	// Block on the wait channel, with optional reserve-pool kick at T.
	totalTimeout := p.cfg.QueryWait
	reserveAfter := p.cfg.ReservePoolTimeout
	useReserve := p.cfg.ReservePoolSize > 0
	var totalCh <-chan time.Time
	var totalT *time.Timer
	if totalTimeout > 0 {
		totalT = time.NewTimer(totalTimeout)
		totalCh = totalT.C
		defer totalT.Stop()
	}
	var reserveCh <-chan time.Time
	var reserveT *time.Timer
	if useReserve {
		reserveT = time.NewTimer(reserveAfter)
		reserveCh = reserveT.C
		defer reserveT.Stop()
	}

	for {
		select {
		case pc := <-w.ch:
			if pc == nil {
				return nil, ErrPoolClosed
			}
			p.mu.Lock()
			p.totalAcquired++
			p.mu.Unlock()
			pc.Lifecycle.MarkActive(time.Now())
			p.emitWait(start)
			return pc.Conn, nil

		case <-ctx.Done():
			p.cancelWaiter(w)
			return nil, ctx.Err()

		case <-totalCh:
			p.cancelWaiter(w)
			return nil, ErrAcquireTimeout

		case <-reserveCh:
			// reserve_pool_timeout elapsed: if we have headroom under
			// N+R, dial a reserve backend and unpark this waiter.
			reserveCh = nil // fire once
			c, ok, err := p.tryDialReserve(ctx, w, start)
			if err != nil {
				return nil, err
			}
			if ok {
				return c, nil
			}
			// Else continue blocking — the reserve was already maxed out;
			// keep waiting for a regular Release.
		}
	}
}

// dialWithLockHeld is called with p.mu held; it counts a regular-pool
// slot, releases the lock, dials, and returns the new backend.
func (p *Pool) dialWithLockHeld(ctx context.Context, start time.Time) (*backend.Conn, error) {
	p.active++
	p.totalAcquired++
	p.totalSpawned++
	p.mu.Unlock()
	if cb := p.cfg.Callbacks.OnDial; cb != nil {
		cb(p.name)
	}
	c, err := p.dial(ctx)
	if err != nil {
		if cb := p.cfg.Callbacks.OnDialError; cb != nil {
			cb(p.name, err)
		}
		p.mu.Lock()
		p.active--
		p.cond.Broadcast()
		p.mu.Unlock()
		return nil, fmt.Errorf("pool %s: dial: %w", p.name, err)
	}
	p.emitWait(start)
	return c, nil
}

// tryDialReserve attempts to allocate a reserve slot for the given
// waiter. Returns (conn, true, nil) if a reserve backend was opened.
// Returns (nil, false, nil) if no reserve headroom remains.
// Returns (nil, false, err) on dial error.
func (p *Pool) tryDialReserve(ctx context.Context, w *waiter, start time.Time) (*backend.Conn, bool, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, false, ErrPoolClosed
	}
	cap := p.cfg.DefaultPoolSize + p.cfg.ReservePoolSize
	if p.active >= cap {
		p.mu.Unlock()
		return nil, false, nil
	}
	// Remove ourselves from the waiter queue — we're satisfying ourselves.
	for i, ww := range p.waiters {
		if ww == w {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			break
		}
	}
	p.active++
	p.totalAcquired++
	p.totalSpawned++
	p.mu.Unlock()
	if cb := p.cfg.Callbacks.OnDial; cb != nil {
		cb(p.name)
	}
	c, err := p.dial(ctx)
	if err != nil {
		if cb := p.cfg.Callbacks.OnDialError; cb != nil {
			cb(p.name, err)
		}
		p.mu.Lock()
		p.active--
		p.cond.Broadcast()
		p.mu.Unlock()
		return nil, false, fmt.Errorf("pool %s: reserve dial: %w", p.name, err)
	}
	p.emitWait(start)
	return c, true, nil
}

func (p *Pool) emitWait(start time.Time) {
	if cb := p.cfg.Callbacks.OnAcquireWait; cb != nil {
		cb(p.name, time.Since(start))
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
			p.cond.Broadcast()
			p.mu.Unlock()
			return
		}
	}

	now := time.Now()
	lifecycle := backend.NewLifecycle(now) // fresh per-pooled-entry lifecycle
	lifecycle.MarkIdle(now)

	pc := &pooledConn{Conn: c, Lifecycle: lifecycle}

	p.mu.Lock()
	p.totalReleased++

	// If pool was closed while this conn was active, drop it.
	if p.closed {
		p.active--
		p.cond.Broadcast()
		p.mu.Unlock()
		_ = c.Close()
		return
	}

	// Hand off to next waiter under lock to preserve FIFO.
	// The active count is invariant across a handoff: the slot we just
	// released is being claimed by the waiter atomically. We do NOT
	// decrement active here. If we did, another goroutine could see
	// active<limit between Release and the waiter's resume and dial a
	// new backend, leaking a slot.
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
	p.cond.Broadcast()
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
	// we drain it and put it back into idle so it's not leaked. Active
	// is already pinned for it.
	select {
	case pc := <-w.ch:
		p.idle = append(p.idle, pc)
		// active was bumped by no one for this case; restore parity by
		// leaving active alone (the slot is parked in idle now).
	default:
	}
	p.mu.Unlock()
}

// Stats is a point-in-time snapshot of the pool's counters. Cheap.
type Stats struct {
	Name          string
	Active        int
	Idle          int
	Waiters       int
	TotalAcquired uint64
	TotalReleased uint64
	TotalSpawned  uint64
	TotalEvicted  uint64
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

// Close drains the pool. Idle backends are closed immediately. Active
// checkouts are allowed to release naturally; their Release just closes
// the conn instead of pooling.
//
// Idempotent. To wait for drain to complete with a deadline, use
// CloseWithDeadline.
func (p *Pool) Close() {
	_ = p.CloseWithDeadline(time.Time{})
}

// CloseWithDeadline marks the pool closed, wakes waiters with
// ErrPoolClosed, closes idle backends, and blocks until `deadline` (or
// `time.Time{}` for "no wait, return immediately"). Returns
// ErrDrainTimeout if active checkouts haven't released by deadline.
//
// Typical shutdown:
//
//	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
//	defer cancel()
//	p.CloseWithDeadline(time.Now().Add(30*time.Second))
func (p *Pool) CloseWithDeadline(deadline time.Time) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
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
		close(w.ch) // signals Acquire to error with ErrPoolClosed
	}

	if deadline.IsZero() {
		return nil
	}

	// Wait for active to drain.
	done := make(chan struct{})
	go func() {
		p.mu.Lock()
		for p.active > 0 {
			p.cond.Wait()
		}
		p.mu.Unlock()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(time.Until(deadline)):
		return ErrDrainTimeout
	}
}

// EvictIdleOnce sweeps idle backends and closes any that are past the
// configured ServerIdle / ServerLifetime threshold, BUT never reduces
// the total backend count below MinPoolSize.
//
// Returns the count of evicted backends.
func (p *Pool) EvictIdleOnce(now time.Time) int {
	maxIdle := p.cfg.ServerIdle
	maxLife := p.cfg.ServerLifetime
	if maxIdle <= 0 && maxLife <= 0 {
		return 0
	}

	p.mu.Lock()
	floor := p.cfg.MinPoolSize
	// Backends we could keep without going below the floor.
	keepCount := floor - p.active
	if keepCount < 0 {
		keepCount = 0
	}

	kept := p.idle[:0]
	var evicted []*pooledConn
	for _, pc := range p.idle {
		// Lifetime eviction is unconditional (hard recycle cap), but
		// only if maxLife is set.
		expiredLifetime := maxLife > 0 && pc.Lifecycle.ShouldRecycle(now, maxLife)
		expiredIdle := maxIdle > 0 && pc.Lifecycle.ShouldEvict(now, maxIdle)
		if !expiredLifetime && !expiredIdle {
			kept = append(kept, pc)
			continue
		}
		if expiredIdle && !expiredLifetime && len(kept) < keepCount {
			// Keep this one to satisfy MinPoolSize.
			kept = append(kept, pc)
			continue
		}
		evicted = append(evicted, pc)
	}
	p.idle = kept
	p.totalEvicted += uint64(len(evicted))
	p.mu.Unlock()

	for _, pc := range evicted {
		_ = pc.Conn.Close()
	}
	if n := len(evicted); n > 0 {
		if cb := p.cfg.Callbacks.OnEvict; cb != nil {
			cb(p.name, n)
		}
	}
	return len(evicted)
}

// EnsureWarm spawns backends until idle+active >= MinPoolSize. Returns
// the number of backends dialed. Call once per janitor sweep (alongside
// EvictIdleOnce) to keep the warm floor populated.
//
// Dial failures stop the warming but are not returned as errors —
// they'd be repeated on every sweep otherwise. They're emitted via the
// OnDialError callback.
func (p *Pool) EnsureWarm(ctx context.Context) int {
	floor := p.cfg.MinPoolSize
	if floor <= 0 {
		return 0
	}
	spawned := 0
	for {
		p.mu.Lock()
		total := p.active + len(p.idle)
		if total >= floor || p.closed {
			p.mu.Unlock()
			return spawned
		}
		p.active++ // count it provisionally
		p.totalSpawned++
		p.mu.Unlock()

		if cb := p.cfg.Callbacks.OnDial; cb != nil {
			cb(p.name)
		}
		c, err := p.dial(ctx)
		if err != nil {
			if cb := p.cfg.Callbacks.OnDialError; cb != nil {
				cb(p.name, err)
			}
			p.mu.Lock()
			p.active--
			p.cond.Broadcast()
			p.mu.Unlock()
			return spawned
		}
		// Treat as Released-from-active so it lands in idle for callers.
		p.Release(c, false)
		spawned++
	}
}
