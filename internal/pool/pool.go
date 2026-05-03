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
	"sync/atomic"
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
//
// PreAcquire fires once at the very start of every Acquire call, BEFORE
// the pool mutex is taken. It blocks until the caller decides Acquire
// may proceed (typically used to enforce global per-db/per-user caps
// via a chan-backed semaphore). Returning a non-nil error aborts
// Acquire with that error and PostRelease is NOT called.
//
// PostRelease fires exactly once per successful PreAcquire — either at
// the next Release of the returned backend, OR when Acquire failed
// internally AFTER PreAcquire succeeded. This invariant lets callers
// use PreAcquire/PostRelease as a leak-free semaphore pair.
type Callbacks struct {
	OnAcquireWait func(pool string, d time.Duration)
	OnDial        func(pool string)
	OnDialError   func(pool string, err error)
	OnEvict       func(pool string, n int)
	PreAcquire    func(ctx context.Context) error
	PostRelease   func()
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
	// ResetQuery is sent on Release to clear per-session state. Empty
	// string means the backend's default (DISCARD ALL). Multi-statement
	// queries are honoured (e.g. "DELETE FROM tmp; DISCARD ALL").
	ResetQuery string
	Log        *slog.Logger
	Callbacks  Callbacks
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

	// cachedParams: the ParameterStatus map captured from the first
	// successful upstream dial. PooledConn welcomes new clients with
	// these values so things like server_version reflect the actual
	// upstream rather than a hardcoded canned default. Atomic pointer
	// lets `CachedParams` be lock-free on the hot path.
	cachedParams atomic.Pointer[map[string]string]

	// dialAttempted is true once at least one Acquire has reached the
	// dial path (successfully or not). PooledConn uses this to avoid
	// re-trying an eager-warm on every cold client when the upstream
	// is known to not emit ParameterStatus (test fakes, mostly).
	dialAttempted atomic.Bool
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

// Resize updates the live DefaultPoolSize cap. Growing wakes any
// blocked waiters; shrinking lets the janitor's next sweep evict
// excess idle backends. Returns the previous size.
//
// Active checkouts above the new cap are NOT yanked — they finish
// normally and just don't return to the idle stack on Release.
func (p *Pool) Resize(newSize int) int {
	if newSize < 1 {
		newSize = 1
	}
	p.mu.Lock()
	prev := p.cfg.DefaultPoolSize
	p.cfg.DefaultPoolSize = newSize
	// MinPoolSize must stay <= DefaultPoolSize.
	if p.cfg.MinPoolSize > newSize {
		p.cfg.MinPoolSize = newSize
	}
	// On shrink, immediately evict excess idle backends so they don't
	// hang around forever (the janitor would also do this).
	excess := (p.active + len(p.idle)) - newSize
	for excess > 0 && len(p.idle) > 0 {
		pc := p.idle[0]
		p.idle = p.idle[1:]
		go pc.Conn.Close()
		excess--
		p.totalEvicted++
	}
	p.mu.Unlock()
	// Wake parked waiters; growth may have created free slots.
	p.cond.Broadcast()
	return prev
}

// Size returns the current DefaultPoolSize cap. Useful for the admin
// API + config-reload diff display.
func (p *Pool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cfg.DefaultPoolSize
}

// Acquire returns a backend ready for use, blocking until one is
// available or ctx fires.
//
// If Callbacks.PreAcquire is set it fires first (outside the pool lock)
// and may block — typically to enforce a global per-db/per-user cap. On
// PreAcquire failure Acquire returns immediately and PostRelease is NOT
// called. On every other Acquire failure path PostRelease IS called so
// the caller's semaphore stays leak-free.
func (p *Pool) Acquire(ctx context.Context) (*backend.Conn, error) {
	if cb := p.cfg.Callbacks.PreAcquire; cb != nil {
		if err := cb(ctx); err != nil {
			return nil, err
		}
	}
	c, err := p.acquireInternal(ctx)
	if err != nil {
		if cb := p.cfg.Callbacks.PostRelease; cb != nil {
			cb()
		}
	}
	return c, err
}

// acquireInternal is the original Acquire body. PreAcquire / PostRelease
// orchestration is handled by the public Acquire wrapper above.
func (p *Pool) acquireInternal(ctx context.Context) (*backend.Conn, error) {
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
	p.captureParams(c)
	p.emitWait(start)
	return c, nil
}

// captureParams stores the upstream's ParameterStatus map on the first
// successful dial. Subsequent dials are no-ops: the first upstream's
// values "win" (matches PgBouncer's behaviour). Safe to call from any
// dial path.
func (p *Pool) captureParams(c *backend.Conn) {
	// Record that a dial completed, even if c.Params is empty — that
	// lets DialAttempted() report true and short-circuit further
	// eager-warm attempts that would never populate the cache.
	p.dialAttempted.Store(true)
	if c == nil || len(c.Params) == 0 {
		return
	}
	if p.cachedParams.Load() != nil {
		return
	}
	m := make(map[string]string, len(c.Params))
	for k, v := range c.Params {
		m[k] = v
	}
	p.cachedParams.CompareAndSwap(nil, &m)
}

// CachedParams returns the ParameterStatus map captured from the first
// successful upstream dial, or nil if no dial has succeeded yet (or
// the upstream emitted no params, in which case DialAttempted() will
// be true).
//
// PooledConn welcomes new clients with these values so server_version,
// client_encoding etc. reflect the real upstream. The returned map is
// the cache's own copy; callers must NOT mutate it.
func (p *Pool) CachedParams() map[string]string {
	if ptr := p.cachedParams.Load(); ptr != nil {
		return *ptr
	}
	return nil
}

// DialAttempted is true once at least one Acquire reached the dial
// path. Used by PooledConn to skip eager-warm when a previous dial
// already proved the upstream doesn't emit ParameterStatus.
func (p *Pool) DialAttempted() bool {
	return p.dialAttempted.Load()
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
	p.captureParams(c)
	p.emitWait(start)
	return c, true, nil
}

func (p *Pool) emitWait(start time.Time) {
	if cb := p.cfg.Callbacks.OnAcquireWait; cb != nil {
		cb(p.name, time.Since(start))
	}
}

// Release returns a backend to the pool. If resetSession is true, the
// pool's configured ResetQuery is run before pooling (defaults to
// DISCARD ALL). Failed reset → backend is closed instead of pooled.
//
// PostRelease is fired (if set) at end of Release regardless of whether
// the backend was successfully returned to idle, closed due to drain, or
// discarded due to a failed reset — the invariant is exactly one
// PostRelease per successful PreAcquire.
func (p *Pool) Release(c *backend.Conn, resetSession bool) {
	if c == nil {
		return
	}
	defer func() {
		if cb := p.cfg.Callbacks.PostRelease; cb != nil {
			cb()
		}
	}()

	// Optional reset BEFORE the lock — it does I/O.
	if resetSession {
		if err := c.ResetStateWith(p.cfg.ResetQuery); err != nil {
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
	// Clamp negative wait to 0 — time.After(negative) fires
	// immediately, which would return ErrDrainTimeout even on a
	// healthy pool. Callers passing deadline_seconds=0 (or a typo'd
	// past timestamp) get "immediate drain attempt" semantics rather
	// than instant failure.
	wait := time.Until(deadline)
	if wait < 0 {
		wait = 0
	}
	select {
	case <-done:
		return nil
	case <-time.After(wait):
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
		p.captureParams(c)
		// Treat as Released-from-active so it lands in idle for callers.
		p.Release(c, false)
		spawned++
	}
}
