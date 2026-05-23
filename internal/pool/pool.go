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
//  1. If an idle backend exists, pop + mark active + return.
//  2. Else if active < N, dial a fresh backend (counted against N).
//  3. Else park in the FIFO wait queue. If we've waited longer than T
//     and active < N+R, dial a reserve backend (counted against R).
//  4. Else keep waiting until a Release fires or ctx/timeout fires.
//
// Release path: optional reset (DISCARD ALL) → push to idle channel OR
// hand off to next waiter, preserving FIFO and the active-slot invariant.
//
// Concurrency model:
//   - Hot path (Acquire idle-pop, Release-to-idle): lock-free via
//     buffered channel + atomic counters.
//   - Slow path (dial, waiter park, eviction, resize, close):
//     serialized by wmu (waiter mutex).
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
// Concurrency: the hot path (idle-pop on Acquire, push-to-idle on
// Release) is lock-free using a buffered channel + atomic counters.
// The slow path (dial, waiter queue, eviction, resize, close) is
// serialized by wmu.
type Pool struct {
	cfg  Config
	dial Dialer
	name string

	// Hot-path fields — accessed atomically, no lock needed.
	idle   chan *pooledConn // buffered chan, cap = DefaultPoolSize
	active atomic.Int64     // currently checked-out backends
	closed atomic.Bool      // set by Close, checked on Acquire/Release

	// Slow-path fields — guarded by wmu.
	wmu     sync.Mutex  // protects waiters, eviction, resize, close
	waiters []*waiter   // FIFO queue
	cond    *sync.Cond  // for drain coordination in CloseWithDeadline

	// Metrics-friendly counters — accessed atomically.
	totalAcquired atomic.Uint64
	totalReleased atomic.Uint64
	totalSpawned  atomic.Uint64
	totalEvicted  atomic.Uint64
	waitersCount  atomic.Int64 // approximate waiter count for Stats

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
	Lifecycle backend.Lifecycle
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
	p := &Pool{
		cfg:  cfg,
		dial: dial,
		name: name,
		idle: make(chan *pooledConn, cfg.DefaultPoolSize),
	}
	p.cond = sync.NewCond(&p.wmu)
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
	p.wmu.Lock()
	prev := p.cfg.DefaultPoolSize
	p.cfg.DefaultPoolSize = newSize
	if p.cfg.MinPoolSize > newSize {
		p.cfg.MinPoolSize = newSize
	}
	// Drain idle channel, close excess, rebuild channel with new cap.
	var kept []*pooledConn
loop:
	for {
		select {
		case pc := <-p.idle:
			kept = append(kept, pc)
		default:
			break loop
		}
	}
	curActive := int(p.active.Load())
	excess := (curActive + len(kept)) - newSize
	for excess > 0 && len(kept) > 0 {
		go kept[0].Conn.Close()
		kept = kept[1:]
		excess--
		p.totalEvicted.Add(1)
	}
	p.idle = make(chan *pooledConn, newSize)
	for _, pc := range kept {
		p.idle <- pc
	}
	p.wmu.Unlock()
	p.cond.Broadcast()
	return prev
}

// Size returns the current DefaultPoolSize cap. Useful for the admin
// API + config-reload diff display.
func (p *Pool) Size() int {
	p.wmu.Lock()
	defer p.wmu.Unlock()
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

// acquireInternal is the Acquire body. PreAcquire / PostRelease
// orchestration is handled by the public Acquire wrapper above.
//
// Three paths, in order of fast→slow:
//  1. Idle channel pop (no dial, no wait) — lock-free.
//  2. Dial under DefaultPoolSize cap (dial, no wait).
//  3. Park in wait queue with QueryWait timeout + reserve-pool kick.
func (p *Pool) acquireInternal(ctx context.Context) (*backend.Conn, error) {
	start := time.Now()

	for {
		// Fast path: pop from idle channel (lock-free).
		if p.closed.Load() {
			return nil, ErrPoolClosed
		}
		select {
		case pc := <-p.idle:
			p.active.Add(1)
			p.totalAcquired.Add(1)
			pc.Lifecycle.MarkActive(time.Now())
			p.emitWait(start)
			return pc.Conn, nil
		default:
		}

		// Slow path: need to dial or wait.
		// CAS loop: only one goroutine wins the "dial" race.
		cur := int(p.active.Load())
		if cur < p.cfg.DefaultPoolSize {
			if p.active.CompareAndSwap(int64(cur), int64(cur+1)) {
				p.totalAcquired.Add(1)
				p.totalSpawned.Add(1)
				if cb := p.cfg.Callbacks.OnDial; cb != nil {
					cb(p.name)
				}
				c, err := p.dial(ctx)
				if err != nil {
					if cb := p.cfg.Callbacks.OnDialError; cb != nil {
						cb(p.name, err)
					}
					p.active.Add(-1)
					p.cond.Broadcast()
					return nil, fmt.Errorf("pool %s: dial: %w", p.name, err)
				}
				p.captureParams(c)
				p.emitWait(start)
				return c, nil
			}
			// Lost the CAS race — loop back and try idle again.
			continue
		}

		w := p.parkWaiter()
		return p.waitForBackend(ctx, w, start)
	}
}

// parkWaiter creates a waiter, enqueues it under wmu, and returns it.
func (p *Pool) parkWaiter() *waiter {
	w := &waiter{
		ch:       make(chan *pooledConn, 1),
		canceled: make(chan struct{}),
		parkedAt: time.Now(),
	}
	p.wmu.Lock()
	p.waiters = append(p.waiters, w)
	p.wmu.Unlock()
	p.waitersCount.Add(1)
	return w
}

// waitForBackend blocks on the waiter channel until one of:
//   - Release hands us a backend → return it.
//   - ctx fires → cancel + return ctx.Err.
//   - QueryWait elapses → cancel + ErrAcquireTimeout.
//   - ReservePoolTimeout elapses → attempt a reserve dial, return on
//     success, otherwise resume waiting.
func (p *Pool) waitForBackend(ctx context.Context, w *waiter, start time.Time) (*backend.Conn, error) {
	totalCh, stopTotal := p.timerCh(p.cfg.QueryWait)
	defer stopTotal()
	useReserve := p.cfg.ReservePoolSize > 0
	var reserveCh <-chan time.Time
	var stopReserve func()
	if useReserve {
		reserveCh, stopReserve = p.timerCh(p.cfg.ReservePoolTimeout)
		defer stopReserve()
	}
	for {
		select {
		case pc := <-w.ch:
			p.waitersCount.Add(-1)
			if pc == nil {
				return nil, ErrPoolClosed
			}
			p.totalAcquired.Add(1)
			pc.Lifecycle.MarkActive(time.Now())
			p.emitWait(start)
			return pc.Conn, nil

		case <-ctx.Done():
			p.waitersCount.Add(-1)
			p.cancelWaiter(w)
			return nil, ctx.Err()

		case <-totalCh:
			p.waitersCount.Add(-1)
			p.cancelWaiter(w)
			return nil, ErrAcquireTimeout

		case <-reserveCh:
			// reserve_pool_timeout elapsed: if we have headroom under
			// N+R, dial a reserve backend and unpark this waiter.
			reserveCh = nil // fire once
			c, ok, err := p.tryDialReserve(ctx, w, start)
			if err != nil {
				p.waitersCount.Add(-1)
				return nil, err
			}
			if ok {
				p.waitersCount.Add(-1)
				return c, nil
			}
			// Else continue blocking — the reserve was already maxed out;
			// keep waiting for a regular Release.
		}
	}
}

// timerCh returns a channel that fires after d, plus a stop fn. When
// d ≤ 0 the channel is nil (never fires) + stop is a no-op.
func (p *Pool) timerCh(d time.Duration) (<-chan time.Time, func()) {
	if d <= 0 {
		return nil, func() {}
	}
	t := time.NewTimer(d)
	return t.C, func() { t.Stop() }
}

// tryDialReserve attempts to allocate a reserve slot for the given
// waiter. Returns (conn, true, nil) if a reserve backend was opened.
// Returns (nil, false, nil) if no reserve headroom remains.
// Returns (nil, false, err) on dial error.
func (p *Pool) tryDialReserve(ctx context.Context, w *waiter, start time.Time) (*backend.Conn, bool, error) {
	p.wmu.Lock()
	if p.closed.Load() {
		p.wmu.Unlock()
		return nil, false, ErrPoolClosed
	}
	cap := p.cfg.DefaultPoolSize + p.cfg.ReservePoolSize
	if int(p.active.Load()) >= cap {
		p.wmu.Unlock()
		return nil, false, nil
	}
	// Remove ourselves from the waiter queue — we're satisfying ourselves.
	for i, ww := range p.waiters {
		if ww == w {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			break
		}
	}
	p.wmu.Unlock()
	p.active.Add(1)
	p.totalAcquired.Add(1)
	p.totalSpawned.Add(1)
	if cb := p.cfg.Callbacks.OnDial; cb != nil {
		cb(p.name)
	}
	c, err := p.dial(ctx)
	if err != nil {
		if cb := p.cfg.Callbacks.OnDialError; cb != nil {
			cb(p.name, err)
		}
		p.active.Add(-1)
		p.cond.Broadcast()
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

	// Optional reset BEFORE returning to pool — it does I/O.
	if resetSession {
		if err := c.ResetStateWith(p.cfg.ResetQuery); err != nil {
			p.cfg.Log.Warn("backend reset failed; discarding", "err", err)
			_ = c.Close()
			p.active.Add(-1)
			p.cond.Broadcast()
			return
		}
	}

	now := time.Now()
	pc := &pooledConn{Conn: c}
	pc.Lifecycle.Init(now)
	pc.Lifecycle.MarkIdle(now)

	// If pool was closed while this conn was active, drop it.
	if p.closed.Load() {
		p.active.Add(-1)
		p.cond.Broadcast()
		_ = c.Close()
		return
	}

	// Hand off to next waiter under wmu to preserve FIFO.
	// The active count is invariant across a handoff: the slot we just
	// released is being claimed by the waiter atomically. We do NOT
	// decrement active here. If we did, another goroutine could see
	// active<limit between Release and the waiter's resume and dial a
	// new backend, leaking a slot.
	p.wmu.Lock()
	for len(p.waiters) > 0 {
		w := p.waiters[0]
		p.waiters = p.waiters[1:]
		p.wmu.Unlock()
		select {
		case w.ch <- pc:
			p.totalReleased.Add(1)
			return
		case <-w.canceled:
			// already gone; try next waiter
			p.wmu.Lock()
		}
	}
	p.wmu.Unlock()

	// No waiters: return to idle channel (lock-free).
	// IMPORTANT: push to idle BEFORE decrementing active to avoid a
	// race where another goroutine sees active < poolSize and dials.
	p.totalReleased.Add(1)
	sent := false
	select {
	case p.idle <- pc:
		sent = true
	default:
	}
	p.active.Add(-1)
	p.cond.Broadcast()
	if !sent {
		_ = c.Close()
	}
}

// cancelWaiter removes `w` from the queue. Called from ctx.Done /
// timeout paths. We mark `canceled` so a racing Release can skip past.
func (p *Pool) cancelWaiter(w *waiter) {
	close(w.canceled)
	p.wmu.Lock()
	for i, ww := range p.waiters {
		if ww == w {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			break
		}
	}
	// If a backend was racing to hand us a conn between cancel + dequeue,
	// we drain it and put it back into idle so it's not leaked.
	p.wmu.Unlock()
	select {
	case pc := <-w.ch:
		// Return to idle channel (or close if full/closed).
		if p.closed.Load() {
			_ = pc.Conn.Close()
		} else {
			select {
			case p.idle <- pc:
			default:
				_ = pc.Conn.Close()
			}
		}
	default:
	}
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

// Stats returns a snapshot. All fields are read atomically — no lock
// needed for the common case.
func (p *Pool) Stats() Stats {
	return Stats{
		Name:          p.name,
		Active:        int(p.active.Load()),
		Idle:          len(p.idle),
		Waiters:       int(p.waitersCount.Load()),
		TotalAcquired: p.totalAcquired.Load(),
		TotalReleased: p.totalReleased.Load(),
		TotalSpawned:  p.totalSpawned.Load(),
		TotalEvicted:  p.totalEvicted.Load(),
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
	if p.closed.Swap(true) {
		return nil // already closed
	}

	var idle []*pooledConn
loop:
	for {
		select {
		case pc := <-p.idle:
			idle = append(idle, pc)
		default:
			break loop
		}
	}

	p.wmu.Lock()
	waiters := p.waiters
	p.waiters = nil
	p.wmu.Unlock()

	for _, pc := range idle {
		_ = pc.Conn.Close()
	}
	for _, w := range waiters {
		close(w.ch) // signals Acquire to error with ErrPoolClosed
	}

	if deadline.IsZero() {
		return nil
	}

	done := make(chan struct{})
	go func() {
		p.wmu.Lock()
		for p.active.Load() > 0 {
			p.cond.Wait()
		}
		p.wmu.Unlock()
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

	p.wmu.Lock()
	floor := p.cfg.MinPoolSize
	curActive := int(p.active.Load())
	// Backends we could keep without going below the floor.
	keepCount := floor - curActive
	if keepCount < 0 {
		keepCount = 0
	}

	var all []*pooledConn
loop:
	for {
		select {
		case pc := <-p.idle:
			all = append(all, pc)
		default:
			break loop
		}
	}

	var kept, evicted []*pooledConn
	for _, pc := range all {
		expiredLifetime := maxLife > 0 && pc.Lifecycle.ShouldRecycle(now, maxLife)
		expiredIdle := maxIdle > 0 && pc.Lifecycle.ShouldEvict(now, maxIdle)
		if !expiredLifetime && !expiredIdle {
			kept = append(kept, pc)
			continue
		}
		if expiredIdle && !expiredLifetime && len(kept) < keepCount {
			kept = append(kept, pc)
			continue
		}
		evicted = append(evicted, pc)
	}

	p.idle = make(chan *pooledConn, p.cfg.DefaultPoolSize)
	for _, pc := range kept {
		p.idle <- pc
	}
	p.totalEvicted.Add(uint64(len(evicted)))
	p.wmu.Unlock()

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
		total := int(p.active.Load()) + len(p.idle)
		if total >= floor || p.closed.Load() {
			return spawned
		}
		p.active.Add(1)
		p.totalSpawned.Add(1)

		if cb := p.cfg.Callbacks.OnDial; cb != nil {
			cb(p.name)
		}
		c, err := p.dial(ctx)
		if err != nil {
			if cb := p.cfg.Callbacks.OnDialError; cb != nil {
				cb(p.name, err)
			}
			p.active.Add(-1)
			p.cond.Broadcast()
			return spawned
		}
		p.captureParams(c)
		// Treat as Released-from-active so it lands in idle for callers.
		p.Release(c, false)
		spawned++
	}
}

// captureParams stores the upstream's ParameterStatus map on the first
// successful dial. Subsequent dials are no-ops: the first upstream's
// values "win" (matches PgBouncer's behaviour). Safe to call from any
// dial path.
func (p *Pool) captureParams(c *backend.Conn) {
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
