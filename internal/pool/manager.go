package pool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
)

// Key uniquely identifies one pool. Postgres conn semantics are (db,
// user) scoped: the same `db` accessed by two different users gets two
// separate pools so per-user GUCs / RLS / auth don't leak across.
type Key struct {
	DB   string
	User string
}

func (k Key) String() string { return k.DB + "/" + k.User }

// DialerFor is the per-(db, user) dialer factory.
type DialerFor func(k Key) Dialer

// ConfigFor optionally returns a per-key Config override. If nil, the
// Manager applies its defaultCfg to every pool. Return a *Config that
// the Manager will merge over the default. Fields set to their zero
// value in the override are taken from the default.
type ConfigFor func(k Key) *Config

// Manager owns all per-(db, user) pools + the janitor goroutine that
// sweeps idle / lifetime-expired backends.
type Manager struct {
	defaultCfg Config
	dialerFor  DialerFor
	configFor  ConfigFor // optional; nil → use defaultCfg verbatim
	log        *slog.Logger

	mu    sync.RWMutex
	pools map[Key]*Pool

	janitorStop chan struct{}
	janitorDone chan struct{}
}

// NewManager builds a registry. Janitor isn't started until Start is called.
func NewManager(defaultCfg Config, dialerFor DialerFor) *Manager {
	if defaultCfg.Log == nil {
		defaultCfg.Log = slog.Default()
	}
	return &Manager{
		defaultCfg: defaultCfg,
		dialerFor:  dialerFor,
		log:        defaultCfg.Log,
		pools:      map[Key]*Pool{},
	}
}

// WithConfigFor sets a per-key Config-override factory.
//
// Returns the Manager for builder-style chaining.
func (m *Manager) WithConfigFor(fn ConfigFor) *Manager {
	m.configFor = fn
	return m
}

// mergeConfig overlays `override` (non-zero fields only) on `base`.
func mergeConfig(base Config, override *Config) Config {
	if override == nil {
		return base
	}
	out := base
	if override.DefaultPoolSize > 0 {
		out.DefaultPoolSize = override.DefaultPoolSize
	}
	if override.MinPoolSize > 0 {
		out.MinPoolSize = override.MinPoolSize
	}
	if override.ReservePoolSize > 0 {
		out.ReservePoolSize = override.ReservePoolSize
	}
	if override.ReservePoolTimeout > 0 {
		out.ReservePoolTimeout = override.ReservePoolTimeout
	}
	if override.QueryWait > 0 {
		out.QueryWait = override.QueryWait
	}
	if override.ServerIdle > 0 {
		out.ServerIdle = override.ServerIdle
	}
	if override.ServerLifetime > 0 {
		out.ServerLifetime = override.ServerLifetime
	}
	if override.Log != nil {
		out.Log = override.Log
	}
	// Callbacks are taken from the override only if non-empty (i.e. any
	// non-nil field). We OR them by field rather than struct-compare
	// because zero-valued callbacks are valid (just no-op).
	if override.Callbacks.OnAcquireWait != nil {
		out.Callbacks.OnAcquireWait = override.Callbacks.OnAcquireWait
	}
	if override.Callbacks.OnDial != nil {
		out.Callbacks.OnDial = override.Callbacks.OnDial
	}
	if override.Callbacks.OnDialError != nil {
		out.Callbacks.OnDialError = override.Callbacks.OnDialError
	}
	if override.Callbacks.OnEvict != nil {
		out.Callbacks.OnEvict = override.Callbacks.OnEvict
	}
	return out
}

// Get returns the pool for k, lazily creating one on first request.
func (m *Manager) Get(k Key) *Pool {
	m.mu.RLock()
	p, ok := m.pools[k]
	m.mu.RUnlock()
	if ok {
		return p
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := m.pools[k]; ok {
		return p
	}
	cfg := m.defaultCfg
	if m.configFor != nil {
		cfg = mergeConfig(cfg, m.configFor(k))
	}
	p = New(k.String(), m.dialerFor(k), cfg)
	m.pools[k] = p
	m.log.Info("pool created", "key", k.String(),
		"default_pool_size", cfg.DefaultPoolSize,
		"min_pool_size", cfg.MinPoolSize,
		"reserve_pool_size", cfg.ReservePoolSize,
		"query_wait", cfg.QueryWait,
	)
	return p
}

// Acquire is a convenience wrapper: Manager.Get(k).Acquire(ctx).
func (m *Manager) Acquire(ctx context.Context, k Key) (*backend.Conn, error) {
	return m.Get(k).Acquire(ctx)
}

// Release returns a backend to its pool.
func (m *Manager) Release(k Key, c *backend.Conn, reset bool) {
	if p := m.lookup(k); p != nil {
		p.Release(c, reset)
	}
}

func (m *Manager) lookup(k Key) *Pool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pools[k]
}

// Pools returns a snapshot of all currently-registered pools. Useful
// for the admin console + metrics.
func (m *Manager) Pools() []*Pool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Pool, 0, len(m.pools))
	for _, p := range m.pools {
		out = append(out, p)
	}
	return out
}

// AllStats is a single-shot snapshot across every pool.
func (m *Manager) AllStats() []Stats {
	pools := m.Pools()
	out := make([]Stats, 0, len(pools))
	for _, p := range pools {
		out = append(out, p.Stats())
	}
	return out
}

// Start launches the janitor goroutine. Safe to call once.
//
// The janitor runs every `interval` and asks each pool to evict expired
// idle / lifetime-aged backends.
func (m *Manager) Start(interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	m.janitorStop = make(chan struct{})
	m.janitorDone = make(chan struct{})
	go m.janitorLoop(interval)
}

func (m *Manager) janitorLoop(interval time.Duration) {
	defer close(m.janitorDone)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-m.janitorStop:
			return
		case now := <-t.C:
			m.evictAll(now)
		}
	}
}

func (m *Manager) evictAll(now time.Time) {
	pools := m.Pools()
	for _, p := range pools {
		if n := p.EvictIdleOnce(now); n > 0 {
			m.log.Debug("janitor evicted", "pool", p.Name(), "count", n)
		}
	}
}

// Close stops the janitor (if started) and closes all pools. Returns
// after all pools have been Closed (no drain wait).
func (m *Manager) Close() {
	_ = m.CloseWithDeadline(time.Time{})
}

// CloseWithDeadline stops the janitor and drains all pools in
// parallel, waiting until `deadline` (or zero for no wait). Returns
// the first ErrDrainTimeout if any pool didn't drain.
//
// The pools map stays populated until drain completes so that
// concurrent m.Release calls during shutdown still route to the right
// pool. After drain, the map is cleared.
func (m *Manager) CloseWithDeadline(deadline time.Time) error {
	if m.janitorStop != nil {
		close(m.janitorStop)
		<-m.janitorDone
		m.janitorStop = nil
	}
	m.mu.RLock()
	pools := make([]*Pool, 0, len(m.pools))
	for _, p := range m.pools {
		pools = append(pools, p)
	}
	m.mu.RUnlock()

	errCh := make(chan error, len(pools))
	var wg sync.WaitGroup
	for _, p := range pools {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- p.CloseWithDeadline(deadline)
		}()
	}
	wg.Wait()
	close(errCh)

	m.mu.Lock()
	m.pools = nil
	m.mu.Unlock()

	var firstErr error
	for e := range errCh {
		if e != nil && firstErr == nil {
			firstErr = e
		}
	}
	return firstErr
}

// String returns a one-line summary suitable for log lines.
func (m *Manager) String() string {
	return fmt.Sprintf("pool.Manager(%d pools)", len(m.Pools()))
}
