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

// LimitObserver reports global-cap rejections to a metrics surface.
// `scope` is "db" or "user".
type LimitObserver func(scope, name string)

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

	// Global per-(db | user) connection caps. 0 = unlimited.
	maxDBConn   int
	maxUserConn int
	limitObs    LimitObserver
	limitMu     sync.Mutex
	dbSemas     map[string]chan struct{} // db -> sema cap=maxDBConn
	userSemas   map[string]chan struct{} // user -> sema cap=maxUserConn
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
		dbSemas:    map[string]chan struct{}{},
		userSemas:  map[string]chan struct{}{},
	}
}

// WithConfigFor sets a per-key Config-override factory.
//
// Returns the Manager for builder-style chaining.
func (m *Manager) WithConfigFor(fn ConfigFor) *Manager {
	m.configFor = fn
	return m
}

// WithGlobalLimits enables PgBouncer-style global caps:
//
//	maxDBConn   — max concurrent checkouts across ALL pools sharing a db
//	maxUserConn — max concurrent checkouts across ALL pools sharing a user
//
// 0 = unlimited. `obs`, if non-nil, fires on every rejection so callers
// can wire a metric counter; pass nil to skip.
//
// Must be called BEFORE any pool is lazily created (i.e. before the first
// Get/Acquire). Returns the Manager for chaining.
func (m *Manager) WithGlobalLimits(maxDBConn, maxUserConn int, obs LimitObserver) *Manager {
	m.maxDBConn = maxDBConn
	m.maxUserConn = maxUserConn
	m.limitObs = obs
	return m
}

// limitersFor returns PreAcquire/PostRelease callbacks that gate Acquire
// behind the global db + user semaphores. Returns (nil, nil) if no caps
// are configured.
//
// The callbacks acquire in (db, user) order and release in reverse so
// callers can't deadlock on cross-dependence (db then user is stable).
func (m *Manager) limitersFor(k Key) (preAcquire func(context.Context) error, postRelease func()) {
	if m.maxDBConn <= 0 && m.maxUserConn <= 0 {
		return nil, nil
	}
	var dbSema, userSema chan struct{}
	m.limitMu.Lock()
	if m.maxDBConn > 0 {
		s, ok := m.dbSemas[k.DB]
		if !ok {
			s = make(chan struct{}, m.maxDBConn)
			m.dbSemas[k.DB] = s
		}
		dbSema = s
	}
	if m.maxUserConn > 0 {
		s, ok := m.userSemas[k.User]
		if !ok {
			s = make(chan struct{}, m.maxUserConn)
			m.userSemas[k.User] = s
		}
		userSema = s
	}
	m.limitMu.Unlock()

	obs := m.limitObs
	preAcquire = func(ctx context.Context) error {
		if dbSema != nil {
			select {
			case dbSema <- struct{}{}:
			case <-ctx.Done():
				if obs != nil {
					obs("db", k.DB)
				}
				return ctx.Err()
			}
		}
		if userSema != nil {
			select {
			case userSema <- struct{}{}:
			case <-ctx.Done():
				if dbSema != nil {
					<-dbSema
				}
				if obs != nil {
					obs("user", k.User)
				}
				return ctx.Err()
			}
		}
		return nil
	}
	postRelease = func() {
		if userSema != nil {
			<-userSema
		}
		if dbSema != nil {
			<-dbSema
		}
	}
	return preAcquire, postRelease
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
	if override.ResetQuery != "" {
		out.ResetQuery = override.ResetQuery
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
	// Inject global db/user semaphore limiters (no-op if no caps set).
	if pre, post := m.limitersFor(k); pre != nil {
		cfg.Callbacks.PreAcquire = pre
		cfg.Callbacks.PostRelease = post
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

// ApplyDefaultSize updates the per-pool default cap. `defaultSize`
// is the new global DefaultPoolSize; perKey, if non-nil, returns the
// per-(db, user) override (0 = use default). Resizes that don't change
// the current cap are skipped. Returns a summary slice of pools that
// actually changed.
func (m *Manager) ApplyDefaultSize(defaultSize int, perKey func(k Key) int) []ResizeRecord {
	pools := m.Pools()
	var out []ResizeRecord
	for _, p := range pools {
		k := SplitName(p.Name())
		want := defaultSize
		if perKey != nil {
			if v := perKey(k); v > 0 {
				want = v
			}
		}
		if want <= 0 {
			continue
		}
		prev := p.Size()
		if prev == want {
			continue
		}
		p.Resize(want)
		out = append(out, ResizeRecord{Key: k, From: prev, To: want})
	}
	// Also bump the Manager's default so future Get() calls use it.
	m.mu.Lock()
	m.defaultCfg.DefaultPoolSize = defaultSize
	m.mu.Unlock()
	return out
}

// ResizeRecord is one row in ApplyDefaultSize's return value.
type ResizeRecord struct {
	Key  Key
	From int
	To   int
}

// SplitName reverses Key.String — splits "db/user" back into a Key.
// Tolerant: a name without "/" goes into DB.
//
// Exported because admin_console + cmd both reverse pool names back
// into (db, user) tuples; this used to live in three slightly-
// different implementations.
func SplitName(name string) Key {
	for i := 0; i < len(name); i++ {
		if name[i] == '/' {
			return Key{DB: name[:i], User: name[i+1:]}
		}
	}
	return Key{DB: name}
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
