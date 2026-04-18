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

// Manager owns all per-(db, user) pools + the janitor goroutine that
// sweeps idle / lifetime-expired backends.
type Manager struct {
	defaultCfg Config
	dialerFor  DialerFor
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
	p = New(k.String(), m.dialerFor(k), m.defaultCfg)
	m.pools[k] = p
	m.log.Info("pool created", "key", k.String(),
		"default_pool_size", m.defaultCfg.DefaultPoolSize,
		"query_wait", m.defaultCfg.QueryWait,
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

// Close stops the janitor (if started) and closes all pools.
func (m *Manager) Close() {
	if m.janitorStop != nil {
		close(m.janitorStop)
		<-m.janitorDone
		m.janitorStop = nil
	}
	m.mu.Lock()
	for _, p := range m.pools {
		p.Close()
	}
	m.pools = nil
	m.mu.Unlock()
}

// String returns a one-line summary suitable for log lines.
func (m *Manager) String() string {
	return fmt.Sprintf("pool.Manager(%d pools)", len(m.Pools()))
}
