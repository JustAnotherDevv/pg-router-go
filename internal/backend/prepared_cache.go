package backend

import (
	"container/list"
	"sync"
)

// PreparedCache is a per-backend LRU of server-side prepared-statement
// names that have been Parsed on THIS connection.
//
// It exists so pgrouter can transparently re-use named prepared
// statements across transaction-mode backend swaps:
//
//   - On the first Parse(name=foo) for a given SQL, pgrouter computes a
//     stable server-side name S = "pgr_<hash(sql)>", forwards Parse(S)
//     to the backend, and records S in this cache.
//   - On a later Bind with name=foo, pgrouter rewrites the message to
//     Bind(S). If S is still in this cache the backend already has the
//     plan; if not, pgrouter re-sends Parse(S) transparently first.
//   - When the cache hits capacity, the LRU entry is evicted and the
//     caller is expected to send DEALLOCATE S so the backend's planner
//     memory stays bounded.
//
// MVP M.11.2 scope. Concurrency: PreparedCache is goroutine-safe; one
// Conn may be touched only by its owning client goroutine in practice,
// but tests + future async janitors may read concurrently so we lock.
type PreparedCache struct {
	mu    sync.Mutex
	cap   int
	items map[string]*list.Element // name → element holding (string)
	order *list.List               // front = MRU, back = LRU
}

// DefaultPreparedCacheCapacity is the per-backend ceiling when callers
// don't override. Matches pgcat's prepared_statements_cache_size default
// — large enough that ORMs (Prisma, GORM) hit it almost always, small
// enough that PG planner state stays bounded.
const DefaultPreparedCacheCapacity = 250

// NewPreparedCache returns an empty cache. cap <= 0 → use
// DefaultPreparedCacheCapacity.
func NewPreparedCache(cap int) *PreparedCache {
	if cap <= 0 {
		cap = DefaultPreparedCacheCapacity
	}
	return &PreparedCache{
		cap:   cap,
		items: make(map[string]*list.Element, cap),
		order: list.New(),
	}
}

// Cap returns the configured capacity.
func (c *PreparedCache) Cap() int {
	if c == nil {
		return 0
	}
	return c.cap
}

// Len returns the current entry count.
func (c *PreparedCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

// Has reports whether `name` is in the cache WITHOUT bumping LRU
// position. Use this for a "check then decide whether to send Parse"
// flow.
func (c *PreparedCache) Has(name string) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.items[name]
	return ok
}

// Touch bumps `name` to MRU. No-op if not present.
func (c *PreparedCache) Touch(name string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[name]; ok {
		c.order.MoveToFront(e)
	}
}

// Add records that `name` was just Parsed on the backend, then bumps it
// to MRU. Returns the LRU-evicted name (empty if no eviction). The
// caller MUST send DEALLOCATE for the evicted name so the backend's
// prepared-statement memory matches our cache view.
//
// Adding an existing name is treated as a hit — moves it to MRU,
// returns "".
func (c *PreparedCache) Add(name string) (evicted string) {
	if c == nil || name == "" {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[name]; ok {
		c.order.MoveToFront(e)
		return ""
	}
	e := c.order.PushFront(name)
	c.items[name] = e
	if c.order.Len() > c.cap {
		oldest := c.order.Back()
		if oldest != nil {
			evicted = oldest.Value.(string)
			c.order.Remove(oldest)
			delete(c.items, evicted)
		}
	}
	return evicted
}

// Remove drops `name` from the cache (e.g. after a DEALLOCATE).
func (c *PreparedCache) Remove(name string) {
	if c == nil || name == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[name]; ok {
		c.order.Remove(e)
		delete(c.items, name)
	}
}

// Clear empties the cache. Call after DISCARD ALL / DEALLOCATE ALL /
// backend reset — the backend has dropped all named prepared statements
// and our view must match.
func (c *PreparedCache) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*list.Element, c.cap)
	c.order.Init()
}

// Snapshot returns the entries from MRU → LRU. Cheap to call; copies.
func (c *PreparedCache) Snapshot() []string {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, c.order.Len())
	for e := c.order.Front(); e != nil; e = e.Next() {
		out = append(out, e.Value.(string))
	}
	return out
}
