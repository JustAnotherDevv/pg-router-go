// Per-client prepared-statement tracking.
//
// In session-mode pooling this is trivial (each client owns a backend
// for its full lifetime). In transaction-mode the picture is harder:
// the same client may end up talking to different backends across its
// transactions, and a Parse(name=stmt1, query=...) the client made on
// backend A is meaningless on backend B.
//
// MVP M.11 builds the data model that M.15 will wire into the backend
// pool:
//
//	PrepareCache (per-client): tracks (client-visible name) -> Stmt.
//	The backend pool's per-conn cache will memoise (sql-fingerprint)
//	-> server-visible name; on cache hit, no extra Parse round trip.
//
// Out of scope:
//   - SQL fingerprinting itself (sha256 of the normalised query)
//   - Backend-side LRU eviction (lives on internal/backend/Conn)
//
// Those land alongside M.15 release prep.

package client

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sync"
)

// Stmt is one prepared-statement entry.
type Stmt struct {
	// Name is the client-visible statement name (the value of
	// `Parse.Name`). Empty for unnamed statements.
	Name string
	// ServerName is the rewritten server-side name pgrouter sends to
	// the backend in place of Name. Format: "pgr_<16hex>" where the
	// hex is a 64-bit hash of Query. Stable across all clients that
	// share the same SQL → enables cross-client cache hits on a
	// shared backend.
	ServerName string
	// Query is the SQL the client parsed.
	Query string
	// ParamOIDs are the type OIDs the client specified.
	ParamOIDs []uint32
}

// ServerNameFor returns the deterministic server-side prepared name for
// a given SQL string. Two clients sending the same Parse(query=X) get
// the same ServerName, so a backend cache hit by one is shareable with
// the other.
//
// FNV-1a 64-bit is chosen for speed + adequate collision resistance:
// at 4 billion distinct queries the expected collision count is ~0.5
// (birthday bound √(2*2^64) ≈ 2^32). Real workloads have ~10²–10⁴
// distinct prepared queries — collision is effectively impossible.
func ServerNameFor(query string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(query))
	return "pgr_" + hex.EncodeToString(h.Sum(nil))
}

// PrepareCache is the per-client map of name -> Stmt.
type PrepareCache struct {
	mu    sync.RWMutex
	stmts map[string]*Stmt
}

// NewPrepareCache returns an empty cache.
func NewPrepareCache() *PrepareCache {
	return &PrepareCache{stmts: map[string]*Stmt{}}
}

// Observe records a Parse message. Returns the previous Stmt if `name`
// was reused (which is technically a protocol violation by the client
// but pg drivers sometimes do it; we mirror PgBouncer's behaviour and
// silently overwrite).
func (c *PrepareCache) Observe(name, query string, oids []uint32) *Stmt {
	if name == "" {
		return nil // unnamed statements aren't tracked
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	prev := c.stmts[name]
	c.stmts[name] = &Stmt{
		Name:       name,
		ServerName: ServerNameFor(query),
		Query:      query,
		ParamOIDs:  append([]uint32(nil), oids...),
	}
	return prev
}

// ServerNameOf returns the rewritten server-side name for a tracked
// client name, or empty if the name isn't tracked.
func (c *PrepareCache) ServerNameOf(name string) string {
	if name == "" {
		return ""
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if s := c.stmts[name]; s != nil {
		return s.ServerName
	}
	return ""
}

// Close removes a tracked statement (in response to Close('S', name)).
// Returns true if present.
func (c *PrepareCache) Close(name string) bool {
	if name == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.stmts[name]; ok {
		delete(c.stmts, name)
		return true
	}
	return false
}

// CloseAll empties the cache. Called on DEALLOCATE ALL / DISCARD ALL /
// session reset.
func (c *PrepareCache) CloseAll() {
	c.mu.Lock()
	c.stmts = map[string]*Stmt{}
	c.mu.Unlock()
}

// Get returns the Stmt for `name`, or nil if not tracked.
func (c *PrepareCache) Get(name string) *Stmt {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stmts[name]
}

// Len returns the number of named statements currently tracked.
func (c *PrepareCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.stmts)
}

// Snapshot returns a copy of (name -> *Stmt) for inspection / replay.
func (c *PrepareCache) Snapshot() map[string]*Stmt {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]*Stmt, len(c.stmts))
	for k, v := range c.stmts {
		out[k] = v
	}
	return out
}

// String returns a one-line summary.
func (c *PrepareCache) String() string {
	return fmt.Sprintf("PrepareCache(%d stmts)", c.Len())
}
