// Per-tenant routing decisions consolidated behind one interface.
//
// PooledHandler used to ship four separate callbacks — ReplicaPickerFor,
// StickyReadWindowFor, PrimaryHealthyFor, QPSCapFor — each binding
// independently to the same cfg + replica/primary maps. Adding a new
// per-tenant decision (e.g. multi-region routing, blue/green pinning)
// meant another callback + another wiring path through cmd + pkg +
// every PooledConn construction.
//
// Router collapses them into one interface that wire.BuildPooledHandler
// satisfies in a single struct. Tests that don't care about routing
// can leave PooledHandler.Router=nil; defaultRouter is consulted as
// fallback (primary always wins; no QPS cap; healthy).

package client

import (
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/pool"
)

// Router answers per-tenant routing questions for one connection's
// lifetime. All four methods are called per-message-class:
//   - ReplicaPool: lazily on first Read message of a new tx
//   - StickyReadWindow: same
//   - PrimaryHealthy: same (or per-Acquire if PrimaryMonitor is wired)
//   - QPSCap: once per PooledConn at Serve() init
//
// Implementations must be cheap + non-blocking. Returning nil/0/true
// from any method disables that routing facet for the call.
type Router interface {
	// ReplicaPool returns a replica pool to acquire READ-classified
	// queries from for db. Returns nil to route to the primary (no
	// replica available, none under lag cap, or db has no replicas).
	ReplicaPool(db string) *pool.Pool

	// StickyReadWindow returns the per-database read-your-own-writes
	// window. A read on this db within the window of a preceding
	// write on the SAME client conn is pinned to the primary.
	// 0 = sticky-read disabled.
	StickyReadWindow(db string) time.Duration

	// PrimaryHealthy reports whether the primary backing db is
	// currently considered healthy. Returns true when no failover
	// monitor is configured for this db.
	PrimaryHealthy(db string) bool

	// QPSCap returns the per-(db, user) max-QPS cap. 0 disables
	// rate-limiting for that tenant.
	QPSCap(db, user string) float64
}

// defaultRouter is consulted when PooledHandler.Router is nil — every
// method returns the "no-op" answer (primary always wins; sticky-read
// disabled; primary healthy; no QPS cap).
type defaultRouter struct{}

func (defaultRouter) ReplicaPool(string) *pool.Pool          { return nil }
func (defaultRouter) StickyReadWindow(string) time.Duration  { return 0 }
func (defaultRouter) PrimaryHealthy(string) bool             { return true }
func (defaultRouter) QPSCap(string, string) float64          { return 0 }

// routerOr returns r if non-nil, else the default no-op router.
func routerOr(r Router) Router {
	if r != nil {
		return r
	}
	return defaultRouter{}
}
