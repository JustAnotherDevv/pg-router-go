// Bounded-cardinality label sanitiser for client-controlled
// application_name.
//
// Client sends `application_name` in StartupMessage. It flows directly
// into the {database, user, application_name} Prometheus label tuple
// on QueriesTotal, TxStarts, QueryDuration, etc. A hostile client can
// open N conns with random app_names → N metric series per (db, user)
// → Prometheus head-block memory explodes.
//
// SanitizeAppName tracks the distinct values seen process-wide; once
// the cap is reached, every new value collapses to "__other__" so the
// label set stays bounded.
//
// The tracker is intentionally process-wide (not per-tenant): an
// attacker could otherwise distribute the cardinality bomb across
// multiple (db, user) pairs by re-authing.

package stats

import "sync"

// AppNameCardinalityCap is the maximum number of distinct
// application_name values pgrouter will admit before bucketing the
// rest into "__other__". Operators tune via config.Metrics.MaxAppName
// in production wiring (cmd + pkg should call SetAppNameCap at boot).
const DefaultAppNameCardinalityCap = 100

// OverflowAppName is the bucket value emitted for app_name once the
// cardinality cap is hit.
const OverflowAppName = "__other__"

var appNameCard = struct {
	mu   sync.RWMutex
	seen map[string]struct{}
	cap  int
}{
	seen: map[string]struct{}{},
	cap:  DefaultAppNameCardinalityCap,
}

// SetAppNameCap configures the process-wide cap. Callers should call
// this once at boot before traffic flows. A cap <= 0 disables the
// limit entirely (rolls back to client-controlled cardinality —
// acceptable in trusted internal-only deployments).
func SetAppNameCap(n int) {
	appNameCard.mu.Lock()
	appNameCard.cap = n
	// Drop the seen set so a SIGHUP reload can re-evaluate.
	appNameCard.seen = map[string]struct{}{}
	appNameCard.mu.Unlock()
}

// SanitizeAppName returns either `raw` (if already-seen or below the
// cap) or OverflowAppName. Cheap: RLock fast-path for the common
// already-seen case; only first-time values take the write lock.
func SanitizeAppName(raw string) string {
	if raw == "" {
		return ""
	}
	appNameCard.mu.RLock()
	if appNameCard.cap <= 0 {
		appNameCard.mu.RUnlock()
		return raw
	}
	if _, ok := appNameCard.seen[raw]; ok {
		appNameCard.mu.RUnlock()
		return raw
	}
	appNameCard.mu.RUnlock()

	appNameCard.mu.Lock()
	defer appNameCard.mu.Unlock()
	// Re-check under write lock (another writer may have raced).
	if _, ok := appNameCard.seen[raw]; ok {
		return raw
	}
	if len(appNameCard.seen) >= appNameCard.cap {
		return OverflowAppName
	}
	appNameCard.seen[raw] = struct{}{}
	return raw
}
