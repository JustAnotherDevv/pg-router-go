// Per-(db, user, app) counter handles resolved once per conn.
//
// Every package-level On* function (OnQuery, OnTxStart, etc.) calls
// `Active.<vec>.WithLabelValues(db, user, app)` which under the
// covers takes the Prometheus client's internal mutex + does a map
// lookup for every increment. At 10k+ QPS per PooledConn this is
// pure overhead — labels are FIXED for the lifetime of a client conn
// since (db, user, app) are set at StartupMessage time and never
// change.
//
// TenantCounters resolves all needed handles once at conn-init time
// and caches them. PooledConn calls e.g. `tc.Query.Inc()` instead of
// `stats.OnQuery(db, user, app)`. Hot-path WithLabelValues traffic
// drops from per-message to per-conn.

package stats

import "github.com/prometheus/client_golang/prometheus"

// TenantCounters bundles every per-(db, user, app) counter + the
// duration histogram observer for a single PooledConn. nil-safe: if
// Active is nil (tests that don't init the metrics surface), the
// factory returns nil and every method becomes a no-op.
type TenantCounters struct {
	Query             prometheus.Counter
	TxStart           prometheus.Counter
	TxCommit          prometheus.Counter
	TxRollback        prometheus.Counter
	QueryTimeout      prometheus.Counter
	ClientIdleTimeout prometheus.Counter
	IdleTxTimeout     prometheus.Counter
	PreparedHit       prometheus.Counter
	PreparedMiss      prometheus.Counter
	PreparedEviction  prometheus.Counter
	QueryDuration     prometheus.Observer
	BytesIn           prometheus.Counter
	BytesOut          prometheus.Counter
}

// NewTenantCounters resolves every per-tenant handle in one batch.
// Cheap relative to per-message cost — typical client opens, does
// thousands of queries, closes. Returns nil when the metrics surface
// hasn't been initialised (tests / library mode without stats.New()).
func NewTenantCounters(db, user, app string) *TenantCounters {
	if Active == nil {
		return nil
	}
	return &TenantCounters{
		Query:             Active.QueriesTotal.WithLabelValues(db, user, app),
		TxStart:           Active.TxStarts.WithLabelValues(db, user, app),
		TxCommit:          Active.TxCommits.WithLabelValues(db, user, app),
		TxRollback:        Active.TxRollbacks.WithLabelValues(db, user, app),
		QueryTimeout:      Active.QueryTimeouts.WithLabelValues(db, user, app),
		ClientIdleTimeout: Active.ClientIdleTimeouts.WithLabelValues(db, user, app),
		IdleTxTimeout:     Active.IdleTxTimeouts.WithLabelValues(db, user, app),
		PreparedHit:       Active.PreparedHits.WithLabelValues(db, user, app),
		PreparedMiss:      Active.PreparedMisses.WithLabelValues(db, user, app),
		PreparedEviction:  Active.PreparedEvictions.WithLabelValues(db, user, app),
		QueryDuration:     Active.QueryDurationSec.WithLabelValues(db, user, app),
		BytesIn:           Active.BytesInPerTenant.WithLabelValues(db, user),
		BytesOut:          Active.BytesOutPerTenant.WithLabelValues(db, user),
	}
}

// OnQuery is the cached-handle equivalent of stats.OnQuery.
func (t *TenantCounters) OnQuery() {
	if t != nil && t.Query != nil {
		t.Query.Inc()
	}
}

// OnTxStart is the cached-handle equivalent of stats.OnTxStart.
func (t *TenantCounters) OnTxStart() {
	if t != nil && t.TxStart != nil {
		t.TxStart.Inc()
	}
}

// OnTxCommit is the cached-handle equivalent of stats.OnTxCommit.
func (t *TenantCounters) OnTxCommit() {
	if t != nil && t.TxCommit != nil {
		t.TxCommit.Inc()
	}
}

// OnTxRollback is the cached-handle equivalent of stats.OnTxRollback.
func (t *TenantCounters) OnTxRollback() {
	if t != nil && t.TxRollback != nil {
		t.TxRollback.Inc()
	}
}

// OnQueryTimeout is the cached-handle equivalent of stats.OnQueryTimeout.
func (t *TenantCounters) OnQueryTimeout() {
	if t != nil && t.QueryTimeout != nil {
		t.QueryTimeout.Inc()
	}
}

// OnClientIdleTimeout is the cached-handle equivalent of stats.OnClientIdleTimeout.
func (t *TenantCounters) OnClientIdleTimeout() {
	if t != nil && t.ClientIdleTimeout != nil {
		t.ClientIdleTimeout.Inc()
	}
}

// OnIdleTxTimeout is the cached-handle equivalent of stats.OnIdleTxTimeout.
func (t *TenantCounters) OnIdleTxTimeout() {
	if t != nil && t.IdleTxTimeout != nil {
		t.IdleTxTimeout.Inc()
	}
}

// OnPreparedHit is the cached-handle equivalent of stats.OnPreparedHit.
func (t *TenantCounters) OnPreparedHit() {
	if t != nil && t.PreparedHit != nil {
		t.PreparedHit.Inc()
	}
}

// OnPreparedMiss is the cached-handle equivalent of stats.OnPreparedMiss.
func (t *TenantCounters) OnPreparedMiss() {
	if t != nil && t.PreparedMiss != nil {
		t.PreparedMiss.Inc()
	}
}

// OnPreparedEviction is the cached-handle equivalent of stats.OnPreparedEviction.
func (t *TenantCounters) OnPreparedEviction() {
	if t != nil && t.PreparedEviction != nil {
		t.PreparedEviction.Inc()
	}
}

// OnQueryDuration is the cached-handle equivalent of stats.OnQueryDuration.
func (t *TenantCounters) OnQueryDuration(seconds float64) {
	if t != nil && t.QueryDuration != nil {
		t.QueryDuration.Observe(seconds)
	}
}

// OnBytesIn is the cached-handle equivalent of stats.OnBytesIn.
// Called on every client Read — must be allocation-free.
func (t *TenantCounters) OnBytesIn(n int) {
	if t != nil && n > 0 && t.BytesIn != nil {
		t.BytesIn.Add(float64(n))
	}
}

// OnBytesOut is the cached-handle equivalent of stats.OnBytesOut.
// Called on every client Write — must be allocation-free.
func (t *TenantCounters) OnBytesOut(n int) {
	if t != nil && n > 0 && t.BytesOut != nil {
		t.BytesOut.Add(float64(n))
	}
}
