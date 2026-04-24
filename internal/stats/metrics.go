// Prometheus metrics surface for pgrouter.
//
// All metrics live in one package so callers don't have to thread a
// metrics handle through every constructor: each package imports
// internal/stats and calls the appropriate `On*` event.
//
// Naming follows the Prometheus best-practices style:
//   pgrouter_<subsystem>_<noun>_<unit>
//
// e.g. pgrouter_client_connections_total, pgrouter_pool_acquire_seconds.

package stats

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Build is overridable from main; reported as a `pgrouter_build_info`
// gauge so dashboards can pin themselves to a known release.
var Build = struct {
	Version string
	Commit  string
}{
	Version: "dev",
}

// Active is the singleton Metrics handle initialised by New(). Other
// packages reach into it via the small public hooks below so they
// don't have to thread *Metrics through every constructor.
var Active *Metrics

// OnPoolAcquireWait satisfies pool.Callbacks.OnAcquireWait. No-op if
// Active is nil (tests that don't initialise the metrics surface).
func OnPoolAcquireWait(name string, d float64) {
	if Active != nil {
		Active.PoolAcquireSeconds.WithLabelValues(name).Observe(d)
	}
}

// OnPoolDial increments backend dial counters.
func OnPoolDial(_ string) {
	if Active != nil {
		Active.BackendDialsTotal.Inc()
	}
}

// OnPoolDialError increments dial-error counters.
func OnPoolDialError(_ string, _ error) {
	if Active != nil {
		Active.BackendDialErrorsTot.Inc()
	}
}

// OnPoolEvict increments eviction counters.
func OnPoolEvict(_ string, n int) {
	if Active != nil {
		Active.BackendEvictionsTotal.Add(float64(n))
	}
}

// OnQuery increments the per-(database, user, application_name) query
// counter. Fired from PooledConn each time a Query or Parse is
// forwarded to a backend. `app` is the application_name the client
// sent in StartupMessage; empty string when not provided.
func OnQuery(db, user, app string) {
	if Active != nil {
		Active.QueriesTotal.WithLabelValues(db, user, app).Inc()
	}
}

// OnTxStart increments the per-(db, user, app) transaction-start counter.
func OnTxStart(db, user, app string) {
	if Active != nil {
		Active.TxStarts.WithLabelValues(db, user, app).Inc()
	}
}

// OnTxCommit increments the per-(db, user, app) commit counter.
func OnTxCommit(db, user, app string) {
	if Active != nil {
		Active.TxCommits.WithLabelValues(db, user, app).Inc()
	}
}

// OnTxRollback increments the per-(db, user, app) rollback counter.
func OnTxRollback(db, user, app string) {
	if Active != nil {
		Active.TxRollbacks.WithLabelValues(db, user, app).Inc()
	}
}

// OnQueryDuration observes a per-(db, user, app) query duration in seconds.
func OnQueryDuration(db, user, app string, seconds float64) {
	if Active != nil {
		Active.QueryDurationSec.WithLabelValues(db, user, app).Observe(seconds)
	}
}

// OnGlobalLimitReject counts Acquires denied by the global db/user cap.
// `scope` is "db" or "user".
func OnGlobalLimitReject(scope, name string) {
	if Active != nil {
		Active.GlobalLimitRejects.WithLabelValues(scope, name).Inc()
	}
}

// OnQueryTimeout counts queries killed by query_timeout.
func OnQueryTimeout(db, user, app string) {
	if Active != nil {
		Active.QueryTimeouts.WithLabelValues(db, user, app).Inc()
	}
}

// OnClientIdleTimeout counts clients evicted by client_idle_timeout.
func OnClientIdleTimeout(db, user, app string) {
	if Active != nil {
		Active.ClientIdleTimeouts.WithLabelValues(db, user, app).Inc()
	}
}

// OnIdleTxTimeout counts clients killed by idle_transaction_timeout.
func OnIdleTxTimeout(db, user, app string) {
	if Active != nil {
		Active.IdleTxTimeouts.WithLabelValues(db, user, app).Inc()
	}
}

// OnSighupReload counts SIGHUP-driven config reloads.
// `outcome` = "ok" | "fail".
func OnSighupReload(outcome string) {
	if Active != nil {
		Active.SighupReloads.WithLabelValues(outcome).Inc()
	}
}

// OnBytesIn adds n bytes to the per-(db, user) inbound byte counter.
// Wired from the CountingConn wrap installed by PooledHandler.
func OnBytesIn(db, user string, n int) {
	if Active != nil && n > 0 {
		Active.BytesInPerTenant.WithLabelValues(db, user).Add(float64(n))
	}
}

// OnBytesOut adds n bytes to the per-(db, user) outbound byte counter.
func OnBytesOut(db, user string, n int) {
	if Active != nil && n > 0 {
		Active.BytesOutPerTenant.WithLabelValues(db, user).Add(float64(n))
	}
}

// OnQPSReject counts per-tenant token-bucket rejections.
// `scope` is "db" or "user"; `name` is the db or user name.
func OnQPSReject(scope, name string) {
	if Active != nil {
		Active.QPSRejects.WithLabelValues(scope, name).Inc()
	}
}

// OnSighupUserlistReload counts SIGHUP-driven userlist.txt reloads.
// `outcome` = "ok" | "fail" | "skip" (skip = no userlist configured).
func OnSighupUserlistReload(outcome string) {
	if Active != nil {
		Active.SighupUserlistReloads.WithLabelValues(outcome).Inc()
	}
}

// OnPreparedHit increments the per-(db, user, app) prepared-statement
// cache hit counter (Parse for a SQL whose server-side name is already
// cached on the backend → no extra Parse round trip).
func OnPreparedHit(db, user, app string) {
	if Active != nil {
		Active.PreparedHits.WithLabelValues(db, user, app).Inc()
	}
}

// OnPreparedMiss increments the per-(db, user, app) prepared-statement
// cache miss counter (first Parse for this SQL on this backend).
func OnPreparedMiss(db, user, app string) {
	if Active != nil {
		Active.PreparedMisses.WithLabelValues(db, user, app).Inc()
	}
}

// OnPreparedEviction increments the per-(db, user, app) eviction counter
// (LRU pushed out an entry, prompting an automatic DEALLOCATE).
func OnPreparedEviction(db, user, app string) {
	if Active != nil {
		Active.PreparedEvictions.WithLabelValues(db, user, app).Inc()
	}
}

// Reg is the *prometheus.Registry pgrouter writes to. Production main
// passes this into the metrics HTTP handler.
var Reg = prometheus.NewRegistry()

// Metrics is the central handle. Initialised once at process start.
type Metrics struct {
	// Client-facing.
	ClientConnsTotal    prometheus.Counter
	ClientDisconnsTotal prometheus.Counter
	ClientActiveGauge   prometheus.Gauge
	ClientBytesIn       prometheus.Counter
	ClientBytesOut      prometheus.Counter

	// Backend-facing.
	BackendDialsTotal     prometheus.Counter
	BackendDialErrorsTot  prometheus.Counter
	BackendActiveGauge    prometheus.Gauge
	BackendIdleGauge      prometheus.Gauge
	BackendEvictionsTotal prometheus.Counter

	// Pool.
	PoolAcquireSeconds *prometheus.HistogramVec
	PoolWaitersGauge   *prometheus.GaugeVec

	// Wire-protocol — labeled by {database, user} so operators can slice
	// by which pool is hot.
	QueriesTotal     *prometheus.CounterVec
	TxStarts         *prometheus.CounterVec
	TxCommits        *prometheus.CounterVec
	TxRollbacks      *prometheus.CounterVec
	QueryDurationSec *prometheus.HistogramVec

	// Auth.
	AuthAttempts prometheus.Counter
	AuthFailures prometheus.Counter

	// Cancel routing.
	CancelsReceived  prometheus.Counter
	CancelsForwarded prometheus.Counter
	CancelsDropped   prometheus.Counter

	// Enforcement counters.
	GlobalLimitRejects *prometheus.CounterVec // {"scope": "db"|"user", "name": <db|user>}
	QueryTimeouts      *prometheus.CounterVec // {database, user}
	ClientIdleTimeouts *prometheus.CounterVec // {database, user}
	IdleTxTimeouts     *prometheus.CounterVec // {database, user}

	// Lifecycle.
	SighupReloads         *prometheus.CounterVec // {"outcome": "ok"|"fail"}
	SighupUserlistReloads *prometheus.CounterVec // {"outcome": "ok"|"fail"|"skip"}

	// QPS rate-limit rejections (#116).
	QPSRejects *prometheus.CounterVec // {"scope": "db"|"user", "name": <db|user>}

	// Per-tenant bandwidth (#117).
	BytesInPerTenant  *prometheus.CounterVec // {database, user}
	BytesOutPerTenant *prometheus.CounterVec // {database, user}

	// Prepared statement cross-backend cache (M.11.2).
	PreparedHits      *prometheus.CounterVec // {database, user}
	PreparedMisses    *prometheus.CounterVec // {database, user}
	PreparedEvictions *prometheus.CounterVec // {database, user}
}

// New constructs + registers a Metrics. Process should hold ONE Metrics
// for its lifetime; calling New twice will panic with `prometheus.AlreadyRegisteredError`.
func New() *Metrics {
	m := &Metrics{
		ClientConnsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_client_connections_total",
			Help: "Total client connections accepted since start.",
		}),
		ClientDisconnsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_client_disconnections_total",
			Help: "Total client disconnections.",
		}),
		ClientActiveGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgrouter_client_active",
			Help: "Currently-connected clients.",
		}),
		ClientBytesIn: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_client_bytes_in_total",
			Help: "Bytes received from clients.",
		}),
		ClientBytesOut: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_client_bytes_out_total",
			Help: "Bytes sent to clients.",
		}),

		BackendDialsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_backend_dials_total",
			Help: "Total upstream connect attempts.",
		}),
		BackendDialErrorsTot: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_backend_dial_errors_total",
			Help: "Upstream connect failures.",
		}),
		BackendActiveGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgrouter_backend_active",
			Help: "Currently checked-out upstream backends.",
		}),
		BackendIdleGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pgrouter_backend_idle",
			Help: "Currently-idle pooled backends.",
		}),
		BackendEvictionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_backend_evictions_total",
			Help: "Backends evicted by the janitor (idle / lifetime).",
		}),

		PoolAcquireSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "pgrouter_pool_acquire_seconds",
			Help:    "Time clients spent waiting for a pool slot.",
			Buckets: defaultBuckets,
		}, []string{"pool"}),
		PoolWaitersGauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgrouter_pool_waiters",
			Help: "Clients currently queued in a pool's wait queue.",
		}, []string{"pool"}),

		QueriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_queries_total",
			Help: "Total Query + Parse messages forwarded.",
		}, []string{"database", "user", "application_name"}),
		TxStarts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_tx_starts_total",
			Help: "Transactions opened (BEGIN observed).",
		}, []string{"database", "user", "application_name"}),
		TxCommits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_tx_commits_total",
			Help: "Transactions committed (T -> I via success).",
		}, []string{"database", "user", "application_name"}),
		TxRollbacks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_tx_rollbacks_total",
			Help: "Transactions rolled back (E -> I, or explicit ROLLBACK).",
		}, []string{"database", "user", "application_name"}),
		QueryDurationSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "pgrouter_query_duration_seconds",
			Help:    "Per-query duration, from first byte to ReadyForQuery.",
			Buckets: defaultBuckets,
		}, []string{"database", "user", "application_name"}),

		AuthAttempts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_auth_attempts_total",
			Help: "Client auth attempts.",
		}),
		AuthFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_auth_failures_total",
			Help: "Client auth failures.",
		}),

		CancelsReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_cancels_received_total",
			Help: "CancelRequest packets received from clients.",
		}),
		CancelsForwarded: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_cancels_forwarded_total",
			Help: "CancelRequest packets successfully forwarded.",
		}),
		CancelsDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_cancels_dropped_total",
			Help: "CancelRequest packets dropped (unknown PID/secret).",
		}),

		GlobalLimitRejects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_global_limit_rejects_total",
			Help: "Acquires rejected by max_db_connections / max_user_connections.",
		}, []string{"scope", "name"}),
		QueryTimeouts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_query_timeouts_total",
			Help: "Queries killed by query_timeout.",
		}, []string{"database", "user", "application_name"}),
		ClientIdleTimeouts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_client_idle_timeouts_total",
			Help: "Clients evicted by client_idle_timeout.",
		}, []string{"database", "user", "application_name"}),
		IdleTxTimeouts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_idle_transaction_timeouts_total",
			Help: "Clients killed by idle_transaction_timeout.",
		}, []string{"database", "user", "application_name"}),
		SighupReloads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_sighup_reloads_total",
			Help: "SIGHUP-driven config reloads.",
		}, []string{"outcome"}),
		SighupUserlistReloads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_sighup_userlist_reloads_total",
			Help: "SIGHUP-driven userlist.txt reloads.",
		}, []string{"outcome"}),
		QPSRejects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_qps_rejects_total",
			Help: "Queries rejected by per-tenant max_qps token bucket.",
		}, []string{"scope", "name"}),
		BytesInPerTenant: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_tenant_bytes_in_total",
			Help: "Bytes received from clients, per (database, user).",
		}, []string{"database", "user"}),
		BytesOutPerTenant: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_tenant_bytes_out_total",
			Help: "Bytes sent to clients, per (database, user).",
		}, []string{"database", "user"}),

		PreparedHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_prepared_cache_hits_total",
			Help: "Parse messages whose server-side name was already cached on the backend.",
		}, []string{"database", "user", "application_name"}),
		PreparedMisses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_prepared_cache_misses_total",
			Help: "Parse messages requiring a backend round trip (first time on this backend).",
		}, []string{"database", "user", "application_name"}),
		PreparedEvictions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_prepared_cache_evictions_total",
			Help: "Prepared-statement LRU evictions (DEALLOCATE sent to backend).",
		}, []string{"database", "user", "application_name"}),
	}

	// Add Go runtime + process collectors so the standard
	// `go_*` / `process_*` metrics show up.
	Reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.ClientConnsTotal, m.ClientDisconnsTotal, m.ClientActiveGauge,
		m.ClientBytesIn, m.ClientBytesOut,
		m.BackendDialsTotal, m.BackendDialErrorsTot, m.BackendActiveGauge,
		m.BackendIdleGauge, m.BackendEvictionsTotal,
		m.PoolAcquireSeconds, m.PoolWaitersGauge,
		m.QueriesTotal, m.TxStarts, m.TxCommits, m.TxRollbacks,
		m.QueryDurationSec,
		m.AuthAttempts, m.AuthFailures,
		m.CancelsReceived, m.CancelsForwarded, m.CancelsDropped,
		m.GlobalLimitRejects, m.QueryTimeouts,
		m.ClientIdleTimeouts, m.IdleTxTimeouts,
		m.SighupReloads, m.SighupUserlistReloads, m.QPSRejects,
		m.BytesInPerTenant, m.BytesOutPerTenant,
		m.PreparedHits, m.PreparedMisses, m.PreparedEvictions,
	)
	Active = m
	return m
}

// defaultBuckets is tuned for pgwire RTTs (sub-ms to multi-second slow
// queries). Override per-histogram if a different distribution fits.
var defaultBuckets = []float64{
	0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1,
	0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
}
