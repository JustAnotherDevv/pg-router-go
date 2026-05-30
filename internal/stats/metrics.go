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

// OnQuery increments the per-(database, user) query counter. Fired from
// PooledConn each time a Query or Parse is forwarded to a backend.
func OnQuery(db, user string) {
	if Active != nil {
		Active.QueriesTotal.WithLabelValues(db, user).Inc()
	}
}

// OnTxStart increments the per-(db, user) transaction-start counter.
func OnTxStart(db, user string) {
	if Active != nil {
		Active.TxStarts.WithLabelValues(db, user).Inc()
	}
}

// OnTxCommit increments the per-(db, user) commit counter.
func OnTxCommit(db, user string) {
	if Active != nil {
		Active.TxCommits.WithLabelValues(db, user).Inc()
	}
}

// OnTxRollback increments the per-(db, user) rollback counter.
func OnTxRollback(db, user string) {
	if Active != nil {
		Active.TxRollbacks.WithLabelValues(db, user).Inc()
	}
}

// OnQueryDuration observes a per-(db, user) query duration in seconds.
func OnQueryDuration(db, user string, seconds float64) {
	if Active != nil {
		Active.QueryDurationSec.WithLabelValues(db, user).Observe(seconds)
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
func OnQueryTimeout(db, user string) {
	if Active != nil {
		Active.QueryTimeouts.WithLabelValues(db, user).Inc()
	}
}

// OnClientIdleTimeout counts clients evicted by client_idle_timeout.
func OnClientIdleTimeout(db, user string) {
	if Active != nil {
		Active.ClientIdleTimeouts.WithLabelValues(db, user).Inc()
	}
}

// OnIdleTxTimeout counts clients killed by idle_transaction_timeout.
func OnIdleTxTimeout(db, user string) {
	if Active != nil {
		Active.IdleTxTimeouts.WithLabelValues(db, user).Inc()
	}
}

// OnSighupReload counts SIGHUP-driven config reloads.
// `outcome` = "ok" | "fail".
func OnSighupReload(outcome string) {
	if Active != nil {
		Active.SighupReloads.WithLabelValues(outcome).Inc()
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
	SighupReloads *prometheus.CounterVec // {"outcome": "ok"|"fail"}
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
		}, []string{"database", "user"}),
		TxStarts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_tx_starts_total",
			Help: "Transactions opened (BEGIN observed).",
		}, []string{"database", "user"}),
		TxCommits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_tx_commits_total",
			Help: "Transactions committed (T -> I via success).",
		}, []string{"database", "user"}),
		TxRollbacks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_tx_rollbacks_total",
			Help: "Transactions rolled back (E -> I, or explicit ROLLBACK).",
		}, []string{"database", "user"}),
		QueryDurationSec: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "pgrouter_query_duration_seconds",
			Help:    "Per-query duration, from first byte to ReadyForQuery.",
			Buckets: defaultBuckets,
		}, []string{"database", "user"}),

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
		}, []string{"database", "user"}),
		ClientIdleTimeouts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_client_idle_timeouts_total",
			Help: "Clients evicted by client_idle_timeout.",
		}, []string{"database", "user"}),
		IdleTxTimeouts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_idle_transaction_timeouts_total",
			Help: "Clients killed by idle_transaction_timeout.",
		}, []string{"database", "user"}),
		SighupReloads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgrouter_sighup_reloads_total",
			Help: "SIGHUP-driven config reloads.",
		}, []string{"outcome"}),
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
		m.SighupReloads,
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
