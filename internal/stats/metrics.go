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

	// Wire-protocol.
	QueriesTotal     prometheus.Counter
	TxStarts         prometheus.Counter
	TxCommits        prometheus.Counter
	TxRollbacks      prometheus.Counter
	QueryDurationSec prometheus.Histogram

	// Auth.
	AuthAttempts prometheus.Counter
	AuthFailures prometheus.Counter

	// Cancel routing.
	CancelsReceived  prometheus.Counter
	CancelsForwarded prometheus.Counter
	CancelsDropped   prometheus.Counter
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

		QueriesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_queries_total",
			Help: "Total Query + Parse messages forwarded.",
		}),
		TxStarts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_tx_starts_total",
			Help: "Transactions opened (BEGIN observed).",
		}),
		TxCommits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_tx_commits_total",
			Help: "Transactions committed (T -> I via success).",
		}),
		TxRollbacks: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pgrouter_tx_rollbacks_total",
			Help: "Transactions rolled back (E -> I, or explicit ROLLBACK).",
		}),
		QueryDurationSec: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "pgrouter_query_duration_seconds",
			Help:    "Per-query duration, from first byte to ReadyForQuery.",
			Buckets: defaultBuckets,
		}),

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
