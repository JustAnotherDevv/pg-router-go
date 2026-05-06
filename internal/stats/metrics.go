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

// Per-(db, user, app) tenant counters used to have package-level
// On* wrappers (OnQuery, OnTxStart, OnTxCommit, OnTxRollback,
// OnQueryDuration, OnQueryTimeout, OnClientIdleTimeout, OnIdleTxTimeout,
// OnPreparedHit, OnPreparedMiss, OnPreparedEviction). They were
// retired in WIN4: PooledConn caches *prometheus.Counter handles via
// stats.TenantCounters at Serve init, so the per-message hot path no
// longer goes through these wrappers. The fields on *Metrics + the
// TenantCounters surface are the public API now.

// OnGlobalLimitReject counts Acquires denied by the global db/user cap.
// `scope` is "db" or "user".
func OnGlobalLimitReject(scope, name string) {
	if Active != nil {
		Active.GlobalLimitRejects.WithLabelValues(scope, name).Inc()
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

// OnAuditWriteError counts audit-file write failures (SB5). Sticky in
// the sense that a disk-full event will tick rapidly; combined with a
// one-shot slog.Warn in AuditWriter, the operator gets a clear signal
// instead of silent record loss.
func OnAuditWriteError() {
	if Active != nil {
		Active.AuditWriteErrors.Inc()
	}
}

// OnProxyProtoMissing counts connections rejected (strict mode) or
// gracefully accepted (lax mode) when the PROXY preamble was expected
// but absent (SB8).
func OnProxyProtoMissing() {
	if Active != nil {
		Active.ProxyProtoMissing.Inc()
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

	// Audit + PROXY-protocol ops counters.
	AuditWriteErrors  prometheus.Counter // SB5: disk-full / EBADF on AuditWriter
	ProxyProtoMissing prometheus.Counter // SB8: PROXY preamble expected but absent

	// Inflight-clients gauge for graceful-drain observability (SB6).
	InflightClients prometheus.GaugeFunc

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
//
// WIN1 refactor: per-metric init was 4-5 LOC × 35 metrics. Now a per-
// type closure (c/cv/g/gv/h/gf) builds the metric in one line each;
// the field assignments read as a flat 35-row table. registered
// collects everything for one MustRegister at the end.
func New() *Metrics {
	registered := []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	}
	// Per-kind builders capture `registered` so every metric is auto-
	// added to the MustRegister batch. Saves repeating field names a
	// second time + keeps the registration list in sync.
	c := func(name, help string) prometheus.Counter {
		m := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
		registered = append(registered, m)
		return m
	}
	cv := func(name, help string, labels ...string) *prometheus.CounterVec {
		m := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
		registered = append(registered, m)
		return m
	}
	g := func(name, help string) prometheus.Gauge {
		m := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
		registered = append(registered, m)
		return m
	}
	gv := func(name, help string, labels ...string) *prometheus.GaugeVec {
		m := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
		registered = append(registered, m)
		return m
	}
	hv := func(name, help string, labels ...string) *prometheus.HistogramVec {
		m := prometheus.NewHistogramVec(
			prometheus.HistogramOpts{Name: name, Help: help, Buckets: defaultBuckets},
			labels)
		registered = append(registered, m)
		return m
	}
	gf := func(name, help string, fn func() float64) prometheus.GaugeFunc {
		m := prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, fn)
		registered = append(registered, m)
		return m
	}
	tenant := []string{"database", "user", "application_name"}

	m := &Metrics{
		// Client-facing.
		ClientConnsTotal:    c("pgrouter_client_connections_total", "Total client connections accepted since start."),
		ClientDisconnsTotal: c("pgrouter_client_disconnections_total", "Total client disconnections."),
		ClientActiveGauge:   g("pgrouter_client_active", "Currently-connected clients."),
		ClientBytesIn:       c("pgrouter_client_bytes_in_total", "Bytes received from clients."),
		ClientBytesOut:      c("pgrouter_client_bytes_out_total", "Bytes sent to clients."),
		// Backend-facing.
		BackendDialsTotal:     c("pgrouter_backend_dials_total", "Total upstream connect attempts."),
		BackendDialErrorsTot:  c("pgrouter_backend_dial_errors_total", "Upstream connect failures."),
		BackendActiveGauge:    g("pgrouter_backend_active", "Currently checked-out upstream backends."),
		BackendIdleGauge:      g("pgrouter_backend_idle", "Currently-idle pooled backends."),
		BackendEvictionsTotal: c("pgrouter_backend_evictions_total", "Backends evicted by the janitor (idle / lifetime)."),
		// Pool.
		PoolAcquireSeconds: hv("pgrouter_pool_acquire_seconds", "Time clients spent waiting for a pool slot.", "pool"),
		PoolWaitersGauge:   gv("pgrouter_pool_waiters", "Clients currently queued in a pool's wait queue.", "pool"),
		// Per-tenant wire counters.
		QueriesTotal:     cv("pgrouter_queries_total", "Total Query + Parse messages forwarded.", tenant...),
		TxStarts:         cv("pgrouter_tx_starts_total", "Transactions opened (BEGIN observed).", tenant...),
		TxCommits:        cv("pgrouter_tx_commits_total", "Transactions committed (T -> I via success).", tenant...),
		TxRollbacks:      cv("pgrouter_tx_rollbacks_total", "Transactions rolled back (E -> I, or explicit ROLLBACK).", tenant...),
		QueryDurationSec: hv("pgrouter_query_duration_seconds", "Per-query duration, from first byte to ReadyForQuery.", tenant...),
		// Auth.
		AuthAttempts: c("pgrouter_auth_attempts_total", "Client auth attempts."),
		AuthFailures: c("pgrouter_auth_failures_total", "Client auth failures."),
		// Cancel routing.
		CancelsReceived:  c("pgrouter_cancels_received_total", "CancelRequest packets received from clients."),
		CancelsForwarded: c("pgrouter_cancels_forwarded_total", "CancelRequest packets successfully forwarded."),
		CancelsDropped:   c("pgrouter_cancels_dropped_total", "CancelRequest packets dropped (unknown PID/secret)."),
		// Enforcement counters.
		GlobalLimitRejects: cv("pgrouter_global_limit_rejects_total", "Acquires rejected by max_db_connections / max_user_connections.", "scope", "name"),
		QueryTimeouts:      cv("pgrouter_query_timeouts_total", "Queries killed by query_timeout.", tenant...),
		ClientIdleTimeouts: cv("pgrouter_client_idle_timeouts_total", "Clients evicted by client_idle_timeout.", tenant...),
		IdleTxTimeouts:     cv("pgrouter_idle_transaction_timeouts_total", "Clients killed by idle_transaction_timeout.", tenant...),
		// Lifecycle.
		SighupReloads:         cv("pgrouter_sighup_reloads_total", "SIGHUP-driven config reloads.", "outcome"),
		SighupUserlistReloads: cv("pgrouter_sighup_userlist_reloads_total", "SIGHUP-driven userlist.txt reloads.", "outcome"),
		// Audit + PROXY-protocol ops.
		AuditWriteErrors:  c("pgrouter_audit_write_errors_total", "Audit-file Write() failures (disk full / IO error)."),
		ProxyProtoMissing: c("pgrouter_proxy_proto_missing_total", "Accepted connections that did not present a PROXY preamble (rejected when proxy_protocol_strict=true)."),
		InflightClients: gf("pgrouter_inflight_clients",
			"Client conns currently being served (decremented on Handle return).",
			func() float64 {
				if InflightFn == nil {
					return 0
				}
				return float64(InflightFn())
			}),
		// QPS + bandwidth + prepared.
		QPSRejects:        cv("pgrouter_qps_rejects_total", "Queries rejected by per-tenant max_qps token bucket.", "scope", "name"),
		BytesInPerTenant:  cv("pgrouter_tenant_bytes_in_total", "Bytes received from clients, per (database, user).", "database", "user"),
		BytesOutPerTenant: cv("pgrouter_tenant_bytes_out_total", "Bytes sent to clients, per (database, user).", "database", "user"),
		PreparedHits:      cv("pgrouter_prepared_cache_hits_total", "Parse messages whose server-side name was already cached on the backend.", tenant...),
		PreparedMisses:    cv("pgrouter_prepared_cache_misses_total", "Parse messages requiring a backend round trip (first time on this backend).", tenant...),
		PreparedEvictions: cv("pgrouter_prepared_cache_evictions_total", "Prepared-statement LRU evictions (DEALLOCATE sent to backend).", tenant...),
	}
	Reg.MustRegister(registered...)
	Active = m
	return m
}

// InflightFn is set by main / pkg.Server at boot so the
// pgrouter_inflight_clients gauge can return the live count without
// internal/stats importing internal/client (which would be a cycle).
// nil → gauge reports 0.
var InflightFn func() int64

// defaultBuckets is tuned for pgwire RTTs (sub-ms to multi-second slow
// queries). Override per-histogram if a different distribution fits.
var defaultBuckets = []float64{
	0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1,
	0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
}
