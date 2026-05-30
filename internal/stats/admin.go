// Admin HTTP API.
//
// Read-only GET endpoints (open):
//
//   GET  /api/v1/version    → {"version":"...","commit":"..."}
//   GET  /api/v1/pools      → [{name, idle, active, waiters, ...}, ...]
//   GET  /api/v1/stats      → flat metric snapshot (Prom families)
//   GET  /api/v1/healthz    → "ok" (alias for /healthz)
//
// State-changing POST endpoints (gated by Bearer token if set):
//
//   POST /api/v1/drain      → graceful drain of all pools; body
//                             {"deadline_seconds": int} optional
//   POST /api/v1/reload     → re-read pgrouter.yaml + userlist.txt
//                             (same as SIGHUP)
//
// To keep the stats package free of import cycles, all operations
// are supplied by the cmd binary via the AdminAPI struct: closures
// over pool.Manager / config reload / etc.
//
// The token check is fail-closed: if cfg.AuthToken is non-empty,
// POSTs without the matching `Authorization: Bearer <token>` are
// rejected with 401. Empty token = open (dev/local use; production
// must set one).

package stats

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// AdminAPI is the per-operation surface the admin HTTP endpoints call.
// Any nil handler returns 501 Not Implemented.
type AdminAPI struct {
	Pools  func() ([]PoolSnapshot, error)
	Stats  func() (StatsSnapshot, error)
	Drain  func(deadline time.Duration) error
	Reload func() error
}

// PoolSnapshot is one row in GET /api/v1/pools.
type PoolSnapshot struct {
	Name    string `json:"name"`     // "db/user"
	DB      string `json:"db"`
	User    string `json:"user"`
	Size    int    `json:"size"`     // configured DefaultPoolSize
	Idle    int    `json:"idle"`     // backends sitting in LIFO stack
	Active  int    `json:"active"`   // backends currently checked out
	Waiters int    `json:"waiters"`  // clients queued in Acquire
}

// StatsSnapshot is a compact rollup for GET /api/v1/stats.
type StatsSnapshot struct {
	UptimeSeconds   float64 `json:"uptime_seconds"`
	ClientsActive   int     `json:"clients_active"`
	BackendsActive  int     `json:"backends_active"`
	BackendsIdle    int     `json:"backends_idle"`
	QueriesTotal    float64 `json:"queries_total"`
	TxStartsTotal   float64 `json:"tx_starts_total"`
	PreparedHits    float64 `json:"prepared_hits_total"`
	PreparedMisses  float64 `json:"prepared_misses_total"`
}

// VersionInfo is GET /api/v1/version.
type VersionInfo struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
}

// AdminServerOptions configures ServeMetricsAndAdmin.
type AdminServerOptions struct {
	Addr        string    // ":9090" if empty
	MetricsPath string    // "/metrics" if empty
	AuthToken   string    // "" → POST endpoints are open (dev mode)
	Admin       *AdminAPI // nil → API endpoints return 501
}

// ServeMetricsAndAdmin runs the combined HTTP listener: Prometheus
// /metrics + /healthz + /api/v1/*. Returns nil on clean shutdown via
// ctx; the underlying ListenAndServe error otherwise.
func ServeMetricsAndAdmin(ctx context.Context, opts AdminServerOptions, log *slog.Logger) error {
	if opts.Addr == "" {
		opts.Addr = ":9090"
	}
	if opts.MetricsPath == "" {
		opts.MetricsPath = "/metrics"
	}

	mux := http.NewServeMux()
	mux.Handle(opts.MetricsPath, promhttp.HandlerFor(Reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/api/v1/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/api/v1/version", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, VersionInfo{
			Version: Build.Version, Commit: Build.Commit,
		})
	})

	mux.HandleFunc("/api/v1/pools", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if opts.Admin == nil || opts.Admin.Pools == nil {
			http.Error(w, "pools API not wired", http.StatusNotImplemented)
			return
		}
		ps, err := opts.Admin.Pools()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, ps)
	})
	mux.HandleFunc("/api/v1/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if opts.Admin == nil || opts.Admin.Stats == nil {
			http.Error(w, "stats API not wired", http.StatusNotImplemented)
			return
		}
		s, err := opts.Admin.Stats()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, s)
	})

	mux.HandleFunc("/api/v1/drain", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !checkToken(r, opts.AuthToken) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if opts.Admin == nil || opts.Admin.Drain == nil {
			http.Error(w, "drain API not wired", http.StatusNotImplemented)
			return
		}
		var body struct {
			DeadlineSeconds int `json:"deadline_seconds"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		deadline := time.Duration(body.DeadlineSeconds) * time.Second
		if deadline == 0 {
			deadline = 30 * time.Second
		}
		if err := opts.Admin.Drain(deadline); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "drained"})
	})
	mux.HandleFunc("/api/v1/reload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !checkToken(r, opts.AuthToken) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if opts.Admin == nil || opts.Admin.Reload == nil {
			http.Error(w, "reload API not wired", http.StatusNotImplemented)
			return
		}
		if err := opts.Admin.Reload(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "reloaded"})
	})

	srv := &http.Server{
		Addr:              opts.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("metrics+admin server listening", "addr", opts.Addr,
			"metrics_path", opts.MetricsPath,
			"admin_token_set", opts.AuthToken != "")
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		<-errCh
		return nil
	case err := <-errCh:
		return err
	}
}

// checkToken returns true when the request is authorised. Empty
// `want` is treated as "auth disabled" (open). Otherwise the caller
// must provide `Authorization: Bearer <token>` matching exactly.
func checkToken(r *http.Request, want string) bool {
	if want == "" {
		return true
	}
	h := r.Header.Get("Authorization")
	prefix := "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return false
	}
	return strings.TrimPrefix(h, prefix) == want
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// SnapshotFromRegistry scrapes the package Reg for a few rollup counts.
// Sums across all label values for vec metrics so the snapshot is
// genuinely "across the whole pgrouter process."
func SnapshotFromRegistry(uptime time.Duration) StatsSnapshot {
	out := StatsSnapshot{UptimeSeconds: uptime.Seconds()}
	families, err := Reg.Gather()
	if err != nil {
		return out
	}
	for _, mf := range families {
		switch mf.GetName() {
		case "pgrouter_client_active":
			for _, m := range mf.GetMetric() {
				if g := m.GetGauge(); g != nil {
					out.ClientsActive = int(g.GetValue())
				}
			}
		case "pgrouter_backend_active":
			for _, m := range mf.GetMetric() {
				if g := m.GetGauge(); g != nil {
					out.BackendsActive = int(g.GetValue())
				}
			}
		case "pgrouter_backend_idle":
			for _, m := range mf.GetMetric() {
				if g := m.GetGauge(); g != nil {
					out.BackendsIdle = int(g.GetValue())
				}
			}
		case "pgrouter_queries_total":
			for _, m := range mf.GetMetric() {
				if c := m.GetCounter(); c != nil {
					out.QueriesTotal += c.GetValue()
				}
			}
		case "pgrouter_tx_starts_total":
			for _, m := range mf.GetMetric() {
				if c := m.GetCounter(); c != nil {
					out.TxStartsTotal += c.GetValue()
				}
			}
		case "pgrouter_prepared_cache_hits_total":
			for _, m := range mf.GetMetric() {
				if c := m.GetCounter(); c != nil {
					out.PreparedHits += c.GetValue()
				}
			}
		case "pgrouter_prepared_cache_misses_total":
			for _, m := range mf.GetMetric() {
				if c := m.GetCounter(); c != nil {
					out.PreparedMisses += c.GetValue()
				}
			}
		}
	}
	return out
}
