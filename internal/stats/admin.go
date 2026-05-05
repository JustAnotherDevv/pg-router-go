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
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
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

// StatsSnapshot + SnapshotFromRegistry live in snapshot.go since the
// AL8 refactor — admin.go strictly holds the AdminAPI surface + HTTP
// handlers.

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
	// /healthz = liveness. Always 200 unless the process is broken
	// (in which case the HTTP server is broken too).
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// /readyz = readiness. 200 only when the pooler is accepting
	// traffic; 503 during graceful drain or before listeners are up.
	// K8s readinessProbe should target /readyz so it stops routing
	// new traffic the moment SIGTERM lands.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if Ready.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("draining"))
	})
	mux.HandleFunc("/api/v1/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if Ready.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("draining"))
	})

	mux.HandleFunc("/api/v1/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Read-only GETs (no token check).
	get := func(path, missing string, fn func() (any, error)) {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if fn == nil {
				http.Error(w, missing, http.StatusNotImplemented)
				return
			}
			v, err := fn()
			if err != nil {
				writeJSON(w, http.StatusInternalServerError,
					map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, v)
		})
	}
	// State-changing POSTs (token-gated, decodes a body via decoder if non-nil).
	post := func(path, missing string, decoder func(*http.Request) error, fn func() (any, error)) {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if !checkToken(r, opts.AuthToken) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			if fn == nil {
				http.Error(w, missing, http.StatusNotImplemented)
				return
			}
			if decoder != nil {
				if err := decoder(r); err != nil {
					writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
					return
				}
			}
			v, err := fn()
			if err != nil {
				writeJSON(w, http.StatusInternalServerError,
					map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, v)
		})
	}

	get("/api/v1/version", "version not wired", func() (any, error) {
		return VersionInfo{Version: Build.Version, Commit: Build.Commit}, nil
	})
	get("/api/v1/pools", "pools API not wired", apiFn(opts.Admin, func(a *AdminAPI) (any, error) {
		if a.Pools == nil {
			return nil, nil // → 501 via fn-nil path; mapped below
		}
		return a.Pools()
	}))
	get("/api/v1/stats", "stats API not wired", apiFn(opts.Admin, func(a *AdminAPI) (any, error) {
		if a.Stats == nil {
			return nil, nil
		}
		return a.Stats()
	}))

	// /api/v1/drain accepts {"deadline_seconds": int}; 30s default.
	deadline := 30 * time.Second
	post("/api/v1/drain", "drain API not wired",
		func(r *http.Request) error {
			var body struct {
				DeadlineSeconds int `json:"deadline_seconds"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.DeadlineSeconds > 0 {
				deadline = time.Duration(body.DeadlineSeconds) * time.Second
			}
			return nil
		},
		apiFn(opts.Admin, func(a *AdminAPI) (any, error) {
			if a.Drain == nil {
				return nil, nil
			}
			if err := a.Drain(deadline); err != nil {
				return nil, err
			}
			return map[string]string{"status": "drained"}, nil
		}))

	post("/api/v1/reload", "reload API not wired", nil,
		apiFn(opts.Admin, func(a *AdminAPI) (any, error) {
			if a.Reload == nil {
				return nil, nil
			}
			if err := a.Reload(); err != nil {
				return nil, err
			}
			return map[string]string{"status": "reloaded"}, nil
		}))

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
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return false
	}
	got := strings.TrimPrefix(h, prefix)
	// HB1: constant-time compare. Plain == short-circuits on first
	// byte mismatch + leaks token length via timing; an attacker can
	// recover the token byte-by-byte by measuring response latency.
	// subtle.ConstantTimeCompare runs in time proportional to len(want)
	// regardless of input — and length-mismatched inputs are rejected
	// without short-circuiting via a fixed-cost path.
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// apiFn returns nil when AdminAPI is nil so the get/post wrappers
// treat "not wired" uniformly. Otherwise it binds the caller's
// handler to the live AdminAPI.
func apiFn(a *AdminAPI, h func(*AdminAPI) (any, error)) func() (any, error) {
	if a == nil {
		return nil
	}
	return func() (any, error) { return h(a) }
}

// Ready is the process-wide readiness flag served by /readyz.
//
// Set true by cmd.cmdRun / pkg.Server.Start once the listener is
// accepting connections. Set false at the start of CloseWithDeadline /
// Server.Stop so K8s readiness probes flip to 503 immediately.
var Ready atomic.Bool

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

