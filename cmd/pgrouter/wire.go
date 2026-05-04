// Extracted builders for cmdRun. Each takes the already-parsed
// config + dependencies, returns the constructed component.
//
// Goal: keep cmdRun a top-down orchestration that reads like a
// table of contents.

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/auth"
	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/cancel"
	"github.com/JustAnotherDevv/pgrouter/internal/client"
	"github.com/JustAnotherDevv/pgrouter/internal/config"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
	"github.com/JustAnotherDevv/pgrouter/internal/replica"
	"github.com/JustAnotherDevv/pgrouter/internal/stats"
)

// buildAuthOpts wires userlist, hba file, auth_query fetcher into a
// single ServerAuthOptions. Returns (nil, nil, nil) when auth_type
// is trust + no userlist/hba/fetcher are configured.
//
// The userlist is also returned separately because the SIGHUP reloader
// + the per-db dialer fallback both need direct access.
func buildAuthOpts(cfg *config.Config, backendTLS *tls.Config,
	backendTLSRequired bool, log *slog.Logger,
) (*auth.ServerAuthOptions, *auth.Userlist, error) {
	var (
		userlist *auth.Userlist
		hba      *auth.HBAFile
		fetcher  *auth.AuthQueryFetcher
	)
	if cfg.Auth.UserlistFile != "" {
		ul, err := auth.NewUserlist(cfg.Auth.UserlistFile)
		if err != nil {
			return nil, nil, fmt.Errorf("userlist: %w", err)
		}
		userlist = ul
		log.Info("userlist loaded",
			"path", cfg.Auth.UserlistFile, "entries", ul.Len())
	}
	if cfg.Auth.HBAFile != "" {
		h, err := auth.NewHBAFile(cfg.Auth.HBAFile)
		if err != nil {
			return nil, nil, fmt.Errorf("hba: %w", err)
		}
		hba = h
		log.Info("hba loaded", "path", cfg.Auth.HBAFile)
	}
	if cfg.Auth.AuthQuery != "" {
		fetcher = auth.NewAuthQueryFetcher(
			func(ctx context.Context, dbAlias string) (auth.QueryConn, error) {
				db, ok := cfg.Databases[dbAlias]
				if !ok {
					return nil, fmt.Errorf("auth_query: unknown db %q", dbAlias)
				}
				addr := net.JoinHostPort(db.Host, strconv.Itoa(db.Port))
				dbName := db.DBName
				if dbName == "" {
					dbName = dbAlias
				}
				c, err := backend.Dial(ctx, backend.DialOptions{
					Addr:        addr,
					User:        cfg.Auth.AuthUser,
					Database:    dbName,
					AppName:     "pgrouter-auth_query",
					Password:    db.Password,
					TLSConfig:   backendTLS,
					TLSRequired: backendTLSRequired,
					Log:         log,
				})
				if err != nil {
					return nil, err
				}
				return &auth.FrontendAdapter{
					Frontend: c.Frontend,
					Closer:   c.Close,
				}, nil
			},
			cfg.Auth.AuthQuery,
			60*time.Second,
		)
		log.Info("auth_query configured", "user", cfg.Auth.AuthUser)
	}
	if cfg.Auth.Type == config.AuthTrust {
		return nil, userlist, nil
	}
	return &auth.ServerAuthOptions{
		Type:     cfg.Auth.Type,
		Userlist: userlist,
		HBA:      hba,
		Fetcher:  fetcher,
		Log:      log,
	}, userlist, nil
}

// buildPrimaryMonitors creates one PrimaryMonitor per configured
// database. Each owns its own probe conn (NOT borrowed from client
// pool) so client traffic spikes can't starve the health check.
func buildPrimaryMonitors(cfg *config.Config, backendTLS *tls.Config,
	backendTLSRequired bool, log *slog.Logger,
) map[string]*replica.PrimaryMonitor {
	out := make(map[string]*replica.PrimaryMonitor, len(cfg.Databases))
	for dbName := range cfg.Databases {
		dbName := dbName
		probeDial := primaryProbeDialer(cfg, dbName, backendTLS,
			backendTLSRequired, log)
		pm := replica.NewPrimaryMonitor(dbName, probeDial,
			cfg.Pool.ServerCheckDelay, 3, cfg.Pool.ServerCheckQuery, log)
		pm.Start()
		out[dbName] = pm
	}
	return out
}

// buildAdminAPI wires the HTTP admin endpoint to the live pool
// manager + reload channel. Drains never run synchronously on the
// HTTP request path; Drain just calls CloseWithDeadline which
// internally honours its own deadline.
func buildAdminAPI(mgr *pool.Manager, adminReloadCh chan os.Signal,
	startTime time.Time,
) *stats.AdminAPI {
	return &stats.AdminAPI{
		Pools: func() ([]stats.PoolSnapshot, error) {
			out := []stats.PoolSnapshot{}
			for _, ps := range mgr.AllStats() {
				db, user, _ := splitPoolName(ps.Name)
				out = append(out, stats.PoolSnapshot{
					Name:    ps.Name,
					DB:      db,
					User:    user,
					Idle:    ps.Idle,
					Active:  ps.Active,
					Waiters: ps.Waiters,
				})
			}
			return out, nil
		},
		Stats: func() (stats.StatsSnapshot, error) {
			return stats.SnapshotFromRegistry(time.Since(startTime)), nil
		},
		Drain: func(deadline time.Duration) error {
			return mgr.CloseWithDeadline(time.Now().Add(deadline))
		},
		Reload: func() error {
			select {
			case adminReloadCh <- syscall.SIGHUP:
				return nil
			default:
				return fmt.Errorf("reload channel busy")
			}
		},
	}
}

// buildHandlerInput consolidates the dozen-plus dependencies the
// PooledHandler constructor needs into a single struct so cmdRun's
// call site reads as a setup table rather than a long arg list.
type buildHandlerInput struct {
	cfg             *config.Config
	log             *slog.Logger
	mgr             *pool.Manager
	clientTLS       *tls.Config
	authOpts        *auth.ServerAuthOptions
	cancelTracker   *cancel.Tracker
	cannedParams    map[string]string
	logSQLMode      string
	auditWriter     *client.AuditWriter
	adminReloadCh   chan os.Signal
	replicaMgrs     map[string]*replica.Manager
	primaryMonitors map[string]*replica.PrimaryMonitor
}

// buildPooledHandler converts cfg + live dependencies into the
// configured PooledHandler. All replica-routing / pool-mode /
// QPS-cap closures bind to the cfg + dependency maps captured here;
// SIGHUP reload updates cfg in place so the closures keep returning
// fresh values.
func buildPooledHandler(in buildHandlerInput) *client.PooledHandler {
	return &client.PooledHandler{
		Log:               in.log,
		Manager:           in.mgr,
		TLSConfig:         in.clientTLS,
		Auth:              in.authOpts,
		CancelTracker:     in.cancelTracker,
		CannedParams:      in.cannedParams,
		ResetOnRelease:    true,
		QueryTimeout:      in.cfg.Pool.QueryTimeout,
		ClientIdleTimeout: in.cfg.Server.ClientIdle,
		IdleTxTimeout:     in.cfg.Server.IdleTx,
		SlowQuery:         in.cfg.Logging.SlowQuery,
		LogSQL:            in.logSQLMode,
		Audit:             in.auditWriter,
		AdminReload: func() error {
			select {
			case in.adminReloadCh <- syscall.SIGHUP:
				return nil
			default:
				return fmt.Errorf("reload channel busy")
			}
		},
		ReplicaPickerFor: func(db string) *pool.Pool {
			rm, ok := in.replicaMgrs[db]
			if !ok {
				return nil
			}
			r, err := rm.Pick()
			if err != nil {
				return nil
			}
			return r.Pool
		},
		StickyReadWindowFor: func(db string) time.Duration {
			if d, ok := in.cfg.Databases[db]; ok {
				return d.StickyReadWindow
			}
			return 0
		},
		PrimaryHealthyFor: func(db string) bool {
			pm, ok := in.primaryMonitors[db]
			if !ok {
				return true
			}
			return pm.Healthy()
		},
		PoolMode: string(in.cfg.Pool.Mode),
		PoolModeFor: func(db string) string {
			if d, ok := in.cfg.Databases[db]; ok && d.PoolMode != "" {
				return string(d.PoolMode)
			}
			return ""
		},
		QPSCapFor: func(db, user string) float64 {
			// Per-user cap wins if set; else per-db; else 0 (disabled).
			if u, ok := in.cfg.Users[user]; ok && u.MaxQPS > 0 {
				return u.MaxQPS
			}
			if d, ok := in.cfg.Databases[db]; ok && d.MaxQPS > 0 {
				return d.MaxQPS
			}
			return 0
		},
	}
}
