// Package wire collects the shared cfg→component builders used by
// both cmd/pgrouter (binary mode) and pkg/pgrouter (library mode).
//
// Before this package existed, TLS configs, userlist+hba+auth_query
// loading, pool dialers, replica managers, primary monitors, audit
// writers, and PooledHandler construction were each duplicated in
// roughly the same shape across cmd/main.go + cmd/wire.go and
// pkg/pgrouter/server.go. Bug fixes had to be applied in two places
// (the missing M.7.6 dialer retry on the library side was one such
// drift). This package is the canonical source.
//
// cmd/main.go layers on top: signal handling, SIGHUP reload, the
// admin HTTP API, and a few command-line concerns.
// pkg/pgrouter.Server layers on top: Run/Start/Stop lifecycle, no
// process-wide signal handling, optional metrics registration.

package wire

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/auth"
	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/cancel"
	"github.com/JustAnotherDevv/pgrouter/internal/client"
	"github.com/JustAnotherDevv/pgrouter/internal/config"
	"github.com/JustAnotherDevv/pgrouter/internal/listener"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
	"github.com/JustAnotherDevv/pgrouter/internal/replica"
	"github.com/JustAnotherDevv/pgrouter/internal/stats"
)

// BuildTLS returns (clientTLS, backendTLS, backendTLSRequired).
// Both nil when TLS isn't configured; backendTLSRequired stays false
// in that case as well.
func BuildTLS(cfg *config.Config) (*tls.Config, *tls.Config, bool, error) {
	clientTLS, _, err := listener.BuildServerTLSConfig(cfg.TLS)
	if err != nil {
		return nil, nil, false, fmt.Errorf("client TLS: %w", err)
	}
	backendTLS, err := listener.BuildBackendTLSConfig(cfg.TLS)
	if err != nil {
		return nil, nil, false, fmt.Errorf("backend TLS: %w", err)
	}
	required := cfg.TLS.ServerMode == config.SSLRequire ||
		cfg.TLS.ServerMode == config.SSLVerifyCA ||
		cfg.TLS.ServerMode == config.SSLVerifyFull
	return clientTLS, backendTLS, required, nil
}

// BuildAuthOpts wires userlist, hba file, and auth_query fetcher into a
// single ServerAuthOptions. Returns (nil, nil, nil) when auth_type is
// trust + no userlist/hba/fetcher are configured.
//
// The userlist is also returned separately because the SIGHUP reloader
// + the per-db dialer fallback both need direct access.
//
// `appName` is the application_name sent on the bootstrap auth_query
// dial; cmd-mode passes "pgrouter-auth_query", lib-mode passes
// "pgrouter-lib-auth_query".
func BuildAuthOpts(cfg *config.Config, backendTLS *tls.Config,
	backendTLSRequired bool, appName string, log *slog.Logger,
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
					AppName:     appName,
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

// BuildPoolDialer returns the dialerFor closure pool.NewManager expects.
// Always wraps the raw dial in DialWithRetry so a flapping backend gets
// the M.7.6 backoff schedule (in both cmd and lib mode — earlier
// versions of pkg/pgrouter forgot the retry wrapper).
//
// `appName` is the application_name reported on each upstream dial;
// cmd passes "pgrouter", lib passes "pgrouter-lib".
func BuildPoolDialer(cfg *config.Config, userlist *auth.Userlist,
	backendTLS *tls.Config, backendTLSRequired bool, appName string,
	log *slog.Logger,
) func(pool.Key) pool.Dialer {
	return func(k pool.Key) pool.Dialer {
		db, ok := cfg.Databases[k.DB]
		if !ok {
			return func(_ context.Context) (*backend.Conn, error) {
				return nil, fmt.Errorf("unknown database %q", k.DB)
			}
		}
		addr := net.JoinHostPort(db.Host, strconv.Itoa(db.Port))
		upstreamUser := k.User
		if db.User != "" {
			upstreamUser = db.User
		}
		dbName := db.DBName
		if dbName == "" {
			dbName = k.DB
		}
		password := db.Password
		if password == "" && userlist != nil {
			if entry, ok := userlist.Lookup(upstreamUser); ok && entry.PlainPassword != "" {
				password = entry.PlainPassword
			}
		}
		dialOnce := func(ctx context.Context) (*backend.Conn, error) {
			return backend.Dial(ctx, backend.DialOptions{
				Addr:        addr,
				User:        upstreamUser,
				Database:    dbName,
				AppName:     appName,
				Password:    password,
				TLSConfig:   backendTLS,
				TLSRequired: backendTLSRequired,
				Log:         log,
			})
		}
		return func(ctx context.Context) (*backend.Conn, error) {
			return backend.DialWithRetry(ctx, addr, log,
				backend.DialRetryConfig{}, dialOnce)
		}
	}
}

// BuildPoolManager constructs the per-(db,user) pool.Manager with the
// standard stats callbacks + per-db config overrides + global
// max_db/user_connections caps.
func BuildPoolManager(cfg *config.Config, cancelTracker *cancel.Tracker,
	dialerFor func(pool.Key) pool.Dialer, log *slog.Logger,
) *pool.Manager {
	_ = cancelTracker // reserved for future use (callbacks that observe cancel routing)
	cbs := pool.Callbacks{
		OnAcquireWait: func(name string, d time.Duration) {
			stats.OnPoolAcquireWait(name, d.Seconds())
		},
		OnDial:      stats.OnPoolDial,
		OnDialError: stats.OnPoolDialError,
		OnEvict:     stats.OnPoolEvict,
	}
	defaultCfg := pool.Config{
		DefaultPoolSize:    cfg.Pool.DefaultPoolSize,
		MinPoolSize:        cfg.Pool.MinPoolSize,
		ReservePoolSize:    cfg.Pool.ReservePoolSize,
		ReservePoolTimeout: cfg.Pool.ReservePoolTimeout,
		QueryWait:          cfg.Pool.QueryWaitTimeout,
		ServerIdle:         cfg.Pool.ServerIdle,
		ServerLifetime:     cfg.Pool.ServerLifetime,
		ResetQuery:         cfg.Pool.ServerResetQuery,
		Log:                log,
		Callbacks:          cbs,
	}
	mgr := pool.NewManager(defaultCfg, dialerFor).
		WithConfigFor(func(k pool.Key) *pool.Config {
			db, ok := cfg.Databases[k.DB]
			if !ok {
				return nil
			}
			ov := &pool.Config{}
			set := false
			if db.PoolSize > 0 {
				ov.DefaultPoolSize = db.PoolSize
				set = true
			}
			if db.ServerResetQuery != "" {
				ov.ResetQuery = db.ServerResetQuery
				set = true
			}
			if !set {
				return nil
			}
			return ov
		}).
		WithGlobalLimits(cfg.Pool.MaxDBConn, cfg.Pool.MaxUserConn,
			stats.OnGlobalLimitReject)
	return mgr
}

// DefaultPoolConfig returns the pool.Config the replica builders use
// for their per-replica pools. Mirrors what BuildPoolManager would
// build internally — exported so replica.BuildManagersFromConfig
// can be called from outside.
func DefaultPoolConfig(cfg *config.Config, log *slog.Logger) pool.Config {
	return pool.Config{
		DefaultPoolSize:    cfg.Pool.DefaultPoolSize,
		MinPoolSize:        cfg.Pool.MinPoolSize,
		ReservePoolSize:    cfg.Pool.ReservePoolSize,
		ReservePoolTimeout: cfg.Pool.ReservePoolTimeout,
		QueryWait:          cfg.Pool.QueryWaitTimeout,
		ServerIdle:         cfg.Pool.ServerIdle,
		ServerLifetime:     cfg.Pool.ServerLifetime,
		ResetQuery:         cfg.Pool.ServerResetQuery,
		Log:                log,
	}
}

// BuildReplicaManagers projects cfg.Databases into the shared
// replica.BuildManagersFromConfig surface.
func BuildReplicaManagers(cfg *config.Config, defaultCfg pool.Config,
	backendTLS *tls.Config, backendTLSRequired bool, log *slog.Logger,
) map[string]*replica.Manager {
	dbs := make([]replica.DBDef, 0, len(cfg.Databases))
	for name, db := range cfg.Databases {
		if len(db.Replicas) == 0 {
			continue
		}
		reps := make([]replica.ReplicaDef, 0, len(db.Replicas))
		for _, r := range db.Replicas {
			reps = append(reps, replica.ReplicaDef{
				Host: r.Host, Port: r.Port, Weight: r.Weight,
			})
		}
		dbs = append(dbs, replica.DBDef{
			Name:               name,
			DBName:             db.DBName,
			User:               db.User,
			Password:           db.Password,
			Replicas:           reps,
			MaxReplicaLagBytes: db.MaxReplicaLagBytes,
		})
	}
	dial := func(addr, user, dbname, password string) backend.DialOptions {
		return backend.DialOptions{
			Addr:        addr,
			User:        user,
			Database:    dbname,
			AppName:     "pgrouter-replica",
			Password:    password,
			TLSConfig:   backendTLS,
			TLSRequired: backendTLSRequired,
			Log:         log,
		}
	}
	return replica.BuildManagersFromConfig(dbs, defaultCfg, dial,
		cfg.Pool.ServerCheckDelay, cfg.Pool.ServerCheckQuery, log)
}

// PrimaryProbeDialer returns the dedicated-conn dialer used by
// PrimaryMonitor. Owns its own *backend.Conn — does NOT go through
// the client pool — so a client traffic spike can't starve the probe.
func PrimaryProbeDialer(cfg *config.Config, dbName string,
	backendTLS *tls.Config, backendTLSRequired bool, log *slog.Logger,
) func(context.Context) (*backend.Conn, error) {
	return func(ctx context.Context) (*backend.Conn, error) {
		db, ok := cfg.Databases[dbName]
		if !ok {
			return nil, fmt.Errorf("primary probe: unknown db %q", dbName)
		}
		addr := net.JoinHostPort(db.Host, strconv.Itoa(db.Port))
		user := db.User
		if user == "" {
			user = "pgrouter"
		}
		dbReal := db.DBName
		if dbReal == "" {
			dbReal = dbName
		}
		return backend.Dial(ctx, backend.DialOptions{
			Addr:        addr,
			User:        user,
			Database:    dbReal,
			AppName:     "pgrouter-primary-health",
			Password:    db.Password,
			TLSConfig:   backendTLS,
			TLSRequired: backendTLSRequired,
			Log:         log,
		})
	}
}

// BuildPrimaryMonitors creates one PrimaryMonitor per configured
// database + Starts each. Caller must Stop them on shutdown.
func BuildPrimaryMonitors(cfg *config.Config, backendTLS *tls.Config,
	backendTLSRequired bool, log *slog.Logger,
) map[string]*replica.PrimaryMonitor {
	out := make(map[string]*replica.PrimaryMonitor, len(cfg.Databases))
	for dbName := range cfg.Databases {
		dbName := dbName
		probeDial := PrimaryProbeDialer(cfg, dbName, backendTLS,
			backendTLSRequired, log)
		pm := replica.NewPrimaryMonitor(dbName, probeDial,
			cfg.Pool.ServerCheckDelay, 3, cfg.Pool.ServerCheckQuery, log)
		pm.Start()
		out[dbName] = pm
	}
	return out
}

// HandlerInput consolidates the dozen-plus dependencies the
// PooledHandler constructor needs into a single struct so callers
// read as a setup table rather than a long arg list.
type HandlerInput struct {
	Cfg             *config.Config
	Log             *slog.Logger
	Mgr             *pool.Manager
	ClientTLS       *tls.Config
	AuthOpts        *auth.ServerAuthOptions
	CancelTracker   *cancel.Tracker
	CannedParams    map[string]string
	LogSQLMode      string
	AuditWriter     *client.AuditWriter
	ReplicaMgrs     map[string]*replica.Manager
	PrimaryMonitors map[string]*replica.PrimaryMonitor
	// AdminReload, if non-nil, is what the SQL admin console's RELOAD
	// command fires. Library mode usually leaves this nil; cmd-mode
	// wires it to the SIGHUP fan-in channel.
	AdminReload func() error
}

// BuildPooledHandler converts cfg + live dependencies into the
// configured PooledHandler. All replica-routing / pool-mode /
// QPS-cap closures bind to the cfg + dependency maps captured here;
// SIGHUP reload (cmd-mode) updates cfg in place so the closures keep
// returning fresh values.
func BuildPooledHandler(in HandlerInput) *client.PooledHandler {
	return &client.PooledHandler{
		Log:               in.Log,
		Manager:           in.Mgr,
		TLSConfig:         in.ClientTLS,
		Auth:              in.AuthOpts,
		CancelTracker:     in.CancelTracker,
		CannedParams:      in.CannedParams,
		ResetOnRelease:    true,
		QueryTimeout:      in.Cfg.Pool.QueryTimeout,
		ClientIdleTimeout: in.Cfg.Server.ClientIdle,
		IdleTxTimeout:     in.Cfg.Server.IdleTx,
		SlowQuery:         in.Cfg.Logging.SlowQuery,
		LogSQL:            in.LogSQLMode,
		Audit:             in.AuditWriter,
		AdminReload:       in.AdminReload,
		ReplicaPickerFor: func(db string) *pool.Pool {
			rm, ok := in.ReplicaMgrs[db]
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
			if d, ok := in.Cfg.Databases[db]; ok {
				return d.StickyReadWindow
			}
			return 0
		},
		PrimaryHealthyFor: func(db string) bool {
			pm, ok := in.PrimaryMonitors[db]
			if !ok {
				return true
			}
			return pm.Healthy()
		},
		PoolMode: string(in.Cfg.Pool.Mode),
		PoolModeFor: func(db string) string {
			if d, ok := in.Cfg.Databases[db]; ok && d.PoolMode != "" {
				return string(d.PoolMode)
			}
			return ""
		},
		QPSCapFor: func(db, user string) float64 {
			if u, ok := in.Cfg.Users[user]; ok && u.MaxQPS > 0 {
				return u.MaxQPS
			}
			if d, ok := in.Cfg.Databases[db]; ok && d.MaxQPS > 0 {
				return d.MaxQPS
			}
			return 0
		},
	}
}

// CannedParams returns the canned StartupMessage values pgrouter
// reports to clients before any real backend is attached. The first
// successful upstream dial replaces these with the live values from
// pool.CachedParams.
//
// These come from real Postgres' standard parameter set so drivers
// that branch on specific fields (psql is_superuser, pgx
// server_version-keyed protocol decisions, dashboards watching
// application_name) don't degrade in the cold-start window.
func CannedParams() map[string]string {
	return map[string]string{
		"server_version":              "16.0 (pgrouter)",
		"server_encoding":             "UTF8",
		"client_encoding":             "UTF8",
		"DateStyle":                   "ISO, MDY",
		"IntervalStyle":               "postgres",
		"TimeZone":                    "UTC",
		"integer_datetimes":           "on",
		"standard_conforming_strings": "on",
		"is_superuser":                "off",
		"session_authorization":       "pgrouter",
		"application_name":            "",
	}
}
