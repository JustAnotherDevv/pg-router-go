// Package wire collects the shared cfgâ†’component builders used by
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
	"strings"
	"time"

	"github.com/JustAnotherDevv/pg-router-go/internal/auth"
	"github.com/JustAnotherDevv/pg-router-go/internal/backend"
	"github.com/JustAnotherDevv/pg-router-go/internal/cancel"
	"github.com/JustAnotherDevv/pg-router-go/internal/client"
	"github.com/JustAnotherDevv/pg-router-go/internal/config"
	"github.com/JustAnotherDevv/pg-router-go/internal/listener"
	"github.com/JustAnotherDevv/pg-router-go/internal/pool"
	"github.com/JustAnotherDevv/pg-router-go/internal/replica"
	"github.com/JustAnotherDevv/pg-router-go/internal/stats"
	"github.com/JustAnotherDevv/pg-router-go/internal/wire/splice"
)

// dialEnv carries the per-process TLS + log defaults every backend
// dial inherits. Avoids re-typing the 3 fields at every call site.
type dialEnv struct {
	tls         *tls.Config
	tlsRequired bool
	log         *slog.Logger
}

// opts builds a backend.DialOptions inheriting the env defaults. Callers
// supply only what varies per call (addr, user, db, app, password).
func (e dialEnv) opts(addr, user, db, appName, password string) backend.DialOptions {
	return backend.DialOptions{
		Addr:        addr,
		User:        user,
		Database:    db,
		AppName:     appName,
		Password:    password,
		TLSConfig:   e.tls,
		TLSRequired: e.tlsRequired,
		Log:         e.log,
	}
}

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
		env := dialEnv{tls: backendTLS, tlsRequired: backendTLSRequired, log: log}
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
				c, err := backend.Dial(ctx, env.opts(addr, cfg.Auth.AuthUser, dbName, appName, db.Password))
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
// the M.7.6 backoff schedule (in both cmd and lib mode â€” earlier
// versions of pkg/pgrouter forgot the retry wrapper).
//
// `appName` is the application_name reported on each upstream dial;
// cmd passes "pgrouter", lib passes "pgrouter-lib".
func BuildPoolDialer(cfg *config.Config, userlist *auth.Userlist,
	backendTLS *tls.Config, backendTLSRequired bool, appName string,
	log *slog.Logger,
) func(pool.Key) pool.Dialer {
	env := dialEnv{tls: backendTLS, tlsRequired: backendTLSRequired, log: log}
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
			return backend.Dial(ctx, env.opts(addr, upstreamUser, dbName, appName, password))
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
// build internally â€” exported so replica.BuildManagersFromConfig
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
	env := dialEnv{tls: backendTLS, tlsRequired: backendTLSRequired, log: log}
	dial := func(addr, user, dbname, password string) backend.DialOptions {
		return env.opts(addr, user, dbname, "pgrouter-replica", password)
	}
	return replica.BuildManagersFromConfig(dbs, defaultCfg, dial,
		cfg.Pool.ServerCheckDelay, cfg.Pool.ServerCheckQuery, log)
}

// PrimaryProbeDialer returns the dedicated-conn dialer used by
// PrimaryMonitor. Owns its own *backend.Conn â€” does NOT go through
// the client pool â€” so a client traffic spike can't starve the probe.
func PrimaryProbeDialer(cfg *config.Config, dbName string,
	backendTLS *tls.Config, backendTLSRequired bool, log *slog.Logger,
) func(context.Context) (*backend.Conn, error) {
	env := dialEnv{tls: backendTLS, tlsRequired: backendTLSRequired, log: log}
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
		return backend.Dial(ctx, env.opts(addr, user, dbReal, "pgrouter-primary-health", db.Password))
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
	var spliceCfg *splice.SpliceConfig
	if in.Cfg.Wire.Splice != nil && *in.Cfg.Wire.Splice {
		spliceCfg = &splice.SpliceConfig{
			Enabled:    true,
			BufferSize: in.Cfg.Wire.SpliceBufferSize,
		}
	}
	preparedCache := true
	if in.Cfg.Wire.PreparedCache != nil {
		preparedCache = *in.Cfg.Wire.PreparedCache
	}
	rawPassthrough := false
	if in.Cfg.Wire.RawPassthrough != nil {
		rawPassthrough = *in.Cfg.Wire.RawPassthrough
	}
	skipReset := in.Cfg.Wire.SkipResetQuery != nil && *in.Cfg.Wire.SkipResetQuery
	gucTracking := true
	if in.Cfg.Wire.GUCTracking != nil {
		gucTracking = *in.Cfg.Wire.GUCTracking
	}
	return &client.PooledHandler{
		Log:               in.Log,
		Manager:           in.Mgr,
		TLSConfig:         in.ClientTLS,
		Auth:              in.AuthOpts,
		CancelTracker:     in.CancelTracker,
		CannedParams:      in.CannedParams,
		ResetOnRelease:    !skipReset,
		QueryTimeout:      in.Cfg.Pool.QueryTimeout,
		ClientIdleTimeout: in.Cfg.Server.ClientIdle,
		IdleTxTimeout:     in.Cfg.Server.IdleTx,
		SlowQuery:         in.Cfg.Logging.SlowQuery,
		LogSQL:            in.LogSQLMode,
		Audit:             in.AuditWriter,
		AdminReload:       in.AdminReload,
		Router:            &cfgRouter{cfg: in.Cfg, replicas: in.ReplicaMgrs, primary: in.PrimaryMonitors},
		PoolMode:          string(in.Cfg.Pool.Mode),
		PoolModeFor: func(db string) string {
			if d, ok := in.Cfg.Databases[db]; ok && d.PoolMode != "" {
				return string(d.PoolMode)
			}
			return ""
		},
		Splice:         spliceCfg,
		PreparedCache:  preparedCache,
		RawPassthrough: rawPassthrough,
		GUCTracking:    gucTracking,
	}
}

// RuntimeOptions names the mode-specific labels and hooks used while
// building a pgrouter runtime.
type RuntimeOptions struct {
	AuthAppName string
	DialAppName string
	AdminReload func() error
}

// Runtime is the shared set of live components used by cmd-mode and
// library-mode pgrouter instances.
type Runtime struct {
	ClientTLS          *tls.Config
	BackendTLS         *tls.Config
	BackendTLSRequired bool
	AuthOpts           *auth.ServerAuthOptions
	Userlist           *auth.Userlist
	CancelTracker      *cancel.Tracker
	Manager            *pool.Manager
	DefaultPoolConfig  pool.Config
	ReplicaManagers    map[string]*replica.Manager
	PrimaryMonitors    map[string]*replica.PrimaryMonitor
	AuditWriter        *client.AuditWriter
	Handler            *client.PooledHandler
}

// BuildRuntime constructs all config-derived runtime components that
// are identical between cmd and library mode. It does not bind
// listeners and does not start replica lag-poll goroutines; callers own
// process lifecycle concerns.
func BuildRuntime(ctx context.Context, cfg *config.Config, log *slog.Logger,
	opts RuntimeOptions,
) (*Runtime, error) {
	if opts.AuthAppName == "" {
		opts.AuthAppName = "pgrouter-auth_query"
	}
	if opts.DialAppName == "" {
		opts.DialAppName = "pgrouter"
	}

	clientTLS, backendTLS, backendTLSRequired, err := BuildTLS(cfg)
	if err != nil {
		return nil, fmt.Errorf("TLS init: %w", err)
	}
	authOpts, userlist, err := BuildAuthOpts(cfg, backendTLS,
		backendTLSRequired, opts.AuthAppName, log)
	if err != nil {
		return nil, fmt.Errorf("auth init: %w", err)
	}

	cancelTracker := cancel.NewTracker()
	dialerFor := BuildPoolDialer(cfg, userlist, backendTLS,
		backendTLSRequired, opts.DialAppName, log)
	mgr := BuildPoolManager(cfg, cancelTracker, dialerFor, log)
	mgr.Start(cfg.Pool.ServerCheckDelay)

	if err := Preflight(ctx, cfg, backendTLS, backendTLSRequired, log); err != nil {
		return nil, fmt.Errorf("preflight: %w", err)
	}

	defaultCfg := DefaultPoolConfig(cfg, log)
	replicaMgrs := BuildReplicaManagers(cfg, defaultCfg, backendTLS,
		backendTLSRequired, log)
	primaryMonitors := BuildPrimaryMonitors(cfg, backendTLS,
		backendTLSRequired, log)

	auditWriter, err := client.OpenAuditFile(cfg.Logging.AuditFile)
	if err != nil {
		return nil, fmt.Errorf("audit file: %w", err)
	}
	if auditWriter != nil {
		log.Info("audit log enabled", "path", cfg.Logging.AuditFile)
	}

	logSQLMode := config.NormalizeLogSQL(cfg.Logging.LogSQL)
	if logSQLMode == "full" {
		log.Warn("logging.log_sql=full enabled; raw SQL with literals will be logged")
	}
	stats.SetAppNameCap(stats.DefaultAppNameCardinalityCap)
	handler := BuildPooledHandler(HandlerInput{
		Cfg:             cfg,
		Log:             log,
		Mgr:             mgr,
		ClientTLS:       clientTLS,
		AuthOpts:        authOpts,
		CancelTracker:   cancelTracker,
		CannedParams:    CannedParams(),
		LogSQLMode:      logSQLMode,
		AuditWriter:     auditWriter,
		AdminReload:     opts.AdminReload,
		ReplicaMgrs:     replicaMgrs,
		PrimaryMonitors: primaryMonitors,
	})
	stats.InflightFn = handler.InflightClients

	return &Runtime{
		ClientTLS:          clientTLS,
		BackendTLS:         backendTLS,
		BackendTLSRequired: backendTLSRequired,
		AuthOpts:           authOpts,
		Userlist:           userlist,
		CancelTracker:      cancelTracker,
		Manager:            mgr,
		DefaultPoolConfig:  defaultCfg,
		ReplicaManagers:    replicaMgrs,
		PrimaryMonitors:    primaryMonitors,
		AuditWriter:        auditWriter,
		Handler:            handler,
	}, nil
}

// cfgRouter is the production client.Router implementation. Reads
// stay live against the cfg pointer + maps so SIGHUP reloads of
// per-database fields (StickyReadWindow, MaxQPS) take effect on
// already-connected clients without reconnect.
type cfgRouter struct {
	cfg      *config.Config
	replicas map[string]*replica.Manager
	primary  map[string]*replica.PrimaryMonitor
}

// db looks up a config.DatabaseConfig by alias. Returns nil when the db is
// unknown â€” callers should use the zero value for the field they want.
func (r *cfgRouter) db(name string) *config.DatabaseConfig {
	d, ok := r.cfg.Databases[name]
	if !ok {
		return nil
	}
	return &d
}

func (r *cfgRouter) ReplicaPool(db string) *pool.Pool {
	rm, ok := r.replicas[db]
	if !ok {
		return nil
	}
	rep, err := rm.Pick()
	if err != nil {
		return nil
	}
	return rep.Pool
}

func (r *cfgRouter) StickyReadWindow(db string) time.Duration {
	if d := r.db(db); d != nil {
		return d.StickyReadWindow
	}
	return 0
}

func (r *cfgRouter) PrimaryHealthy(db string) bool {
	pm, ok := r.primary[db]
	if !ok {
		return true
	}
	return pm.Healthy()
}

func (r *cfgRouter) QPSCap(db, user string) float64 {
	// Per-user cap wins if set; else per-db; else 0 (disabled).
	if u, ok := r.cfg.Users[user]; ok && u.MaxQPS > 0 {
		return u.MaxQPS
	}
	if d := r.db(db); d != nil && d.MaxQPS > 0 {
		return d.MaxQPS
	}
	return 0
}

// Preflight dials each configured database once + closes immediately.
// Returns nil when all dials succeed OR cfg has no databases; returns
// an aggregated error when ALL dials fail (boot should abort).
// Per-DB failures are logged but don't abort if at least one succeeds â€”
// rolling upgrades + lagging warm-up of some backends shouldn't ground
// the pooler.
//
// `appName` is the application_name reported on each probe dial. cmd
// passes "pgrouter-preflight"; lib mode passes the same.
//
// Honour `cfg.Pool.SkipPreflight` â€” operators can opt out for staged
// rollouts where backends warm later than the pooler.
func Preflight(ctx context.Context, cfg *config.Config,
	backendTLS *tls.Config, backendTLSRequired bool, log *slog.Logger,
) error {
	if cfg.Pool.SkipPreflight {
		log.Info("preflight: skipped (cfg.pool.skip_preflight)")
		return nil
	}
	if len(cfg.Databases) == 0 {
		return nil
	}
	env := dialEnv{tls: backendTLS, tlsRequired: backendTLSRequired, log: log}
	var (
		ok       int
		failures []string
	)
	for name, db := range cfg.Databases {
		addr := net.JoinHostPort(db.Host, strconv.Itoa(db.Port))
		user := db.User
		if user == "" {
			user = "pgrouter"
		}
		dbName := db.DBName
		if dbName == "" {
			dbName = name
		}
		dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		c, err := backend.Dial(dialCtx, env.opts(addr, user, dbName,
			"pgrouter-preflight", db.Password))
		cancel()
		if err != nil {
			failures = append(failures,
				fmt.Sprintf("%s (%s): %v", name, addr, err))
			log.Warn("preflight dial failed", "db", name, "addr", addr, "err", err)
			continue
		}
		_ = c.Close()
		ok++
		log.Info("preflight dial ok", "db", name, "addr", addr)
	}
	if ok == 0 {
		return fmt.Errorf("preflight: 0/%d backends reachable:\n  %s",
			len(cfg.Databases), strings.Join(failures, "\n  "))
	}
	if len(failures) > 0 {
		log.Warn("preflight: some backends unreachable; continuing",
			"ok", ok, "failed", len(failures))
	}
	return nil
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
