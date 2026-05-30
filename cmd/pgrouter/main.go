// Command pgrouter is a PostgreSQL connection pooler.
//
// Subcommands:
//
//	pgrouter run --config <path>     run the pooler
//	pgrouter validate <path>         validate a config file and exit
//	pgrouter version                 print version + build info
//	pgrouter --help                  show this help
//
// The PoC's legacy positional flags (--listen, --backend, --log-format,
// --log-level) still work without a subcommand to keep the demo scripts
// from breaking — they implicitly run `pgrouter run` with synthesised
// in-memory config.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/auth"
	"github.com/JustAnotherDevv/pgrouter/internal/backend"

	"github.com/JustAnotherDevv/pgrouter/internal/replica"
	"github.com/JustAnotherDevv/pgrouter/internal/tracing"

	"crypto/tls"
	"github.com/JustAnotherDevv/pgrouter/internal/cancel"
	"github.com/JustAnotherDevv/pgrouter/internal/client"
	"github.com/JustAnotherDevv/pgrouter/internal/config"
	"github.com/JustAnotherDevv/pgrouter/internal/listener"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
	"github.com/JustAnotherDevv/pgrouter/internal/stats"
)

var (
	// version is the release tag, ldflags-stamped at build time
	// (`-X main.version=...`). Defaults reflect the current dev tip.
	version = "0.2.0-mvp"

	// commit is the short git SHA the binary was built from, also
	// ldflags-stamped. Surfaces in `pgrouter version` and in the
	// pgrouter_build_info Prometheus gauge.
	commit = "unknown"
)

func main() {
	os.Exit(realMain(os.Args[1:], os.Stdout, os.Stderr))
}

func realMain(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}

	// Subcommand dispatch on argv[0]. If it looks like a flag (starts
	// with `-`), fall through to the legacy positional-flag path so
	// `pgrouter --listen :6432 --backend 127.0.0.1:5432` keeps working.
	switch args[0] {
	case "run":
		return cmdRun(args[1:], stdout, stderr)
	case "validate":
		return cmdValidate(args[1:], stdout, stderr)
	case "version", "--version", "-v":
		fmt.Fprintf(stdout, "pgrouter %s (%s)\n", version, commit)
		return 0
	case "help", "--help", "-h":
		printUsage(stdout)
		return 0
	}

	// Legacy / PoC path: bare flags, no config file. Kept so demo
	// scripts + integration tests don't have to learn subcommands.
	if len(args) > 0 && (args[0][0] == '-') {
		return cmdLegacyRun(args, stdout, stderr)
	}

	fmt.Fprintf(stderr, "unknown subcommand %q\n\n", args[0])
	printUsage(stderr)
	return 2
}

func printUsage(w io.Writer) {
	fmt.Fprint(w, `pgrouter `+version+`

Usage:
  pgrouter run --config <path>     run the pooler against a YAML config
  pgrouter validate <path>         parse + validate a config file
  pgrouter version                 print version
  pgrouter --help                  this help

Legacy (PoC-style) flags continue to work without a subcommand:
  pgrouter --listen :6432 --backend 127.0.0.1:5432

`)
}

// cmdValidate parses + validates the file at args[0]. Exits 0 on
// success, 1 on validation failure.
func cmdValidate(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: pgrouter validate <path>")
		return 2
	}
	path := args[0]
	cfg, err := config.Load(path)
	if err != nil {
		fmt.Fprintf(stderr, "FAIL: %s\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "OK: %s\n", path)
	fmt.Fprintf(stdout, "  listen          %s:%d\n", cfg.Server.ListenAddr, cfg.Server.ListenPort)
	fmt.Fprintf(stdout, "  pool_mode       %s\n", cfg.Pool.Mode)
	fmt.Fprintf(stdout, "  default_pool    %d\n", cfg.Pool.DefaultPoolSize)
	fmt.Fprintf(stdout, "  auth.type       %s\n", cfg.Auth.Type)
	fmt.Fprintf(stdout, "  client_tls      %s\n", cfg.TLS.ClientMode)
	fmt.Fprintf(stdout, "  server_tls      %s\n", cfg.TLS.ServerMode)
	fmt.Fprintf(stdout, "  databases       %d\n", len(cfg.Databases))
	for name, db := range cfg.Databases {
		fmt.Fprintf(stdout, "    - %s -> %s:%d/%s\n", name, db.Host, db.Port, db.DBName)
	}
	return 0
}

// cmdRun starts the pooler driven by a config file. Wires:
//   - YAML config → TLS, auth, pool sizing
//   - per-(db, user) pool routing via pool.Manager
//   - Prometheus /metrics + /healthz endpoint
//   - cancel.Tracker for per-client (PID, secret) routing
//   - PooledHandler for the listener.Handler role
func cmdRun(args []string, _ io.Writer, stderr io.Writer) int {
	var configPath string
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&configPath, "config", "pgrouter.yaml", "path to YAML config")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(stderr, "config: %s\n", err)
		return 1
	}

	log := newLogger(cfg.Logging.Format, cfg.Logging.Level)
	listenAddr := net.JoinHostPort(cfg.Server.ListenAddr, strconv.Itoa(cfg.Server.ListenPort))
	log.Info("pgrouter starting",
		"version", version,
		"config", configPath,
		"listen", listenAddr,
		"pool_mode", string(cfg.Pool.Mode),
		"databases", len(cfg.Databases),
	)

	// --- OTel tracing (no-op when OTEL_EXPORTER_OTLP_ENDPOINT unset) ---
	tracerShutdown, err := tracing.Init(context.Background(), version, commit)
	if err != nil {
		log.Warn("tracing init failed; continuing without tracing", "err", err)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tracerShutdown(shutCtx)
	}()

	// --- metrics + admin API ---
	stats.Build.Version = version
	stats.Build.Commit = commit
	_ = stats.New() // sets stats.Active
	metricsCtx, metricsCancel := context.WithCancel(context.Background())
	defer metricsCancel()

	// --- TLS configs ---
	clientTLS, _, err := listener.BuildServerTLSConfig(cfg.TLS)
	if err != nil {
		log.Error("client TLS init", "err", err)
		return 1
	}
	backendTLS, err := listener.BuildBackendTLSConfig(cfg.TLS)
	if err != nil {
		log.Error("backend TLS init", "err", err)
		return 1
	}
	backendTLSRequired := cfg.TLS.ServerMode == config.SSLRequire ||
		cfg.TLS.ServerMode == config.SSLVerifyCA ||
		cfg.TLS.ServerMode == config.SSLVerifyFull

	// --- auth ---
	var authOpts *auth.ServerAuthOptions
	var userlist *auth.Userlist
	if cfg.Auth.UserlistFile != "" {
		ul, err := auth.NewUserlist(cfg.Auth.UserlistFile)
		if err != nil {
			log.Error("userlist load", "err", err)
			return 1
		}
		userlist = ul
		log.Info("userlist loaded", "path", cfg.Auth.UserlistFile, "entries", ul.Len())
	}
	var hba *auth.HBAFile
	if cfg.Auth.HBAFile != "" {
		h, err := auth.NewHBAFile(cfg.Auth.HBAFile)
		if err != nil {
			log.Error("hba load", "err", err)
			return 1
		}
		hba = h
		log.Info("hba loaded", "path", cfg.Auth.HBAFile)
	}
	var fetcher *auth.AuthQueryFetcher
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
	if cfg.Auth.Type != config.AuthTrust {
		authOpts = &auth.ServerAuthOptions{
			Type:     cfg.Auth.Type,
			Userlist: userlist,
			HBA:      hba,
			Fetcher:  fetcher,
			Log:      log,
		}
	}

	// --- pool.Manager ---
	cancelTracker := cancel.NewTracker()

	dialerFor := func(k pool.Key) pool.Dialer {
		db, ok := cfg.Databases[k.DB]
		if !ok {
			// Unknown database — return a dialer that always errors so
			// Acquire fails with a clear message.
			return func(_ context.Context) (*backend.Conn, error) {
				return nil, fmt.Errorf("unknown database %q", k.DB)
			}
		}
		addr := net.JoinHostPort(db.Host, strconv.Itoa(db.Port))
		// Per-DB upstream user override: if the config pins a user for
		// this database, use it; otherwise forward the client's.
		upstreamUser := k.User
		if db.User != "" {
			upstreamUser = db.User
		}
		dbName := db.DBName
		if dbName == "" {
			dbName = k.DB
		}
		password := db.Password
		// If a userlist is loaded and we don't have a per-db password,
		// look up the upstream user there. This matches PgBouncer's
		// auth_file behaviour for the upstream side.
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
				AppName:     "pgrouter",
				Password:    password,
				TLSConfig:   backendTLS,
				TLSRequired: backendTLSRequired,
				Log:         log,
			})
		}
		// Retry with capped exponential backoff. Spec M.7.6: handshake
		// auth failures + transient TLS/network errors shouldn't kill
		// the next Acquire instantly — back off so a dying backend
		// doesn't get hammered. Cap at 6 attempts so wrong creds give up
		// in seconds rather than minutes.
		return func(ctx context.Context) (*backend.Conn, error) {
			const maxAttempts = 6
			backoff := 100 * time.Millisecond
			var lastErr error
			for attempt := 1; attempt <= maxAttempts; attempt++ {
				c, err := dialOnce(ctx)
				if err == nil {
					if attempt > 1 {
						log.Info("backend dial succeeded after retry",
							"attempts", attempt, "addr", addr)
					}
					return c, nil
				}
				lastErr = err
				if attempt == maxAttempts {
					break
				}
				log.Warn("backend dial failed; backing off",
					"attempt", attempt, "max", maxAttempts,
					"backoff", backoff, "err", err)
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
			return nil, fmt.Errorf("backend dial gave up after %d attempts: %w",
				maxAttempts, lastErr)
		}
	}

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
	mgr.Start(cfg.Pool.ServerCheckDelay)

	// Per-database replica managers (R/W split).
	replicaMgrs := buildReplicaManagers(cfg, defaultCfg, backendTLS,
		backendTLSRequired, log)
	for _, rm := range replicaMgrs {
		rm.Start()
		rm.StartLagPolls(5 * time.Second)
	}
	defer func() {
		for _, rm := range replicaMgrs {
			rm.Stop()
		}
	}()

	// adminReloadCh fires a synthetic SIGHUP into the same reloader
	// goroutine the OS signal handler uses, so POST /api/v1/reload
	// runs identical code.
	adminReloadCh := make(chan os.Signal, 1)
	startTime := time.Now()
	adminAPI := &stats.AdminAPI{
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
	go func() {
		err := stats.ServeMetricsAndAdmin(metricsCtx, stats.AdminServerOptions{
			Addr:        cfg.Metrics.Listen,
			MetricsPath: cfg.Metrics.Path,
			AuthToken:   cfg.Metrics.AdminToken,
			Admin:       adminAPI,
		}, log)
		if err != nil {
			log.Error("metrics+admin server", "err", err)
		}
	}()

	// --- canned ParameterStatus ---
	// First-client cold-start fallback: pool.CachedParams will be
	// populated on the first successful upstream dial and override
	// these. The full set mirrors what real Postgres emits during the
	// startup phase so drivers that check specific fields (psql's
	// is_superuser, application_name watchers in dashboards, pgx's
	// server_version-keyed protocol decisions, etc.) don't degrade.
	cannedParams := map[string]string{
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

	// --- handler ---
	logSQLMode := string(config.NormalizeLogSQL(cfg.Logging.LogSQL))
	if logSQLMode == string(config.LogSQLFull) {
		log.Warn("logging.log_sql=full enabled — raw SQL (with literals) will be logged. Use only in dev.")
	}
	auditWriter, err := client.OpenAuditFile(cfg.Logging.AuditFile)
	if err != nil {
		log.Error("audit file", "err", err)
		return 1
	}
	if auditWriter != nil {
		log.Info("audit log enabled", "path", cfg.Logging.AuditFile)
		defer auditWriter.Close()
	}
	h := &client.PooledHandler{
		Log:               log,
		Manager:           mgr,
		TLSConfig:         clientTLS,
		Auth:              authOpts,
		CancelTracker:     cancelTracker,
		CannedParams:      cannedParams,
		ResetOnRelease:    true,
		QueryTimeout:      cfg.Pool.QueryTimeout,
		ClientIdleTimeout: cfg.Server.ClientIdle,
		IdleTxTimeout:     cfg.Server.IdleTx,
		SlowQuery:         cfg.Logging.SlowQuery,
		LogSQL:            logSQLMode,
		Audit:             auditWriter,
		AdminReload: func() error {
			select {
			case adminReloadCh <- syscall.SIGHUP:
				return nil
			default:
				return fmt.Errorf("reload channel busy")
			}
		},
		ReplicaPickerFor: func(db string) *pool.Pool {
			rm, ok := replicaMgrs[db]
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
			if d, ok := cfg.Databases[db]; ok {
				return d.StickyReadWindow
			}
			return 0
		},
		PoolMode:          string(cfg.Pool.Mode),
		PoolModeFor: func(db string) string {
			if d, ok := cfg.Databases[db]; ok && d.PoolMode != "" {
				return string(d.PoolMode)
			}
			return ""
		},
		QPSCapFor: func(db, user string) float64 {
			// Per-user cap wins if set; else per-db; else 0 (disabled).
			if u, ok := cfg.Users[user]; ok && u.MaxQPS > 0 {
				return u.MaxQPS
			}
			if d, ok := cfg.Databases[db]; ok && d.MaxQPS > 0 {
				return d.MaxQPS
			}
			return 0
		},
	}

	// --- listener + signal-driven shutdown ---
	// SIGINT + SIGTERM trigger shutdown via the cancel-context. SIGHUP is
	// handled separately: it must NOT shut pgrouter down, only trigger a
	// config reload + log the diff (live pool resize is post-MVP per the
	// roadmap; this fixes the bug where SIGHUP killed the process).
	ctx, signalCancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer signalCancel()

	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	defer signal.Stop(hupCh)
	// Fan-in: OS SIGHUP + admin-API /reload POST share the same goroutine.
	mergedHup := make(chan os.Signal, 4)
	go fanInSignals(ctx, mergedHup, hupCh, adminReloadCh)
	go runSighupReloader(ctx, mergedHup, configPath, cfg, userlist, mgr, log)

	ln, err := listener.New(listenAddr, log)
	if err != nil {
		log.Error("listener init", "err", err)
		return 1
	}
	if cfg.Server.ProxyProtocol {
		ln.EnableProxyProtocol()
		log.Info("PROXY protocol parsing enabled (v1+v2)")
	}
	log.Info("listening", "addr", ln.Addr().String())

	// Optional Unix socket listener for PgBouncer-compat in-host clients
	// + peer auth. unix_socket_dir empty → skip.
	var unixLn *listener.Listener
	if cfg.Server.UnixSocketDir != "" {
		uln, err := listener.NewUnix(cfg.Server.UnixSocketDir,
			cfg.Server.ListenPort, cfg.Server.UnixSocketMode, log)
		if err != nil {
			log.Error("unix listener init", "err", err)
			return 1
		}
		unixLn = uln
		log.Info("listening", "addr", uln.Addr().String())
	}

	// Run both listeners; first non-nil error wins. ctx cancel triggers
	// both to drain in parallel.
	errCh := make(chan error, 2)
	go func() { errCh <- ln.Serve(ctx, h.Handle) }()
	if unixLn != nil {
		go func() { errCh <- unixLn.Serve(ctx, h.Handle) }()
	}
	serveErr := <-errCh
	// Drain the second error if any.
	if unixLn != nil {
		<-errCh
	}

	// Graceful drain: give pools 30s to release before force-close.
	drainDeadline := time.Now().Add(30 * time.Second)
	if err := mgr.CloseWithDeadline(drainDeadline); err != nil {
		log.Warn("pool drain", "err", err)
	}
	if serveErr != nil && !errors.Is(serveErr, context.Canceled) {
		log.Error("serve", "err", serveErr)
		return 1
	}
	log.Info("pgrouter stopped")
	return 0
}

// cmdLegacyRun reproduces the PoC v0.1.0 CLI: bare flags, no config.
func cmdLegacyRun(args []string, _ io.Writer, stderr io.Writer) int {
	fs := flag.NewFlagSet("pgrouter", flag.ContinueOnError)
	fs.SetOutput(stderr)
	listenAddr := fs.String("listen", ":6432", "address to listen on")
	backend := fs.String("backend", "localhost:5432", "upstream Postgres address")
	logFormat := fs.String("log-format", "text", "log format: text | json")
	logLevel := fs.String("log-level", "info", "log level: debug | info | warn | error")
	showVer := fs.Bool("version", false, "print version and exit")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *showVer {
		fmt.Println("pgrouter", version)
		return 0
	}

	log := newLogger(*logFormat, *logLevel)
	log.Info("pgrouter starting (legacy CLI)",
		"version", version,
		"listen", *listenAddr,
		"backend", *backend,
	)
	return runServer(log, *listenAddr, *backend)
}

// runServer is shared by cmdRun + cmdLegacyRun.
func runServer(log *slog.Logger, listenAddr, backendAddr string) int {
	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer cancel()

	ln, err := listener.New(listenAddr, log)
	if err != nil {
		log.Error("listener init", "err", err)
		return 1
	}
	log.Info("listening", "addr", ln.Addr().String())

	h := &client.Conn{Log: log, BackendAddr: backendAddr}
	if err := ln.Serve(ctx, h.Handle); err != nil {
		log.Error("serve", "err", err)
		return 1
	}
	log.Info("pgrouter stopped")
	return 0
}

// runSighupReloader is the SIGHUP handler goroutine. Each HUP re-reads
// the config file off disk and the (optional) userlist.txt, logging a
// diff against the previous state for each.
//
// MVP scope (M.13.5 + M.5.4 / spec gate 11 prep): SIGHUP no longer
// kills pgrouter and the userlist atomically swaps under the lock
// inside *auth.Userlist (new conns immediately see the new map; in
// flight conns keep their already-authenticated identity). Live
// pool resize + db-registry diff applied to running pools remains
// post-MVP — applying those would require a Manager.Reconfigure
// surface that doesn't exist yet. We log the config diff so operators
// can see what WOULD have changed; the running pools stay on the boot
// config.
func runSighupReloader(ctx context.Context, hupCh <-chan os.Signal,
	path string, current *config.Config, userlist *auth.Userlist,
	mgr *pool.Manager, log *slog.Logger,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-hupCh:
			if !ok {
				return
			}
			next, err := config.Load(path)
			if err != nil {
				log.Error("SIGHUP reload failed", "path", path, "err", err)
				stats.OnSighupReload("fail")
				// Even if the YAML failed, still attempt the userlist
				// reload — its file is independent and may be valid.
				reloadUserlist(userlist, log)
				continue
			}
			log.Info("SIGHUP reload",
				"path", path,
				"databases_before", len(current.Databases),
				"databases_after", len(next.Databases),
				"default_pool_size_before", current.Pool.DefaultPoolSize,
				"default_pool_size_after", next.Pool.DefaultPoolSize,
				"pool_mode_before", string(current.Pool.Mode),
				"pool_mode_after", string(next.Pool.Mode),
				"query_timeout_before", current.Pool.QueryTimeout,
				"query_timeout_after", next.Pool.QueryTimeout,
			)

			// Apply live pool resize.
			if mgr != nil {
				changes := mgr.ApplyDefaultSize(next.Pool.DefaultPoolSize,
					func(k pool.Key) int {
						if d, ok := next.Databases[k.DB]; ok && d.PoolSize > 0 {
							return d.PoolSize
						}
						return 0
					})
				for _, c := range changes {
					log.Info("pool resized",
						"pool", c.Key.String(),
						"from", c.From, "to", c.To)
				}
			}

			current = next
			stats.OnSighupReload("ok")
			reloadUserlist(userlist, log)
		}
	}
}

// reloadUserlist re-reads the in-memory userlist (if one is configured)
// and logs the diff. No-op when no userlist_file was set.
func reloadUserlist(ul *auth.Userlist, log *slog.Logger) {
	if ul == nil {
		stats.OnSighupUserlistReload("skip")
		return
	}
	diff, err := ul.ReloadDiff()
	if err != nil {
		log.Error("SIGHUP userlist reload failed", "err", err)
		stats.OnSighupUserlistReload("fail")
		return
	}
	log.Info("SIGHUP userlist reload",
		"before", diff.Before,
		"after", diff.After,
		"added", len(diff.Added),
		"removed", len(diff.Removed),
		"rotated", len(diff.Rotated),
	)
	stats.OnSighupUserlistReload("ok")
}

// buildReplicaManagers iterates cfg.Databases and constructs one
// replica.Manager per database that has replicas configured. Each
// replica gets its own pool.Pool dialed via backend.Dial.
func buildReplicaManagers(cfg *config.Config, defaultCfg pool.Config,
	backendTLS *tls.Config, backendTLSRequired bool, log *slog.Logger,
) map[string]*replica.Manager {
	out := map[string]*replica.Manager{}
	for dbName, db := range cfg.Databases {
		if len(db.Replicas) == 0 {
			continue
		}
		reps := make([]*replica.Replica, 0, len(db.Replicas))
		for _, rspec := range db.Replicas {
			addr := net.JoinHostPort(rspec.Host, strconv.Itoa(rspec.Port))
			user := db.User
			if user == "" {
				user = cfg.Auth.AuthUser
			}
			dialer := func(ctx context.Context) (*backend.Conn, error) {
				return backend.Dial(ctx, backend.DialOptions{
					Addr:        addr,
					User:        user,
					Database:    db.DBName,
					AppName:     "pgrouter-replica",
					Password:    db.Password,
					TLSConfig:   backendTLS,
					TLSRequired: backendTLSRequired,
					Log:         log,
				})
			}
			p := pool.New(fmt.Sprintf("%s-replica-%s:%d", dbName, rspec.Host, rspec.Port),
				dialer, defaultCfg)
			reps = append(reps, &replica.Replica{
				Spec: replica.ReplicaSpec{
					Host:   rspec.Host,
					Port:   rspec.Port,
					Weight: rspec.Weight,
				},
				Pool: p,
			})
		}
		rm := replica.NewManager(dbName, reps,
			cfg.Pool.ServerCheckDelay, cfg.Pool.ServerCheckQuery, log)
		rm.SetMaxLag(db.MaxReplicaLagBytes)
		out[dbName] = rm
	}
	return out
}

// splitPoolName turns "db/user" into (db, user, true). Returns ("", name, false)
// if the format is unexpected — admin API still emits the row.
func splitPoolName(name string) (db, user string, ok bool) {
	for i := 0; i < len(name); i++ {
		if name[i] == '/' {
			return name[:i], name[i+1:], true
		}
	}
	return "", name, false
}

// fanInSignals forwards both source channels into dst until ctx fires.
// Used to merge OS SIGHUP and admin-API reload triggers.
func fanInSignals(ctx context.Context, dst chan<- os.Signal, sources ...<-chan os.Signal) {
	for _, src := range sources {
		go func(c <-chan os.Signal) {
			for {
				select {
				case <-ctx.Done():
					return
				case s, ok := <-c:
					if !ok {
						return
					}
					select {
					case dst <- s:
					default:
					}
				}
			}
		}(src)
	}
}

func newLogger(format, level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if format == "json" {
		h = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		h = slog.NewTextHandler(os.Stderr, opts)
	}
	return slog.New(h)
}
