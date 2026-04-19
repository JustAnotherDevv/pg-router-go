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
	"github.com/JustAnotherDevv/pgrouter/internal/cancel"
	"github.com/JustAnotherDevv/pgrouter/internal/client"
	"github.com/JustAnotherDevv/pgrouter/internal/config"
	"github.com/JustAnotherDevv/pgrouter/internal/listener"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
	"github.com/JustAnotherDevv/pgrouter/internal/stats"
)

var version = "0.2.0-mvp"

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
		fmt.Fprintln(stdout, "pgrouter", version)
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

	// --- metrics ---
	_ = stats.New() // sets stats.Active
	metricsCtx, metricsCancel := context.WithCancel(context.Background())
	defer metricsCancel()
	go func() {
		if err := stats.ServeMetrics(metricsCtx, cfg.Metrics.Listen, cfg.Metrics.Path, log); err != nil {
			log.Error("metrics server", "err", err)
		}
	}()

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
	if cfg.Auth.Type != config.AuthTrust {
		authOpts = &auth.ServerAuthOptions{
			Type:     cfg.Auth.Type,
			Userlist: userlist,
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
		return func(ctx context.Context) (*backend.Conn, error) {
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
		Log:                log,
		Callbacks:          cbs,
	}
	mgr := pool.NewManager(defaultCfg, dialerFor).WithConfigFor(func(k pool.Key) *pool.Config {
		db, ok := cfg.Databases[k.DB]
		if !ok || db.PoolSize == 0 {
			return nil
		}
		return &pool.Config{DefaultPoolSize: db.PoolSize}
	})
	mgr.Start(cfg.Pool.ServerCheckDelay)

	// --- canned ParameterStatus ---
	// Until we capture the first upstream's real ParameterStatus, ship a
	// minimal set so pg drivers don't bail on missing critical vars.
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
	}

	// --- handler ---
	h := &client.PooledHandler{
		Log:            log,
		Manager:        mgr,
		TLSConfig:      clientTLS,
		Auth:           authOpts,
		CancelTracker:  cancelTracker,
		CannedParams:   cannedParams,
		ResetOnRelease: true,
	}

	// --- listener + signal-driven shutdown ---
	ctx, signalCancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer signalCancel()

	ln, err := listener.New(listenAddr, log)
	if err != nil {
		log.Error("listener init", "err", err)
		return 1
	}
	log.Info("listening", "addr", ln.Addr().String())

	serveErr := ln.Serve(ctx, h.Handle)

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
