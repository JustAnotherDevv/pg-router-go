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
	"runtime"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"


	"github.com/JustAnotherDevv/pgrouter/internal/tracing"
	"github.com/JustAnotherDevv/pgrouter/internal/wire"

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

	// --- GOMAXPROCS ---
	singleThread := cfg.Server.SingleThread != nil && *cfg.Server.SingleThread
	if singleThread {
		runtime.GOMAXPROCS(1)
	}

	// --- GOGC ---
	if cfg.Server.GOGC != "" {
		switch cfg.Server.GOGC {
		case "off", "OFF":
			debug.SetGCPercent(-1)
		default:
			if v, err := strconv.Atoi(cfg.Server.GOGC); err == nil && v > 0 {
				debug.SetGCPercent(v)
			}
		}
	}

	log := newLogger(cfg.Logging.Format, cfg.Logging.Level)
	listenAddr := net.JoinHostPort(cfg.Server.ListenAddr, strconv.Itoa(cfg.Server.ListenPort))
	log.Info("pgrouter starting",
		"version", version,
		"config", configPath,
		"listen", listenAddr,
		"pool_mode", string(cfg.Pool.Mode),
		"databases", len(cfg.Databases),
		"single_thread", singleThread,
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
	clientTLS, backendTLS, backendTLSRequired, err := wire.BuildTLS(cfg)
	if err != nil {
		log.Error("TLS init", "err", err)
		return 1
	}

	// --- auth ---
	authOpts, userlist, err := wire.BuildAuthOpts(cfg, backendTLS,
		backendTLSRequired, "pgrouter-auth_query", log)
	if err != nil {
		log.Error("auth init", "err", err)
		return 1
	}

	// --- pool.Manager ---
	cancelTracker := cancel.NewTracker()
	dialerFor := wire.BuildPoolDialer(cfg, userlist, backendTLS,
		backendTLSRequired, "pgrouter", log)
	defaultCfg := wire.DefaultPoolConfig(cfg, log)
	mgr := wire.BuildPoolManager(cfg, cancelTracker, dialerFor, log)
	mgr.Start(cfg.Pool.ServerCheckDelay)

	// SB9: dial each backend once before opening the listener so an
	// obvious misconfig (typo'd host, wrong port, bad creds) aborts at
	// boot instead of leaking into client latency.
	if err := wire.Preflight(context.Background(), cfg, backendTLS, backendTLSRequired, log); err != nil {
		log.Error("preflight failed", "err", err)
		return 1
	}

	// Per-database replica managers (R/W split).
	replicaMgrs := wire.BuildReplicaManagers(cfg, defaultCfg, backendTLS,
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

	primaryMonitors := wire.BuildPrimaryMonitors(cfg, backendTLS, backendTLSRequired, log)
	defer func() {
		for _, pm := range primaryMonitors {
			pm.Stop()
		}
	}()

	// adminReloadCh fires a synthetic SIGHUP into the same reloader
	// goroutine the OS signal handler uses, so POST /api/v1/reload
	// runs identical code.
	adminReloadCh := make(chan os.Signal, 1)
	startTime := time.Now()
	adminAPI := buildAdminAPI(mgr, adminReloadCh, startTime)
	_ = adminAPI // referenced below; kept for symmetry with prior layout
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
	adminReload := func() error {
		select {
		case adminReloadCh <- syscall.SIGHUP:
			return nil
		default:
			return fmt.Errorf("reload channel busy")
		}
	}
	stats.SetAppNameCap(stats.DefaultAppNameCardinalityCap) // HB2 (idempotent)
	h := wire.BuildPooledHandler(wire.HandlerInput{
		Cfg:             cfg,
		Log:             log,
		Mgr:             mgr,
		ClientTLS:       clientTLS,
		AuthOpts:        authOpts,
		CancelTracker:   cancelTracker,
		CannedParams:    cannedParams,
		LogSQLMode:      logSQLMode,
		AuditWriter:     auditWriter,
		AdminReload:     adminReload,
		ReplicaMgrs:     replicaMgrs,
		PrimaryMonitors: primaryMonitors,
	})
	stats.InflightFn = h.InflightClients // SB6: feeds pgrouter_inflight_clients gauge

	// --- listener + signal-driven shutdown ---
	// SIGINT + SIGTERM trigger shutdown via the cancel-context. SIGHUP is
	// handled separately: it must NOT shut pgrouter down, only trigger a
	// config reload + log the diff (live pool resize is post-MVP per the
	// roadmap; this fixes the bug where SIGHUP killed the process).
	ctx, signalCancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer signalCancel()

	// Sweep orphan cancel-tracker entries. The normal Release path
	// (servePooled defer) covers clean shutdowns; the sweeper catches
	// the panic/crash path where Allocate() succeeds but the deferred
	// Release never fires, leaving (PID, secret) entries pinned in the
	// map forever. 5min tick + 1h ttl is generous — real cancels arrive
	// within seconds of the originating query.
	cancelTracker.StartSweeper(ctx, 5*time.Minute, time.Hour)

	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	defer signal.Stop(hupCh)
	// Fan-in: OS SIGHUP + admin-API /reload POST share the same goroutine.
	mergedHup := make(chan os.Signal, 4)
	wire.FanInSignals(ctx, mergedHup, hupCh, adminReloadCh)
	go wire.RunSighupReloader(ctx, mergedHup, configPath, cfg, userlist, mgr, log)

	ln, err := listener.New(listenAddr, log)
	if err != nil {
		log.Error("listener init", "err", err)
		return 1
	}
	if cfg.Server.ProxyProtocol {
		ln.EnableProxyProtocol(cfg.Server.ProxyProtocolStrict)
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
	stats.Ready.Store(true) // /readyz returns 200 from here on
	errCh := make(chan error, 2)
	go func() { errCh <- ln.Serve(ctx, h.Handle) }()
	if unixLn != nil {
		go func() { errCh <- unixLn.Serve(ctx, h.Handle) }()
	}
	serveErr := <-errCh
	// Flip readiness immediately so K8s stops routing new traffic
	// while we drain in-flight queries.
	stats.Ready.Store(false)
	// Drain the second error if any.
	if unixLn != nil {
		<-errCh
	}

	// Graceful drain: wait for in-flight clients (SB6) then close pool.
	drainDeadline := time.Now().Add(30 * time.Second)
	if remaining := h.WaitForDrain(drainDeadline); remaining > 0 {
		log.Warn("drain deadline reached with active clients",
			"remaining", remaining)
	}
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

// splitPoolName forwards to pool.SplitName + indicates whether the
// name was in "db/user" form (ok=false when no slash).
func splitPoolName(name string) (db, user string, ok bool) {
	k := pool.SplitName(name)
	ok = k.User != "" || (len(name) > 0 && name[len(name)-1] == '/')
	return k.DB, k.User, ok
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
