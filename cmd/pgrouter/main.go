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
// from breaking Ã¢â‚¬â€ they implicitly run `pgrouter run` with synthesised
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
	"sync"
	"syscall"
	"time"

	"github.com/JustAnotherDevv/pg-router-go/internal/config"
	"github.com/JustAnotherDevv/pg-router-go/internal/listener"
	"github.com/JustAnotherDevv/pg-router-go/internal/multiproc"
	"github.com/JustAnotherDevv/pg-router-go/internal/stats"
	"github.com/JustAnotherDevv/pg-router-go/internal/tracing"
	"github.com/JustAnotherDevv/pg-router-go/internal/wire"
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

	// Subcommand dispatch on argv[0]. If it looks like a flag, fall
	// through to the bare-flag compatibility path.
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

	// Bare-flag compatibility path.
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

Compatibility flags continue to work without a subcommand:
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
//   - YAML config Ã¢â€ â€™ TLS, auth, pool sizing
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

	// --- GOMEMLIMIT ---
	if cfg.Server.GOMEMLIMIT != "" {
		if limit, err := parseBytes(cfg.Server.GOMEMLIMIT); err == nil && limit > 0 {
			debug.SetMemoryLimit(limit)
			log.Info("memory limit set", "bytes", limit)
		} else {
			log.Warn("invalid gomemlimit, ignoring", "value", cfg.Server.GOMEMLIMIT, "err", err)
		}
	}

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
	// Workers skip metrics (main process serves them).
	if !multiproc.IsWorker() {
		stats.Build.Version = version
		stats.Build.Commit = commit
		_ = stats.New() // sets stats.Active
	}
	metricsCtx, metricsCancel := context.WithCancel(context.Background())
	defer metricsCancel()

	// adminReloadCh fires a synthetic SIGHUP into the same reloader
	// goroutine the OS signal handler uses, so POST /api/v1/reload
	// runs identical code.
	adminReloadCh := make(chan os.Signal, 1)
	adminReload := func() error {
		select {
		case adminReloadCh <- syscall.SIGHUP:
			return nil
		default:
			return fmt.Errorf("reload channel busy")
		}
	}
	rt, err := wire.BuildRuntime(context.Background(), cfg, log, wire.RuntimeOptions{
		AuthAppName: "pgrouter-auth_query",
		DialAppName: "pgrouter",
		AdminReload: adminReload,
	})
	if err != nil {
		log.Error("runtime init", "err", err)
		return 1
	}
	defer func() {
		for _, pm := range rt.PrimaryMonitors {
			pm.Stop()
		}
		if rt.AuditWriter != nil {
			_ = rt.AuditWriter.Close()
		}
	}()
	for _, rm := range rt.ReplicaManagers {
		rm.Start()
		rm.StartLagPolls(5 * time.Second)
	}
	defer func() {
		for _, rm := range rt.ReplicaManagers {
			rm.Stop()
		}
	}()
	startTime := time.Now()
	adminAPI := buildAdminAPI(rt.Manager, adminReloadCh, startTime)
	// --- metrics + admin API (main process only) ---
	if !multiproc.IsWorker() {
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
	}

	// --- listener + signal-driven shutdown ---
	// SIGINT + SIGTERM trigger shutdown via the cancel-context. SIGHUP is
	// handled separately: it must NOT shut pgrouter down, only trigger a
	// config reload and keep the current runtime state on failure.
	ctx, signalCancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer signalCancel()

	// Sweep orphan cancel-tracker entries. The normal Release path
	// (servePooled defer) covers clean shutdowns; the sweeper catches
	// the panic/crash path where Allocate() succeeds but the deferred
	// Release never fires, leaving (PID, secret) entries pinned in the
	// map forever. 5min tick + 1h ttl is generous Ã¢â‚¬â€ real cancels arrive
	// within seconds of the originating query.
	rt.CancelTracker.StartSweeper(ctx, 5*time.Minute, time.Hour)

	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	defer signal.Stop(hupCh)
	// Fan-in: OS SIGHUP + admin-API /reload POST share the same goroutine.
	mergedHup := make(chan os.Signal, 4)
	wire.FanInSignals(ctx, mergedHup, hupCh, adminReloadCh)
	go wire.RunSighupReloader(ctx, mergedHup, configPath, cfg, rt.Userlist, rt.Manager, log)

	// --- workers (SO_REUSEPORT multi-process) ---
	workers := cfg.Server.Workers
	// Default: 1 (single-process). Set workers > 1 + Linux bare-metal
	// to enable SO_REUSEPORT multi-process. Not beneficial in Docker.
	if workers <= 0 {
		workers = 1
	}
	useReuseport := workers > 1

	var ln *listener.Listener
	if useReuseport {
		ln, err = listener.NewReuseport(listenAddr, log)
	} else {
		ln, err = listener.New(listenAddr, log)
	}
	if err != nil {
		log.Error("listener init", "err", err)
		return 1
	}

	// Fork children AFTER listener is bound.
	if useReuseport && !multiproc.IsWorker() {
		log.Info("spawning worker processes", "count", workers-1)
		var childWg *sync.WaitGroup
		childWg = multiproc.SpawnWorkers(workers, configPath)
		defer func() {
			if childWg != nil {
				childWg.Wait()
			}
		}()
	}
	if cfg.Server.ProxyProtocol {
		ln.EnableProxyProtocol(cfg.Server.ProxyProtocolStrict)
		log.Info("PROXY protocol parsing enabled (v1+v2)")
	}
	log.Info("listening", "addr", ln.Addr().String())

	// Optional Unix socket listener for PgBouncer-compat in-host clients
	// + peer auth. unix_socket_dir empty Ã¢â€ â€™ skip.
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
	go func() { errCh <- ln.Serve(ctx, rt.Handler.Handle) }()
	if unixLn != nil {
		go func() { errCh <- unixLn.Serve(ctx, rt.Handler.Handle) }()
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
	if remaining := rt.Handler.WaitForDrain(drainDeadline); remaining > 0 {
		log.Warn("drain deadline reached with active clients",
			"remaining", remaining)
	}
	if err := rt.Manager.CloseWithDeadline(drainDeadline); err != nil {
		log.Warn("pool drain", "err", err)
	}
	if serveErr != nil && !errors.Is(serveErr, context.Canceled) {
		log.Error("serve", "err", serveErr)
		return 1
	}
	log.Info("pgrouter stopped")
	return 0
}

// cmdLegacyRun keeps the old bare-flag CLI shape but runs through the
// production config path so there is only one connection engine.
func cmdLegacyRun(args []string, stdout io.Writer, stderr io.Writer) int {
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
		fmt.Fprintln(stdout, "pgrouter", version)
		return 0
	}

	configPath, cleanup, err := writeLegacyConfig(*listenAddr, *backend,
		*logFormat, *logLevel)
	if err != nil {
		fmt.Fprintf(stderr, "legacy config: %s\n", err)
		return 1
	}
	defer cleanup()
	return cmdRun([]string{"--config", configPath}, stdout, stderr)
}

func writeLegacyConfig(listenAddr, backendAddr, logFormat, logLevel string) (string, func(), error) {
	listenHost, listenPort, err := splitHostPortDefault(listenAddr, "0.0.0.0")
	if err != nil {
		return "", nil, fmt.Errorf("listen: %w", err)
	}
	backendHost, backendPort, err := splitHostPortDefault(backendAddr, "localhost")
	if err != nil {
		return "", nil, fmt.Errorf("backend: %w", err)
	}
	f, err := os.CreateTemp("", "pgrouter-legacy-*.yaml")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.Remove(f.Name()) }
	_, err = fmt.Fprintf(f, `server:
  listen_addr: %s
  listen_port: %d
pool:
  skip_preflight: true
metrics:
  listen: "127.0.0.1:0"
logging:
  format: %s
  level: %s
databases:
  postgres:
    host: %s
    port: %d
    dbname: "postgres"
`,
		strconv.Quote(listenHost), listenPort,
		strconv.Quote(logFormat), strconv.Quote(logLevel),
		strconv.Quote(backendHost), backendPort)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		cleanup()
		if err != nil {
			return "", nil, err
		}
		return "", nil, closeErr
	}
	return f.Name(), cleanup, nil
}

func splitHostPortDefault(addr, defaultHost string) (string, int, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("invalid port %q", portText)
	}
	if host == "" {
		host = defaultHost
	}
	return host, port, nil
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

// parseBytes parses a byte size string like "512MB", "1GB", "1073741824".
func parseBytes(s string) (int64, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("empty string")
	}
	// Try pure numeric first.
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v, nil
	}
	// Strip trailing non-numeric suffix (B, bytes, etc).
	numeric := s
	for len(numeric) > 0 && (numeric[len(numeric)-1] < '0' || numeric[len(numeric)-1] > '9') && numeric[len(numeric)-1] != '.' {
		numeric = numeric[:len(numeric)-1]
	}
	if len(numeric) == 0 {
		return 0, fmt.Errorf("no numeric part in %q", s)
	}
	// Determine multiplier from remaining suffix.
	multiplier := int64(1)
	suffix := s[len(numeric):]
	switch suffix {
	case "K", "k", "KB", "kb", "KiB", "kib":
		multiplier = 1024
	case "M", "m", "MB", "mb", "MiB", "mib":
		multiplier = 1024 * 1024
	case "G", "g", "GB", "gb", "GiB", "gib":
		multiplier = 1024 * 1024 * 1024
	}
	v, err := strconv.ParseFloat(numeric, 64)
	if err != nil {
		return 0, err
	}
	return int64(v * float64(multiplier)), nil
}
