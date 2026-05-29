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
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/JustAnotherDevv/pgrouter/internal/client"
	"github.com/JustAnotherDevv/pgrouter/internal/config"
	"github.com/JustAnotherDevv/pgrouter/internal/listener"
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

// cmdRun starts the pooler driven by a config file.
//
// MVP scope: only the first database in cfg.Databases is wired (single
// upstream). M.7 / M.8 add per-(db,user) pool selection.
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
	log.Info("pgrouter starting",
		"version", version,
		"config", configPath,
		"listen", net.JoinHostPort(cfg.Server.ListenAddr, strconv.Itoa(cfg.Server.ListenPort)),
		"pool_mode", string(cfg.Pool.Mode),
		"databases", len(cfg.Databases),
	)

	// Pick the first database for the M.3 demo. Multi-pool wiring is M.8.
	var dbAddr string
	for _, db := range cfg.Databases {
		dbAddr = net.JoinHostPort(db.Host, strconv.Itoa(db.Port))
		break
	}

	return runServer(log,
		net.JoinHostPort(cfg.Server.ListenAddr, strconv.Itoa(cfg.Server.ListenPort)),
		dbAddr,
	)
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
