// Command pgrouter is a PostgreSQL connection pooler.
// PoC: TCP listener + minimal handler that logs raw bytes per connection.
// Wire protocol parsing wired in P.2.2.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/JustAnotherDevv/pgrouter/internal/handler"
	"github.com/JustAnotherDevv/pgrouter/internal/listener"
)

var version = "0.1.0-poc"

func main() {
	var (
		listenAddr = flag.String("listen", ":6432", "address to listen on")
		backend    = flag.String("backend", "localhost:5432", "upstream Postgres address (used in P.3.x)")
		logFormat  = flag.String("log-format", "text", "log format: text | json")
		logLevel   = flag.String("log-level", "info", "log level: debug | info | warn | error")
		showVer    = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVer {
		fmt.Println("pgrouter", version)
		os.Exit(0)
	}

	log := newLogger(*logFormat, *logLevel)
	log.Info("pgrouter starting",
		"version", version,
		"listen", *listenAddr,
		"backend", *backend,
	)

	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer cancel()

	ln, err := listener.New(*listenAddr, log)
	if err != nil {
		log.Error("listener init", "err", err)
		os.Exit(1)
	}
	log.Info("listening", "addr", ln.Addr().String())

	h := &handler.PoCHandler{Log: log, BackendAddr: *backend}
	if err := ln.Serve(ctx, h.Handle); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
	log.Info("pgrouter stopped")
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
