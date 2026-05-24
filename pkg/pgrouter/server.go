// Package pgrouter is the library-mode pgrouter handle. Build a Server
// from a parsed *config.Config, drive its lifecycle via Run (blocks)
// or Start/Stop (async).
//
// Library mode is a thin wrapper around the internal/wire builders —
// the same code cmd/pgrouter uses. cmd-mode adds signal handling +
// SIGHUP reload + the admin HTTP API on top.
package pgrouter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/config"
	"github.com/JustAnotherDevv/pgrouter/internal/listener"
	"github.com/JustAnotherDevv/pgrouter/internal/stats"
	"github.com/JustAnotherDevv/pgrouter/internal/wire"
)

// Server is one embedded pgrouter instance. Build with New, drive
// with Run (blocks) or Start/Stop (async). Safe to embed in a Go
// service alongside other goroutines.
type Server struct {
	cfg *config.Config
	log *slog.Logger

	rt           *wire.Runtime
	listener     *listener.Listener
	unixListener *listener.Listener

	stopOnce sync.Once
	stopped  chan struct{}
}

// New builds a Server from cfg. Returns an error if config validation
// fails or any listener / pool can't be set up. Does NOT start
// accepting yet — call Start or Run.
//
// log may be nil; defaults to slog.Default().
func New(cfg *config.Config, log *slog.Logger) (*Server, error) {
	if log == nil {
		log = slog.Default()
	}
	if cfg == nil {
		return nil, errors.New("pgrouter: nil config")
	}
	if err := config.Validate(cfg); err != nil {
		return nil, fmt.Errorf("config validate: %w", err)
	}
	stats.Build.Version = "lib"
	stats.Build.Commit = "lib"
	_ = stats.New()

	rt, err := wire.BuildRuntime(context.Background(), cfg, log, wire.RuntimeOptions{
		AuthAppName: "pgrouter-lib-auth_query",
		DialAppName: "pgrouter-lib",
	})
	if err != nil {
		return nil, err
	}

	listenAddr := net.JoinHostPort(cfg.Server.ListenAddr,
		strconv.Itoa(cfg.Server.ListenPort))
	ln, err := listener.New(listenAddr, log)
	if err != nil {
		return nil, fmt.Errorf("tcp listener: %w", err)
	}
	if cfg.Server.ProxyProtocol {
		ln.EnableProxyProtocol(cfg.Server.ProxyProtocolStrict)
	}

	var unixLn *listener.Listener
	if cfg.Server.UnixSocketDir != "" {
		uln, err := listener.NewUnix(cfg.Server.UnixSocketDir,
			cfg.Server.ListenPort, cfg.Server.UnixSocketMode, log)
		if err != nil {
			return nil, fmt.Errorf("unix listener: %w", err)
		}
		unixLn = uln
	}

	return &Server{
		cfg:          cfg,
		log:          log,
		rt:           rt,
		listener:     ln,
		unixListener: unixLn,
		stopped:      make(chan struct{}),
	}, nil
}

// Run blocks until ctx is cancelled or the listener fails. Drains
// pools on exit with a 30s grace.
func (s *Server) Run(ctx context.Context) error {
	if err := s.Start(ctx); err != nil {
		return err
	}
	<-s.stopped
	return s.Stop(30 * time.Second)
}

// Start launches replica goroutines + listener(s) in their own
// goroutines and returns immediately. Use with Stop for async control.
func (s *Server) Start(ctx context.Context) error {
	for _, rm := range s.rt.ReplicaManagers {
		rm.Start()
		rm.StartLagPolls(5 * time.Second)
	}

	// Sweep orphan cancel-tracker entries. See cmd/pgrouter for rationale.
	s.rt.CancelTracker.StartSweeper(ctx, 5*time.Minute, time.Hour)

	stats.Ready.Store(true) // /readyz returns 200 from here on
	errCh := make(chan error, 2)
	go func() { errCh <- s.listener.Serve(ctx, s.rt.Handler.Handle) }()
	if s.unixListener != nil {
		go func() { errCh <- s.unixListener.Serve(ctx, s.rt.Handler.Handle) }()
	}
	go func() {
		<-errCh
		s.stopOnce.Do(func() { close(s.stopped) })
	}()
	return nil
}

// Stop drains pools + closes listeners + stops monitors. Idempotent.
func (s *Server) Stop(deadline time.Duration) error {
	s.stopOnce.Do(func() { close(s.stopped) })
	stats.Ready.Store(false) // K8s readiness flips to 503 immediately
	if d := s.rt.Handler.WaitForDrain(time.Now().Add(deadline)); d > 0 {
		s.log.Warn("drain deadline reached with active clients", "remaining", d)
	}
	for _, pm := range s.rt.PrimaryMonitors {
		pm.Stop()
	}
	for _, rm := range s.rt.ReplicaManagers {
		rm.Stop()
	}
	if s.unixListener != nil {
		_ = s.unixListener.Close()
	}
	_ = s.listener.Close()
	if s.rt.AuditWriter != nil {
		_ = s.rt.AuditWriter.Close()
	}
	return s.rt.Manager.CloseWithDeadline(time.Now().Add(deadline))
}
