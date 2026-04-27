package pgrouter

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
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

// Server is one embedded pgrouter instance. Build with New, drive
// with Run (blocks) or Start/Stop (async). Safe to embed in a Go
// service alongside other goroutines.
type Server struct {
	cfg *config.Config
	log *slog.Logger

	mgr          *pool.Manager
	listener     *listener.Listener
	unixListener *listener.Listener
	handler      *client.PooledHandler
	replicaMgrs  map[string]*replica.Manager
	auditWriter  *client.AuditWriter

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

	clientTLS, _, err := listener.BuildServerTLSConfig(cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("client TLS: %w", err)
	}
	backendTLS, err := listener.BuildBackendTLSConfig(cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("backend TLS: %w", err)
	}
	backendTLSRequired := cfg.TLS.ServerMode == config.SSLRequire ||
		cfg.TLS.ServerMode == config.SSLVerifyCA ||
		cfg.TLS.ServerMode == config.SSLVerifyFull

	var userlist *auth.Userlist
	if cfg.Auth.UserlistFile != "" {
		ul, err := auth.NewUserlist(cfg.Auth.UserlistFile)
		if err != nil {
			return nil, fmt.Errorf("userlist: %w", err)
		}
		userlist = ul
	}
	var hba *auth.HBAFile
	if cfg.Auth.HBAFile != "" {
		h, err := auth.NewHBAFile(cfg.Auth.HBAFile)
		if err != nil {
			return nil, fmt.Errorf("hba: %w", err)
		}
		hba = h
	}
	var authOpts *auth.ServerAuthOptions
	if cfg.Auth.Type != config.AuthTrust {
		authOpts = &auth.ServerAuthOptions{
			Type:     cfg.Auth.Type,
			Userlist: userlist,
			HBA:      hba,
			Log:      log,
		}
	}

	cancelTracker := cancel.NewTracker()
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
	}

	dialerFor := func(k pool.Key) pool.Dialer {
		db, ok := cfg.Databases[k.DB]
		if !ok {
			return func(_ context.Context) (*backend.Conn, error) {
				return nil, fmt.Errorf("unknown database %q", k.DB)
			}
		}
		addr := net.JoinHostPort(db.Host, strconv.Itoa(db.Port))
		user := k.User
		if db.User != "" {
			user = db.User
		}
		return func(ctx context.Context) (*backend.Conn, error) {
			return backend.Dial(ctx, backend.DialOptions{
				Addr:        addr,
				User:        user,
				Database:    db.DBName,
				AppName:     "pgrouter-lib",
				Password:    db.Password,
				TLSConfig:   backendTLS,
				TLSRequired: backendTLSRequired,
				Log:         log,
			})
		}
	}

	mgr := pool.NewManager(defaultCfg, dialerFor).
		WithGlobalLimits(cfg.Pool.MaxDBConn, cfg.Pool.MaxUserConn,
			stats.OnGlobalLimitReject)
	mgr.Start(cfg.Pool.ServerCheckDelay)

	// Replicas.
	replicaMgrs := buildReplicaManagers(cfg, defaultCfg, backendTLS,
		backendTLSRequired, log)

	auditWriter, err := client.OpenAuditFile(cfg.Logging.AuditFile)
	if err != nil {
		return nil, fmt.Errorf("audit: %w", err)
	}

	h := &client.PooledHandler{
		Log:               log,
		Manager:           mgr,
		TLSConfig:         clientTLS,
		Auth:              authOpts,
		CancelTracker:     cancelTracker,
		ResetOnRelease:    true,
		QueryTimeout:      cfg.Pool.QueryTimeout,
		ClientIdleTimeout: cfg.Server.ClientIdle,
		IdleTxTimeout:     cfg.Server.IdleTx,
		SlowQuery:         cfg.Logging.SlowQuery,
		LogSQL:            string(config.NormalizeLogSQL(cfg.Logging.LogSQL)),
		Audit:             auditWriter,
		PoolMode:          string(cfg.Pool.Mode),
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
	}

	listenAddr := net.JoinHostPort(cfg.Server.ListenAddr,
		strconv.Itoa(cfg.Server.ListenPort))
	ln, err := listener.New(listenAddr, log)
	if err != nil {
		return nil, fmt.Errorf("tcp listener: %w", err)
	}
	if cfg.Server.ProxyProtocol {
		ln.EnableProxyProtocol()
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
		mgr:          mgr,
		listener:     ln,
		unixListener: unixLn,
		handler:      h,
		replicaMgrs:  replicaMgrs,
		auditWriter:  auditWriter,
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
	for _, rm := range s.replicaMgrs {
		rm.Start()
		rm.StartLagPolls(5 * time.Second)
	}

	errCh := make(chan error, 2)
	go func() { errCh <- s.listener.Serve(ctx, s.handler.Handle) }()
	if s.unixListener != nil {
		go func() { errCh <- s.unixListener.Serve(ctx, s.handler.Handle) }()
	}
	go func() {
		<-errCh
		s.stopOnce.Do(func() { close(s.stopped) })
	}()
	return nil
}

// Stop drains pools + closes listeners. Idempotent.
func (s *Server) Stop(deadline time.Duration) error {
	s.stopOnce.Do(func() { close(s.stopped) })
	for _, rm := range s.replicaMgrs {
		rm.Stop()
	}
	if s.unixListener != nil {
		_ = s.unixListener.Close()
	}
	_ = s.listener.Close()
	if s.auditWriter != nil {
		_ = s.auditWriter.Close()
	}
	return s.mgr.CloseWithDeadline(time.Now().Add(deadline))
}

// buildReplicaManagers mirrors the cmd-side helper. Kept here so the
// library package is self-contained and doesn't depend on cmd/.
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
