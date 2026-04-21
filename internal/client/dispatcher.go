// PooledHandler is the production listener.Handler that combines:
//   - startup phase (SSL/GSS/Cancel/StartupMessage parsing)
//   - optional TLS upgrade
//   - server-side auth (trust / MD5 / SCRAM via Userlist)
//   - cancel.Tracker allocation for the BackendKeyData advertised to the client
//   - per-(db, user) pool routing via pool.Manager
//   - hand-off to PooledConn.Serve for transaction-mode forwarding
//
// One PooledHandler is shared across all client connections; each
// connection runs Handle in its own goroutine spawned by listener.Serve.

package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/JustAnotherDevv/pgrouter/internal/auth"
	"github.com/JustAnotherDevv/pgrouter/internal/cancel"
	"github.com/JustAnotherDevv/pgrouter/internal/listener"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
)

// PooledHandler is the production dispatcher. Fields can be nil for
// "feature disabled":
//   - TLSConfig nil → SSLRequest is declined with 'N'
//   - Auth nil → trust (no client auth)
//   - CancelTracker nil → random per-welcome PID/secret with no
//     routing (cancel still parsed, just dropped)
type PooledHandler struct {
	Log *slog.Logger

	// Manager owns the per-(db, user) pools.
	Manager *pool.Manager

	// TLSConfig is the client-side TLS config (nil → SSLRequest declined).
	TLSConfig *tls.Config

	// Auth gates the post-StartupMessage auth handshake.
	Auth *auth.ServerAuthOptions

	// CancelTracker, if non-nil, allocates (PID, secret) per client.
	// CancelRequest packets dispatch through it.
	CancelTracker *cancel.Tracker

	// CannedParams are the ParameterStatus values reported in the
	// welcome (before any real backend is attached).
	CannedParams map[string]string

	// ResetOnRelease is forwarded to each PooledConn. Defaults to true
	// when constructed via NewPooledHandler.
	ResetOnRelease bool

	// QueryTimeout / ClientIdleTimeout / IdleTxTimeout are forwarded to
	// each PooledConn (PgBouncer query_timeout / client_idle_timeout /
	// idle_transaction_timeout). 0 disables.
	QueryTimeout      time.Duration
	ClientIdleTimeout time.Duration
	IdleTxTimeout     time.Duration

	// LogSQL is one of: "off" | "redacted" | "full". Forwarded to each
	// PooledConn for per-query logging. Empty string is equivalent to
	// "redacted" — the safe default. "full" should only be used in dev
	// because it lets PII reach the log handler.
	LogSQL string

	// PoolMode is the default pool dispatch mode
	// ("session" | "transaction" | "statement"). Per-DB overrides via
	// PoolModeFor.
	PoolMode string

	// PoolModeFor, if non-nil, returns the per-database pool-mode
	// override (one of "session" | "transaction" | "statement"). An
	// empty return falls back to PoolMode. Lets a config define
	//   pool.mode: transaction
	//   databases:
	//     analytics: { pool_mode: statement }
	// without storing every override in every PooledHandler field.
	PoolModeFor func(db string) string
}

// NewPooledHandler returns a PooledHandler with production defaults
// (ResetOnRelease=true).
func NewPooledHandler(log *slog.Logger, mgr *pool.Manager) *PooledHandler {
	return &PooledHandler{
		Log:            log,
		Manager:        mgr,
		ResetOnRelease: true,
	}
}

// Handle is the listener.Handler signature: one goroutine per client
// connection.
func (h *PooledHandler) Handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	reqID := newRequestID()
	log := h.Log.With("remote", conn.RemoteAddr().String(), "req_id", reqID)
	log.Info("client connected")
	defer log.Info("client disconnected")

	be := pgproto3.NewBackend(conn, conn)

	for {
		msg, err := be.ReceiveStartupMessage()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Debug("startup recv err", "err", err)
			}
			return
		}

		switch m := msg.(type) {
		case *pgproto3.SSLRequest:
			if h.TLSConfig != nil {
				log.Info("SSLRequest received, accepting (TLS upgrade)")
				if err := listener.WriteSSLAccept(conn); err != nil {
					log.Debug("ssl accept write err", "err", err)
					return
				}
				upgraded, err := listener.UpgradeServerToTLS(conn, h.TLSConfig)
				if err != nil {
					log.Warn("tls handshake failed", "err", err)
					return
				}
				conn = upgraded
				be = pgproto3.NewBackend(conn, conn)
				continue
			}
			if err := listener.WriteSSLDecline(conn); err != nil {
				log.Debug("ssl decline write err", "err", err)
				return
			}

		case *pgproto3.GSSEncRequest:
			if err := listener.WriteSSLDecline(conn); err != nil {
				log.Debug("gss decline write err", "err", err)
				return
			}

		case *pgproto3.CancelRequest:
			h.handleCancel(ctx, m, log)
			return

		case *pgproto3.StartupMessage:
			user := m.Parameters["user"]
			db := m.Parameters["database"]
			app := m.Parameters["application_name"]
			log = log.With("user", user, "database", db, "app", app)
			log.Info("StartupMessage received",
				"protocol_version", fmt.Sprintf("%d.%d",
					m.ProtocolVersion>>16, m.ProtocolVersion&0xFFFF),
			)

			if h.Auth != nil {
				if err := auth.PerformServerAuthConn(be, conn, *h.Auth, user); err != nil {
					log.Info("client auth failed", "err", err)
					return
				}
			}

			p := h.Manager.Get(pool.Key{DB: db, User: user})
			h.servePooled(ctx, conn, p, db, user, app, log)
			return

		default:
			log.Warn("unknown startup message", "type", fmt.Sprintf("%T", m))
			return
		}
	}
}

// servePooled is the hand-off from startup to PooledConn.Serve.
func (h *PooledHandler) servePooled(ctx context.Context, conn net.Conn, p *pool.Pool, db, user, app string, log *slog.Logger) {
	var (
		welcomePID    uint32
		welcomeSecret []byte
		cancelKey     cancel.Key
		haveCancelKey bool
	)
	if h.CancelTracker != nil {
		k, err := h.CancelTracker.Allocate()
		if err != nil {
			log.Warn("cancel allocate failed; using random key", "err", err)
		} else {
			cancelKey = k
			haveCancelKey = true
			welcomePID = k.ProcessID
			welcomeSecret = append([]byte(nil), k.SecretKey[:]...)
		}
	}
	defer func() {
		if haveCancelKey {
			h.CancelTracker.Release(cancelKey)
		}
	}()

	mode := h.PoolMode
	if h.PoolModeFor != nil {
		if override := h.PoolModeFor(db); override != "" {
			mode = override
		}
	}
	pc := &PooledConn{
		Log:               log,
		Pool:              p,
		Database:          db,
		User:              user,
		App:               app,
		CannedParams:      h.CannedParams,
		ResetOnRelease:    h.ResetOnRelease,
		WelcomePID:        welcomePID,
		WelcomeSecret:     welcomeSecret,
		QueryTimeout:      h.QueryTimeout,
		ClientIdleTimeout: h.ClientIdleTimeout,
		IdleTxTimeout:     h.IdleTxTimeout,
		LogSQL:            h.LogSQL,
		PoolMode:          mode,
	}
	if err := pc.Serve(ctx, conn); err != nil {
		log.Debug("pooled serve ended", "err", err)
	}
}

// handleCancel routes a CancelRequest to the right upstream and closes.
func (h *PooledHandler) handleCancel(ctx context.Context, m *pgproto3.CancelRequest, log *slog.Logger) {
	log.Info("CancelRequest received",
		"process_id", m.ProcessID,
		"secret_key", fmt.Sprintf("%x", m.SecretKey),
	)
	if h.CancelTracker == nil {
		log.Info("cancel dropped (no tracker)")
		return
	}
	var sec [4]byte
	// pgproto3 represents SecretKey as []byte (PG 18+ supports longer
	// secrets). We always allocate 4-byte keys, so take the first 4.
	if len(m.SecretKey) >= 4 {
		copy(sec[:], m.SecretKey[:4])
	} else {
		copy(sec[:], m.SecretKey)
	}
	target, err := h.CancelTracker.Lookup(cancel.Key{
		ProcessID: m.ProcessID,
		SecretKey: sec,
	})
	if err != nil {
		log.Info("cancel dropped", "err", err)
		return
	}
	if err := cancel.ForwardCancel(ctx, target, 0); err != nil {
		log.Warn("cancel forward failed", "err", err)
		return
	}
	log.Info("cancel forwarded",
		"backend_addr", target.BackendAddr,
		"backend_pid", target.BackendProcessID,
	)
}

