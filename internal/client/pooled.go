// Transaction-mode pooled proxy.
//
// The PoC's client.Conn pins one backend per client conn for the full
// session. PooledConn instead:
//
//  1. Synthesizes the post-startup welcome (AuthenticationOk +
//     ParameterStatus* + BackendKeyData + ReadyForQuery) WITHOUT a
//     backend attached. The client sees pgrouter answering directly.
//  2. On the first client Query / Parse, Acquires a backend from the
//     pool and forwards the message. If the client has set any GUCs
//     during this session, the cache's ReplayQuery is fired first so
//     the (potentially fresh) backend has the right state.
//  3. Keeps forwarding bidirectionally inside the transaction.
//  4. When the backend's ReadyForQuery returns tx_status='I' (idle) AND
//     the client isn't session-pinned (LISTEN / advisory_lock / temp
//     table / cursor), Release the backend back to the pool. The
//     client sees the RFQ as usual.
//  5. The next Query / Parse Acquires again — possibly a different
//     backend.
//
// MVP scope (M.9):
//   - txn mode + automatic session pinning for incompatible features
//   - bare Query / Parse / Bind / Execute / Sync; COPY data is forwarded
//     1:1 inside an implicit transaction.
//   - GUC replay on fresh-backend acquire (per-client GUC cache).
//   - DISCARD ALL on release by default; per-client prepared-statement
//     bookkeeping (Parse / Close('S', name) tracked).

package client

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
	"github.com/JustAnotherDevv/pgrouter/internal/proto"
)

// PooledConn is a transaction-mode pooled handler for one client.
type PooledConn struct {
	Log *slog.Logger

	// Pool is the (db, user) pool to Acquire / Release from.
	Pool *pool.Pool

	// CannedParams are the ParameterStatus values we report to clients
	// before any real backend is attached. Production code populates
	// these from the first successful upstream connect; tests can
	// pass a minimal viable set.
	CannedParams map[string]string

	// ResetOnRelease, when true, sends DISCARD ALL on every Release.
	// Defaults to TRUE in production (NewPooledConn) so a backend never
	// carries session state across clients. Tests may override.
	ResetOnRelease bool

	// WelcomePID + WelcomeSecret, if non-zero, are emitted in the
	// BackendKeyData portion of the welcome message. Callers wire the
	// cancel.Tracker here so subsequent CancelRequest packets can be
	// routed back to this client's currently-attached backend. Zero
	// values cause a one-shot random key to be generated locally.
	WelcomePID    uint32
	WelcomeSecret []byte

	// resetOnReleaseSet is true once a caller has explicitly written to
	// ResetOnRelease (including via the zero value of the bool — but
	// most production paths go via NewPooledConn which sets this).
	// Internal toggle; not exported.
	resetOnReleaseSet bool
}

// NewPooledConn returns a PooledConn with production defaults applied:
// ResetOnRelease=true. Use this from cmd/pgrouter and any orchestration
// code; direct struct literals are fine for tests that want to opt out.
func NewPooledConn(log *slog.Logger, p *pool.Pool, cannedParams map[string]string) *PooledConn {
	return &PooledConn{
		Log:               log,
		Pool:              p,
		CannedParams:      cannedParams,
		ResetOnRelease:    true,
		resetOnReleaseSet: true,
	}
}

// Serve runs the pooled handler against an already-authenticated client.
// Caller is responsible for the startup handshake + auth before invoking.
//
// The function returns when the client disconnects, the ctx is done,
// or an unrecoverable error occurs.
func (h *PooledConn) Serve(ctx context.Context, conn net.Conn) error {
	defer conn.Close()
	log := h.Log.With("remote", conn.RemoteAddr().String())

	be := pgproto3.NewBackend(conn, conn)
	clientSide := proto.WrapClientBackend(be)

	// 1. Send the synthetic welcome.
	if err := h.sendWelcome(be); err != nil {
		return fmt.Errorf("welcome: %w", err)
	}
	log.Info("pooled client ready")

	state := NewClientState()
	gucCache := NewGUCCache()
	prepCache := NewPrepareCache()

	// First synthetic RFQ → mark client idle.
	state.ObserveBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	var bConn *backend.Conn
	// `sessionPinned` flips on once we see LISTEN / advisory_lock / temp
	// table / cursor — after which the backend stays attached for the
	// rest of the client's session.
	sessionPinned := false

	defer func() {
		if bConn != nil {
			// On client disconnect we always release. Reset is best-effort
			// per the user's ResetOnRelease config.
			h.Pool.Release(bConn, h.ResetOnRelease)
		}
	}()

	for {
		// Honour cancellation between messages.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := clientSide.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Debug("client EOF")
				return nil
			}
			return fmt.Errorf("client recv: %w", err)
		}

		// Terminate: tear down without a final round trip.
		if proto.IsTerminate(msg) {
			log.Info("client sent Terminate")
			return nil
		}

		state.ObserveClientMessage(msg)
		h.observeClientMessage(msg, gucCache, prepCache, &sessionPinned, log)

		// Acquire a backend lazily on the first traffic-generating message.
		needsBackend := messageNeedsBackend(msg)
		if needsBackend && bConn == nil {
			bConn, err = h.Pool.Acquire(ctx)
			if err != nil {
				h.sendFatalError(be, "08006",
					fmt.Sprintf("pgrouter: cannot acquire backend: %v", err))
				return err
			}
			log.Debug("backend acquired", "backend_pid", bConn.PostgresPID)

			// Replay tracked GUCs on the fresh backend BEFORE the
			// client's message hits it. Skip if cache is empty (the
			// common case).
			if replay := gucCache.ReplayQuery(); replay != "" {
				if err := h.fireReplay(bConn, replay); err != nil {
					log.Warn("guc replay failed; treating backend as bad",
						"err", err)
					// Defensively discard the backend.
					_ = bConn.Close()
					bConn = nil
					h.sendFatalError(be, "57P03",
						fmt.Sprintf("pgrouter: backend replay failed: %v", err))
					return err
				}
			}
		}

		if bConn != nil {
			// Forward client → server.
			bConn.Frontend.Send(msg.(pgproto3.FrontendMessage))
			if err := bConn.Frontend.Flush(); err != nil {
				return fmt.Errorf("server send: %w", err)
			}

			// Drain backend response up to ReadyForQuery, forwarding each.
			for {
				bmsg, err := bConn.Frontend.Receive()
				if err != nil {
					return fmt.Errorf("server recv: %w", err)
				}
				be.Send(bmsg.(pgproto3.BackendMessage))
				if err := be.Flush(); err != nil {
					return fmt.Errorf("client send: %w", err)
				}
				state.ObserveBackendMessage(bmsg)
				if _, ok := proto.IsReadyForQuery(bmsg); ok {
					// Release whenever the backend reports idle —
					// covers explicit COMMIT/ROLLBACK boundaries AND
					// implicit-transaction queries (e.g. bare SELECT
					// outside BEGIN). PgBouncer's transaction mode
					// behaves identically.
					//
					// EXCEPT when the client has triggered session-pin
					// (LISTEN, advisory_lock, temp table, cursor) —
					// then we hold the backend for the remainder of the
					// session.
					if state.Tx().IsIdle() && !sessionPinned {
						h.Pool.Release(bConn, h.ResetOnRelease)
						bConn = nil
					}
					break
				}
			}
			continue
		}

		// Backend not needed (e.g. Sync without prior Parse) — synthesize
		// a no-op response.
		if _, ok := msg.(*pgproto3.Sync); ok {
			be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			_ = be.Flush()
		}
	}
}

// observeClientMessage runs the per-message hooks that drive the GUC +
// prepare caches and the session-pin trigger.
func (h *PooledConn) observeClientMessage(
	msg pgproto3.FrontendMessage,
	gucCache *GUCCache,
	prepCache *PrepareCache,
	sessionPinned *bool,
	log *slog.Logger,
) {
	switch m := msg.(type) {
	case *pgproto3.Query:
		gucCache.ObserveQuery(m.String)
		if !*sessionPinned && needsSessionPin(m.String) {
			*sessionPinned = true
			log.Info("session pinned (force-session)",
				"reason", "incompatible feature in Query",
				"sql_prefix", truncate(m.String, 64),
			)
		}
	case *pgproto3.Parse:
		prepCache.Observe(m.Name, m.Query, m.ParameterOIDs)
		if !*sessionPinned && needsSessionPin(m.Query) {
			*sessionPinned = true
			log.Info("session pinned (force-session)",
				"reason", "incompatible feature in Parse",
				"sql_prefix", truncate(m.Query, 64),
			)
		}
	case *pgproto3.Close:
		// Close('S', name) untracks a prepared statement.
		if m.ObjectType == 'S' {
			prepCache.Close(m.Name)
		}
	}
}

// truncate is a no-frills string slicer for log fields.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// fireReplay sends `sql` on the backend and drains the response up to
// ReadyForQuery. Returns an error on backend ErrorResponse — the caller
// should treat the backend as poisoned and discard it.
func (h *PooledConn) fireReplay(bConn *backend.Conn, sql string) error {
	bConn.Frontend.Send(&pgproto3.Query{String: sql})
	if err := bConn.Frontend.Flush(); err != nil {
		return fmt.Errorf("replay flush: %w", err)
	}
	for {
		msg, err := bConn.Frontend.Receive()
		if err != nil {
			return fmt.Errorf("replay recv: %w", err)
		}
		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("replay error %s: %s", m.Severity, m.Message)
		case *pgproto3.ReadyForQuery:
			return nil
		default:
			// CommandComplete, RowDescription, etc.: drain
		}
	}
}

// sendWelcome sends AuthOk + canned ParameterStatus + BackendKeyData +
// ReadyForQuery 'I'. PID/secret come from WelcomePID/Secret if set,
// otherwise random one-shot values.
func (h *PooledConn) sendWelcome(be *pgproto3.Backend) error {
	be.Send(&pgproto3.AuthenticationOk{})
	for k, v := range h.CannedParams {
		be.Send(&pgproto3.ParameterStatus{Name: k, Value: v})
	}
	pid := h.WelcomePID
	sec := h.WelcomeSecret
	if pid == 0 || len(sec) == 0 {
		p, s, err := randomBackendKey()
		if err != nil {
			return err
		}
		pid, sec = p, s
	}
	be.Send(&pgproto3.BackendKeyData{ProcessID: pid, SecretKey: sec})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	return be.Flush()
}

func (h *PooledConn) sendFatalError(be *pgproto3.Backend, code, msg string) {
	be.Send(&pgproto3.ErrorResponse{
		Severity: "FATAL",
		Code:     code,
		Message:  msg,
	})
	_ = be.Flush()
}

// messageNeedsBackend returns true if the client message implies we
// must hold a real backend to satisfy it.
//
// Bare Sync without a preceding Parse/Bind/Execute is a no-op the
// proxy can answer itself with ReadyForQuery (matches PgBouncer).
func messageNeedsBackend(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.Query,
		*pgproto3.Parse,
		*pgproto3.Bind,
		*pgproto3.Execute,
		*pgproto3.Describe,
		*pgproto3.Close,
		*pgproto3.CopyData,
		*pgproto3.CopyDone,
		*pgproto3.CopyFail,
		*pgproto3.Flush:
		return true
	case *pgproto3.Sync, *pgproto3.Terminate:
		return false
	default:
		// Safe default: forward to a backend rather than synthesize.
		return true
	}
}

// randomBackendKey is a copy of the helper in conn.go; kept here so
// pooled.go is independently testable.
func randomBackendKey() (uint32, []byte, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, nil, err
	}
	pid := binary.BigEndian.Uint32(buf[0:4])
	if pid == 0 {
		pid = 1
	}
	sec := make([]byte, 4)
	copy(sec, buf[4:8])
	return pid, sec, nil
}
