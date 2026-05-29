// Transaction-mode pooled proxy.
//
// The PoC's client.Conn pins one backend per client conn for the full
// session. PooledConn instead:
//
//  1. Synthesizes the post-startup welcome (AuthenticationOk +
//     ParameterStatus* + BackendKeyData + ReadyForQuery) WITHOUT a
//     backend attached. The client sees pgrouter answering directly.
//  2. On the first client Query / Parse, Acquires a backend from the
//     pool and forwards the message.
//  3. Keeps forwarding bidirectionally inside the transaction.
//  4. When the backend's ReadyForQuery returns tx_status='I' (idle), we
//     just-completed-a-transaction — Release the backend back to the
//     pool. The client sees the RFQ as usual.
//  5. The next Query / Parse Acquires again — possibly a different
//     backend.
//
// MVP scope (M.9):
//   - txn mode only (session/statement modes coming after M.9.4)
//   - bare Query / Parse / Bind / Execute / Sync; COPY + LISTEN /
//     NOTIFY land in M.9.4 with force-session logic.
//   - GUC tracking is M.10 — for now, SETs inside a transaction commit
//     with the transaction. SETs OUTSIDE a transaction are lost on
//     Release; clients that need persistent SETs must wrap them in BEGIN.

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
	// Default true in production; tests may disable.
	ResetOnRelease bool
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
	// First synthetic RFQ → mark client idle.
	state.ObserveBackendMessage(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	var bConn *backend.Conn
	releaseOnExit := true
	defer func() {
		if bConn != nil && releaseOnExit {
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
		}

		if bConn != nil {
			// Forward client → server.
			bConn.Frontend.Send(msg.(pgproto3.FrontendMessage))
			if err := bConn.Frontend.Flush(); err != nil {
				return fmt.Errorf("server send: %w", err)
			}

			// Drain backend response up to ReadyForQuery, forwarding each.
			released := false
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
					if state.Tx().IsIdle() {
						h.Pool.Release(bConn, h.ResetOnRelease)
						bConn = nil
						released = true
					}
					break
				}
			}
			_ = released
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

// sendWelcome sends AuthOk + canned ParameterStatus + synthetic
// BackendKeyData + ReadyForQuery 'I'.
func (h *PooledConn) sendWelcome(be *pgproto3.Backend) error {
	be.Send(&pgproto3.AuthenticationOk{})
	for k, v := range h.CannedParams {
		be.Send(&pgproto3.ParameterStatus{Name: k, Value: v})
	}
	pid, sec, err := randomBackendKey()
	if err != nil {
		return err
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
