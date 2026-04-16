// Package handler implements per-connection Postgres wire-protocol
// handling. PoC scope: parse StartupMessage + dispatch SSL/GSS/Cancel +
// open a per-client upstream backend.
// Forwarding wired in P.3.2.
package handler

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
)

// PoCHandler handles one accepted client connection. PoC scope: parse
// the startup message, decline SSL/GSS, accept Cancel, open a per-client
// upstream backend connection (P.3.1).
type PoCHandler struct {
	Log *slog.Logger

	// BackendAddr is the upstream Postgres "host:port" to dial on a
	// successful client StartupMessage. If empty, no upstream is opened.
	BackendAddr string
}

// Handle is the listener.Handler signature.
func (h *PoCHandler) Handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	log := h.Log.With("remote", conn.RemoteAddr().String())
	log.Info("client connected")
	defer log.Info("client disconnected")

	be := pgproto3.NewBackend(conn, conn)

	for {
		msg, err := be.ReceiveStartupMessage()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Debug("startup msg read err", "err", err)
			}
			return
		}

		switch m := msg.(type) {
		case *pgproto3.SSLRequest:
			// PoC: decline SSL. TLS wired in P.4 of MVP roadmap (M.4).
			log.Info("SSLRequest received, declining (PoC trust mode)")
			if _, err := conn.Write([]byte{'N'}); err != nil {
				log.Debug("ssl decline write err", "err", err)
				return
			}
			// Continue: client may now send a plain StartupMessage.

		case *pgproto3.GSSEncRequest:
			log.Info("GSSEncRequest received, declining")
			if _, err := conn.Write([]byte{'N'}); err != nil {
				log.Debug("gss decline write err", "err", err)
				return
			}

		case *pgproto3.CancelRequest:
			log.Info("CancelRequest received",
				"process_id", m.ProcessID,
				"secret_key", fmt.Sprintf("%08x", m.SecretKey),
			)
			// Real cancel routing in M.12. PoC: just log + close.
			return

		case *pgproto3.StartupMessage:
			log.Info("StartupMessage received",
				"protocol_version", fmt.Sprintf("%d.%d",
					m.ProtocolVersion>>16,
					m.ProtocolVersion&0xFFFF),
				"parameters", m.Parameters,
			)

			if h.BackendAddr == "" {
				// No upstream configured: trust-mode canned response (PoC P.2.3 path).
				if err := h.sendStartupResponse(be, conn, log); err != nil {
					log.Debug("startup response err", "err", err)
					return
				}
				h.idleLoop(ctx, be, conn, log)
				return
			}

			// P.3.1: open upstream backend (per-client, no pooling yet).
			user := m.Parameters["user"]
			db := m.Parameters["database"]
			app := m.Parameters["application_name"]
			bctx, bcancel := context.WithTimeout(ctx, 5*time.Second)
			bConn, err := backend.Dial(bctx, backend.DialOptions{
				Addr:     h.BackendAddr,
				User:     user,
				Database: db,
				AppName:  app,
				Log:      log,
			})
			bcancel()
			if err != nil {
				log.Error("backend dial failed", "err", err)
				be.Send(&pgproto3.ErrorResponse{
					Severity: "FATAL",
					Code:     "08006",
					Message:  fmt.Sprintf("pgrouter: cannot reach upstream: %v", err),
				})
				_ = be.Flush()
				return
			}
			defer bConn.Close()
			log.Info("backend connected",
				"backend_pid", bConn.PostgresPID,
				"backend_params_count", len(bConn.Params),
			)

			// P.3.2: forward backend's startup state to client + proxy.
			if err := h.forwardStartupToClient(be, bConn, log); err != nil {
				log.Debug("forward startup err", "err", err)
				return
			}
			h.proxy(ctx, be, bConn, log)
			return

		default:
			log.Warn("unknown startup message", "type", fmt.Sprintf("%T", m))
			return
		}
	}
}

// sendStartupResponse emits the trust-mode startup sequence:
// AuthenticationOk -> ParameterStatus* -> BackendKeyData -> ReadyForQuery 'I'.
// Note: pgx/v5 pgproto3 Backend.Send() returns no error; errors surface via Flush.
func (h *PoCHandler) sendStartupResponse(be *pgproto3.Backend, conn net.Conn, log *slog.Logger) error {
	// 1. AuthenticationOk.
	be.Send(&pgproto3.AuthenticationOk{})

	// 2. ParameterStatus — minimal viable set so psql is happy.
	params := []struct{ k, v string }{
		{"server_version", "16.4 (pgrouter PoC)"},
		{"server_encoding", "UTF8"},
		{"client_encoding", "UTF8"},
		{"DateStyle", "ISO, MDY"},
		{"IntervalStyle", "postgres"},
		{"TimeZone", "UTC"},
		{"integer_datetimes", "on"},
		{"standard_conforming_strings", "on"},
		{"is_superuser", "off"},
		{"session_authorization", "pgrouter"},
		{"application_name", ""},
	}
	for _, p := range params {
		be.Send(&pgproto3.ParameterStatus{Name: p.k, Value: p.v})
	}

	// 3. BackendKeyData with random PID + secret (4-byte secret).
	pid, sec, err := randomKey()
	if err != nil {
		return fmt.Errorf("randomKey: %w", err)
	}
	be.Send(&pgproto3.BackendKeyData{ProcessID: pid, SecretKey: sec})

	// 4. ReadyForQuery 'I' (idle).
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	// Flush to client — this is where any queued Send errors surface.
	if err := be.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	log.Info("startup response sent",
		"backend_pid", pid,
		"backend_secret", fmt.Sprintf("%x", sec),
	)
	return nil
}

// forwardStartupToClient relays the backend's startup state to the client.
// The backend's StartupMessage has already been processed by backend.Dial;
// we synthesize the equivalent client-facing sequence using captured values.
func (h *PoCHandler) forwardStartupToClient(be *pgproto3.Backend, bConn *backend.Conn, log *slog.Logger) error {
	// AuthenticationOk.
	be.Send(&pgproto3.AuthenticationOk{})

	// Forward each ParameterStatus from the backend.
	for k, v := range bConn.Params {
		be.Send(&pgproto3.ParameterStatus{Name: k, Value: v})
	}

	// BackendKeyData: forward the real PG pid/secret so future
	// cancellations could route to the right backend (cancel routing is
	// implemented properly in M.12).
	be.Send(&pgproto3.BackendKeyData{
		ProcessID: bConn.PostgresPID,
		SecretKey: bConn.SecretKey,
	})

	// ReadyForQuery 'I' (we've drained the backend up to its own Ready).
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	if err := be.Flush(); err != nil {
		return fmt.Errorf("flush startup to client: %w", err)
	}
	log.Info("startup forwarded to client",
		"backend_pid", bConn.PostgresPID,
		"params_forwarded", len(bConn.Params),
	)
	return nil
}

// proxy runs two goroutines that forward messages between the client and
// the backend. Returns when either side disconnects or ctx is cancelled.
//
// PoC scope: full pass-through, no per-message inspection. Pool reuse +
// transaction-boundary detection are MVP-scope (M.6 / M.9).
func (h *PoCHandler) proxy(ctx context.Context, be *pgproto3.Backend, bConn *backend.Conn, log *slog.Logger) {
	done := make(chan struct{}, 2)

	// client → backend
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			msg, err := be.Receive()
			if err != nil {
				log.Debug("client recv err", "err", err)
				return
			}
			bConn.Frontend.Send(msg.(pgproto3.FrontendMessage))
			if err := bConn.Frontend.Flush(); err != nil {
				log.Debug("backend send err", "err", err)
				return
			}
			if _, ok := msg.(*pgproto3.Terminate); ok {
				log.Info("client sent Terminate")
				return
			}
		}
	}()

	// backend → client
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			msg, err := bConn.Frontend.Receive()
			if err != nil {
				log.Debug("backend recv err", "err", err)
				return
			}
			be.Send(msg.(pgproto3.BackendMessage))
			if err := be.Flush(); err != nil {
				log.Debug("client send err", "err", err)
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("proxy ctx cancelled")
	case <-done:
	}

	// Best-effort close the other side to unblock the second goroutine.
	_ = bConn.NetConn.Close()

	// Drain remaining done signal so goroutine exits cleanly.
	<-done
}

// idleLoop reads client messages and responds with a minimal stub so the
// PoC can keep a session alive when no backend is configured.
func (h *PoCHandler) idleLoop(ctx context.Context, be *pgproto3.Backend, conn net.Conn, log *slog.Logger) {
	for {
		// Honor ctx cancellation.
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := be.Receive()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Debug("idle receive err", "err", err)
			}
			return
		}

		switch m := msg.(type) {
		case *pgproto3.Terminate:
			log.Info("client terminate")
			return
		case *pgproto3.Query:
			log.Info("PoC query received (no upstream yet)", "sql", m.String)
			// Respond with a synthetic error so client doesn't hang.
			be.Send(&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "0A000",
				Message:  "pgrouter PoC: query handling wired in P.3.x",
			})
			be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			_ = be.Flush()
		default:
			log.Debug("idle msg", "type", fmt.Sprintf("%T", m))
		}
	}
}

// randomKey returns a 32-bit PID + 4-byte secret for BackendKeyData.
// pgx/v5 pgproto3 represents SecretKey as []byte (variable-length to
// support Postgres 18+ longer secrets); we always emit the classic 4-byte form.
func randomKey() (uint32, []byte, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, nil, err
	}
	pid := binary.BigEndian.Uint32(buf[0:4])
	if pid == 0 {
		pid = 1 // 0 is reserved/invalid
	}
	sec := make([]byte, 4)
	copy(sec, buf[4:8])
	return pid, sec, nil
}
