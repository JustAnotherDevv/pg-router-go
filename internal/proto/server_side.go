package proto

import (
	"io"

	"github.com/jackc/pgx/v5/pgproto3"
)

// ServerSide wraps a single upstream-Postgres TCP connection from
// pgrouter's perspective: pgrouter is acting AS A POSTGRES FRONTEND
// (client) towards the upstream, so it Sends frontend-direction
// messages and Receives backend-direction messages.
type ServerSide struct {
	fe *pgproto3.Frontend
}

// NewServerSide wraps the given duplex stream.
func NewServerSide(rw io.ReadWriter) *ServerSide {
	return &ServerSide{fe: pgproto3.NewFrontend(rw, rw)}
}

// WrapServerFrontend adopts an already-constructed pgproto3.Frontend.
// Used by internal/backend where Dial keeps its own Frontend alive for
// the startup handshake; subsequent forwarding uses the typed wrapper
// without re-allocating.
func WrapServerFrontend(fe *pgproto3.Frontend) *ServerSide {
	return &ServerSide{fe: fe}
}

// Frontend exposes the underlying pgproto3.Frontend for places that
// still need it during M.2's incremental migration. New code should
// avoid it — callers should reach for the typed methods instead.
//
// TODO(M.6): remove once internal/backend stops needing it.
func (s *ServerSide) Frontend() *pgproto3.Frontend {
	return s.fe
}

// Receive reads the next BackendMessage from the upstream.
func (s *ServerSide) Receive() (pgproto3.BackendMessage, error) {
	return s.fe.Receive()
}

// Send queues a FrontendMessage. Errors surface from Flush.
func (s *ServerSide) Send(msg pgproto3.FrontendMessage) {
	s.fe.Send(msg)
}

// Flush writes any queued messages upstream.
func (s *ServerSide) Flush() error {
	return s.fe.Flush()
}

// SendAndFlush is a one-shot helper.
func (s *ServerSide) SendAndFlush(msg pgproto3.FrontendMessage) error {
	s.fe.Send(msg)
	return s.fe.Flush()
}
