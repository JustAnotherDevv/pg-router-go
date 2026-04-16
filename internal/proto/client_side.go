package proto

import (
	"io"

	"github.com/jackc/pgx/v5/pgproto3"
)

// ClientSide wraps a single client TCP connection from pgrouter's
// perspective: pgrouter is acting AS A POSTGRES BACKEND towards the
// client, so it Sends backend-direction messages and Receives
// frontend-direction messages.
//
// It is a thin typed wrapper around pgproto3.Backend; higher-level
// packages (internal/client, internal/proto/forward.go) use this so
// they never import pgproto3 directly.
type ClientSide struct {
	be *pgproto3.Backend
}

// NewClientSide wraps the given duplex stream. Reads from `rw`, writes to `rw`.
func NewClientSide(rw io.ReadWriter) *ClientSide {
	return &ClientSide{be: pgproto3.NewBackend(rw, rw)}
}

// WrapClientBackend adopts an already-constructed pgproto3.Backend.
// Used during the M.2 → M.6 migration when internal/client owns the
// startup-phase Backend and wants to reuse it for the proxy loop.
func WrapClientBackend(be *pgproto3.Backend) *ClientSide {
	return &ClientSide{be: be}
}

// ReceiveStartup reads the next startup-phase message:
// StartupMessage, SSLRequest, GSSEncRequest, or CancelRequest.
//
// Use this until a StartupMessage is received; afterwards switch to
// Receive() for the regular message stream.
func (c *ClientSide) ReceiveStartup() (pgproto3.FrontendMessage, error) {
	return c.be.ReceiveStartupMessage()
}

// Receive reads the next FrontendMessage (Query, Parse, Bind, Execute,
// Sync, Describe, Close, Flush, Terminate, PasswordMessage / SASL*,
// CopyData / CopyDone / CopyFail).
func (c *ClientSide) Receive() (pgproto3.FrontendMessage, error) {
	return c.be.Receive()
}

// Send queues a BackendMessage. Errors surface from Flush.
func (c *ClientSide) Send(msg pgproto3.BackendMessage) {
	c.be.Send(msg)
}

// Flush writes any queued messages to the client.
func (c *ClientSide) Flush() error {
	return c.be.Flush()
}

// SendAndFlush is a one-shot helper for low-frequency messages
// (handshake, error responses).
func (c *ClientSide) SendAndFlush(msg pgproto3.BackendMessage) error {
	c.be.Send(msg)
	return c.be.Flush()
}
