package proto

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

// ErrForwardStopped is returned by ForwardUntil when one of the stop
// types has been forwarded. The message itself is delivered to the
// destination before the function returns.
var ErrForwardStopped = errors.New("forward: stop type reached")

// ForwardClientToServer reads ONE FrontendMessage from `src` and
// queues it on `dst`. Caller is responsible for `dst.Flush()` — that
// lets callers batch many messages into one syscall when they choose.
//
// Returns the message itself so callers can inspect it (e.g. detect
// Terminate, count Parse/Bind for the prepared-stmt cache).
func ForwardClientToServer(src *ClientSide, dst *ServerSide) (pgproto3.FrontendMessage, error) {
	msg, err := src.Receive()
	if err != nil {
		return nil, fmt.Errorf("client recv: %w", err)
	}
	dst.Send(msg)
	return msg, nil
}

// ForwardServerToClient reads ONE BackendMessage from `src` and queues
// it on `dst`. Caller flushes.
func ForwardServerToClient(src *ServerSide, dst *ClientSide) (pgproto3.BackendMessage, error) {
	msg, err := src.Receive()
	if err != nil {
		return nil, fmt.Errorf("server recv: %w", err)
	}
	dst.Send(msg)
	return msg, nil
}

// IsTerminate returns true if the frontend message is a Terminate ('X')
// — the client is closing the connection cleanly.
func IsTerminate(msg pgproto3.FrontendMessage) bool {
	_, ok := msg.(*pgproto3.Terminate)
	return ok
}

// IsReadyForQuery returns true with the tx-status byte if the message
// is a ReadyForQuery. Use to detect transaction boundaries.
func IsReadyForQuery(msg pgproto3.BackendMessage) (status byte, ok bool) {
	if r, isRfq := msg.(*pgproto3.ReadyForQuery); isRfq {
		return r.TxStatus, true
	}
	return 0, false
}

// IsErrorResponse returns true if the backend message is an ErrorResponse.
// Useful for downgrading the connection on FATAL errors.
func IsErrorResponse(msg pgproto3.BackendMessage) (severity, code string, ok bool) {
	if e, isErr := msg.(*pgproto3.ErrorResponse); isErr {
		return e.Severity, e.Code, true
	}
	return "", "", false
}
