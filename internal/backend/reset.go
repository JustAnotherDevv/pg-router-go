package backend

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

// ResetQuery is sent to a backend before returning it to the pool. It
// clears any per-session state the previous client may have set:
//
//   - DISCARD ALL: temp tables, prepared statements, listening
//     channels, cursors, advisory locks, session GUCs, sequence
//     state. Equivalent to PgBouncer's server_reset_query default.
//   - SET extra_float_digits = 3: a parity GUC pgx clients always
//     set; pre-setting it avoids one round-trip per acquired conn.
//     Optional — omit if you want strict reset semantics.
//
// MVP M.7.3 keeps it simple: just DISCARD ALL.
const ResetQuery = "DISCARD ALL"

// ResetState sends ResetQuery on the connection and drains the response
// up to and including ReadyForQuery. Returns an error if the backend
// reported an error or if the connection died.
//
// Must be called on an IDLE backend (between transactions). Caller is
// responsible for that — see internal/client.ClientState.Tx().
func (c *Conn) ResetState() error {
	if c == nil || c.NetConn == nil {
		return errors.New("reset: nil backend")
	}

	c.Frontend.Send(&pgproto3.Query{String: ResetQuery})
	if err := c.Frontend.Flush(); err != nil {
		return fmt.Errorf("reset send: %w", err)
	}

	for {
		msg, err := c.Frontend.Receive()
		if err != nil {
			return fmt.Errorf("reset recv: %w", err)
		}
		switch m := msg.(type) {
		case *pgproto3.CommandComplete:
			// "DISCARD ALL" -> CommandTag "DISCARD ALL". Just absorb.
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("reset error %s: %s", m.Severity, m.Message)
		case *pgproto3.NoticeResponse:
			// PG may emit notices; ignore.
		case *pgproto3.ReadyForQuery:
			if m.TxStatus != 'I' {
				return fmt.Errorf("reset left tx_status=%c (expected I)", m.TxStatus)
			}
			return nil
		case *pgproto3.RowDescription, *pgproto3.DataRow:
			// Unexpected for DISCARD ALL but harmless; drain.
		default:
			// Unknown msg — keep draining.
		}
	}
}

// HealthCheck sends an empty `;` query (PgBouncer's
// `server_check_query` default) and waits for ReadyForQuery. Returns
// nil if the backend is responsive.
//
// Cheap (~1 RTT) — safe to call before borrowing a long-idle backend
// from the pool to verify it hasn't been killed by the network or
// reaped by Postgres `idle_in_transaction_session_timeout`.
func (c *Conn) HealthCheck() error {
	if c == nil || c.NetConn == nil {
		return errors.New("healthcheck: nil backend")
	}
	c.Frontend.Send(&pgproto3.Query{String: ";"})
	if err := c.Frontend.Flush(); err != nil {
		return fmt.Errorf("healthcheck send: %w", err)
	}
	for {
		msg, err := c.Frontend.Receive()
		if err != nil {
			return fmt.Errorf("healthcheck recv: %w", err)
		}
		switch m := msg.(type) {
		case *pgproto3.EmptyQueryResponse, *pgproto3.CommandComplete:
			// expected; drain to RFQ
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("healthcheck error %s: %s", m.Severity, m.Message)
		case *pgproto3.ReadyForQuery:
			return nil
		}
	}
}
