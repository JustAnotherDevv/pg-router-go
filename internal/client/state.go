// Per-client state machine + transaction-boundary detection.
//
// MVP scope (M.6.2 + M.6.3): the proxy loop needs to know when a
// transaction begins/ends so it can release a pooled backend at
// transaction boundaries (M.9). The signal we look at is the
// `tx_status` byte in `ReadyForQuery`:
//
//	'I'  - idle (not in transaction)
//	'T'  - inside transaction block
//	'E'  - inside failed transaction block (needs ROLLBACK)
//
// State transitions are driven entirely by ReadyForQuery messages from
// the backend (the source of truth for transaction state).

package client

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TxState mirrors Postgres's tx_status byte.
type TxState byte

const (
	TxIdle    TxState = 'I'
	TxInBlock TxState = 'T'
	TxFailed  TxState = 'E'
)

// String returns the human-readable name.
func (s TxState) String() string {
	switch s {
	case TxIdle:
		return "idle"
	case TxInBlock:
		return "in-transaction"
	case TxFailed:
		return "failed-transaction"
	case 0:
		return "uninitialized"
	default:
		return fmt.Sprintf("unknown(%c)", byte(s))
	}
}

// IsIdle is true when the client is not inside a transaction. This is
// the safe point at which a pooled backend may be released back to the
// pool in transaction mode.
func (s TxState) IsIdle() bool { return s == TxIdle }

// ClientState is the per-client connection state machine.
//
// Goroutine-affinity: one ClientState per client conn; mutated only by
// the proxy goroutine. No locking needed.
type ClientState struct {
	tx TxState

	// counters for per-client metrics (M.13 hooks).
	QueriesIssued  uint64 // Query + Parse seen
	TxStarts       uint64
	TxCommits      uint64
	TxRollbacks    uint64
}

// NewClientState returns a fresh state machine. Initial tx is
// uninitialized (0); the first ReadyForQuery from the backend
// transitions us to 'I'.
func NewClientState() *ClientState {
	return &ClientState{}
}

// Tx returns the current transaction state.
func (s *ClientState) Tx() TxState { return s.tx }

// ObserveBackendMessage updates state based on one backend → client message.
// Returns true if a transaction boundary was crossed (idle ↔ in-transaction).
//
// Callers use the return value to drive M.9 pool release on the idle edge:
//
//	if state.ObserveBackendMessage(msg) && state.Tx().IsIdle() {
//	    pool.Release(backend)
//	}
func (s *ClientState) ObserveBackendMessage(msg pgproto3.BackendMessage) bool {
	rfq, ok := msg.(*pgproto3.ReadyForQuery)
	if !ok {
		return false
	}
	prev := s.tx
	next := TxState(rfq.TxStatus)
	s.tx = next
	// Boundary if prev was idle and we entered a transaction OR if we
	// returned to idle from in/failed.
	switch {
	case prev != TxInBlock && prev != TxFailed && (next == TxInBlock):
		s.TxStarts++
		return true
	case (prev == TxInBlock || prev == TxFailed) && next == TxIdle:
		// Distinguish commit vs rollback by previous state — failed → rollback.
		if prev == TxFailed {
			s.TxRollbacks++
		} else {
			s.TxCommits++
		}
		return true
	}
	return false
}

// ObserveClientMessage tracks client → server message counts. No state
// transitions; tx state changes only via ReadyForQuery.
func (s *ClientState) ObserveClientMessage(msg pgproto3.FrontendMessage) {
	switch msg.(type) {
	case *pgproto3.Query, *pgproto3.Parse:
		s.QueriesIssued++
	}
}
