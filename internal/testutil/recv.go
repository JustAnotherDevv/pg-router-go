package testutil

import (
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// defaultDrainDeadline is the per-Receive deadline DrainToRFQ applies so
// a misbehaving server doesn't hang the whole test on net.Pipe.
const defaultDrainDeadline = 500 * time.Millisecond

// DrainToRFQ reads frontend messages until ReadyForQuery, returning the
// observed RFQ. Applies a fresh read deadline before each Receive (so a
// stalled peer can't block forever) and clears the deadline before
// returning so the caller can continue with no inherited timeout.
//
// Replaces the boilerplate that appears 47+ times across the unit tests:
//
//	for {
//		_ = clt.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
//		m, err := fe.Receive()
//		require.NoError(t, err)
//		if _, ok := m.(*pgproto3.ReadyForQuery); ok { break }
//	}
//	_ = clt.SetReadDeadline(time.Time{})
//
// If conn is nil, the deadline-set steps are skipped (useful for the few
// callers who use a fake conn without deadline support).
func DrainToRFQ(t *testing.T, conn net.Conn, fe *pgproto3.Frontend) *pgproto3.ReadyForQuery {
	t.Helper()
	for {
		if conn != nil {
			_ = conn.SetReadDeadline(time.Now().Add(defaultDrainDeadline))
		}
		m, err := fe.Receive()
		require.NoError(t, err)
		if rfq, ok := m.(*pgproto3.ReadyForQuery); ok {
			if conn != nil {
				_ = conn.SetReadDeadline(time.Time{})
			}
			return rfq
		}
	}
}
