// Statement-mode dispatch tests for PooledConn.
//
// In statement mode:
//   1. Explicit BEGIN / START TRANSACTION is REJECTED with SQLSTATE 25001
//      before reaching the backend; the client connection stays open.
//   2. The backend is released after every RFQ, even when the underlying
//      protocol would have stayed checked out (the BEGIN guard means we
//      shouldn't observe non-idle RFQ in practice — but the release path
//      is verified explicitly).
//   3. Implicit single-statement queries (SELECT, INSERT) still work.

package client

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestStatementModeRejectsExplicitBegin(t *testing.T) {
	_, p := newPoolWithFake(t, 2)
	_, fe, _ := startPooled(t, p, &PooledConn{
		PooledConfig: PooledConfig{PoolMode: "statement"},
	})

	// Client sends BEGIN.
	fe.Send(&pgproto3.Query{String: "BEGIN"})
	require.NoError(t, fe.Flush())

	// Expect ErrorResponse + ReadyForQuery 'I' from pgrouter, NOT from
	// the backend. The fake backend has zero queued expects.
	var sawErr, sawRFQ bool
	for !sawRFQ {
		m, err := fe.Receive()
		require.NoError(t, err)
		switch x := m.(type) {
		case *pgproto3.ErrorResponse:
			sawErr = true
			require.Equal(t, "25001", x.Code)
		case *pgproto3.ReadyForQuery:
			sawRFQ = true
			require.Equal(t, byte('I'), x.TxStatus)
		}
	}
	require.True(t, sawErr, "client did not receive 25001 ErrorResponse")
	require.True(t, sawRFQ)

	// No backend should have been acquired — pool stays idle/empty.
	stats := p.Stats()
	require.Equal(t, 0, stats.Active)

}

func TestStatementModeAllowsImplicitSelect(t *testing.T) {
	fb, p := newPoolWithFake(t, 2)
	_, fe, _ := startPooled(t, p, &PooledConn{
		PooledConfig: PooledConfig{PoolMode: "statement"},
	})

	// Script backend response.
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		q, ok := msg.(*pgproto3.Query)
		require.True(t, ok)
		require.Equal(t, "SELECT 1", q.String)
		be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{Name: []byte("?column?"), DataTypeOID: 23, DataTypeSize: 4},
		}})
		be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("1")}})
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})

	fe.Send(&pgproto3.Query{String: "SELECT 1"})
	require.NoError(t, fe.Flush())

	var sawRow, sawRFQ bool
	for !sawRFQ {
		m, err := fe.Receive()
		require.NoError(t, err)
		switch m.(type) {
		case *pgproto3.DataRow:
			sawRow = true
		case *pgproto3.ReadyForQuery:
			sawRFQ = true
		}
	}
	require.True(t, sawRow)

	// Backend released after RFQ.
	require.Eventually(t, func() bool {
		s := p.Stats()
		return s.Idle == 1 && s.Active == 0
	}, time.Second, 5*time.Millisecond)

}

func TestStatementModeRejectsBeginViaParse(t *testing.T) {
	_, p := newPoolWithFake(t, 2)
	_, fe, _ := startPooled(t, p, &PooledConn{
		PooledConfig: PooledConfig{PoolMode: "statement"},
	})

	// Extended-protocol BEGIN via Parse.
	fe.Send(&pgproto3.Parse{Name: "", Query: "BEGIN"})
	require.NoError(t, fe.Flush())

	var sawErr bool
	for i := 0; i < 4; i++ {
		m, err := fe.Receive()
		require.NoError(t, err)
		if e, ok := m.(*pgproto3.ErrorResponse); ok {
			sawErr = true
			require.Equal(t, "25001", e.Code)
		}
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	require.True(t, sawErr)
}

func TestTransactionModeAllowsExplicitBegin(t *testing.T) {
	// Sanity: NON-statement mode still permits BEGIN end-to-end.
	fb, p := newPoolWithFake(t, 2)
	_, fe, _ := startPooled(t, p, &PooledConn{
		PooledConfig: PooledConfig{PoolMode: "transaction"},
	})

	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		q, ok := msg.(*pgproto3.Query)
		require.True(t, ok)
		require.Equal(t, "BEGIN", q.String)
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
		_ = be.Flush()
	})

	fe.Send(&pgproto3.Query{String: "BEGIN"})
	require.NoError(t, fe.Flush())

	for {
		m, err := fe.Receive()
		require.NoError(t, err)
		if r, ok := m.(*pgproto3.ReadyForQuery); ok {
			require.Equal(t, byte('T'), r.TxStatus, "transaction-mode should preserve T")
			break
		}
	}

}
