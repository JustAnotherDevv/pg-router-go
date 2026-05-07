package client

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
	"github.com/JustAnotherDevv/pgrouter/internal/util"
)

func TestQPSLimiterRejectsAfterBurst(t *testing.T) {
	fb := newFakeBackend(t)
	dial := func(ctx context.Context) (*backend.Conn, error) {
		return fb.Conn(), nil
	}
	p := newDialPool(t, "test", dial, 2)

	clt, fe, _ := startPooled(t, p, &PooledConn{
		QPSLimiter: util.NewTokenBucket(1, 0.1), // 1 burst, 0.1/s refill
	})

	// First Query succeeds → backend responds.
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})
	fe.Send(&pgproto3.Query{String: "SELECT 1"})
	require.NoError(t, fe.Flush())
	testutil.DrainToRFQ(t, clt, fe)

	// Second Query immediately → bucket empty → pgrouter rejects locally.
	fe.Send(&pgproto3.Query{String: "SELECT 2"})
	require.NoError(t, fe.Flush())

	var sawReject bool
	for i := 0; i < 4; i++ {
		m, err := fe.Receive()
		require.NoError(t, err)
		switch x := m.(type) {
		case *pgproto3.ErrorResponse:
			sawReject = true
			require.Equal(t, "53300", x.Code)
		case *pgproto3.ReadyForQuery:
			require.True(t, sawReject)
			_ = clt.Close()
			return
		}
	}
	t.Fatal("no reject seen")
}

func TestQPSLimiterAllowsWhenDisabled(t *testing.T) {
	fb := newFakeBackend(t)
	dial := func(ctx context.Context) (*backend.Conn, error) {
		return fb.Conn(), nil
	}
	p := newDialPool(t, "test", dial, 2)

	_, fe, _ := startPooled(t, p, &PooledConn{
		QPSLimiter: nil, // disabled
	})
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})
	fe.Send(&pgproto3.Query{String: "SELECT 1"})
	require.NoError(t, fe.Flush())
	var sawRFQ bool
	for !sawRFQ {
		m, _ := fe.Receive()
		_, sawRFQ = m.(*pgproto3.ReadyForQuery)
	}
}
