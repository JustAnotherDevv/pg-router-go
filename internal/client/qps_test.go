package client

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pg-router-go/internal/testutil"
	"github.com/JustAnotherDevv/pg-router-go/internal/util"
)

func TestQPSLimiterRejectsAfterBurst(t *testing.T) {
	fb, p := newPoolWithFake(t, 2)

	clt, fe, _ := startPooled(t, p, &PooledConn{
		QPSLimiter: util.NewTokenBucket(1, 0.1), // 1 burst, 0.1/s refill
	})

	// First Query succeeds â†’ backend responds.
	fb.scriptReply("SELECT 1", 'I')
	fe.Send(&pgproto3.Query{String: "SELECT 1"})
	require.NoError(t, fe.Flush())
	testutil.DrainToRFQ(t, clt, fe)

	// Second Query immediately â†’ bucket empty â†’ pgrouter rejects locally.
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
	fb, p := newPoolWithFake(t, 2)

	_, fe, _ := startPooled(t, p, &PooledConn{
		QPSLimiter: nil, // disabled
	})
	fb.scriptReply("SELECT 1", 'I')
	fe.Send(&pgproto3.Query{String: "SELECT 1"})
	require.NoError(t, fe.Flush())
	var sawRFQ bool
	for !sawRFQ {
		m, _ := fe.Receive()
		_, sawRFQ = m.(*pgproto3.ReadyForQuery)
	}
}
