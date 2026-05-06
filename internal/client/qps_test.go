package client

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
	"github.com/JustAnotherDevv/pgrouter/internal/util"
)

func TestQPSLimiterRejectsAfterBurst(t *testing.T) {
	fb := newFakeBackend(t)
	dial := func(ctx context.Context) (*backend.Conn, error) {
		return fb.Conn(), nil
	}
	p := pool.New("test", dial, pool.Config{
		DefaultPoolSize: 2,
		QueryWait:       time.Second,
		Log:             testutil.Discard,
	})

	clt, srv := net.Pipe()
	defer clt.Close()
	go func() {
		h := &PooledConn{
			Log:        testutil.Discard,
			Pool:       p,
			QPSLimiter: util.NewTokenBucket(1, 0.1), // 1 burst, 0.1/s refill
		}
		_ = h.Serve(context.Background(), srv)
	}()

	fe := pgproto3.NewFrontend(clt, clt)
	for {
		m, _ := fe.Receive()
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	// First Query succeeds → backend responds.
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})
	fe.Send(&pgproto3.Query{String: "SELECT 1"})
	require.NoError(t, fe.Flush())
	for {
		m, _ := fe.Receive()
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

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
	p := pool.New("test", dial, pool.Config{
		DefaultPoolSize: 2,
		QueryWait:       time.Second,
		Log:             testutil.Discard,
	})

	clt, srv := net.Pipe()
	defer clt.Close()
	go func() {
		h := &PooledConn{
			Log:        testutil.Discard,
			Pool:       p,
			QPSLimiter: nil, // disabled
		}
		_ = h.Serve(context.Background(), srv)
	}()

	fe := pgproto3.NewFrontend(clt, clt)
	for {
		m, _ := fe.Receive()
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
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
