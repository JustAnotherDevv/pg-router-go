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
)

// fakeBackend is a goroutine that speaks pgproto3.Backend on the server
// side of a net.Pipe and lets tests script responses.
type fakeBackend struct {
	t       *testing.T
	srv     net.Conn
	cli     net.Conn
	be      *pgproto3.Backend
	scriptC chan func(*pgproto3.Backend, pgproto3.FrontendMessage)
	doneC   chan struct{}
}

func newFakeBackend(t *testing.T) *fakeBackend {
	t.Helper()
	cli, srv := net.Pipe()
	fb := &fakeBackend{
		t:       t,
		srv:     srv,
		cli:     cli,
		be:      pgproto3.NewBackend(srv, srv),
		scriptC: make(chan func(*pgproto3.Backend, pgproto3.FrontendMessage), 16),
		doneC:   make(chan struct{}),
	}
	go fb.run()
	t.Cleanup(func() {
		close(fb.scriptC)
		<-fb.doneC
		_ = cli.Close()
		_ = srv.Close()
	})
	return fb
}

// newPoolWithFake returns a fakeBackend + a one-conn pool that dials
// into it. Collapses the boilerplate every PooledConn test repeats:
//
//	fb := newFakeBackend(t)
//	dial := func(ctx) (*backend.Conn, error) { return fb.Conn(), nil }
//	p := pool.New("test", dial, pool.Config{...})
//
// onto one call. `size` is DefaultPoolSize (most tests want 1 or 2).
func newPoolWithFake(t *testing.T, size int) (*fakeBackend, *pool.Pool) {
	t.Helper()
	fb := newFakeBackend(t)
	dial := func(context.Context) (*backend.Conn, error) { return fb.Conn(), nil }
	p := pool.New("test", dial, pool.Config{
		DefaultPoolSize: size,
		QueryWait:       time.Second,
		Log:             testutil.Discard,
	})
	return fb, p
}

func (fb *fakeBackend) run() {
	defer close(fb.doneC)
	for fn := range fb.scriptC {
		_ = fb.srv.SetDeadline(time.Now().Add(2 * time.Second))
		msg, err := fb.be.Receive()
		if err != nil {
			return
		}
		fn(fb.be, msg)
	}
}

// expect queues a handler. The handler MUST emit at least one
// ReadyForQuery (test invariants assume that).
func (fb *fakeBackend) expect(fn func(*pgproto3.Backend, pgproto3.FrontendMessage)) {
	fb.scriptC <- fn
}

// Conn returns a *backend.Conn whose Frontend talks to the fake.
func (fb *fakeBackend) Conn() *backend.Conn {
	return &backend.Conn{
		NetConn:  fb.cli,
		Frontend: pgproto3.NewFrontend(fb.cli, fb.cli),
		Params:   map[string]string{},
		Log:      testutil.Discard,
	}
}

func TestPooledServeSelect(t *testing.T) {
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
			PooledConfig: PooledConfig{
				CannedParams: map[string]string{"server_version": "16.4"},
			},
			Log:  testutil.Discard,
			Pool: p,
		}
		_ = h.Serve(context.Background(), srv)
	}()

	// Script the backend response for one SELECT.
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

	fe := pgproto3.NewFrontend(clt, clt)

	// Drain welcome: AuthOk + ParameterStatus + BackendKeyData + RFQ.
	for {
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	fe.Send(&pgproto3.Query{String: "SELECT 1"})
	require.NoError(t, fe.Flush())

	// Expect RowDescription, DataRow, CommandComplete, ReadyForQuery.
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

	// Backend should have been released after the idle RFQ.
	require.Eventually(t, func() bool {
		s := p.Stats()
		return s.Idle == 1 && s.Active == 0
	}, time.Second, 5*time.Millisecond)

	_ = clt.Close()
}

func TestPooledReleasesAtTransactionBoundary(t *testing.T) {
	fb, p := newPoolWithFake(t, 2)

	clt, srv := net.Pipe()
	defer clt.Close()
	go func() {
		h := &PooledConn{
			Log:  testutil.Discard,
			Pool: p,
		}
		_ = h.Serve(context.Background(), srv)
	}()

	fe := pgproto3.NewFrontend(clt, clt)
	// Drain welcome.
	for {
		m, _ := fe.Receive()
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	// BEGIN.
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
		_ = be.Flush()
	})
	fe.Send(&pgproto3.Query{String: "BEGIN"})
	require.NoError(t, fe.Flush())

	for {
		m, _ := fe.Receive()
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	// Inside a transaction — pool should still hold the backend ACTIVE.
	require.Eventually(t, func() bool {
		s := p.Stats()
		return s.Active == 1 && s.Idle == 0
	}, time.Second, 5*time.Millisecond)

	// COMMIT.
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})
	fe.Send(&pgproto3.Query{String: "COMMIT"})
	require.NoError(t, fe.Flush())

	for {
		m, _ := fe.Receive()
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	// Boundary crossed — backend released.
	require.Eventually(t, func() bool {
		s := p.Stats()
		return s.Active == 0 && s.Idle == 1
	}, time.Second, 5*time.Millisecond)
	_ = clt.Close()
}

// NOTE: a multi-client share-test would need a fake upstream that
// supports concurrent connections (one fakeBackend per Acquire). The
// pool.Pool tests already cover pooled reuse independently of the
// proxy loop; the PooledConn-level coverage here focuses on
// boundary-driven Release.

func TestPooledClientTerminateReleasesBackend(t *testing.T) {
	_, p := newPoolWithFake(t, 1)

	clt, srv := net.Pipe()
	defer clt.Close()
	doneServe := make(chan struct{})
	go func() {
		defer close(doneServe)
		h := &PooledConn{Log: testutil.Discard, Pool: p}
		_ = h.Serve(context.Background(), srv)
	}()
	fe := pgproto3.NewFrontend(clt, clt)
	for {
		m, _ := fe.Receive()
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	fe.Send(&pgproto3.Terminate{})
	require.NoError(t, fe.Flush())

	select {
	case <-doneServe:
	case <-time.After(time.Second):
		t.Fatal("Serve did not return on Terminate")
	}
	require.Equal(t, 0, p.Stats().Active)
}
