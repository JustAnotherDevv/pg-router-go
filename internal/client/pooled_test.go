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

// newDialPool builds a *pool.Pool wired for client tests: caller-
// supplied dial, default 1s QueryWait, shared discard logger. opts
// tweak the config in-place (size is required; everything else has
// sensible defaults). t.Cleanup registers Pool.Close so test code
// doesn't need its own defer.
//
// Use this directly when you need a custom dial (real PG, stall conn,
// error injection). Use newPoolWithFake when a scripted fake backend
// is enough.
func newDialPool(t *testing.T, name string, dial pool.Dialer, size int, opts ...func(*pool.Config)) *pool.Pool {
	t.Helper()
	cfg := pool.Config{
		DefaultPoolSize: size,
		QueryWait:       time.Second,
		Log:             testutil.Discard,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	p := pool.New(name, dial, cfg)
	t.Cleanup(p.Close)
	return p
}

// withQueryWait overrides QueryWait for newDialPool (default 1s).
func withQueryWait(d time.Duration) func(*pool.Config) {
	return func(c *pool.Config) { c.QueryWait = d }
}

// newDispatcherMgr builds a pool.Manager wired for client/dispatcher
// tests: configurable DefaultPoolSize, configurable QueryWait (0 = no
// QueryWait), shared discard logger, every-key returns the supplied
// dial. Saves the 7-line `pool.NewManager(pool.Config{...}, dialerFor)
// + defer mgr.Close()` boilerplate.
func newDispatcherMgr(t *testing.T, dial pool.Dialer, size int, queryWait time.Duration) *pool.Manager {
	t.Helper()
	cfg := pool.Config{
		DefaultPoolSize: size,
		QueryWait:       queryWait,
		Log:             testutil.Discard,
	}
	m := pool.NewManager(cfg, func(_ pool.Key) pool.Dialer { return dial })
	t.Cleanup(m.Close)
	return m
}

// requirePoolStats polls p.Stats() and waits up to 1s for s.Idle ==
// wantIdle && s.Active == wantActive. Replaces the per-test boilerplate:
//
//	require.Eventually(t, func() bool {
//		s := p.Stats()
//		return s.Idle == X && s.Active == Y
//	}, time.Second, 5*time.Millisecond)
func requirePoolStats(t *testing.T, p *pool.Pool, wantIdle, wantActive int) {
	t.Helper()
	require.Eventually(t, func() bool {
		s := p.Stats()
		return s.Idle == wantIdle && s.Active == wantActive
	}, time.Second, 5*time.Millisecond,
		"pool stats never reached idle=%d active=%d", wantIdle, wantActive)
}

// newPoolWithFake returns a fakeBackend + a one-conn pool dialing into
// it. Sugar over newDialPool — covers the most common client-test shape.
func newPoolWithFake(t *testing.T, size int) (*fakeBackend, *pool.Pool) {
	t.Helper()
	fb := newFakeBackend(t)
	dial := func(context.Context) (*backend.Conn, error) { return fb.Conn(), nil }
	return fb, newDialPool(t, "test", dial, size)
}

// startPooled wires h to a fresh net.Pipe pair, launches h.Serve in a
// goroutine, drains the welcome to RFQ, and returns the client-side
// conn + frontend + a "Serve has returned" channel. Replaces the ~25-
// line boilerplate every PooledConn dispatch test repeats.
//
// Side effects on h: sets Pool=p; sets Log=testutil.Discard if unset.
// Caller retains ownership of all other fields (PooledConfig, Database,
// User, Router etc.). clt is registered with t.Cleanup so the test
// shouldn't add `defer clt.Close()` (idempotent if it does).
func startPooled(t *testing.T, p *pool.Pool, h *PooledConn) (clt net.Conn, fe *pgproto3.Frontend, done <-chan struct{}) {
	t.Helper()
	clt, srv := net.Pipe()
	t.Cleanup(func() { _ = clt.Close() })
	if h.Log == nil {
		h.Log = testutil.Discard
	}
	h.Pool = p
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		_ = h.Serve(context.Background(), srv)
	}()
	fe = pgproto3.NewFrontend(clt, clt)
	testutil.DrainToRFQ(t, clt, fe)
	return clt, fe, doneCh
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

// scriptReply queues an expect handler that sends CommandComplete +
// ReadyForQuery + Flush. Use for queries where the test only cares
// about the response, not the inbound message content.
func (fb *fakeBackend) scriptReply(tag string, txStatus byte) {
	fb.expect(func(be *pgproto3.Backend, _ pgproto3.FrontendMessage) {
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus})
		_ = be.Flush()
	})
}

// scriptQuery is like scriptReply but also asserts the inbound message
// is *pgproto3.Query{String: wantSQL}. Use when the test verifies the
// exact SQL pgrouter forwarded.
func (fb *fakeBackend) scriptQuery(t *testing.T, wantSQL, tag string, txStatus byte) {
	t.Helper()
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		q, ok := msg.(*pgproto3.Query)
		require.True(t, ok, "expected *Query, got %T", msg)
		require.Equal(t, wantSQL, q.String)
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus})
		_ = be.Flush()
	})
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
	p := newDialPool(t, "test", dial, 2)
	_, fe, _ := startPooled(t, p, &PooledConn{
		PooledConfig: PooledConfig{
			CannedParams: map[string]string{"server_version": "16.4"},
		},
	})

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
	requirePoolStats(t, p, 1, 0)

}

func TestPooledReleasesAtTransactionBoundary(t *testing.T) {
	fb, p := newPoolWithFake(t, 2)
	_, fe, _ := startPooled(t, p, &PooledConn{})

	// BEGIN.
	fb.scriptReply("BEGIN", 'T')
	fe.Send(&pgproto3.Query{String: "BEGIN"})
	require.NoError(t, fe.Flush())

	testutil.DrainToRFQ(t, nil, fe)
	// Inside a transaction — pool should still hold the backend ACTIVE.
	requirePoolStats(t, p, 0, 1)

	// COMMIT.
	fb.scriptReply("COMMIT", 'I')
	fe.Send(&pgproto3.Query{String: "COMMIT"})
	require.NoError(t, fe.Flush())

	testutil.DrainToRFQ(t, nil, fe)
	// Boundary crossed — backend released.
	requirePoolStats(t, p, 1, 0)
}

// NOTE: a multi-client share-test would need a fake upstream that
// supports concurrent connections (one fakeBackend per Acquire). The
// pool.Pool tests already cover pooled reuse independently of the
// proxy loop; the PooledConn-level coverage here focuses on
// boundary-driven Release.

func TestPooledClientTerminateReleasesBackend(t *testing.T) {
	_, p := newPoolWithFake(t, 1)
	_, fe, doneServe := startPooled(t, p, &PooledConn{})

	fe.Send(&pgproto3.Terminate{})
	require.NoError(t, fe.Flush())

	select {
	case <-doneServe:
	case <-time.After(time.Second):
		t.Fatal("Serve did not return on Terminate")
	}
	require.Equal(t, 0, p.Stats().Active)
}
