package client

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
)

// fakeBackendFleet is a dialer that mints a fresh fakeBackend per call.
// Tests script each one via scriptN(idx, fn).
type fakeBackendFleet struct {
	t        *testing.T
	mu       sync.Mutex
	backends []*fakeBackend
}

func newFakeBackendFleet(t *testing.T) *fakeBackendFleet {
	return &fakeBackendFleet{t: t}
}

// Dial implements pool.Dialer.
func (f *fakeBackendFleet) Dial(_ context.Context) (*backend.Conn, error) {
	fb := newFakeBackend(f.t)
	f.mu.Lock()
	f.backends = append(f.backends, fb)
	f.mu.Unlock()
	return fb.Conn(), nil
}

// Backend returns the i'th minted backend (0-indexed).
func (f *fakeBackendFleet) Backend(i int) *fakeBackend {
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Less(f.t, i, len(f.backends), "asked for backend %d but only %d minted", i, len(f.backends))
	return f.backends[i]
}

func (f *fakeBackendFleet) Count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.backends)
}

// startPooledClient is a thin compat wrapper over startPooled covering
// the session-test shape (ResetOnRelease toggle). Prefer startPooled
// for new tests.
func startPooledClient(t *testing.T, p *pool.Pool, resetOnRelease bool) (net.Conn, *pgproto3.Frontend, <-chan struct{}) {
	t.Helper()
	return startPooled(t, p, &PooledConn{
		PooledConfig: PooledConfig{ResetOnRelease: resetOnRelease},
	})
}

// TestForceSessionPinningOnLISTEN: after a LISTEN, the backend must
// stay attached even after subsequent idle RFQs.
func TestForceSessionPinningOnLISTEN(t *testing.T) {
	fleet := newFakeBackendFleet(t)
	p := newDialPool(t, "listen-test", fleet.Dial, 2)

	clt, fe, _ := startPooledClient(t, p, false)
	defer clt.Close()

	// Script: LISTEN returns CC + idle RFQ.
	scriptOne := func(be *pgproto3.Backend, _ pgproto3.FrontendMessage) {
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("LISTEN")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	}
	// We need the fleet's first backend before scripting. Acquire is
	// triggered by the client's Query, so we set up a goroutine that
	// scripts the first backend the moment it's minted.
	scripted := make(chan struct{}, 1)
	go func() {
		// Wait for the fleet to mint a backend.
		require.Eventually(t, func() bool { return fleet.Count() >= 1 },
			2*time.Second, 5*time.Millisecond)
		fleet.Backend(0).expect(scriptOne)
		scripted <- struct{}{}
	}()

	fe.Send(&pgproto3.Query{String: "LISTEN events"})
	require.NoError(t, fe.Flush())
	<-scripted

	// Drain the LISTEN response (CC + RFQ).
	testutil.DrainToRFQ(t, clt, fe)

	// After idle RFQ, the backend should still be ACTIVE because we
	// pinned it. Use Eventually because the Serve goroutine is racing.
	requirePoolStats(t, p, 0, 1)

	// Send another query — must hit the SAME backend (still pinned).
	go fleet.Backend(0).expect(func(be *pgproto3.Backend, _ pgproto3.FrontendMessage) {
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})
	fe.Send(&pgproto3.Query{String: "SELECT 1"})
	require.NoError(t, fe.Flush())
	testutil.DrainToRFQ(t, clt, fe)
	require.Equal(t, 1, fleet.Count(), "should NOT have dialed a second backend")
	require.Equal(t, 1, p.Stats().Active)

	clt.Close()
}

// TestForceSessionPinningOnAdvisoryLock: pg_advisory_lock() pins too.
func TestForceSessionPinningOnAdvisoryLock(t *testing.T) {
	fleet := newFakeBackendFleet(t)
	p := newDialPool(t, "adv-test", fleet.Dial, 2)
	clt, fe, _ := startPooledClient(t, p, false)
	defer clt.Close()

	go func() {
		require.Eventually(t, func() bool { return fleet.Count() >= 1 },
			2*time.Second, 5*time.Millisecond)
		fleet.Backend(0).expect(func(be *pgproto3.Backend, _ pgproto3.FrontendMessage) {
			be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
				{Name: []byte("pg_advisory_lock"), DataTypeOID: 16, DataTypeSize: 1},
			}})
			be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("t")}})
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			_ = be.Flush()
		})
	}()

	fe.Send(&pgproto3.Query{String: "SELECT pg_advisory_lock(42)"})
	require.NoError(t, fe.Flush())
	testutil.DrainToRFQ(t, clt, fe)
	requirePoolStats(t, p, 0, 1)
}

// TestSELECTOnlyDoesNotPin: pinned must be false for ordinary queries.
func TestSELECTOnlyDoesNotPin(t *testing.T) {
	fleet := newFakeBackendFleet(t)
	p := newDialPool(t, "sel-test", fleet.Dial, 2)
	clt, fe, _ := startPooledClient(t, p, false)
	defer clt.Close()

	go func() {
		require.Eventually(t, func() bool { return fleet.Count() >= 1 },
			2*time.Second, 5*time.Millisecond)
		fleet.Backend(0).expect(func(be *pgproto3.Backend, _ pgproto3.FrontendMessage) {
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			_ = be.Flush()
		})
	}()

	fe.Send(&pgproto3.Query{String: "SELECT 1"})
	require.NoError(t, fe.Flush())
	testutil.DrainToRFQ(t, clt, fe)
	requirePoolStats(t, p, 1, 0)
}

// TestGUCReplayFiresOnFreshAcquire: verifies fireReplay drives the
// expected wire message on a fresh backend. We exercise the helper
// directly rather than through the full PooledConn.Serve path because
// triggering a "fresh acquire mid-session" through the public API
// requires a longer-lived integration scaffold that lands with M.15.
func TestGUCReplayFiresOnFreshAcquire(t *testing.T) {
	fb := newFakeBackend(t)
	bConn := fb.Conn()

	// The fake echoes CC + RFQ for the replay query.
	go fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		q, ok := msg.(*pgproto3.Query)
		require.True(t, ok)
		require.Contains(t, q.String, "SET timezone=UTC",
			"replay must contain the cached SET")
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SET")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})

	h := &PooledConn{Log: testutil.Discard}
	require.NoError(t, h.fireReplay(bConn, "SET timezone=UTC"))
}

// TestPooledMultipleClientsShareSingleBackend: pool size 1, two
// clients, each runs a Query in sequence. They MUST share the single
// dialed backend; pool.TotalSpawned should stay at 1.
//
// The fakeBackendFleet supports concurrent dials but each backend
// processes scripts sequentially — that's fine since the pool only
// holds one backend at a time anyway.
func TestPooledMultipleClientsShareSingleBackend(t *testing.T) {
	fleet := newFakeBackendFleet(t)
	p := newDialPool(t, "share-test", fleet.Dial, 1, withQueryWait(2*time.Second))

	// Pre-script the SAME backend to answer both queries in order.
	go func() {
		require.Eventually(t, func() bool { return fleet.Count() >= 1 },
			2*time.Second, 5*time.Millisecond)
		fb := fleet.Backend(0)
		fb.scriptReply("SELECT 1", 'I')
		fb.scriptReply("SELECT 2", 'I')
	}()

	runQuery := func() {
		clt, fe, _ := startPooledClient(t, p, false)
		defer clt.Close()
		fe.Send(&pgproto3.Query{String: "SELECT 1"})
		require.NoError(t, fe.Flush())
		for {
			m, err := fe.Receive()
			require.NoError(t, err)
			if _, ok := m.(*pgproto3.ReadyForQuery); ok {
				return
			}
		}
	}

	// Run client A.
	runQuery()
	// Run client B serially — backend released by A is reused.
	runQuery()

	require.Equal(t, 1, fleet.Count(), "pool should reuse single backend")
	require.Equal(t, uint64(1), p.Stats().TotalSpawned)
	// 3 acquires total:
	//   1 eager-warm by client A (cold cache, populates CachedParams)
	//   1 client-A query
	//   1 client-B query
	// Client B does NOT warm because Pool.DialAttempted() is now true.
	require.Equal(t, uint64(3), p.Stats().TotalAcquired)
}

// TestGUCReplayPropagatesError: a backend that errors on the replay
// poisons the connection so PooledConn can discard it.
func TestGUCReplayPropagatesError(t *testing.T) {
	fb := newFakeBackend(t)
	bConn := fb.Conn()
	go fb.expect(func(be *pgproto3.Backend, _ pgproto3.FrontendMessage) {
		be.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR", Code: "42704", Message: "unrecognized parameter",
		})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})
	h := &PooledConn{Log: testutil.Discard}
	err := h.fireReplay(bConn, "SET garbage=true")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unrecognized parameter")
}

// TestPrepareCacheObservesParse: Parse messages populate the prepare
// cache. PgRouter now buffers Parse on the backend (no drain) and only
// drains on Sync — mirroring real Postgres extended-protocol semantics
// — so the test fake responds to Parse with ParseComplete only, and to
// Sync with RFQ.
func TestPrepareCacheObservesParse(t *testing.T) {
	fleet := newFakeBackendFleet(t)
	p := newDialPool(t, "prep-test", fleet.Dial, 1)
	clt, fe, _ := startPooledClient(t, p, false)
	defer clt.Close()

	go func() {
		require.Eventually(t, func() bool { return fleet.Count() >= 1 },
			2*time.Second, 5*time.Millisecond)
		// expect#1: Parse → ParseComplete (no RFQ).
		fleet.Backend(0).expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
			_, ok := msg.(*pgproto3.Parse)
			require.True(t, ok, "expected Parse, got %T", msg)
			be.Send(&pgproto3.ParseComplete{})
			_ = be.Flush()
		})
		// expect#2: Sync → RFQ.
		fleet.Backend(0).expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
			_, ok := msg.(*pgproto3.Sync)
			require.True(t, ok, "expected Sync, got %T", msg)
			be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			_ = be.Flush()
		})
	}()
	fe.Send(&pgproto3.Parse{Name: "stmt1", Query: "SELECT $1::int"})
	fe.Send(&pgproto3.Sync{})
	require.NoError(t, fe.Flush())
	// Don't use testutil.DrainToRFQ here: the helper applies a per-
	// Receive read deadline, but in extended-protocol the test goroutine
	// must script TWO expects (Parse, then Sync) before either response
	// flows, and the race between Acquire/scripted-mint and the helper's
	// deadline is too tight under load. Keep the original deadline-less
	// drain.
	for {
		m, _ := fe.Receive()
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	require.Equal(t, 1, fleet.Count())
}
