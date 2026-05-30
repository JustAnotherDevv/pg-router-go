// Phase B tests: cross-backend prepared statement cache + name rewrite.

package client

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
)

// --- ServerNameFor ---

func TestServerNameForIsDeterministic(t *testing.T) {
	a := ServerNameFor("SELECT $1::int")
	b := ServerNameFor("SELECT $1::int")
	require.Equal(t, a, b, "same SQL must yield same server name")
	require.True(t, strings.HasPrefix(a, "pgr_"), "server name must be prefixed pgr_")
	require.Equal(t, 4+16, len(a), "pgr_ + 16 hex chars")
}

func TestServerNameForDiffersByQuery(t *testing.T) {
	a := ServerNameFor("SELECT 1")
	b := ServerNameFor("SELECT 2")
	require.NotEqual(t, a, b)
}

// --- PrepareCache.ServerNameOf ---

func TestPrepareCacheServerNameOf(t *testing.T) {
	c := NewPrepareCache()
	c.Observe("foo", "SELECT $1", []uint32{23})
	require.Equal(t, ServerNameFor("SELECT $1"), c.ServerNameOf("foo"))
	require.Equal(t, "", c.ServerNameOf("missing"))
	require.Equal(t, "", c.ServerNameOf(""))
}

// --- PooledConn: cache miss forwards rewritten Parse ---

// connWithCache returns a backend.Conn pointing at the given fake AND
// armed with a fresh PreparedCache of the requested capacity.
func connWithCache(fb *fakeBackend, cap int) *backend.Conn {
	c := fb.Conn()
	c.Prepared = backend.NewPreparedCache(cap)
	return c
}

func TestPooledParseMissRewritesNameAndCachesIt(t *testing.T) {
	fb := newFakeBackend(t)
	bConn := connWithCache(fb, 8)
	dial := func(_ context.Context) (*backend.Conn, error) { return bConn, nil }
	p := pool.New("t", dial, pool.Config{
		DefaultPoolSize: 1,
		QueryWait:       time.Second,
		Log:             slog.New(slog.DiscardHandler),
	})

	clt, srv := net.Pipe()
	defer clt.Close()
	go func() {
		h := &PooledConn{
			Log:          slog.New(slog.DiscardHandler),
			Pool:         p,
			Database:     "appdb",
			User:         "alice",
			CannedParams: map[string]string{"server_version": "16.0"},
		}
		_ = h.Serve(context.Background(), srv)
	}()

	fe := pgproto3.NewFrontend(clt, clt)
	drainWelcome(t, clt, fe)

	expectedServerName := ServerNameFor("SELECT $1::int")

	// expect Parse — record + assertion; PG semantics: NO response until
	// Sync arrives (response burst is buffered).
	fb.expect(func(_ *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		m, ok := msg.(*pgproto3.Parse)
		require.True(t, ok, "expected Parse, got %T", msg)
		require.Equal(t, expectedServerName, m.Name,
			"server name must be rewritten to pgr_<hash>")
		require.Equal(t, "SELECT $1::int", m.Query)
	})
	// expect Sync — now the buffered ParseComplete + RFQ flush.
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		_, ok := msg.(*pgproto3.Sync)
		require.True(t, ok, "expected Sync, got %T", msg)
		be.Send(&pgproto3.ParseComplete{})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})

	fe.Send(&pgproto3.Parse{Name: "stmt1", Query: "SELECT $1::int"})
	fe.Send(&pgproto3.Sync{})
	require.NoError(t, fe.Flush())
	drainToRFQ(t, clt, fe)
}

// --- PooledConn: cache hit synthesizes ParseComplete locally ---

func TestPooledParseHitSynthesizesNoBackendRoundTrip(t *testing.T) {
	fb := newFakeBackend(t)
	// Pre-warm the backend cache with our SQL's hash.
	preCached := ServerNameFor("SELECT 1")
	bConn := connWithCache(fb, 8)
	bConn.Prepared.Add(preCached) // simulate "previous client already Parsed this"

	dial := func(_ context.Context) (*backend.Conn, error) { return bConn, nil }
	p := pool.New("t", dial, pool.Config{
		DefaultPoolSize: 1,
		QueryWait:       time.Second,
		Log:             slog.New(slog.DiscardHandler),
	})

	clt, srv := net.Pipe()
	defer clt.Close()
	go func() {
		h := &PooledConn{
			Log:          slog.New(slog.DiscardHandler),
			Pool:         p,
			Database:     "appdb",
			User:         "alice",
			CannedParams: map[string]string{"server_version": "16.0"},
		}
		_ = h.Serve(context.Background(), srv)
	}()

	fe := pgproto3.NewFrontend(clt, clt)
	drainWelcome(t, clt, fe)

	// Backend should ONLY receive Sync — Parse is suppressed by cache hit.
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		_, ok := msg.(*pgproto3.Sync)
		require.True(t, ok, "expected Sync (Parse should have been suppressed), got %T", msg)
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})

	fe.Send(&pgproto3.Parse{Name: "stmtX", Query: "SELECT 1"})
	fe.Send(&pgproto3.Sync{})
	require.NoError(t, fe.Flush())

	// Client must receive a synthesized ParseComplete then RFQ.
	_ = clt.SetReadDeadline(time.Now().Add(time.Second))
	got1, err := fe.Receive()
	require.NoError(t, err)
	_, ok := got1.(*pgproto3.ParseComplete)
	require.True(t, ok, "first msg must be synthesized ParseComplete, got %T", got1)
	got2, err := fe.Receive()
	require.NoError(t, err)
	_, ok = got2.(*pgproto3.ReadyForQuery)
	require.True(t, ok, "second msg must be RFQ, got %T", got2)
}

// --- PooledConn: Bind rewrites the prepared-statement field ---

func TestPooledBindRewritesPreparedStatementName(t *testing.T) {
	fb := newFakeBackend(t)
	bConn := connWithCache(fb, 8)
	_ = bConn
	dial := func(_ context.Context) (*backend.Conn, error) { return bConn, nil }
	p := pool.New("t", dial, pool.Config{
		DefaultPoolSize: 1,
		QueryWait:       time.Second,
		Log:             slog.New(slog.DiscardHandler),
	})

	clt, srv := net.Pipe()
	defer clt.Close()
	go func() {
		h := &PooledConn{
			Log:          slog.New(slog.DiscardHandler),
			Pool:         p,
			Database:     "appdb",
			User:         "alice",
			CannedParams: map[string]string{"server_version": "16.0"},
		}
		_ = h.Serve(context.Background(), srv)
	}()

	fe := pgproto3.NewFrontend(clt, clt)
	drainWelcome(t, clt, fe)

	wantServerName := ServerNameFor("SELECT $1::int")

	// PG semantics: Parse + Bind silent; Sync flushes ParseComplete +
	// BindComplete + RFQ.
	fb.expect(func(_ *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		m := msg.(*pgproto3.Parse)
		require.Equal(t, wantServerName, m.Name)
	})
	fb.expect(func(_ *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		m, ok := msg.(*pgproto3.Bind)
		require.True(t, ok, "expected Bind, got %T", msg)
		require.Equal(t, wantServerName, m.PreparedStatement,
			"Bind.PreparedStatement must be rewritten to server-side name")
	})
	fb.expect(func(be *pgproto3.Backend, _ pgproto3.FrontendMessage) {
		be.Send(&pgproto3.ParseComplete{})
		be.Send(&pgproto3.BindComplete{})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})

	fe.Send(&pgproto3.Parse{Name: "stmt1", Query: "SELECT $1::int"})
	fe.Send(&pgproto3.Bind{PreparedStatement: "stmt1"})
	fe.Send(&pgproto3.Sync{})
	require.NoError(t, fe.Flush())
	drainToRFQ(t, clt, fe)
}

// --- PooledConn: Close('S') is suppressed (statement stays cached) ---

func TestPooledCloseStatementSuppressedAndCloseCompleteSynthesized(t *testing.T) {
	fb := newFakeBackend(t)
	bConn := connWithCache(fb, 8)
	dial := func(_ context.Context) (*backend.Conn, error) { return bConn, nil }
	p := pool.New("t", dial, pool.Config{
		DefaultPoolSize: 1,
		QueryWait:       time.Second,
		Log:             slog.New(slog.DiscardHandler),
	})

	clt, srv := net.Pipe()
	defer clt.Close()
	go func() {
		h := &PooledConn{
			Log:          slog.New(slog.DiscardHandler),
			Pool:         p,
			Database:     "appdb",
			User:         "alice",
			CannedParams: map[string]string{"server_version": "16.0"},
		}
		_ = h.Serve(context.Background(), srv)
	}()

	fe := pgproto3.NewFrontend(clt, clt)
	drainWelcome(t, clt, fe)

	// First Parse so the client cache has stmt1 → server-name.
	fb.expect(func(_ *pgproto3.Backend, _ pgproto3.FrontendMessage) {})
	fb.expect(func(be *pgproto3.Backend, _ pgproto3.FrontendMessage) {
		be.Send(&pgproto3.ParseComplete{})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})
	fe.Send(&pgproto3.Parse{Name: "stmt1", Query: "SELECT 1"})
	fe.Send(&pgproto3.Sync{})
	require.NoError(t, fe.Flush())
	drainToRFQ(t, clt, fe)

	// Now Close('S', stmt1) — should be SUPPRESSED on backend.
	// Only Sync should reach the backend.
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		_, ok := msg.(*pgproto3.Sync)
		require.True(t, ok, "expected Sync (Close should have been suppressed), got %T", msg)
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})
	fe.Send(&pgproto3.Close{ObjectType: 'S', Name: "stmt1"})
	fe.Send(&pgproto3.Sync{})
	require.NoError(t, fe.Flush())

	// Client should see synthesized CloseComplete then RFQ.
	_ = clt.SetReadDeadline(time.Now().Add(time.Second))
	got, err := fe.Receive()
	require.NoError(t, err)
	_, ok := got.(*pgproto3.CloseComplete)
	require.True(t, ok, "first msg after Close should be synthesized CloseComplete, got %T", got)
	got, err = fe.Receive()
	require.NoError(t, err)
	_, ok = got.(*pgproto3.ReadyForQuery)
	require.True(t, ok)

	// Server-side name should STILL be cached on backend.
	require.True(t, bConn.Prepared.Has(ServerNameFor("SELECT 1")),
		"Close('S') must NOT evict from backend cache (keep for next client)")
}

// --- PooledConn: LRU eviction injects Close('S') and filters CloseComplete ---

func TestPooledParseEvictionInjectsBackendCloseAndFiltersCC(t *testing.T) {
	fb := newFakeBackend(t)
	// Cap=1 → every new Parse evicts the previous.
	bConn := connWithCache(fb, 1)
	preCached := ServerNameFor("SELECT 'first'")
	bConn.Prepared.Add(preCached) // capacity full

	dial := func(_ context.Context) (*backend.Conn, error) { return bConn, nil }
	p := pool.New("t", dial, pool.Config{
		DefaultPoolSize: 1,
		QueryWait:       time.Second,
		Log:             slog.New(slog.DiscardHandler),
	})

	clt, srv := net.Pipe()
	defer clt.Close()
	go func() {
		h := &PooledConn{
			Log:          slog.New(slog.DiscardHandler),
			Pool:         p,
			Database:     "appdb",
			User:         "alice",
			CannedParams: map[string]string{"server_version": "16.0"},
		}
		_ = h.Serve(context.Background(), srv)
	}()

	fe := pgproto3.NewFrontend(clt, clt)
	drainWelcome(t, clt, fe)

	wantNewServerName := ServerNameFor("SELECT 'second'")

	// Three expects: Close (eviction) silent, Parse silent, Sync flushes
	// CloseComplete + ParseComplete + RFQ in one burst. The drain filters
	// the CloseComplete so the client only sees ParseComplete + RFQ.
	fb.expect(func(_ *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		m, ok := msg.(*pgproto3.Close)
		require.True(t, ok, "first backend msg must be Close('S', evicted), got %T", msg)
		require.Equal(t, byte('S'), m.ObjectType)
		require.Equal(t, preCached, m.Name,
			"injected Close must target the LRU-evicted server name")
	})
	fb.expect(func(_ *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		m, ok := msg.(*pgproto3.Parse)
		require.True(t, ok, "second backend msg must be Parse, got %T", msg)
		require.Equal(t, wantNewServerName, m.Name)
	})
	fb.expect(func(be *pgproto3.Backend, _ pgproto3.FrontendMessage) {
		be.Send(&pgproto3.CloseComplete{}) // filtered by pendingEvictCloseCompletes
		be.Send(&pgproto3.ParseComplete{})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})

	fe.Send(&pgproto3.Parse{Name: "newstmt", Query: "SELECT 'second'"})
	fe.Send(&pgproto3.Sync{})
	require.NoError(t, fe.Flush())

	// Client must NOT see the injected CloseComplete — only ParseComplete + RFQ.
	_ = clt.SetReadDeadline(time.Now().Add(2 * time.Second))
	seen := []string{}
	for i := 0; i < 3; i++ {
		m, err := fe.Receive()
		require.NoError(t, err)
		switch m.(type) {
		case *pgproto3.CloseComplete:
			seen = append(seen, "CloseComplete")
		case *pgproto3.ParseComplete:
			seen = append(seen, "ParseComplete")
		case *pgproto3.ReadyForQuery:
			seen = append(seen, "RFQ")
		}
		if seen[len(seen)-1] == "RFQ" {
			break
		}
	}
	require.Equal(t, []string{"ParseComplete", "RFQ"}, seen,
		"client must see exactly ParseComplete + RFQ; injected CloseComplete must be filtered")
}

// --- triggersBackendDrain coverage ---

func TestTriggersBackendDrain(t *testing.T) {
	require.True(t, triggersBackendDrain(&pgproto3.Query{}))
	require.True(t, triggersBackendDrain(&pgproto3.Sync{}))
	require.True(t, triggersBackendDrain(&pgproto3.CopyDone{}))
	require.True(t, triggersBackendDrain(&pgproto3.CopyFail{}))
	require.False(t, triggersBackendDrain(&pgproto3.Parse{}))
	require.False(t, triggersBackendDrain(&pgproto3.Bind{}))
	require.False(t, triggersBackendDrain(&pgproto3.Execute{}))
	require.False(t, triggersBackendDrain(&pgproto3.Describe{}))
	require.False(t, triggersBackendDrain(&pgproto3.Close{}))
	require.False(t, triggersBackendDrain(&pgproto3.Flush{}))
}

// --- DISCARD ALL via reset clears backend Prepared cache ---

func TestBackendResetClearsPreparedCache(t *testing.T) {
	// This isn't a PooledConn test — it directly invokes ResetState on a
	// fake backend.Conn that has a cache.
	fb := newFakeBackend(t)
	bConn := connWithCache(fb, 8)
	bConn.Prepared.Add("pgr_aaa")
	bConn.Prepared.Add("pgr_bbb")
	require.Equal(t, 2, bConn.Prepared.Len())

	// Script: receive DISCARD ALL → CommandComplete + RFQ 'I'.
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		_, ok := msg.(*pgproto3.Query)
		require.True(t, ok)
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("DISCARD ALL")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})

	err := bConn.ResetState()
	require.NoError(t, err)
	require.Equal(t, 0, bConn.Prepared.Len(),
		"DISCARD ALL must clear the per-backend prepared cache to match real PG semantics")
}

// TestPhaseBWelcomeAloneDoesNotHang is a minimal repro: spawn Serve,
// drain welcome, exit. Same setup as the Miss test but no Parse step.
func TestPhaseBWelcomeAloneDoesNotHang(t *testing.T) {
	fb := newFakeBackend(t)
	bConn := connWithCache(fb, 8)
	dial := func(_ context.Context) (*backend.Conn, error) { return bConn, nil }
	p := pool.New("welc-isolated", dial, pool.Config{
		DefaultPoolSize: 1,
		QueryWait:       time.Second,
		Log:             slog.New(slog.DiscardHandler),
	})

	clt, srv := net.Pipe()
	defer clt.Close()
	go func() {
		h := &PooledConn{
			Log:          slog.New(slog.DiscardHandler),
			Pool:         p,
			Database:     "appdb",
			User:         "alice",
			CannedParams: map[string]string{"server_version": "16.0"},
		}
		_ = h.Serve(context.Background(), srv)
	}()

	fe := pgproto3.NewFrontend(clt, clt)
	drainWelcome(t, clt, fe)
}

// --- helpers used by Phase B pooled tests ---

func drainWelcome(t *testing.T, clt net.Conn, fe *pgproto3.Frontend) {
	t.Helper()
	for {
		_ = clt.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			_ = clt.SetReadDeadline(time.Time{})
			return
		}
	}
}

func drainToRFQ(t *testing.T, clt net.Conn, fe *pgproto3.Frontend) {
	t.Helper()
	for {
		_ = clt.SetReadDeadline(time.Now().Add(time.Second))
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			_ = clt.SetReadDeadline(time.Time{})
			return
		}
	}
}
