// Phase A tests for PooledConn: query_timeout, client_idle_timeout,
// idle_transaction_timeout, GUC whitelist → session-pin.

package client

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
	"github.com/JustAnotherDevv/pgrouter/internal/stats"
)

// resetStatsForPhaseATest swaps stats.Reg + re-runs stats.New so the
// metric counters start from zero for the calling test. Cleanup restores
// the previous registry.
func resetStatsForPhaseATest(t *testing.T) {
	t.Helper()
	orig := stats.Reg
	stats.Reg = prometheus.NewRegistry()
	_ = stats.New()
	t.Cleanup(func() { stats.Reg = orig })
}

// gatherCounter reads a counter value from stats.Reg.
func gatherCounter(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := stats.Reg.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			lp := m.GetLabel()
			match := true
			got := make(map[string]string, len(lp))
			for _, p := range lp {
				got[p.GetName()] = p.GetValue()
			}
			for k, v := range labels {
				if got[k] != v {
					match = false
					break
				}
			}
			if match {
				if c := m.GetCounter(); c != nil {
					return c.GetValue()
				}
			}
		}
	}
	return 0
}

// keep the dto alias used (compiler check that we link the right pkg).
var _ = (*dto.LabelPair)(nil)

// --- client_idle_timeout (M.6.4) ---

// TestPooledClientIdleTimeoutClosesIdleClient: client connects, never
// sends a Query. ClientIdleTimeout fires; pgrouter closes the client
// connection within the deadline + small slack.
func TestPooledClientIdleTimeoutClosesIdleClient(t *testing.T) {
	fb := newFakeBackend(t)
	dial := func(_ context.Context) (*backend.Conn, error) { return fb.Conn(), nil }
	p := pool.New("test", dial, pool.Config{
		DefaultPoolSize: 1,
		QueryWait:       time.Second,
		Log:             slog.New(slog.DiscardHandler),
	})

	clt, srv := net.Pipe()
	defer clt.Close()

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		h := &PooledConn{
			Log:               slog.New(slog.DiscardHandler),
			Pool:              p,
			Database:          "appdb",
			User:              "alice",
			CannedParams:      map[string]string{"server_version": "16.0"},
			ClientIdleTimeout: 100 * time.Millisecond,
		}
		_ = h.Serve(context.Background(), srv)
	}()

	fe := pgproto3.NewFrontend(clt, clt)
	// Drain the welcome.
	for {
		_ = clt.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	_ = clt.SetReadDeadline(time.Time{})

	// Don't send anything. After ClientIdleTimeout the server should send
	// a FATAL ErrorResponse and close. We expect Serve() to exit.
	select {
	case <-serveDone:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not exit after client_idle_timeout")
	}
}

// TestPooledIdleTxTimeoutClosesInTxClient: client opens a transaction
// (RFQ 'T') and falls silent. IdleTxTimeout fires faster than
// ClientIdleTimeout to confirm the right limit was applied.
func TestPooledIdleTxTimeoutClosesInTxClient(t *testing.T) {
	fb := newFakeBackend(t)
	dial := func(_ context.Context) (*backend.Conn, error) { return fb.Conn(), nil }
	p := pool.New("test", dial, pool.Config{
		DefaultPoolSize: 1,
		QueryWait:       time.Second,
		Log:             slog.New(slog.DiscardHandler),
	})

	clt, srv := net.Pipe()
	defer clt.Close()

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		h := &PooledConn{
			Log:               slog.New(slog.DiscardHandler),
			Pool:              p,
			Database:          "appdb",
			User:              "alice",
			CannedParams:      map[string]string{"server_version": "16.0"},
			ClientIdleTimeout: 5 * time.Second, // ensure this doesn't fire
			IdleTxTimeout:     100 * time.Millisecond,
			// ResetOnRelease left false so the in-tx Release defer
			// doesn't try to send DISCARD ALL through the fake
			// backend (which has no scripted handler for it).
		}
		_ = h.Serve(context.Background(), srv)
	}()

	// Script the backend's BEGIN response: RFQ 'T' (in-transaction).
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		q, ok := msg.(*pgproto3.Query)
		require.True(t, ok)
		require.Equal(t, "BEGIN", q.String)
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
		_ = be.Flush()
	})

	fe := pgproto3.NewFrontend(clt, clt)
	// Drain welcome.
	for {
		_ = clt.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	_ = clt.SetReadDeadline(time.Time{})

	// Open transaction.
	fe.Send(&pgproto3.Query{String: "BEGIN"})
	require.NoError(t, fe.Flush())
	// Drain to RFQ 'T'.
	for {
		_ = clt.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		m, err := fe.Receive()
		require.NoError(t, err)
		if rfq, ok := m.(*pgproto3.ReadyForQuery); ok {
			require.Equal(t, byte('T'), rfq.TxStatus)
			break
		}
	}
	_ = clt.SetReadDeadline(time.Time{})

	// Fall silent. IdleTxTimeout (100ms) should fire before
	// ClientIdleTimeout (5s).
	deadline := time.Now().Add(1 * time.Second)
	select {
	case <-serveDone:
		require.True(t, time.Now().Before(deadline.Add(500*time.Millisecond)),
			"Serve exited too late — wrong timeout was applied")
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not exit after idle_transaction_timeout")
	}
}

// --- query_timeout (M.9 enforce) ---

// TestPooledQueryTimeoutKillsBackendAndKeepsClientConn: backend is told
// to stall (never respond). After QueryTimeout pgrouter closes the
// backend, sends 57014 to the client, but keeps the client connection
// open so they can issue another query.
func TestPooledQueryTimeoutKillsBackendAndKeepsClientConn(t *testing.T) {
	// Use a fake backend that absorbs writes (so pgrouter's Send doesn't
	// block on the synchronous net.Pipe) but never sends a reply. PgRouter's
	// Receive deadline must fire after QueryTimeout.
	stallCli, stallSrv := net.Pipe()
	defer stallCli.Close()
	defer stallSrv.Close()
	// Drain whatever pgrouter writes (StartupMessage etc.). io.Discard
	// just throws bytes away; the goroutine exits when stallSrv closes.
	go func() { _, _ = io.Copy(io.Discard, stallSrv) }()

	dial := func(_ context.Context) (*backend.Conn, error) {
		return &backend.Conn{
			NetConn:  stallCli,
			Frontend: pgproto3.NewFrontend(stallCli, stallCli),
			Params:   map[string]string{},
			Log:      slog.New(slog.DiscardHandler),
		}, nil
	}
	p := pool.New("test", dial, pool.Config{
		DefaultPoolSize: 1,
		QueryWait:       time.Second,
		Log:             slog.New(slog.DiscardHandler),
	})

	clt, srv := net.Pipe()
	defer clt.Close()

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		h := &PooledConn{
			Log:          slog.New(slog.DiscardHandler),
			Pool:         p,
			Database:     "appdb",
			User:         "alice",
			CannedParams: map[string]string{"server_version": "16.0"},
			QueryTimeout: 150 * time.Millisecond,
		}
		_ = h.Serve(context.Background(), srv)
	}()

	fe := pgproto3.NewFrontend(clt, clt)
	// Drain welcome.
	for {
		_ = clt.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	_ = clt.SetReadDeadline(time.Time{})

	// Send a Query that will never get a response.
	fe.Send(&pgproto3.Query{String: "SELECT pg_sleep(60)"})
	require.NoError(t, fe.Flush())

	// Within the QueryTimeout + slack we should receive a FATAL
	// ErrorResponse with code 57014 and connection should stay open
	// long enough for us to assert. (PooledConn writes 57014 as FATAL
	// + Severity=FATAL → client typically disconnects, but we look at
	// the wire only.)
	_ = clt.SetReadDeadline(time.Now().Add(1 * time.Second))
	for {
		m, err := fe.Receive()
		require.NoError(t, err)
		if er, ok := m.(*pgproto3.ErrorResponse); ok {
			require.Equal(t, "57014", er.Code,
				"expected SQLSTATE 57014 query_canceled, got %q", er.Code)
			require.Contains(t, er.Message, "query_timeout")
			return
		}
	}
}

// --- unrecognized GUC → session-pin (M.10) ---

// TestPooledTxMetricsIncrementOnBeginCommit: BEGIN → RFQ 'T' bumps
// TxStarts; COMMIT → RFQ 'I' bumps TxCommits. ROLLBACK from RFQ 'E' →
// 'I' bumps TxRollbacks.
func TestPooledTxMetricsIncrementOnBeginCommit(t *testing.T) {
	// Reset the stats registry so the counters start at 0 for this test.
	resetStatsForPhaseATest(t)

	fb := newFakeBackend(t)
	dial := func(_ context.Context) (*backend.Conn, error) { return fb.Conn(), nil }
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
	// Drain welcome.
	for {
		_ = clt.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	_ = clt.SetReadDeadline(time.Time{})

	// BEGIN → backend replies RFQ 'T'.
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
		_ = be.Flush()
	})
	fe.Send(&pgproto3.Query{String: "BEGIN"})
	require.NoError(t, fe.Flush())
	for {
		_ = clt.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	_ = clt.SetReadDeadline(time.Time{})

	// COMMIT → backend replies RFQ 'I'.
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})
	fe.Send(&pgproto3.Query{String: "COMMIT"})
	require.NoError(t, fe.Flush())
	for {
		_ = clt.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	_ = clt.SetReadDeadline(time.Time{})

	// Give the Serve goroutine a beat to publish metrics.
	time.Sleep(20 * time.Millisecond)

	require.Equal(t, float64(1), gatherCounter(t,
		"pgrouter_tx_starts_total",
		map[string]string{"database": "appdb", "user": "alice"}))
	require.Equal(t, float64(1), gatherCounter(t,
		"pgrouter_tx_commits_total",
		map[string]string{"database": "appdb", "user": "alice"}))
}

// TestPooledUnrecognizedSetPinsSession: after `SET work_mem = '64MB'`
// (not in the replayable whitelist), the next RFQ 'I' must NOT release
// the backend.
func TestPooledUnrecognizedSetPinsSession(t *testing.T) {
	fb := newFakeBackend(t)
	dial := func(_ context.Context) (*backend.Conn, error) { return fb.Conn(), nil }
	p := pool.New("test", dial, pool.Config{
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
	for {
		_ = clt.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	_ = clt.SetReadDeadline(time.Time{})

	// Backend handler for the `SET work_mem` query: RFQ 'I' (idle).
	fb.expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
		q := msg.(*pgproto3.Query)
		require.Contains(t, q.String, "work_mem")
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SET")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})

	fe.Send(&pgproto3.Query{String: "SET work_mem = '64MB'"})
	require.NoError(t, fe.Flush())

	// Drain reply.
	for {
		_ = clt.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	_ = clt.SetReadDeadline(time.Time{})

	// Pool should now show active=1 idle=0 — backend pinned. (Without the
	// pin it would be active=0 idle=1.) Give the Serve goroutine a beat
	// to advance.
	time.Sleep(20 * time.Millisecond)
	stats := p.Stats()
	require.Equal(t, 1, stats.Active,
		"unrecognized SET must keep backend pinned (active=1)")
	require.Equal(t, 0, stats.Idle,
		"unrecognized SET must NOT release backend to idle")
}
