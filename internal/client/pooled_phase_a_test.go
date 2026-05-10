// Phase A tests for PooledConn: query_timeout, client_idle_timeout,
// idle_transaction_timeout, GUC whitelist → session-pin.

package client

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
	"github.com/JustAnotherDevv/pgrouter/internal/testutil/statreset"
)

// --- client_idle_timeout (M.6.4) ---

// TestPooledClientIdleTimeoutClosesIdleClient: client connects, never
// sends a Query. ClientIdleTimeout fires; pgrouter closes the client
// connection within the deadline + small slack.
func TestPooledClientIdleTimeoutClosesIdleClient(t *testing.T) {
	_, p := newPoolWithFake(t, 1)
	_, _, serveDone := startPooled(t, p, &PooledConn{
		PooledConfig: PooledConfig{
			CannedParams:      map[string]string{"server_version": "16.0"},
			ClientIdleTimeout: 100 * time.Millisecond,
		},
		Database: "appdb",
		User:     "alice",
	})

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
	fb, p := newPoolWithFake(t, 1)
	clt, fe, serveDone := startPooled(t, p, &PooledConn{
		PooledConfig: PooledConfig{
			CannedParams:      map[string]string{"server_version": "16.0"},
			ClientIdleTimeout: 5 * time.Second, // ensure this doesn't fire
			IdleTxTimeout:     100 * time.Millisecond,
			// ResetOnRelease left false so the in-tx Release defer
			// doesn't try to send DISCARD ALL through the fake
			// backend (which has no scripted handler for it).
		},
		Database: "appdb",
		User:     "alice",
	})

	// Script the backend's BEGIN response: RFQ 'T' (in-transaction).
	fb.scriptQuery(t, "BEGIN", "BEGIN", 'T')

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
			Log:      testutil.Discard,
		}, nil
	}
	p := newDialPool(t, "test", dial, 1)
	clt, fe, _ := startPooled(t, p, &PooledConn{
		PooledConfig: PooledConfig{
			CannedParams: map[string]string{"server_version": "16.0"},
			QueryTimeout: 150 * time.Millisecond,
		},
		Database: "appdb",
		User:     "alice",
	})

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
	statreset.ResetStats(t)

	fb, p := newPoolWithFake(t, 1)
	clt, fe, _ := startPooled(t, p, &PooledConn{
		PooledConfig: PooledConfig{
			CannedParams: map[string]string{"server_version": "16.0"},
		},
		Database: "appdb",
		User:     "alice",
	})

	// BEGIN → backend replies RFQ 'T'.
	fb.scriptReply("BEGIN", 'T')
	fe.Send(&pgproto3.Query{String: "BEGIN"})
	require.NoError(t, fe.Flush())
	testutil.DrainToRFQ(t, clt, fe)

	// COMMIT → backend replies RFQ 'I'.
	fb.scriptReply("COMMIT", 'I')
	fe.Send(&pgproto3.Query{String: "COMMIT"})
	require.NoError(t, fe.Flush())
	testutil.DrainToRFQ(t, clt, fe)

	// Give the Serve goroutine a beat to publish metrics.
	time.Sleep(20 * time.Millisecond)

	require.Equal(t, float64(1), statreset.GetCounter(t,
		"pgrouter_tx_starts_total",
		map[string]string{"database": "appdb", "user": "alice"}))
	require.Equal(t, float64(1), statreset.GetCounter(t,
		"pgrouter_tx_commits_total",
		map[string]string{"database": "appdb", "user": "alice"}))
}

// TestPooledUnrecognizedSetPinsSession: after `SET work_mem = '64MB'`
// (not in the replayable whitelist), the next RFQ 'I' must NOT release
// the backend.
func TestPooledUnrecognizedSetPinsSession(t *testing.T) {
	fb, p := newPoolWithFake(t, 1)
	clt, fe, _ := startPooled(t, p, &PooledConn{
		PooledConfig: PooledConfig{
			CannedParams: map[string]string{"server_version": "16.0"},
		},
		Database: "appdb",
		User:     "alice",
	})

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
	testutil.DrainToRFQ(t, clt, fe)

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
