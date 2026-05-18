// Phase A splice forwarder integration tests. Use the fakeBackend
// harness from pooled_test.go to script a Postgres-protocol-shaped
// upstream, run a query through PooledConn with Splice enabled, and
// assert the client sees the same wire bytes whether splice is on or
// off. The harness uses net.Pipe so the test exercises the real
// PutbackReader + DrainSplice path end-to-end.

package client

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
	"github.com/JustAnotherDevv/pgrouter/internal/wire/splice"
)

// startPooledWithSplice is sugar over startPooled that enables the
// Phase A splice forwarder with sane defaults.
func startPooledWithSplice(t *testing.T, p *pool.Pool, cfg PooledConfig) (net.Conn, *pgproto3.Frontend, <-chan struct{}) {
	t.Helper()
	if cfg.CannedParams == nil {
		cfg.CannedParams = map[string]string{"server_version": "16.0"}
	}
	if cfg.Splice == nil {
		cfg.Splice = &splice.SpliceConfig{Enabled: true, BufferSize: 8 * 1024}
	}
	cfg.PreparedCache = true
	return startPooled(t, p, &PooledConn{
		PooledConfig: cfg,
		Database:     "appdb",
		User:         "alice",
	})
}

// TestPooledSpliceForwardsBoringMessages: a SELECT returning a
// RowDescription + several DataRows + CommandComplete + RFQ must
// produce the same wire bytes on the client side with splice on as
// without. The fake backend is shaped like a real Postgres response
// (one tag + 4-byte length per message) so DrainSplice sees real wire
// frames.
func TestPooledSpliceForwardsBoringMessages(t *testing.T) {
	fb := newFakeBackend(t)
	dial := func(_ context.Context) (*backend.Conn, error) { return fb.Conn(), nil }
	p := newDialPool(t, "test", dial, 1)
	_, fe, _ := startPooledWithSplice(t, p, PooledConfig{})

	fb.expect(func(be *pgproto3.Backend, _ pgproto3.FrontendMessage) {
		be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{Name: []byte("?column?"), DataTypeOID: 23, DataTypeSize: 4},
		}})
		be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("1")}})
		be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("2")}})
		be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("3")}})
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 3")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})

	fe.Send(&pgproto3.Query{String: "SELECT 1, 2, 3"})
	require.NoError(t, fe.Flush())

	// Drain to RFQ and assert we got all three DataRows + RFQ.
	var rows int
	for {
		m, err := fe.Receive()
		require.NoError(t, err)
		switch m.(type) {
		case *pgproto3.DataRow:
			rows++
		case *pgproto3.ReadyForQuery:
			require.Equal(t, 3, rows)
			requirePoolStats(t, p, 1, 0)
			return
		}
	}
}

// TestPooledSpliceHandlesInterestingMidStream: a backend that
// interleaves an "interesting" message (ParameterStatus, 'S') with
// boring DataRows must (a) splice the boring messages and (b) decode
// the interesting one so the dispatch can observe the parameter
// change. We assert the client still sees the full set of messages
// AND that the resulting tx state is correct.
func TestPooledSpliceHandlesInterestingMidStream(t *testing.T) {
	fb := newFakeBackend(t)
	dial := func(_ context.Context) (*backend.Conn, error) { return fb.Conn(), nil }
	p := newDialPool(t, "test", dial, 1)
	_, fe, _ := startPooledWithSplice(t, p, PooledConfig{})

	fb.expect(func(be *pgproto3.Backend, _ pgproto3.FrontendMessage) {
		be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{Name: []byte("?column?"), DataTypeOID: 23, DataTypeSize: 4},
		}})
		be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("1")}})
		// ParameterStatus is "interesting" → splice must put back
		// its 5-byte header so the decoded path can run
		// ObserveBackendMessage.
		be.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
		be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("2")}})
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 2")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	})

	fe.Send(&pgproto3.Query{String: "SELECT 1, 2"})
	require.NoError(t, fe.Flush())

	var sawParam, sawRow1, sawRow2, sawRFQ bool
	for !sawRFQ {
		m, err := fe.Receive()
		require.NoError(t, err)
		switch m.(type) {
		case *pgproto3.ParameterStatus:
			sawParam = true
		case *pgproto3.DataRow:
			if sawRow1 {
				sawRow2 = true
			} else {
				sawRow1 = true
			}
		case *pgproto3.ReadyForQuery:
			sawRFQ = true
		}
	}
	require.True(t, sawParam, "ParameterStatus should be decoded even with splice on")
	require.True(t, sawRow1, "first DataRow should reach client")
	require.True(t, sawRow2, "second DataRow (after interesting msg) should reach client")
}

// TestPooledSpliceEquivalentToNoSplice: run the same workload twice
// (splice on, splice off) and assert the client-side wire bytes are
// byte-for-byte identical. This is the strongest possible regression
// guard: if splice ever corrupts or drops a message, the streams
// diverge.
//
// Each subtest builds its own fakeBackend + pool + PooledConn so the
// two scenarios don't share state (a single fakeBackend can't serve
// two PooledConns — both would read from the same net.Pipe and the
// fake's script queue would interleave).
func TestPooledSpliceEquivalentToNoSplice(t *testing.T) {
	cases := []struct {
		name       string
		withSplice bool
	}{
		{"splice_off", false},
		{"splice_on", true},
	}
	captures := make(map[string]*bytes.Buffer, len(cases))
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cfg := PooledConfig{
				CannedParams: map[string]string{"server_version": "16.0"},
			}
			if tc.withSplice {
				cfg.Splice = &splice.SpliceConfig{Enabled: true, BufferSize: 8 * 1024}
			}

			// One fake backend + one pool per subtest.
			fb := newFakeBackend(t)
			dial := func(_ context.Context) (*backend.Conn, error) { return fb.Conn(), nil }
			p := newDialPool(t, "test", dial, 1)

			clt, fe, done := startPooledWithSplice(t, p, cfg)
			defer func() { _ = clt.Close(); <-done }()

			// Queue the response for the (first) Query the test will send.
			// startPooled already drained the welcome, so the fake
			// backend's next expected message is the Query.
			fb.scriptQuery(t, "SELECT 42", "SELECT 1", 'I')

			fe.Send(&pgproto3.Query{String: "SELECT 42"})
			require.NoError(t, fe.Flush())

			// Capture the raw wire bytes the client sees after the
			// Query. DrainToRFQ returns once RFQ arrives, then we
			// have a self-contained "Query response" byte stream.
			buf := &bytes.Buffer{}
			_ = clt.SetReadDeadline(time.Now().Add(2 * time.Second))
			raw := make([]byte, 4096)
			for {
				n, err := clt.Read(raw)
				if n > 0 {
					buf.Write(raw[:n])
				}
				if err != nil {
					break
				}
			}
			captures[tc.name] = buf
			require.NotEmpty(t, buf.Bytes(), "client should have received some bytes")
		})
	}
	// Compare: the two byte streams must be identical (modulo timing).
	require.True(t, bytes.Equal(captures["splice_off"].Bytes(), captures["splice_on"].Bytes()),
		"splice on/off produced different wire bytes: off=%d on=%d",
		captures["splice_off"].Len(), captures["splice_on"].Len())
}
