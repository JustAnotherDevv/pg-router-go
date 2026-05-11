package client

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func testConn() *Conn {
	return &Conn{Log: testutil.Discard}
}

// pair returns a connected pair of net.Conn for testing.
func pair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	c1, c2 := net.Pipe()
	t.Cleanup(func() { _ = c1.Close(); _ = c2.Close() })
	return c1, c2
}

// runConn starts the client handler on server side and returns a done chan.
func runConn(t *testing.T, server net.Conn) chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		testConn().Handle(context.Background(), server)
	}()
	return done
}

// readBackendMsg reads one BackendMessage from a client-side pipe using
// pgproto3 Frontend (client-perspective reader).
func readBackendMsg(t *testing.T, c net.Conn) pgproto3.BackendMessage {
	t.Helper()
	fe := pgproto3.NewFrontend(c, c)
	msg, err := fe.Receive()
	require.NoError(t, err)
	return msg
}

// TestStartupResponseSequence checks the full trust-mode startup:
// StartupMessage -> AuthOk + ParameterStatus* + BackendKeyData + ReadyForQuery.
func TestStartupResponseSequence(t *testing.T) {
	clt, server := pair(t)
	done := runConn(t, server)

	// Send a StartupMessage.
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":     "alice",
			"database": "appdb",
		},
	}
	buf, err := startup.Encode(nil)
	require.NoError(t, err)
	_, err = clt.Write(buf)
	require.NoError(t, err)

	fe := pgproto3.NewFrontend(clt, clt)

	// 1. AuthenticationOk.
	msg, err := fe.Receive()
	require.NoError(t, err)
	authOk, ok := msg.(*pgproto3.AuthenticationOk)
	require.True(t, ok, "expected AuthenticationOk, got %T", msg)
	_ = authOk

	// 2. Eleven ParameterStatus messages followed by BackendKeyData + ReadyForQuery.
	paramCount := 0
	var sawKeyData, sawReady bool
	for !sawReady {
		msg, err := fe.Receive()
		require.NoError(t, err)
		switch m := msg.(type) {
		case *pgproto3.ParameterStatus:
			paramCount++
			require.NotEmpty(t, m.Name)
		case *pgproto3.BackendKeyData:
			sawKeyData = true
			require.NotZero(t, m.ProcessID, "PID is non-zero")
			require.Len(t, m.SecretKey, 4, "classic 4-byte secret")
		case *pgproto3.ReadyForQuery:
			sawReady = true
			require.Equal(t, byte('I'), m.TxStatus, "starts idle")
		default:
			t.Fatalf("unexpected msg: %T", m)
		}
	}
	require.True(t, sawKeyData, "BackendKeyData received")
	require.GreaterOrEqual(t, paramCount, 8, "expected several ParameterStatus messages")

	// Terminate to let handler exit.
	be := pgproto3.NewBackend(clt, clt)
	_ = be // not used; we send Terminate manually since pgproto3 Frontend lacks Send for FE messages here.
	term := &pgproto3.Terminate{}
	enc, _ := term.Encode(nil)
	_, _ = clt.Write(enc)
	_ = clt.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit after Terminate")
	}
}

// TestQueryInIdleModeReturnsError verifies the no-upstream idle loop responds
// to a Query with ErrorResponse + ReadyForQuery (no upstream proxy yet).
func TestQueryInIdleModeReturnsError(t *testing.T) {
	clt, server := pair(t)
	done := runConn(t, server)

	// Startup.
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "u", "database": "d"},
	}
	enc, err := startup.Encode(nil)
	require.NoError(t, err)
	_, _ = clt.Write(enc)

	fe := pgproto3.NewFrontend(clt, clt)
	// Drain until ReadyForQuery 'I'.
	for {
		m, err := fe.Receive()
		require.NoError(t, err)
		if r, ok := m.(*pgproto3.ReadyForQuery); ok && r.TxStatus == 'I' {
			break
		}
	}

	// Send Query.
	q := &pgproto3.Query{String: "SELECT 1"}
	qenc, err := q.Encode(nil)
	require.NoError(t, err)
	_, _ = clt.Write(qenc)

	// Expect ErrorResponse + ReadyForQuery.
	m, err := fe.Receive()
	require.NoError(t, err)
	er, ok := m.(*pgproto3.ErrorResponse)
	require.True(t, ok, "expected ErrorResponse, got %T", m)
	require.Equal(t, "ERROR", er.Severity)
	require.Equal(t, "0A000", er.Code)

	m, err = fe.Receive()
	require.NoError(t, err)
	rfq, ok := m.(*pgproto3.ReadyForQuery)
	require.True(t, ok)
	require.Equal(t, byte('I'), rfq.TxStatus)

	// Cleanup.
	term := &pgproto3.Terminate{}
	tenc, _ := term.Encode(nil)
	_, _ = clt.Write(tenc)
	_ = clt.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit")
	}
}

// TestSSLRequestDeclinedThenStartup checks SSL decline + StartupMessage
// continuation works.
func TestSSLRequestDeclinedThenStartup(t *testing.T) {
	clt, server := pair(t)
	done := runConn(t, server)

	// Encode an SSLRequest: int32 length=8, int32 magic 80877103.
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], 8)
	binary.BigEndian.PutUint32(buf[4:8], 80877103)
	_, err := clt.Write(buf)
	require.NoError(t, err)

	resp := make([]byte, 1)
	_ = clt.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(clt, resp)
	require.NoError(t, err)
	require.Equal(t, byte('N'), resp[0])

	// Now send a real StartupMessage and verify the handler responds.
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "u", "database": "d"},
	}
	enc, err := startup.Encode(nil)
	require.NoError(t, err)
	_, _ = clt.Write(enc)

	fe := pgproto3.NewFrontend(clt, clt)
	m, err := fe.Receive()
	require.NoError(t, err)
	_, ok := m.(*pgproto3.AuthenticationOk)
	require.True(t, ok)

	// Cleanup.
	_ = clt.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit")
	}
}

// NOTE: GSSEncRequest decline, CancelRequest dispatch, and bare-EOF
// handler exit were each covered by a tiny dedicated test here. They
// were dropped in P15d because:
//
//   - GSSEncRequest: symmetric to SSLRequest decline (already covered
//     by TestSSLRequestDeclinedThenStartup) — same `case magic ==
//     80877104:` branch as the SSL one.
//   - CancelRequest: M.12.3 integration test (live cancel against real
//     PG) covers the dispatch path end-to-end.
//   - bare-EOF: any io.EOF-returning test (TestEOFOnRead's behavior is
//     reached implicitly by every test that calls clt.Close() before
//     the handler exits — every test in this file).
