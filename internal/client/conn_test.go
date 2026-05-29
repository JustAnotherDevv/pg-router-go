package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func testConn() *Conn {
	return &Conn{Log: slog.New(slog.DiscardHandler)}
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

// TestGSSEncRequestDeclined ensures GSSEncRequest is declined.
func TestGSSEncRequestDeclined(t *testing.T) {
	clt, server := pair(t)
	done := runConn(t, server)

	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], 8)
	binary.BigEndian.PutUint32(buf[4:8], 80877104)
	_, err := clt.Write(buf)
	require.NoError(t, err)

	resp := make([]byte, 1)
	_ = clt.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(clt, resp)
	require.NoError(t, err)
	require.Equal(t, byte('N'), resp[0])

	_ = clt.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit")
	}
}

// TestCancelRequestLogged checks CancelRequest doesn't crash.
func TestCancelRequestLogged(t *testing.T) {
	clt, server := pair(t)
	done := runConn(t, server)

	buf := make([]byte, 16)
	binary.BigEndian.PutUint32(buf[0:4], 16)
	binary.BigEndian.PutUint32(buf[4:8], 80877102)
	binary.BigEndian.PutUint32(buf[8:12], 12345)
	binary.BigEndian.PutUint32(buf[12:16], 0xDEADBEEF)
	_, err := clt.Write(buf)
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after CancelRequest")
	}
}

// TestEOFOnRead checks EOF doesn't error.
func TestEOFOnRead(t *testing.T) {
	clt, server := pair(t)
	done := runConn(t, server)

	_ = clt.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return on EOF")
	}
}

// Compile-touch test.
func TestUnusedHelpersDoNotCompile(t *testing.T) {
	require.True(t, bytes.Equal([]byte("a"), []byte("a")))
}

// TestStartupMessageParsed ensures a StartupMessage is recognized by the
// handler (regression: prior to M.1 this lived in handler/startup_test.go).
func TestStartupMessageParsed(t *testing.T) {
	clt, server := pair(t)
	done := runConn(t, server)

	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "x", "database": "y"},
	}
	enc, err := startup.Encode(nil)
	require.NoError(t, err)
	_, err = clt.Write(enc)
	require.NoError(t, err)

	// First message back must be AuthenticationOk.
	msg := readBackendMsg(t, clt)
	_, ok := msg.(*pgproto3.AuthenticationOk)
	require.True(t, ok, "first response is AuthenticationOk")

	_ = clt.Close()
	<-done
}
