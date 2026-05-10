package proto

import (
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// This file used to contain 32 per-message-type round-trip tests
// verifying that pgproto3 messages survive a pass through pgrouter's
// ClientSide/ServerSide wrappers. The wrappers are pure 1-line
// passthroughs to pgproto3 (see client_side.go / server_side.go) and
// own no encoding logic of their own, so testing every message type
// was essentially testing pgproto3 itself — owned by the pgx team and
// covered by their suite.
//
// What remains: 6 representative tests, one per message-class branch:
//   - simple text frontend  (Query)
//   - extended-protocol     (Parse, with field-type slice)
//   - binary payload        (CopyData)
//   - binary backend struct (BackendKeyData, ParameterStatus)
//   - multi-field error    (ErrorResponse)
//   - startup-phase path   (StartupMessage via ReceiveStartup)
//
// If pgrouter's wrappers ever stop being pure passthroughs (e.g. add
// instrumentation, buffering, or message rewriting), expand this file
// to cover the new behavior at THAT layer — not by re-listing every
// pgproto3 message type.

// pipePair returns two coupled net.Conn ends; reading from one returns
// what was written to the other.
func pipePair(t *testing.T) (a, b net.Conn) {
	t.Helper()
	a, b = net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	return
}

// roundTripFrontend writes a FrontendMessage from a pgproto3.Frontend
// (in a goroutine) and reads it through our ClientSide.
func roundTripFrontend(t *testing.T, msg pgproto3.FrontendMessage) pgproto3.FrontendMessage {
	t.Helper()
	clt, srv := pipePair(t)
	cs := NewClientSide(srv)
	fe := pgproto3.NewFrontend(clt, clt)

	done := make(chan error, 1)
	go func() {
		fe.Send(msg)
		done <- fe.Flush()
	}()

	_ = srv.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := cs.Receive()
	require.NoError(t, err)
	require.NoError(t, <-done)
	return got
}

// roundTripBackend writes a BackendMessage from a pgproto3.Backend
// (in a goroutine) and reads it through our ServerSide.
func roundTripBackend(t *testing.T, msg pgproto3.BackendMessage) pgproto3.BackendMessage {
	t.Helper()
	clt, srv := pipePair(t)
	ss := NewServerSide(clt)
	be := pgproto3.NewBackend(srv, srv)

	done := make(chan error, 1)
	go func() {
		be.Send(msg)
		done <- be.Flush()
	}()

	_ = clt.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := ss.Receive()
	require.NoError(t, err)
	require.NoError(t, <-done)
	return got
}

// --- representative round-trips (see top-of-file rationale) ---

func TestFrontendQuery(t *testing.T) {
	got := roundTripFrontend(t, &pgproto3.Query{String: "SELECT 1"})
	q, ok := got.(*pgproto3.Query)
	require.True(t, ok)
	require.Equal(t, "SELECT 1", q.String)
}

func TestFrontendParse(t *testing.T) {
	got := roundTripFrontend(t, &pgproto3.Parse{
		Name:          "stmt1",
		Query:         "SELECT $1::int",
		ParameterOIDs: []uint32{23},
	})
	p, ok := got.(*pgproto3.Parse)
	require.True(t, ok)
	require.Equal(t, "stmt1", p.Name)
	require.Equal(t, "SELECT $1::int", p.Query)
	require.Equal(t, []uint32{23}, p.ParameterOIDs)
}

func TestFrontendCopyData(t *testing.T) {
	got := roundTripFrontend(t, &pgproto3.CopyData{Data: []byte("row1\trow2\n")})
	d, ok := got.(*pgproto3.CopyData)
	require.True(t, ok)
	require.Equal(t, []byte("row1\trow2\n"), d.Data)
}

func TestBackendBackendKeyData(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.BackendKeyData{
		ProcessID: 12345, SecretKey: []byte{0xde, 0xad, 0xbe, 0xef},
	})
	k, ok := got.(*pgproto3.BackendKeyData)
	require.True(t, ok)
	require.Equal(t, uint32(12345), k.ProcessID)
	require.Equal(t, []byte{0xde, 0xad, 0xbe, 0xef}, k.SecretKey)
}

func TestBackendErrorResponse(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.ErrorResponse{
		Severity: "ERROR", Code: "42601", Message: "syntax error",
	})
	e, ok := got.(*pgproto3.ErrorResponse)
	require.True(t, ok)
	require.Equal(t, "ERROR", e.Severity)
	require.Equal(t, "42601", e.Code)
	require.Equal(t, "syntax error", e.Message)
}

// TestBackendReadyForQuery is kept because IsReadyForQuery is a
// pgrouter helper (see message.go) — this verifies it on a wire-decoded
// message rather than a literal struct.
func TestBackendReadyForQuery(t *testing.T) {
	got := roundTripBackend(t, &pgproto3.ReadyForQuery{TxStatus: 'T'})
	r, ok := got.(*pgproto3.ReadyForQuery)
	require.True(t, ok)
	require.Equal(t, byte('T'), r.TxStatus)
	s, ok := IsReadyForQuery(got)
	require.True(t, ok)
	require.Equal(t, byte('T'), s)
}

// TestFrontendTerminate is kept because IsTerminate is a pgrouter
// helper (see message.go).
func TestFrontendTerminate(t *testing.T) {
	got := roundTripFrontend(t, &pgproto3.Terminate{})
	_, ok := got.(*pgproto3.Terminate)
	require.True(t, ok)
	require.True(t, IsTerminate(got))
}

// TestStartupMessageRoundTrip exercises ClientSide.ReceiveStartup,
// which dispatches to pgproto3.ReceiveStartupMessage — a different
// code path than Receive().
func TestStartupMessageRoundTrip(t *testing.T) {
	clt, srv := pipePair(t)
	cs := NewClientSide(srv)

	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":     "alice",
			"database": "appdb",
		},
	}
	go func() {
		b, _ := startup.Encode(nil)
		_, _ = clt.Write(b)
	}()

	_ = srv.SetReadDeadline(time.Now().Add(2 * time.Second))
	msg, err := cs.ReceiveStartup()
	require.NoError(t, err)
	sm, ok := msg.(*pgproto3.StartupMessage)
	require.True(t, ok)
	require.Equal(t, "alice", sm.Parameters["user"])
	require.Equal(t, "appdb", sm.Parameters["database"])
}
