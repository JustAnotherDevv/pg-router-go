package proto

import (
	"testing"
	"time"

	"github.com/JustAnotherDevv/pg-router-go/internal/testutil"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// TestForwardClientToServer verifies a Query passes through unchanged.
//
// net.Pipe() is fully synchronous (each Write blocks until the matching
// Read drains), so we have to drive every end concurrently.
func TestForwardClientToServer(t *testing.T) {
	cl1, cl2 := testutil.PipePair(t)
	sv1, sv2 := testutil.PipePair(t)

	src := NewClientSide(cl2) // pgrouter reads frontend msgs from the client at cl2
	dst := NewServerSide(sv1) // pgrouter writes frontend msgs to the server via sv1
	upstream := pgproto3.NewBackend(sv2, sv2)

	clientWriteDone := make(chan struct{})
	upstreamRecvCh := make(chan pgproto3.FrontendMessage, 1)
	upstreamErrCh := make(chan error, 1)

	// Goroutine A: client writes Query to cl1.
	go func() {
		defer close(clientWriteDone)
		q := &pgproto3.Query{String: "SELECT 7"}
		b, _ := q.Encode(nil)
		_, _ = cl1.Write(b)
	}()

	// Goroutine B: upstream drains sv2.
	go func() {
		_ = sv2.SetReadDeadline(time.Now().Add(2 * time.Second))
		m, err := upstream.Receive()
		if err != nil {
			upstreamErrCh <- err
			return
		}
		upstreamRecvCh <- m
	}()

	// Test thread: forward client â†’ server.
	_ = cl2.SetReadDeadline(time.Now().Add(2 * time.Second))
	msg, err := ForwardClientToServer(src, dst)
	require.NoError(t, err)
	q, ok := msg.(*pgproto3.Query)
	require.True(t, ok)
	require.Equal(t, "SELECT 7", q.String)
	require.NoError(t, dst.Flush())

	// Upstream must see the same Query.
	select {
	case got := <-upstreamRecvCh:
		gq, ok := got.(*pgproto3.Query)
		require.True(t, ok)
		require.Equal(t, "SELECT 7", gq.String)
	case err := <-upstreamErrCh:
		t.Fatalf("upstream recv: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("upstream did not receive")
	}

	<-clientWriteDone
}

func TestForwardServerToClient(t *testing.T) {
	cl1, cl2 := testutil.PipePair(t)
	sv1, sv2 := testutil.PipePair(t)

	dst := NewClientSide(cl2) // pgrouter writes backend msgs to the client via cl2
	src := NewServerSide(sv1) // pgrouter reads backend msgs from the server via sv1
	upstream := pgproto3.NewBackend(sv2, sv2)
	clientView := pgproto3.NewFrontend(cl1, cl1)

	clientRecvCh := make(chan pgproto3.BackendMessage, 1)
	clientErrCh := make(chan error, 1)

	// Goroutine A: upstream pushes a CommandComplete.
	go func() {
		upstream.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		_ = upstream.Flush()
	}()

	// Goroutine B: client drains cl1.
	go func() {
		_ = cl1.SetReadDeadline(time.Now().Add(2 * time.Second))
		m, err := clientView.Receive()
		if err != nil {
			clientErrCh <- err
			return
		}
		clientRecvCh <- m
	}()

	// Test thread: forward server â†’ client.
	_ = sv1.SetReadDeadline(time.Now().Add(2 * time.Second))
	msg, err := ForwardServerToClient(src, dst)
	require.NoError(t, err)
	cc, ok := msg.(*pgproto3.CommandComplete)
	require.True(t, ok)
	require.Equal(t, "SELECT 1", string(cc.CommandTag))
	require.NoError(t, dst.Flush())

	select {
	case got := <-clientRecvCh:
		gcc, ok := got.(*pgproto3.CommandComplete)
		require.True(t, ok)
		require.Equal(t, "SELECT 1", string(gcc.CommandTag))
	case err := <-clientErrCh:
		t.Fatalf("client recv: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("client did not receive")
	}
}

func TestIsTerminate(t *testing.T) {
	require.True(t, IsTerminate(&pgproto3.Terminate{}))
	require.False(t, IsTerminate(&pgproto3.Query{}))
}

func TestIsReadyForQuery(t *testing.T) {
	s, ok := IsReadyForQuery(&pgproto3.ReadyForQuery{TxStatus: 'T'})
	require.True(t, ok)
	require.Equal(t, byte('T'), s)

	_, ok = IsReadyForQuery(&pgproto3.CommandComplete{})
	require.False(t, ok)
}

func TestIsErrorResponse(t *testing.T) {
	sev, code, ok := IsErrorResponse(&pgproto3.ErrorResponse{
		Severity: "FATAL", Code: "53300",
	})
	require.True(t, ok)
	require.Equal(t, "FATAL", sev)
	require.Equal(t, "53300", code)

	_, _, ok = IsErrorResponse(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	require.False(t, ok)
}
