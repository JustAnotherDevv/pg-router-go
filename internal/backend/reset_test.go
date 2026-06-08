package backend

import (
	"net"
	"testing"
	"time"

	"github.com/JustAnotherDevv/pg-router-go/internal/testutil"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// mockBackendConn returns a Conn whose Frontend is wired to a server-
// side pgproto3.Backend the test drives directly.
func mockBackendConn(t *testing.T) (*Conn, *pgproto3.Backend, net.Conn) {
	t.Helper()
	cli, srv := net.Pipe()
	t.Cleanup(func() { _ = cli.Close(); _ = srv.Close() })

	c := &Conn{
		NetConn:  cli,
		Frontend: pgproto3.NewFrontend(cli, cli),
		Params:   map[string]string{},
		Log:      testutil.Discard,
	}
	be := pgproto3.NewBackend(srv, srv)
	return c, be, srv
}

func TestResetStateSuccess(t *testing.T) {
	c, be, srv := mockBackendConn(t)

	// Server-side: expect Query, respond with CommandComplete + RFQ.
	go func() {
		_ = srv.SetDeadline(time.Now().Add(2 * time.Second))
		msg, _ := be.Receive()
		q, ok := msg.(*pgproto3.Query)
		if !ok || q.String != ResetQuery {
			be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX001"})
			be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			_ = be.Flush()
			return
		}
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("DISCARD ALL")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	}()

	require.NoError(t, c.ResetState())
}

func TestResetStateErrorPropagates(t *testing.T) {
	c, be, srv := mockBackendConn(t)

	go func() {
		_ = srv.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = be.Receive()
		be.Send(&pgproto3.ErrorResponse{
			Severity: "ERROR", Code: "42P01", Message: "no permission",
		})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	}()

	err := c.ResetState()
	require.Error(t, err)
	require.Contains(t, err.Error(), "no permission")
}

func TestResetStateNonIdleAfter(t *testing.T) {
	c, be, srv := mockBackendConn(t)

	go func() {
		_ = srv.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = be.Receive()
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("DISCARD ALL")})
		// Maliciously return non-idle tx_status.
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
		_ = be.Flush()
	}()

	err := c.ResetState()
	require.Error(t, err)
	require.Contains(t, err.Error(), "tx_status")
}

func TestHealthCheckSuccess(t *testing.T) {
	c, be, srv := mockBackendConn(t)
	go func() {
		_ = srv.SetDeadline(time.Now().Add(2 * time.Second))
		msg, _ := be.Receive()
		q, ok := msg.(*pgproto3.Query)
		if !ok || q.String != ";" {
			be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX001"})
			be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			_ = be.Flush()
			return
		}
		be.Send(&pgproto3.EmptyQueryResponse{})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	}()
	require.NoError(t, c.HealthCheck())
}

func TestHealthCheckError(t *testing.T) {
	c, be, srv := mockBackendConn(t)
	go func() {
		_ = srv.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = be.Receive()
		be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "08006", Message: "dead"})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	}()
	err := c.HealthCheck()
	require.Error(t, err)
	require.Contains(t, err.Error(), "dead")
}

func TestResetStateNilSafe(t *testing.T) {
	var c *Conn
	require.Error(t, c.ResetState())
}

func TestHealthCheckNilSafe(t *testing.T) {
	var c *Conn
	require.Error(t, c.HealthCheck())
}
