package proto

import (
	"testing"
	"time"

	"github.com/JustAnotherDevv/pg-router-go/internal/testutil"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func TestServerSideSendAndFlush(t *testing.T) {
	clt, srv := testutil.PipePair(t)
	ss := NewServerSide(clt) // we are the pgrouter-as-frontend driving `clt`

	// Drive a pgproto3.Backend on the server side and read the message.
	be := pgproto3.NewBackend(srv, srv)

	go func() {
		err := ss.SendAndFlush(&pgproto3.Query{String: "SELECT 42"})
		require.NoError(t, err)
	}()

	_ = srv.SetReadDeadline(time.Now().Add(time.Second))
	msg, err := be.Receive()
	require.NoError(t, err)
	q, ok := msg.(*pgproto3.Query)
	require.True(t, ok)
	require.Equal(t, "SELECT 42", q.String)
}

func TestServerSideReceive(t *testing.T) {
	clt, srv := testutil.PipePair(t)
	ss := NewServerSide(clt)
	be := pgproto3.NewBackend(srv, srv)

	go func() {
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		require.NoError(t, be.Flush())
	}()

	_ = clt.SetReadDeadline(time.Now().Add(time.Second))
	msg, err := ss.Receive()
	require.NoError(t, err)
	status, ok := IsReadyForQuery(msg)
	require.True(t, ok)
	require.Equal(t, byte('I'), status)
}
