package proto

import (
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

func newPipe(t *testing.T) (a, b net.Conn) {
	t.Helper()
	a, b = net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	return
}

func TestClientSideReceiveStartup(t *testing.T) {
	clt, srv := newPipe(t)
	cs := NewClientSide(srv)

	// Client encodes + writes a StartupMessage.
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "u", "database": "d"},
	}
	buf, err := startup.Encode(nil)
	require.NoError(t, err)
	go func() { _, _ = clt.Write(buf) }()

	_ = srv.SetReadDeadline(time.Now().Add(time.Second))
	msg, err := cs.ReceiveStartup()
	require.NoError(t, err)
	sm, ok := msg.(*pgproto3.StartupMessage)
	require.True(t, ok)
	require.Equal(t, "u", sm.Parameters["user"])
}

func TestClientSideSendAndFlush(t *testing.T) {
	clt, srv := newPipe(t)
	cs := NewClientSide(srv)

	// Drive the server side and read back via a pgproto3 Frontend.
	fe := pgproto3.NewFrontend(clt, clt)

	go func() {
		err := cs.SendAndFlush(&pgproto3.AuthenticationOk{})
		require.NoError(t, err)
	}()

	_ = clt.SetReadDeadline(time.Now().Add(time.Second))
	msg, err := fe.Receive()
	require.NoError(t, err)
	_, ok := msg.(*pgproto3.AuthenticationOk)
	require.True(t, ok)
}

func TestClientSideRoundTripQuery(t *testing.T) {
	clt, srv := newPipe(t)
	cs := NewClientSide(srv)

	// Client (in a goroutine) sends Startup, then a Query, then Terminate.
	go func() {
		startup := &pgproto3.StartupMessage{
			ProtocolVersion: pgproto3.ProtocolVersionNumber,
			Parameters:      map[string]string{"user": "u", "database": "d"},
		}
		b, _ := startup.Encode(nil)
		_, _ = clt.Write(b)
		q := &pgproto3.Query{String: "SELECT 1"}
		b, _ = q.Encode(nil)
		_, _ = clt.Write(b)
		term := &pgproto3.Terminate{}
		b, _ = term.Encode(nil)
		_, _ = clt.Write(b)
	}()

	// Server reads Startup, then Query, then Terminate.
	_ = srv.SetReadDeadline(time.Now().Add(2 * time.Second))
	startupMsg, err := cs.ReceiveStartup()
	require.NoError(t, err)
	_, ok := startupMsg.(*pgproto3.StartupMessage)
	require.True(t, ok)

	qmsg, err := cs.Receive()
	require.NoError(t, err)
	q, ok := qmsg.(*pgproto3.Query)
	require.True(t, ok)
	require.Equal(t, "SELECT 1", q.String)

	tmsg, err := cs.Receive()
	require.NoError(t, err)
	require.True(t, IsTerminate(tmsg))
}
