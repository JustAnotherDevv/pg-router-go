package client

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/auth"
	"github.com/JustAnotherDevv/pgrouter/internal/backend"
	"github.com/JustAnotherDevv/pgrouter/internal/cancel"
	"github.com/JustAnotherDevv/pgrouter/internal/config"
	"github.com/JustAnotherDevv/pgrouter/internal/pool"
	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
)

// TestDispatcherTrustAuthAndPoolRoute: client sends StartupMessage,
// server dispatches to the right pool (no auth, trust mode), pool dial
// is exercised, query forwards.
func TestDispatcherTrustAuthAndPoolRoute(t *testing.T) {
	fleet := newFakeBackendFleet(t)

	// Manager wires one dialer for any key — the fleet doesn't care
	// which (db, user) it serves.
	mgr := pool.NewManager(pool.Config{
		DefaultPoolSize: 2,
		QueryWait:       time.Second,
		Log:             testutil.Discard,
	}, func(_ pool.Key) pool.Dialer { return fleet.Dial })
	defer mgr.Close()

	h := &PooledHandler{
		Log:            testutil.Discard,
		Manager:        mgr,
		CannedParams:   map[string]string{"server_version": "16.0"},
		ResetOnRelease: false,
	}

	cli, srv := net.Pipe()
	defer cli.Close()
	doneC := make(chan struct{})
	go func() {
		defer close(doneC)
		h.Handle(context.Background(), srv)
	}()

	// Send StartupMessage for (appdb, alice).
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "alice", "database": "appdb"},
	}
	buf, _ := startup.Encode(nil)
	_, err := cli.Write(buf)
	require.NoError(t, err)

	fe := pgproto3.NewFrontend(cli, cli)

	// Drain welcome up to RFQ.
	for {
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	// Script the fake backend's response for the first Query.
	go func() {
		require.Eventually(t, func() bool { return fleet.Count() >= 1 },
			2*time.Second, 5*time.Millisecond)
		fleet.Backend(0).expect(func(be *pgproto3.Backend, msg pgproto3.FrontendMessage) {
			q, ok := msg.(*pgproto3.Query)
			require.True(t, ok)
			require.Equal(t, "SELECT 1", q.String)
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			_ = be.Flush()
		})
	}()

	fe.Send(&pgproto3.Query{String: "SELECT 1"})
	require.NoError(t, fe.Flush())
	for {
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	// Pool now has one idle backend serving (appdb, alice).
	require.Eventually(t, func() bool {
		st := mgr.Get(pool.Key{DB: "appdb", User: "alice"}).Stats()
		return st.Idle == 1
	}, time.Second, 5*time.Millisecond)

	_ = cli.Close()
	<-doneC
}

// TestDispatcherCancelRequestForwarded: after a client gets its
// BackendKeyData, a fresh connection sending CancelRequest with the
// SAME PID + secret should reach the upstream's cancel side-channel.
func TestDispatcherCancelRequestForwarded(t *testing.T) {
	tracker := cancel.NewTracker()

	// Pre-allocate a cancel key + bind it to a fake upstream.
	k, err := tracker.Allocate()
	require.NoError(t, err)

	// Stand up a TCP listener that captures whatever the client writes.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	gotCh := make(chan []byte, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 16)
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := readFull(c, buf); err == nil {
			gotCh <- buf
		}
	}()

	tracker.Bind(k, cancel.Target{
		BackendAddr:      ln.Addr().String(),
		BackendProcessID: 0xCAFEBABE,
		BackendSecretKey: []byte{0xDE, 0xAD, 0xBE, 0xEF},
	})

	// Dummy manager — we don't go through the StartupMessage path.
	mgr := pool.NewManager(pool.Config{
		DefaultPoolSize: 1,
		Log:             testutil.Discard,
	}, func(_ pool.Key) pool.Dialer {
		return func(_ context.Context) (*backend.Conn, error) {
			return &backend.Conn{}, nil
		}
	})
	defer mgr.Close()

	h := &PooledHandler{
		Log:           testutil.Discard,
		Manager:       mgr,
		CancelTracker: tracker,
	}

	// Client sends a CancelRequest packet at startup.
	cli, srv := net.Pipe()
	defer cli.Close()
	doneC := make(chan struct{})
	go func() {
		defer close(doneC)
		h.Handle(context.Background(), srv)
	}()

	// 16-byte CancelRequest: length=16, magic=80877102, PID, secret(4).
	pkt := make([]byte, 16)
	binary.BigEndian.PutUint32(pkt[0:4], 16)
	binary.BigEndian.PutUint32(pkt[4:8], cancel.CancelMagic)
	binary.BigEndian.PutUint32(pkt[8:12], k.ProcessID)
	copy(pkt[12:16], k.SecretKey[:])
	_, _ = cli.Write(pkt)

	// PooledHandler should now have dialed the upstream and written
	// the right cancel packet there.
	select {
	case got := <-gotCh:
		require.Equal(t, uint32(16), binary.BigEndian.Uint32(got[0:4]))
		require.Equal(t, cancel.CancelMagic, binary.BigEndian.Uint32(got[4:8]))
		require.Equal(t, uint32(0xCAFEBABE), binary.BigEndian.Uint32(got[8:12]))
		require.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, got[12:16])
	case <-time.After(2 * time.Second):
		t.Fatal("cancel never reached upstream")
	}

	_ = cli.Close()
	<-doneC
}

// TestDispatcherUnknownDatabaseFails: StartupMessage references a db
// that's not in the Manager's dialer factory. The dialer must surface
// an error and the client must see a FATAL ErrorResponse.
func TestDispatcherUnknownDatabaseFails(t *testing.T) {
	// Manager whose dialer always errors (mimics "unknown database").
	mgr := pool.NewManager(pool.Config{
		DefaultPoolSize: 1,
		QueryWait:       100 * time.Millisecond,
		Log:             testutil.Discard,
	}, func(_ pool.Key) pool.Dialer {
		return func(_ context.Context) (*backend.Conn, error) {
			return nil, &fakeErr{msg: "unknown database \"nope\""}
		}
	})
	defer mgr.Close()

	h := &PooledHandler{
		Log:          testutil.Discard,
		Manager:      mgr,
		CannedParams: map[string]string{"server_version": "16.0"},
	}

	cli, srv := net.Pipe()
	defer cli.Close()
	doneC := make(chan struct{})
	go func() {
		defer close(doneC)
		h.Handle(context.Background(), srv)
	}()

	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "u", "database": "nope"},
	}
	buf, _ := startup.Encode(nil)
	_, _ = cli.Write(buf)

	fe := pgproto3.NewFrontend(cli, cli)

	// Drain welcome up to RFQ.
	for {
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	// Now send a Query — Acquire should fail, ErrorResponse arrives.
	fe.Send(&pgproto3.Query{String: "SELECT 1"})
	require.NoError(t, fe.Flush())

	msg, err := fe.Receive()
	require.NoError(t, err)
	er, ok := msg.(*pgproto3.ErrorResponse)
	require.True(t, ok, "expected ErrorResponse, got %T", msg)
	require.Equal(t, "FATAL", er.Severity)

	_ = cli.Close()
	<-doneC
}

// TestDispatcherWithUserlistAuth: SCRAM-auth a real client via Userlist.
func TestDispatcherWithUserlistAuth(t *testing.T) {
	password := "wonderland"
	verifier, err := auth.MakeSCRAMVerifier(password)
	require.NoError(t, err)
	ulPath := writeUserlist(t, "alice", verifier.String())
	ul, err := auth.NewUserlist(ulPath)
	require.NoError(t, err)

	fleet := newFakeBackendFleet(t)
	mgr := pool.NewManager(pool.Config{
		DefaultPoolSize: 1,
		QueryWait:       time.Second,
		Log:             testutil.Discard,
	}, func(_ pool.Key) pool.Dialer { return fleet.Dial })
	defer mgr.Close()

	h := &PooledHandler{
		Log:     testutil.Discard,
		Manager: mgr,
		Auth: &auth.ServerAuthOptions{
			Type:     config.AuthSCRAM,
			Userlist: ul,
		},
		CannedParams: map[string]string{"server_version": "16.0"},
	}

	cli, srv := net.Pipe()
	defer cli.Close()
	doneC := make(chan struct{})
	go func() {
		defer close(doneC)
		h.Handle(context.Background(), srv)
	}()

	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "alice", "database": "appdb"},
	}
	buf, _ := startup.Encode(nil)
	_, _ = cli.Write(buf)

	fe := pgproto3.NewFrontend(cli, cli)

	// Auth flow.
	msg, err := fe.Receive()
	require.NoError(t, err)
	require.NoError(t, auth.PerformClientAuth(fe, "alice", password, msg))

	// Drain welcome up to RFQ.
	for {
		m, err := fe.Receive()
		require.NoError(t, err)
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	_ = cli.Close()
	<-doneC
}

// readFull is io.ReadFull without needing the io import in tests.
func readFull(r net.Conn, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		k, err := r.Read(buf[n:])
		n += k
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

type fakeErr struct{ msg string }

func (e *fakeErr) Error() string { return e.msg }
