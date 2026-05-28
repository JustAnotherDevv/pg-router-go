package auth

import (
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/config"
	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
)

// TestWireSCRAMHandshake runs PerformServerAuth on one end of a net.Pipe
// and PerformClientAuth on the other, asserting the proof check passes
// when both ends use the same password.
//
// This is the unit-level equivalent of the e2e test in internal/client;
// failures here isolate to the auth layer.
func TestWireSCRAMHandshake(t *testing.T) {
	password := "wonderland"
	verifier, err := MakeSCRAMVerifier(password)
	require.NoError(t, err)
	username := "alice"

	srvConn, cliConn := net.Pipe()
	defer srvConn.Close()
	defer cliConn.Close()

	be := pgproto3.NewBackend(srvConn, srvConn)
	fe := pgproto3.NewFrontend(cliConn, cliConn)

	srvDone := make(chan error, 1)
	go func() {
		err := PerformServerAuth(be, ServerAuthOptions{
			Type: config.AuthSCRAM,
			Userlist: &Userlist{
				entries: map[string]*UserEntry{
					username: {Username: username, SCRAMVerifier: verifier},
				},
			},
			Log: testutil.Discard,
		}, username)
		if err == nil {
			// Mirror the client startup path after PerformServerAuth.
			be.Send(&pgproto3.AuthenticationOk{})
			_ = be.Flush()
		}
		srvDone <- err
	}()

	// Client side: wait for the first auth message, then hand to
	// PerformClientAuth.
	_ = cliConn.SetDeadline(time.Now().Add(3 * time.Second))
	msg, err := fe.Receive()
	require.NoError(t, err)
	cliErr := PerformClientAuth(fe, username, password, msg)

	select {
	case err := <-srvDone:
		require.NoError(t, err, "server auth error")
	case <-time.After(3 * time.Second):
		t.Fatal("server did not finish")
	}
	require.NoError(t, cliErr, "client auth error")
}
