//go:build linux

package auth

import (
	"io"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/JustAnotherDevv/pgrouter/internal/config"
	"github.com/JustAnotherDevv/pgrouter/internal/testutil"
)

// TestPerformServerAuthConnPeerOK runs a real peer-auth handshake over
// a Unix socket. We supply our own OS username — match → success.
func TestPerformServerAuthConnPeerOK(t *testing.T) {
	me, err := user.LookupId(strconv.Itoa(os.Getuid()))
	require.NoError(t, err)

	srv, cli := setupUnixPair(t)
	defer srv.Close()
	defer cli.Close()

	opts := ServerAuthOptions{
		Type: config.AuthPeer,
		Log:  testutil.Discard,
	}
	be := pgproto3.NewBackend(srv, srv)
	err = PerformServerAuthConn(be, srv, opts, me.Username)
	require.NoError(t, err)
}

func TestPerformServerAuthConnPeerMismatch(t *testing.T) {
	srv, cli := setupUnixPair(t)
	defer srv.Close()
	defer cli.Close()

	opts := ServerAuthOptions{
		Type: config.AuthPeer,
		Log:  testutil.Discard,
	}
	be := pgproto3.NewBackend(srv, srv)
	// Drain the FATAL ErrorResponse on the client side in a goroutine
	// so the server flush doesn't block.
	go io.Copy(io.Discard, cli)
	err := PerformServerAuthConn(be, srv, opts,
		"definitely-not-a-real-username-987")
	require.Error(t, err)
}

func TestPerformServerAuthConnPeerRequiresUnixConn(t *testing.T) {
	// net.Pipe — not a Unix socket; peer subsystem rejects.
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	opts := ServerAuthOptions{
		Type: config.AuthPeer,
		Log:  testutil.Discard,
	}
	be := pgproto3.NewBackend(c1, c1)
	go io.Copy(io.Discard, c2)
	err := PerformServerAuthConn(be, c1, opts, "bob")
	require.Error(t, err)
}

// setupUnixPair returns (server-side, client-side) of a one-shot Unix
// socket connection.
func setupUnixPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	dir := t.TempDir()
	sock := filepath.Join(dir, "test.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	defer ln.Close()

	var (
		srvConn net.Conn
		wg      sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, err := ln.Accept()
		if err == nil {
			srvConn = c
		}
	}()

	cliConn, err := net.Dial("unix", sock)
	require.NoError(t, err)
	wg.Wait()
	require.NotNil(t, srvConn)
	return srvConn, cliConn
}
