//go:build linux

package auth

import (
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPeerUsernameSelfConn verifies SO_PEERCRED returns OUR uid when
// the client and server are the same process talking over an
// in-process Unix socket.
func TestPeerUsernameSelfConn(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "test.sock")

	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	defer ln.Close()

	var (
		serverConn net.Conn
		wg         sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, err := ln.Accept()
		if err == nil {
			serverConn = c
		}
	}()

	clientConn, err := net.Dial("unix", sock)
	require.NoError(t, err)
	defer clientConn.Close()

	wg.Wait()
	require.NotNil(t, serverConn)
	defer serverConn.Close()

	got, err := PeerUsername(serverConn)
	require.NoError(t, err)

	// Confirm it matches our own uid → username mapping.
	me, err := user.LookupId(strconv.Itoa(os.Getuid()))
	require.NoError(t, err)
	require.Equal(t, me.Username, got)
}

func TestPeerUsernameRejectsNonUnix(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	_, err := PeerUsername(c1)
	require.Error(t, err)
}
