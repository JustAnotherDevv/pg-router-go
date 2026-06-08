//go:build !windows

package listener

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/JustAnotherDevv/pg-router-go/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewUnixCreatesSocket(t *testing.T) {
	dir := t.TempDir()
	ln, err := NewUnix(dir, 16432, "0770", testutil.Discard)
	require.NoError(t, err)
	defer ln.Close()
	require.Contains(t, ln.Addr().String(), filepath.Join(dir, ".s.PGSQL.16432"))
}

func TestNewUnixRemovesStaleSocket(t *testing.T) {
	dir := t.TempDir()
	// First listener.
	ln1, err := NewUnix(dir, 16432, "", testutil.Discard)
	require.NoError(t, err)
	// Close â†’ leaves the inode behind on disk.
	require.NoError(t, ln1.Close())
	// Second listener should succeed by removing the stale inode.
	ln2, err := NewUnix(dir, 16432, "", testutil.Discard)
	require.NoError(t, err)
	require.NoError(t, ln2.Close())
}

func TestUnixListenerAcceptsConnection(t *testing.T) {
	dir := t.TempDir()
	ln, err := NewUnix(dir, 16433, "", testutil.Discard)
	require.NoError(t, err)
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotData := make(chan []byte, 1)
	go func() {
		_ = ln.Serve(ctx, func(_ context.Context, c net.Conn) {
			defer c.Close()
			buf := make([]byte, 4)
			_, _ = io.ReadFull(c, buf)
			gotData <- buf
		})
	}()
	// Give Serve a moment to call Accept.
	time.Sleep(50 * time.Millisecond)

	c, err := net.Dial("unix", ln.Addr().String())
	require.NoError(t, err)
	_, err = c.Write([]byte("hi!!"))
	require.NoError(t, err)
	_ = c.Close()

	select {
	case b := <-gotData:
		require.Equal(t, []byte("hi!!"), b)
	case <-time.After(2 * time.Second):
		t.Fatal("handler never received data")
	}
}
